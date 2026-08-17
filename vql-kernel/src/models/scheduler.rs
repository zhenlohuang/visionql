use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef};
use image::DynamicImage;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::backend::ModelBackend;
use super::pipeline::BatchingOwner;
use crate::resources::{QueryBudget, QueryReservation};
use crate::session::QueryMetrics;
use crate::{ErrorCode, Result, VqlError};

pub(super) const MAX_BATCH: usize = 16;
pub(super) const MAX_WAIT: Duration = Duration::from_millis(5);
pub(super) const QUEUE_CAPACITY: usize = 64;
pub(super) const SERVICE_CONCURRENCY: usize = 4;

type Response = oneshot::Sender<Result<ArrayRef>>;

#[derive(Debug)]
pub(crate) struct InferenceReservations {
    _reservations: Vec<QueryReservation>,
}

impl InferenceReservations {
    pub(crate) fn new(reservations: Vec<QueryReservation>) -> Arc<Self> {
        Arc::new(Self {
            _reservations: reservations,
        })
    }
}

struct Request {
    images: Vec<DynamicImage>,
    response: Response,
    cancel: CancellationToken,
    enqueued_at: Instant,
    budget: QueryBudget,
    metrics: Arc<QueryMetrics>,
    _reservations: Arc<InferenceReservations>,
}

enum SchedulerKind {
    VisionQl {
        sender: mpsc::Sender<Request>,
        max_batch: usize,
    },
    Service {
        backend: Arc<dyn ModelBackend>,
        semaphore: Arc<Semaphore>,
        max_batch: usize,
    },
}

pub(crate) struct ModelScheduler {
    kind: SchedulerKind,
}

impl Debug for ModelScheduler {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            SchedulerKind::VisionQl { max_batch, .. } => formatter
                .debug_struct("ModelScheduler")
                .field("batching_owner", &BatchingOwner::VisionQl)
                .field("max_batch", max_batch)
                .finish(),
            SchedulerKind::Service {
                semaphore,
                max_batch,
                ..
            } => formatter
                .debug_struct("ModelScheduler")
                .field("batching_owner", &BatchingOwner::Service)
                .field("max_batch", max_batch)
                .field("available_permits", &semaphore.available_permits())
                .finish(),
        }
    }
}

impl ModelScheduler {
    pub(crate) fn new(backend: Arc<dyn ModelBackend>, batching_owner: BatchingOwner) -> Self {
        Self::with_config(
            backend,
            batching_owner,
            MAX_BATCH,
            MAX_WAIT,
            QUEUE_CAPACITY,
            SERVICE_CONCURRENCY,
        )
    }

    fn with_config(
        backend: Arc<dyn ModelBackend>,
        batching_owner: BatchingOwner,
        max_batch: usize,
        max_wait: Duration,
        capacity: usize,
        service_concurrency: usize,
    ) -> Self {
        let max_batch = max_batch.max(1);
        let kind = match batching_owner {
            BatchingOwner::VisionQl => {
                let (sender, receiver) = mpsc::channel(capacity.max(1));
                tokio::spawn(drive(receiver, backend, max_batch, max_wait));
                SchedulerKind::VisionQl { sender, max_batch }
            }
            BatchingOwner::Service => SchedulerKind::Service {
                backend,
                semaphore: Arc::new(Semaphore::new(service_concurrency.max(1))),
                max_batch,
            },
        };
        Self { kind }
    }

    #[cfg(test)]
    pub(crate) async fn infer_with_cancel(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
    ) -> Result<ArrayRef> {
        let metrics = Arc::new(QueryMetrics::default());
        let budget = QueryBudget::new(usize::MAX / 2, Arc::clone(&metrics.resources));
        self.infer_with_metrics(
            images,
            cancel,
            budget,
            metrics,
            InferenceReservations::new(Vec::new()),
        )
        .await
    }

    pub(crate) async fn infer_with_metrics(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: QueryBudget,
        metrics: Arc<QueryMetrics>,
        reservations: Arc<InferenceReservations>,
    ) -> Result<ArrayRef> {
        let _reservations = Arc::clone(&reservations);
        match &self.kind {
            SchedulerKind::VisionQl { max_batch, .. } if images.len() > *max_batch => {
                let mut images = images.into_iter();
                let mut outputs = Vec::new();
                loop {
                    let chunk = images.by_ref().take(*max_batch).collect::<Vec<_>>();
                    if chunk.is_empty() {
                        break;
                    }
                    outputs.push(
                        self.submit_visionql(
                            chunk,
                            cancel.clone(),
                            budget.clone(),
                            Arc::clone(&metrics),
                            Arc::clone(&reservations),
                        )
                        .await?,
                    );
                }
                concat_arrays(&outputs)
            }
            SchedulerKind::VisionQl { .. } => {
                self.submit_visionql(images, cancel, budget, metrics, reservations)
                    .await
            }
            SchedulerKind::Service { max_batch, .. } if images.len() > *max_batch => {
                let mut images = images.into_iter();
                let mut outputs = Vec::new();
                loop {
                    let chunk = images.by_ref().take(*max_batch).collect::<Vec<_>>();
                    if chunk.is_empty() {
                        break;
                    }
                    outputs.push(
                        self.submit_service(chunk, cancel.clone(), &budget, Arc::clone(&metrics))
                            .await?,
                    );
                }
                concat_arrays(&outputs)
            }
            SchedulerKind::Service { .. } => {
                self.submit_service(images, cancel, &budget, metrics).await
            }
        }
    }

    async fn submit_service(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: &QueryBudget,
        metrics: Arc<QueryMetrics>,
    ) -> Result<ArrayRef> {
        let SchedulerKind::Service {
            backend, semaphore, ..
        } = &self.kind
        else {
            unreachable!("service submission requires a service-owned scheduler");
        };
        let queued_at = Instant::now();
        let permit = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            permit = Arc::clone(semaphore).acquire_owned() => permit.map_err(|_| {
                VqlError::new(ErrorCode::Execution, "model service scheduler stopped")
            })?,
        };
        let queue_wait_micros = queued_at.elapsed().as_micros() as u64;
        let service_started = Instant::now();
        let rows = images.len();
        let result = backend.infer(images, cancel, budget).await;
        let service_micros = service_started.elapsed().as_micros() as u64;
        drop(permit);
        if result.is_ok() {
            metrics.record_inference(
                rows,
                queued_at.elapsed().as_micros() as u64,
                queue_wait_micros,
                service_micros,
            );
        }
        result
    }

    async fn submit_visionql(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: QueryBudget,
        metrics: Arc<QueryMetrics>,
        reservations: Arc<InferenceReservations>,
    ) -> Result<ArrayRef> {
        let SchedulerKind::VisionQl { sender, .. } = &self.kind else {
            unreachable!("VisionQL submission requires a VisionQL-owned scheduler");
        };
        let (response, receiver) = oneshot::channel();
        let request = Request {
            images,
            response,
            cancel: cancel.clone(),
            enqueued_at: Instant::now(),
            budget,
            metrics,
            _reservations: reservations,
        };
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            result = sender.send(request) => result.map_err(|_| {
                VqlError::new(ErrorCode::Execution, "model scheduler stopped")
            })?,
        }
        tokio::select! {
            _ = cancel.cancelled() => Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled")),
            result = receiver => result.map_err(|_| {
                VqlError::new(ErrorCode::Execution, "model scheduler dropped response")
            })?,
        }
    }
}

fn concat_arrays(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    if arrays.len() == 1 {
        return Ok(Arc::clone(&arrays[0]));
    }
    let arrays = arrays
        .iter()
        .map(|array| array.as_ref() as &dyn Array)
        .collect::<Vec<_>>();
    arrow::compute::concat(&arrays).map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            "failed to concatenate model batch output",
        )
        .with_source(error)
    })
}

async fn drive(
    mut receiver: mpsc::Receiver<Request>,
    backend: Arc<dyn ModelBackend>,
    max_batch: usize,
    max_wait: Duration,
) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => break,
            },
        };
        if first.cancel.is_cancelled() {
            continue;
        }
        let started = Instant::now();
        let mut requests = vec![first];
        let mut rows = requests[0].images.len();
        while rows < max_batch {
            let remaining = max_wait.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, receiver.recv()).await {
                Ok(Some(request)) if request.cancel.is_cancelled() => {}
                Ok(Some(request)) if !requests[0].budget.same_query(&request.budget) => {
                    pending = Some(request);
                    break;
                }
                Ok(Some(request)) if rows + request.images.len() > max_batch => {
                    pending = Some(request);
                    break;
                }
                Ok(Some(request)) => {
                    rows += request.images.len();
                    requests.push(request);
                }
                Ok(None) | Err(_) => break,
            }
        }
        requests.retain(|request| !request.cancel.is_cancelled());
        if requests.is_empty() {
            continue;
        }
        rows = requests.iter().map(|request| request.images.len()).sum();
        let enqueued_at = requests
            .iter()
            .map(|request| request.enqueued_at)
            .min()
            .expect("non-empty request batch has an enqueue time");
        let budget = requests[0].budget.clone();
        let metrics = Arc::clone(&requests[0].metrics);
        let sizes = requests
            .iter()
            .map(|request| request.images.len())
            .collect::<Vec<_>>();
        let images = requests
            .iter_mut()
            .flat_map(|request| std::mem::take(&mut request.images))
            .collect();
        let service_started = Instant::now();
        let output = backend
            .infer(images, CancellationToken::new(), &budget)
            .await;
        let service_micros = service_started.elapsed().as_micros() as u64;
        match output {
            Ok(output) if output.len() == rows => {
                metrics.record_inference(
                    rows,
                    enqueued_at.elapsed().as_micros() as u64,
                    service_started
                        .saturating_duration_since(enqueued_at)
                        .as_micros() as u64,
                    service_micros,
                );
                let mut offset = 0;
                for (request, size) in requests.into_iter().zip(sizes) {
                    let values = output.slice(offset, size);
                    offset += size;
                    let _ = request.response.send(Ok(values));
                }
            }
            Ok(output) => {
                let message = format!(
                    "model backend returned {} rows for {rows} inputs",
                    output.len()
                );
                for request in requests {
                    let _ = request
                        .response
                        .send(Err(VqlError::new(ErrorCode::Execution, message.clone())));
                }
            }
            Err(error) => {
                let code = error.code;
                let message = error.message;
                for request in requests {
                    let _ = request
                        .response
                        .send(Err(VqlError::new(code, message.clone())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow::array::ListArray;
    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;
    use crate::models::backend::MockBackend;
    use crate::models::postprocess::mock_detection_output;

    #[derive(Debug)]
    struct RecordingBackend {
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl ModelBackend for RecordingBackend {
        async fn infer(
            &self,
            images: Vec<DynamicImage>,
            _cancel: CancellationToken,
            _budget: &QueryBudget,
        ) -> Result<ArrayRef> {
            self.batch_sizes.lock().unwrap().push(images.len());
            Ok(mock_detection_output("person", images.len()))
        }
    }

    #[tokio::test]
    async fn scheduler_preserves_batch_order() {
        let scheduler = ModelScheduler::with_config(
            Arc::new(MockBackend::new("person")),
            BatchingOwner::VisionQl,
            8,
            Duration::from_millis(1),
            4,
            1,
        );
        let output = scheduler
            .infer_with_cancel(
                vec![DynamicImage::new_rgb8(1, 1), DynamicImage::new_rgb8(2, 2)],
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.as_any().is::<ListArray>());
    }

    #[tokio::test]
    async fn scheduler_drops_cancelled_work() {
        let scheduler = ModelScheduler::with_config(
            Arc::new(MockBackend::new("person")),
            BatchingOwner::VisionQl,
            8,
            Duration::from_millis(1),
            1,
            1,
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = scheduler
            .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], cancel)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::QueryCancelled);
    }

    #[tokio::test]
    async fn scheduler_never_exceeds_max_batch() {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let scheduler = ModelScheduler::with_config(
            Arc::new(RecordingBackend {
                batch_sizes: Arc::clone(&batch_sizes),
            }),
            BatchingOwner::VisionQl,
            3,
            Duration::from_millis(1),
            4,
            1,
        );
        let images = (0..8)
            .map(|_| DynamicImage::new_rgb8(1, 1))
            .collect::<Vec<_>>();
        let output = scheduler
            .infer_with_cancel(images, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(output.len(), 8);
        assert_eq!(*batch_sizes.lock().unwrap(), vec![3, 3, 2]);
    }

    #[tokio::test]
    async fn scheduler_metrics_describe_actual_backend_batches() {
        let scheduler = ModelScheduler::with_config(
            Arc::new(MockBackend::new("person")),
            BatchingOwner::VisionQl,
            3,
            Duration::from_millis(1),
            4,
            1,
        );
        let metrics = Arc::new(QueryMetrics::default());
        let budget = QueryBudget::new(1024 * 1024, Arc::clone(&metrics.resources));
        let images = (0..8)
            .map(|_| DynamicImage::new_rgb8(1, 1))
            .collect::<Vec<_>>();

        let output = scheduler
            .infer_with_metrics(
                images,
                CancellationToken::new(),
                budget,
                Arc::clone(&metrics),
                InferenceReservations::new(Vec::new()),
            )
            .await
            .unwrap();

        assert_eq!(output.len(), 8);
        assert_eq!(metrics.inference_rows(), 8);
        assert_eq!(metrics.inference_batches(), 3);
        let histogram = metrics.batch_histogram();
        assert_eq!(histogram[3], 2);
        assert_eq!(histogram[2], 1);
    }

    #[tokio::test]
    async fn service_owned_scheduler_never_exceeds_max_batch() {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let scheduler = ModelScheduler::with_config(
            Arc::new(RecordingBackend {
                batch_sizes: Arc::clone(&batch_sizes),
            }),
            BatchingOwner::Service,
            3,
            Duration::ZERO,
            1,
            2,
        );
        let images = (0..8)
            .map(|_| DynamicImage::new_rgb8(1, 1))
            .collect::<Vec<_>>();
        let output = scheduler
            .infer_with_cancel(images, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(output.len(), 8);
        assert_eq!(*batch_sizes.lock().unwrap(), vec![3, 3, 2]);
    }

    #[tokio::test]
    async fn scheduler_rejects_backend_cardinality_mismatch() {
        #[derive(Debug)]
        struct EmptyBackend;

        #[async_trait]
        impl ModelBackend for EmptyBackend {
            async fn infer(
                &self,
                _images: Vec<DynamicImage>,
                _cancel: CancellationToken,
                _budget: &QueryBudget,
            ) -> Result<ArrayRef> {
                Ok(mock_detection_output("person", 0))
            }
        }

        let scheduler = ModelScheduler::with_config(
            Arc::new(EmptyBackend),
            BatchingOwner::VisionQl,
            8,
            Duration::from_millis(1),
            1,
            1,
        );
        let error = scheduler
            .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("0 rows for 1 inputs"));
    }

    #[tokio::test]
    async fn scheduler_preserves_resource_exhaustion_errors() {
        #[derive(Debug)]
        struct ExhaustedBackend;

        #[async_trait]
        impl ModelBackend for ExhaustedBackend {
            async fn infer(
                &self,
                _images: Vec<DynamicImage>,
                _cancel: CancellationToken,
                _budget: &QueryBudget,
            ) -> Result<ArrayRef> {
                Err(VqlError::new(
                    ErrorCode::ResourceExhausted,
                    "model tensor budget exceeded",
                ))
            }
        }

        let scheduler = ModelScheduler::with_config(
            Arc::new(ExhaustedBackend),
            BatchingOwner::VisionQl,
            8,
            Duration::from_millis(1),
            1,
            1,
        );
        let error = scheduler
            .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], CancellationToken::new())
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::ResourceExhausted);
    }

    #[tokio::test]
    async fn cancellation_does_not_wait_for_blocking_backend() {
        #[derive(Debug)]
        struct SlowBackend {
            started: Arc<Notify>,
        }

        #[async_trait]
        impl ModelBackend for SlowBackend {
            async fn infer(
                &self,
                images: Vec<DynamicImage>,
                _cancel: CancellationToken,
                _budget: &QueryBudget,
            ) -> Result<ArrayRef> {
                self.started.notify_one();
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(mock_detection_output("person", images.len()))
            }
        }

        let backend_started = Arc::new(Notify::new());
        let scheduler = ModelScheduler::with_config(
            Arc::new(SlowBackend {
                started: Arc::clone(&backend_started),
            }),
            BatchingOwner::VisionQl,
            8,
            Duration::from_millis(1),
            1,
            1,
        );
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            backend_started.notified().await;
            trigger.cancel();
        });
        let metrics = Arc::new(QueryMetrics::default());
        let budget = QueryBudget::new(1024, Arc::clone(&metrics.resources));
        let media = budget.reserve(crate::QueryResource::Media, 64).unwrap();
        let started = Instant::now();
        let error = scheduler
            .infer_with_metrics(
                vec![DynamicImage::new_rgb8(1, 1)],
                cancel,
                budget,
                Arc::clone(&metrics),
                InferenceReservations::new(vec![media]),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::QueryCancelled);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(
            metrics
                .resource_usage(crate::QueryResource::Media)
                .current_bytes,
            64,
            "the scheduler must retain request resources until the backend stops using them"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(metrics.total_resource_usage().current_bytes, 0);
    }

    #[tokio::test]
    async fn service_owned_scheduler_allows_bounded_overlap() {
        #[derive(Debug)]
        struct ConcurrentBackend {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl ModelBackend for ConcurrentBackend {
            async fn infer(
                &self,
                images: Vec<DynamicImage>,
                _cancel: CancellationToken,
                _budget: &QueryBudget,
            ) -> Result<ArrayRef> {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(mock_detection_output("person", images.len()))
            }
        }

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let scheduler = Arc::new(ModelScheduler::with_config(
            Arc::new(ConcurrentBackend {
                active,
                peak: Arc::clone(&peak),
            }),
            BatchingOwner::Service,
            16,
            Duration::ZERO,
            1,
            2,
        ));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let scheduler = Arc::clone(&scheduler);
            tasks.push(tokio::spawn(async move {
                scheduler
                    .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], CancellationToken::new())
                    .await
                    .unwrap()
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }
}
