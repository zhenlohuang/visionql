use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use image::DynamicImage;
use tokio_util::sync::CancellationToken;

use super::Detection;
use super::backend::ModelBackend;
use crate::{ErrorCode, Result, VqlError};

type Response = std::sync::mpsc::Sender<Result<Vec<Vec<Detection>>>>;

struct Request {
    images: Vec<DynamicImage>,
    response: Response,
    cancel: CancellationToken,
}

#[derive(Debug)]
pub(crate) struct ModelScheduler {
    sender: SyncSender<Request>,
    max_batch: usize,
}

impl ModelScheduler {
    pub(crate) fn new(
        backend: Arc<dyn ModelBackend>,
        max_batch: usize,
        max_wait: Duration,
        capacity: usize,
    ) -> Self {
        let (sender, receiver) = sync_channel::<Request>(capacity);
        let max_batch = max_batch.max(1);
        thread::Builder::new()
            .name("vql-model-scheduler".to_owned())
            .spawn(move || drive(receiver, backend, max_batch, max_wait))
            .expect("model scheduler thread can be created");
        Self { sender, max_batch }
    }

    #[cfg(test)]
    pub(crate) fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
        self.infer_with_cancel(images, CancellationToken::new())
    }

    pub(crate) fn infer_with_cancel(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
    ) -> Result<Vec<Vec<Detection>>> {
        if images.len() > self.max_batch {
            let mut images = images.into_iter();
            let mut output = Vec::new();
            loop {
                let chunk = images.by_ref().take(self.max_batch).collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                output.extend(self.submit(chunk, cancel.clone())?);
            }
            return Ok(output);
        }
        self.submit(images, cancel)
    }

    fn submit(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
    ) -> Result<Vec<Vec<Detection>>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut request = Request {
            images,
            response: sender,
            cancel: cancel.clone(),
        };
        loop {
            if cancel.is_cancelled() {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            match self.sender.try_send(request) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    request = returned;
                    thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(VqlError::new(
                        ErrorCode::Execution,
                        "model scheduler stopped",
                    ));
                }
            }
        }
        loop {
            if cancel.is_cancelled() {
                return Err(VqlError::new(ErrorCode::QueryCancelled, "query cancelled"));
            }
            match receiver.recv_timeout(Duration::from_millis(5)) {
                Ok(result) => return result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(VqlError::new(
                        ErrorCode::Execution,
                        "model scheduler dropped response",
                    ));
                }
            }
        }
    }
}

fn drive(
    receiver: Receiver<Request>,
    backend: Arc<dyn ModelBackend>,
    max_batch: usize,
    max_wait: Duration,
) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(request) => request,
            None => match receiver.recv() {
                Ok(request) => request,
                Err(_) => break,
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
            match receiver.recv_timeout(remaining) {
                Ok(request) => {
                    if request.cancel.is_cancelled() {
                        continue;
                    }
                    if rows + request.images.len() > max_batch {
                        pending = Some(request);
                        break;
                    } else {
                        rows += request.images.len();
                        requests.push(request);
                    }
                }
                Err(_) => break,
            }
        }
        let sizes = requests
            .iter()
            .map(|request| request.images.len())
            .collect::<Vec<_>>();
        let images = requests
            .iter_mut()
            .flat_map(|request| std::mem::take(&mut request.images))
            .collect();
        match backend.infer(images) {
            Ok(output) if output.len() == rows => {
                let mut output = output.into_iter();
                for (request, size) in requests.into_iter().zip(sizes) {
                    let values = output.by_ref().take(size).collect();
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
                let message = error.to_string();
                for request in requests {
                    let _ = request
                        .response
                        .send(Err(VqlError::new(ErrorCode::Execution, message.clone())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::backend::MockBackend;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct RecordingBackend {
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl ModelBackend for RecordingBackend {
        fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
            self.batch_sizes.lock().unwrap().push(images.len());
            Ok(images.into_iter().map(|_| Vec::new()).collect())
        }
    }

    #[test]
    fn scheduler_preserves_batch_order() {
        let scheduler = ModelScheduler::new(
            Arc::new(MockBackend::new("person")),
            8,
            Duration::from_millis(1),
            4,
        );
        let images = vec![DynamicImage::new_rgb8(1, 1), DynamicImage::new_rgb8(2, 2)];
        let result = scheduler.infer(images).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0].label, "person");
        assert_eq!(result[1][0].confidence, 0.9);
    }

    #[test]
    fn scheduler_drops_cancelled_work() {
        let scheduler = ModelScheduler::new(
            Arc::new(MockBackend::new("person")),
            8,
            Duration::from_millis(1),
            1,
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = scheduler
            .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], cancel)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::QueryCancelled);
    }

    #[test]
    fn scheduler_never_exceeds_max_batch() {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let scheduler = ModelScheduler::new(
            Arc::new(RecordingBackend {
                batch_sizes: Arc::clone(&batch_sizes),
            }),
            3,
            Duration::from_millis(1),
            4,
        );
        let images = (0..8)
            .map(|_| DynamicImage::new_rgb8(1, 1))
            .collect::<Vec<_>>();

        let output = scheduler.infer(images).unwrap();

        assert_eq!(output.len(), 8);
        assert_eq!(*batch_sizes.lock().unwrap(), vec![3, 3, 2]);
    }

    #[test]
    fn scheduler_rejects_backend_cardinality_mismatch() {
        #[derive(Debug)]
        struct EmptyBackend;

        impl ModelBackend for EmptyBackend {
            fn infer(&self, _images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
                Ok(Vec::new())
            }
        }

        let scheduler = ModelScheduler::new(Arc::new(EmptyBackend), 8, Duration::from_millis(1), 1);
        let error = scheduler
            .infer(vec![DynamicImage::new_rgb8(1, 1)])
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("0 rows for 1 inputs"));
    }

    #[test]
    fn scheduler_cancellation_does_not_wait_for_blocking_backend() {
        #[derive(Debug)]
        struct SlowBackend;

        impl ModelBackend for SlowBackend {
            fn infer(&self, images: Vec<DynamicImage>) -> Result<Vec<Vec<Detection>>> {
                std::thread::sleep(Duration::from_millis(500));
                Ok(images.into_iter().map(|_| Vec::new()).collect())
            }
        }

        let scheduler = ModelScheduler::new(Arc::new(SlowBackend), 8, Duration::from_millis(1), 1);
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            trigger.cancel();
        });
        let started = Instant::now();

        let error = scheduler
            .infer_with_cancel(vec![DynamicImage::new_rgb8(1, 1)], cancel)
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::QueryCancelled);
        assert!(started.elapsed() < Duration::from_millis(400));
    }
}
