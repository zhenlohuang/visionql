use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueryResource {
    Arrow,
    Media,
    FrameBuffer,
    ModelTensor,
    ModelQueue,
    TritonPayload,
    WindowState,
    SinkBuffer,
    DeviceMemory,
}

impl QueryResource {
    const COUNT: usize = 9;

    const fn index(self) -> usize {
        match self {
            Self::Arrow => 0,
            Self::Media => 1,
            Self::FrameBuffer => 2,
            Self::ModelTensor => 3,
            Self::ModelQueue => 4,
            Self::TritonPayload => 5,
            Self::WindowState => 6,
            Self::SinkBuffer => 7,
            Self::DeviceMemory => 8,
        }
    }

    const fn consumer_name(self) -> &'static str {
        match self {
            Self::Arrow => "vql.arrow",
            Self::Media => "vql.media",
            Self::FrameBuffer => "vql.frame_buffer",
            Self::ModelTensor => "vql.model_tensor",
            Self::ModelQueue => "vql.model_queue",
            Self::TritonPayload => "vql.triton_payload",
            Self::WindowState => "vql.window_state",
            Self::SinkBuffer => "vql.sink_buffer",
            Self::DeviceMemory => "vql.device_memory",
        }
    }

    fn from_consumer(consumer: &MemoryConsumer) -> Self {
        match consumer.name() {
            "vql.media" => Self::Media,
            "vql.frame_buffer" => Self::FrameBuffer,
            "vql.model_tensor" => Self::ModelTensor,
            "vql.model_queue" => Self::ModelQueue,
            "vql.triton_payload" => Self::TritonPayload,
            "vql.window_state" | "TumbleState" => Self::WindowState,
            "vql.sink_buffer" => Self::SinkBuffer,
            "vql.device_memory" => Self::DeviceMemory,
            _ => Self::Arrow,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceUsage {
    pub current_bytes: u64,
    pub peak_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct ResourceMetrics {
    current: [AtomicU64; QueryResource::COUNT],
    peak: [AtomicU64; QueryResource::COUNT],
    total_current: AtomicU64,
    total_peak: AtomicU64,
}

impl Default for ResourceMetrics {
    fn default() -> Self {
        Self {
            current: std::array::from_fn(|_| AtomicU64::new(0)),
            peak: std::array::from_fn(|_| AtomicU64::new(0)),
            total_current: AtomicU64::new(0),
            total_peak: AtomicU64::new(0),
        }
    }
}

impl ResourceMetrics {
    fn grow(&self, resource: QueryResource, bytes: usize) {
        let bytes = bytes as u64;
        let current = self.current[resource.index()].fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak[resource.index()].fetch_max(current, Ordering::Relaxed);
        let total = self.total_current.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.total_peak.fetch_max(total, Ordering::Relaxed);
    }

    fn shrink(&self, resource: QueryResource, bytes: usize) {
        let bytes = bytes as u64;
        self.current[resource.index()].fetch_sub(bytes, Ordering::Relaxed);
        self.total_current.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(crate) fn usage(&self, resource: QueryResource) -> ResourceUsage {
        ResourceUsage {
            current_bytes: self.current[resource.index()].load(Ordering::Relaxed),
            peak_bytes: self.peak[resource.index()].load(Ordering::Relaxed),
        }
    }

    pub(crate) fn total_usage(&self) -> ResourceUsage {
        ResourceUsage {
            current_bytes: self.total_current.load(Ordering::Relaxed),
            peak_bytes: self.total_peak.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionMemoryPool {
    inner: GreedyMemoryPool,
    limit: usize,
}

impl SessionMemoryPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            inner: GreedyMemoryPool::new(limit),
            limit,
        }
    }

    fn limit(&self) -> usize {
        self.limit
    }
}

impl Display for SessionMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "vql-session({})", self.inner)
    }
}

impl MemoryPool for SessionMemoryPool {
    fn name(&self) -> &str {
        "vql-session"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

#[derive(Debug)]
struct QueryMemoryPool {
    session: Arc<SessionMemoryPool>,
    metrics: Arc<ResourceMetrics>,
}

impl QueryMemoryPool {
    fn new(session: Arc<SessionMemoryPool>, metrics: Arc<ResourceMetrics>) -> Self {
        Self { session, metrics }
    }
}

impl Display for QueryMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "vql-query({})", self.session)
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        "vql-query"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.session.grow(reservation, additional);
        self.metrics.grow(
            QueryResource::from_consumer(reservation.consumer()),
            additional,
        );
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.session.shrink(reservation, shrink);
        self.metrics
            .shrink(QueryResource::from_consumer(reservation.consumer()), shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.session.try_grow(reservation, additional)?;
        self.metrics.grow(
            QueryResource::from_consumer(reservation.consumer()),
            additional,
        );
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.session.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.session.memory_limit()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryBudget {
    limit: usize,
    session: Arc<SessionMemoryPool>,
    pool: Arc<dyn MemoryPool>,
}

impl QueryBudget {
    #[cfg(test)]
    pub(crate) fn new(limit: usize, metrics: Arc<ResourceMetrics>) -> Self {
        Self::for_session(Arc::new(SessionMemoryPool::new(limit)), metrics)
    }

    pub(crate) fn for_session(
        session: Arc<SessionMemoryPool>,
        metrics: Arc<ResourceMetrics>,
    ) -> Self {
        let limit = session.limit();
        let pool: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(
            Arc::clone(&session),
            Arc::clone(&metrics),
        ));
        Self {
            limit,
            session,
            pool,
        }
    }

    pub(crate) fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.pool)
    }

    pub(crate) fn same_query(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.pool, &other.pool)
    }

    pub(crate) fn reserve(
        &self,
        resource: QueryResource,
        bytes: usize,
    ) -> Result<QueryReservation> {
        let reservation = MemoryConsumer::new(resource.consumer_name()).register(&self.pool);
        reservation.try_resize(bytes).map_err(|error| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "session memory limit of {} bytes exceeded: {} bytes reserved; cannot reserve {bytes} bytes for {}",
                    self.limit,
                    self.session.reserved(),
                    resource.consumer_name()
                ),
            )
            .with_source(error)
        })?;
        Ok(QueryReservation {
            reservation,
            resource,
            limit: self.limit,
            session: Arc::clone(&self.session),
        })
    }
}

#[derive(Debug)]
pub(crate) struct QueryReservation {
    reservation: MemoryReservation,
    resource: QueryResource,
    limit: usize,
    session: Arc<SessionMemoryPool>,
}

impl QueryReservation {
    pub(crate) fn size(&self) -> usize {
        self.reservation.size()
    }

    pub(crate) fn try_grow(&mut self, additional: usize) -> Result<()> {
        let size = self.size().checked_add(additional).ok_or_else(|| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "session memory reservation for {} exceeds platform limits",
                    self.resource.consumer_name()
                ),
            )
        })?;
        self.try_resize(size)
    }

    pub(crate) fn try_resize(&mut self, size: usize) -> Result<()> {
        self.reservation.try_resize(size).map_err(|error| {
            VqlError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "session memory limit of {} bytes exceeded: {} bytes reserved; cannot resize reservation to {size} bytes for {}",
                    self.limit,
                    self.session.reserved(),
                    self.resource.consumer_name()
                ),
            )
            .with_source(error)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_pool_enforces_the_total_and_releases_all_categories() {
        let metrics = Arc::new(ResourceMetrics::default());
        let budget = QueryBudget::new(100, Arc::clone(&metrics));
        let frames = budget.reserve(QueryResource::FrameBuffer, 60).unwrap();
        let tensor = budget.reserve(QueryResource::ModelTensor, 40).unwrap();
        assert_eq!(metrics.total_usage().current_bytes, 100);
        assert_eq!(
            budget
                .reserve(QueryResource::SinkBuffer, 1)
                .unwrap_err()
                .code,
            ErrorCode::ResourceExhausted
        );
        drop(tensor);
        assert_eq!(metrics.total_usage().current_bytes, 60);
        drop(frames);
        assert_eq!(metrics.total_usage().current_bytes, 0);
        assert_eq!(metrics.total_usage().peak_bytes, 100);
        assert_eq!(
            metrics.usage(QueryResource::FrameBuffer),
            ResourceUsage {
                current_bytes: 0,
                peak_bytes: 60,
            }
        );
    }

    #[test]
    fn query_views_share_one_session_limit_but_keep_separate_metrics() {
        let session = Arc::new(SessionMemoryPool::new(100));
        let first_metrics = Arc::new(ResourceMetrics::default());
        let second_metrics = Arc::new(ResourceMetrics::default());
        let first = QueryBudget::for_session(Arc::clone(&session), Arc::clone(&first_metrics));
        let second = QueryBudget::for_session(Arc::clone(&session), Arc::clone(&second_metrics));

        let frames = first.reserve(QueryResource::FrameBuffer, 60).unwrap();
        let tensor = second.reserve(QueryResource::ModelTensor, 40).unwrap();

        assert_eq!(session.reserved(), 100);
        assert_eq!(first_metrics.total_usage().current_bytes, 60);
        assert_eq!(second_metrics.total_usage().current_bytes, 40);
        assert_eq!(
            second
                .reserve(QueryResource::SinkBuffer, 1)
                .unwrap_err()
                .code,
            ErrorCode::ResourceExhausted
        );

        drop(frames);
        assert_eq!(session.reserved(), 40);
        drop(tensor);
        assert_eq!(session.reserved(), 0);
    }
}
