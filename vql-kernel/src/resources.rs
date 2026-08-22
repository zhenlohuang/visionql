use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueryResource {
    Arrow,
    Media,
    FrameBuffer,
    ModelTensor,
    ModelQueue,
    TritonPayload,
    SinkBuffer,
}

impl QueryResource {
    const fn consumer_name(self) -> &'static str {
        match self {
            Self::Arrow => "vql.arrow",
            Self::Media => "vql.media",
            Self::FrameBuffer => "vql.frame_buffer",
            Self::ModelTensor => "vql.model_tensor",
            Self::ModelQueue => "vql.model_queue",
            Self::TritonPayload => "vql.triton_payload",
            Self::SinkBuffer => "vql.sink_buffer",
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
}

impl QueryMemoryPool {
    fn new(session: Arc<SessionMemoryPool>) -> Self {
        Self { session }
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
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.session.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.session.try_grow(reservation, additional)
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
    pub(crate) fn new(limit: usize) -> Self {
        Self::for_session(Arc::new(SessionMemoryPool::new(limit)))
    }

    pub(crate) fn for_session(session: Arc<SessionMemoryPool>) -> Self {
        let limit = session.limit();
        let pool: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(Arc::clone(&session)));
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
        let budget = QueryBudget::new(100);
        let frames = budget.reserve(QueryResource::FrameBuffer, 60).unwrap();
        let tensor = budget.reserve(QueryResource::ModelTensor, 40).unwrap();
        assert_eq!(budget.session.reserved(), 100);
        assert_eq!(
            budget
                .reserve(QueryResource::SinkBuffer, 1)
                .unwrap_err()
                .code,
            ErrorCode::ResourceExhausted
        );
        drop(tensor);
        assert_eq!(budget.session.reserved(), 60);
        drop(frames);
        assert_eq!(budget.session.reserved(), 0);
    }

    #[test]
    fn query_views_share_one_session_limit() {
        let session = Arc::new(SessionMemoryPool::new(100));
        let first = QueryBudget::for_session(Arc::clone(&session));
        let second = QueryBudget::for_session(Arc::clone(&session));

        let frames = first.reserve(QueryResource::FrameBuffer, 60).unwrap();
        let tensor = second.reserve(QueryResource::ModelTensor, 40).unwrap();

        assert_eq!(session.reserved(), 100);
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
