use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::str;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionCancelQueryRequest, ActionCancelQueryResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, FlightDescriptor,
    FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    Result as FlightActionResult, SchemaAsIpc, Ticket,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use vql_kernel::{
    ErrorCode, PreparedResult, PreparedStatement, QueryHandle, ResultMode, Session, StatementKind,
    VqlError,
};

use crate::config::ServiceConfig;
use crate::controller::QueryController;
use crate::image_boundary::sanitize_batch;

#[derive(Debug, Clone)]
struct AuthContext {
    session_id: String,
    principal: String,
    session: Session,
}

#[derive(Debug, Clone)]
struct SessionEntry {
    context: AuthContext,
    last_used: Instant,
}

#[derive(Debug, Clone)]
struct PreparedEntry {
    session_id: String,
    statement: PreparedStatement,
}

#[derive(Debug, Clone)]
struct ExecutionEntry {
    session_id: String,
    statement: PreparedStatement,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct AttachedExecution {
    session_id: String,
    handle: QueryHandle,
}

#[derive(Debug, Clone)]
enum ExecutionState {
    Pending(Box<ExecutionEntry>),
    Starting { session_id: String, cancelled: bool },
    Attached(Box<AttachedExecution>),
}

impl ExecutionState {
    fn session_id(&self) -> &str {
        match self {
            Self::Pending(entry) => &entry.session_id,
            Self::Starting { session_id, .. } => session_id,
            Self::Attached(entry) => &entry.session_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VqlFlightSqlService {
    engine: vql_kernel::Engine,
    controller: Arc<QueryController>,
    config: Arc<ServiceConfig>,
    sessions: Arc<Mutex<HashMap<String, SessionEntry>>>,
    prepared: Arc<Mutex<HashMap<String, PreparedEntry>>>,
    executions: Arc<Mutex<HashMap<String, ExecutionState>>>,
    ready: Arc<AtomicBool>,
    total_executions: Arc<AtomicU64>,
    failed_executions: Arc<AtomicU64>,
    inference_executions: Arc<AtomicU64>,
    source_executions: Arc<AtomicU64>,
    sink_executions: Arc<AtomicU64>,
    started_at: Instant,
}

impl VqlFlightSqlService {
    pub fn new(
        engine: vql_kernel::Engine,
        controller: Arc<QueryController>,
        config: Arc<ServiceConfig>,
        ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            engine,
            controller,
            config,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            prepared: Arc::new(Mutex::new(HashMap::new())),
            executions: Arc::new(Mutex::new(HashMap::new())),
            ready,
            total_executions: Arc::new(AtomicU64::new(0)),
            failed_executions: Arc::new(AtomicU64::new(0)),
            inference_executions: Arc::new(AtomicU64::new(0)),
            source_executions: Arc::new(AtomicU64::new(0)),
            sink_executions: Arc::new(AtomicU64::new(0)),
            started_at: Instant::now(),
        }
    }

    pub fn ready_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.ready)
    }

    pub fn session_count(&self) -> usize {
        self.cleanup_expired_sessions();
        self.sessions.lock().map(|values| values.len()).unwrap_or(0)
    }

    pub fn pending_execution_count(&self) -> usize {
        self.executions
            .lock()
            .map(|values| {
                values
                    .values()
                    .filter(|state| {
                        matches!(
                            state,
                            ExecutionState::Pending(_) | ExecutionState::Starting { .. }
                        )
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn attached_execution_count(&self) -> usize {
        self.executions
            .lock()
            .map(|values| {
                values
                    .values()
                    .filter(|state| matches!(state, ExecutionState::Attached(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn total_executions(&self) -> u64 {
        self.total_executions.load(Ordering::Relaxed)
    }

    pub fn failed_executions(&self) -> u64 {
        self.failed_executions.load(Ordering::Relaxed)
    }

    pub fn inference_executions(&self) -> u64 {
        self.inference_executions.load(Ordering::Relaxed)
    }

    pub fn source_executions(&self) -> u64 {
        self.source_executions.load(Ordering::Relaxed)
    }

    pub fn sink_executions(&self) -> u64 {
        self.sink_executions.load(Ordering::Relaxed)
    }

    pub fn process_uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    fn auth<T>(&self, request: &Request<T>) -> Result<AuthContext, Status> {
        self.cleanup_expired_sessions();
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        if let Some(token) = authorization.and_then(|value| value.strip_prefix("Bearer ")) {
            if let Some(session) = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("Session registry was poisoned"))?
                .get_mut(token)
            {
                session.last_used = Instant::now();
                return Ok(session.context.clone());
            }
            return Err(Status::unauthenticated("invalid service credential"));
        }
        if self.config.service_token.is_none()
            && request
                .remote_addr()
                .is_none_or(|address| address.ip().is_loopback())
        {
            let session_id = request.remote_addr().map_or_else(
                || "local-loopback".to_owned(),
                |address| format!("local-loopback:{address}"),
            );
            return self.direct_session(&session_id);
        }
        Err(Status::unauthenticated("authentication is required"))
    }

    fn direct_session(&self, session_id: &str) -> Result<AuthContext, Status> {
        self.cleanup_expired_sessions();
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| Status::internal("Session registry was poisoned"))?;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_used = Instant::now();
            return Ok(session.context.clone());
        }
        let context = AuthContext {
            session_id: session_id.to_owned(),
            principal: self.config.principal.clone(),
            session: self
                .engine
                .session()
                .for_service()
                .build()
                .map_err(|error| status_from_vql(&error))?,
        };
        sessions.insert(
            session_id.to_owned(),
            SessionEntry {
                context: context.clone(),
                last_used: Instant::now(),
            },
        );
        Ok(context)
    }

    fn cleanup_expired_sessions(&self) {
        let timeout = self.config.session_idle_timeout();
        let active_sessions = self
            .executions
            .lock()
            .map(|executions| {
                executions
                    .values()
                    .filter(|state| {
                        matches!(
                            state,
                            ExecutionState::Starting { .. } | ExecutionState::Attached(_)
                        )
                    })
                    .map(|state| state.session_id().to_owned())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let expired = if let Ok(mut sessions) = self.sessions.lock() {
            let expired = sessions
                .iter()
                .filter(|(session_id, session)| {
                    !active_sessions.contains(*session_id) && session.last_used.elapsed() > timeout
                })
                .map(|(session_id, _)| session_id.clone())
                .collect::<HashSet<_>>();
            sessions.retain(|session_id, _| !expired.contains(session_id));
            expired
        } else {
            return;
        };
        if expired.is_empty() {
            return;
        }
        if let Ok(mut prepared) = self.prepared.lock() {
            prepared.retain(|_, entry| !expired.contains(&entry.session_id));
        }
        if let Ok(mut executions) = self.executions.lock() {
            executions.retain(|_, state| {
                !matches!(state, ExecutionState::Pending(_))
                    || !expired.contains(state.session_id())
            });
        }
    }

    async fn prepare_statement(
        &self,
        auth: &AuthContext,
        sql: &str,
    ) -> Result<PreparedStatement, Status> {
        let session = auth.session.clone();
        let principal = auth.principal.clone();
        let settings = session.semantic_settings();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || session.prepare(&sql, principal, settings))
            .await
            .map_err(|error| Status::internal(format!("Kernel prepare task failed: {error}")))?
            .map_err(|error| status_from_vql(&error))
    }

    fn register_execution(
        &self,
        auth: &AuthContext,
        statement: PreparedStatement,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        if statement.statement_info().kind == StatementKind::Update {
            return Err(Status::invalid_argument(
                "updates must be executed with Flight SQL ExecuteUpdate",
            ));
        }
        let execution_id = uuid::Uuid::new_v4().to_string();
        self.cleanup_unattached();
        self.executions
            .lock()
            .map_err(|_| Status::internal("Execution registry was poisoned"))?
            .insert(
                execution_id.clone(),
                ExecutionState::Pending(Box::new(ExecutionEntry {
                    session_id: auth.session_id.clone(),
                    statement: statement.clone(),
                    created_at: Instant::now(),
                })),
            );
        let ticket = TicketStatementQuery {
            statement_handle: execution_id.clone().into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let metadata = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "execution_id": execution_id,
        }))
        .map_err(|_| Status::internal("failed to encode execution metadata"))?;
        FlightInfo::new()
            .try_with_schema(statement.result_schema().as_ref())
            .map_err(|error| Status::internal(error.to_string()))
            .map(|info| {
                info.with_endpoint(endpoint)
                    .with_descriptor(descriptor)
                    .with_app_metadata(metadata)
            })
    }

    fn cleanup_unattached(&self) {
        let timeout = self.config.attach_timeout();
        if let Ok(mut executions) = self.executions.lock() {
            executions.retain(|_, state| {
                !matches!(state, ExecutionState::Pending(execution) if execution.created_at.elapsed() > timeout)
            });
        }
    }

    async fn execute_entry(
        &self,
        execution_id: String,
        entry: ExecutionEntry,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.total_executions.fetch_add(1, Ordering::Relaxed);
        if entry.statement.uses_inference() {
            self.inference_executions.fetch_add(1, Ordering::Relaxed);
        }
        if entry.statement.uses_source() {
            self.source_executions.fetch_add(1, Ordering::Relaxed);
        }
        if entry.statement.uses_sink() {
            self.sink_executions.fetch_add(1, Ordering::Relaxed);
        }
        let schema = entry.statement.result_schema();
        if entry.statement.persistent_command().is_some() {
            let batches = self
                .controller
                .execute(&entry.statement)
                .await
                .map_err(|error| {
                    self.failed_executions.fetch_add(1, Ordering::Relaxed);
                    status_from_vql(&error)
                })?;
            if self.finish_starting_execution(&execution_id) {
                return Err(cancelled_status());
            }
            return Ok(encode_batches(schema, batches));
        }
        let statement = entry.statement.clone();
        let prepared_result = tokio::task::spawn_blocking(move || statement.execute_query())
            .await
            .map_err(|error| Status::internal(format!("Kernel execution task failed: {error}")))?
            .map_err(|error| {
                self.failed_executions.fetch_add(1, Ordering::Relaxed);
                status_from_vql(&error)
            })?;
        match prepared_result {
            PreparedResult::Batches(batches) => {
                if self.finish_starting_execution(&execution_id) {
                    return Err(cancelled_status());
                }
                Ok(encode_batches(schema, batches))
            }
            PreparedResult::Query(handle) => {
                let handle = *handle;
                let stream_handle = handle.clone();
                let input =
                    tokio::task::spawn_blocking(move || stream_handle.stream_materialized_images())
                        .await
                        .map_err(|error| {
                            Status::internal(format!("Kernel stream task failed: {error}"))
                        })?
                        .map_err(|error| {
                            self.failed_executions.fetch_add(1, Ordering::Relaxed);
                            status_from_vql(&error)
                        })?;
                let cancelled = {
                    let mut executions = self
                        .executions
                        .lock()
                        .map_err(|_| Status::internal("Execution registry was poisoned"))?;
                    let cancelled = match executions.get(&execution_id) {
                        Some(ExecutionState::Starting { cancelled, .. }) => *cancelled,
                        _ => {
                            return Err(Status::internal(
                                "execution left its starting state unexpectedly",
                            ));
                        }
                    };
                    if cancelled {
                        executions.remove(&execution_id);
                    } else {
                        executions.insert(
                            execution_id.clone(),
                            ExecutionState::Attached(Box::new(AttachedExecution {
                                session_id: entry.session_id,
                                handle: handle.clone(),
                            })),
                        );
                    }
                    cancelled
                };
                if cancelled {
                    handle.cancel();
                    return Err(cancelled_status());
                }
                let executions = Arc::clone(&self.executions);
                let config = Arc::clone(&self.config);
                let failed = Arc::clone(&self.failed_executions);
                let enforce_total =
                    entry.statement.statement_info().result_mode != ResultMode::Unbounded;
                let result_bytes = Arc::new(AtomicUsize::new(0));
                let stream = async_stream::stream! {
                    let _guard = AttachedGuard {
                        execution_id: execution_id.clone(),
                        handle,
                        executions,
                    };
                    let mut input = input;
                    while let Some(batch) = input.next().await {
                        let batch = batch
                            .map_err(VqlError::from)
                            .and_then(|batch| sanitize_batch(
                                batch,
                                &config,
                                &result_bytes,
                                enforce_total,
                            ));
                        match batch {
                            Ok(batch) => yield Ok::<RecordBatch, FlightError>(batch),
                            Err(error) => {
                                failed.fetch_add(1, Ordering::Relaxed);
                                yield Err(FlightError::from(status_from_vql(&error)));
                                break;
                            }
                        }
                    }
                };
                let encoded = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_max_flight_data_size(self.config.batch_bytes)
                    .build(stream)
                    .map(|result| result.map_err(Status::from));
                Ok(Response::new(Box::pin(encoded)))
            }
        }
    }

    fn cancel_execution(&self, session_id: &str, execution_id: &str) -> Result<bool, Status> {
        self.cleanup_unattached();
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| Status::internal("Execution registry was poisoned"))?;
        let Some(state) = executions.get_mut(execution_id) else {
            return Ok(false);
        };
        if state.session_id() != session_id {
            return Err(Status::permission_denied(
                "execution belongs to another logical Session",
            ));
        }
        match state {
            ExecutionState::Pending(_) => {
                executions.remove(execution_id);
            }
            ExecutionState::Starting { cancelled, .. } => *cancelled = true,
            ExecutionState::Attached(attached) => attached.handle.cancel(),
        }
        Ok(true)
    }

    fn finish_starting_execution(&self, execution_id: &str) -> bool {
        let Ok(mut executions) = self.executions.lock() else {
            return false;
        };
        let cancelled = matches!(
            executions.get(execution_id),
            Some(ExecutionState::Starting {
                cancelled: true,
                ..
            })
        );
        if matches!(
            executions.get(execution_id),
            Some(ExecutionState::Starting { .. })
        ) {
            executions.remove(execution_id);
        }
        cancelled
    }
}

struct AttachedGuard {
    execution_id: String,
    handle: QueryHandle,
    executions: Arc<Mutex<HashMap<String, ExecutionState>>>,
}

impl Drop for AttachedGuard {
    fn drop(&mut self) {
        self.handle.cancel();
        if let Ok(mut executions) = self.executions.lock() {
            executions.remove(&self.execution_id);
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for VqlFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        self.cleanup_expired_sessions();
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Basic "))
            .ok_or_else(|| Status::unauthenticated("Basic authentication is required"))?;
        let decoded = BASE64_STANDARD
            .decode(authorization)
            .map_err(|_| Status::unauthenticated("invalid Basic authentication"))?;
        let decoded = str::from_utf8(&decoded)
            .map_err(|_| Status::unauthenticated("invalid Basic authentication"))?;
        let (_client_name, credential) = decoded
            .split_once(':')
            .ok_or_else(|| Status::unauthenticated("invalid Basic authentication"))?;
        let expected = self.config.service_token.as_deref().unwrap_or_default();
        if credential != expected {
            return Err(Status::unauthenticated("invalid service credential"));
        }
        let session_token = uuid::Uuid::new_v4().to_string();
        let context = AuthContext {
            session_id: session_token.clone(),
            principal: self.config.principal.clone(),
            session: self
                .engine
                .session()
                .for_service()
                .build()
                .map_err(|error| status_from_vql(&error))?,
        };
        self.sessions
            .lock()
            .map_err(|_| Status::internal("Session registry was poisoned"))?
            .insert(
                session_token.clone(),
                SessionEntry {
                    context,
                    last_used: Instant::now(),
                },
            );
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: session_token.clone().into(),
        };
        let mut response = Response::new(Box::pin(stream::once(async move { Ok(response) }))
            as Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>);
        response.metadata_mut().insert(
            "authorization",
            format!("Bearer {session_token}")
                .parse()
                .map_err(|_| Status::internal("failed to encode Session token"))?,
        );
        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.auth(&request)?;
        if query.transaction_id.is_some() {
            return Err(Status::unimplemented("transactions are not supported"));
        }
        let statement = self.prepare_statement(&auth, &query.query).await?;
        let descriptor = request.into_inner();
        self.register_execution(&auth, statement, descriptor)
            .map(Response::new)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.auth(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("prepared statement handle is invalid"))?;
        let entry = self
            .prepared
            .lock()
            .map_err(|_| Status::internal("Prepared statement registry was poisoned"))?
            .get(&handle)
            .cloned()
            .ok_or_else(|| Status::not_found("prepared statement does not exist"))?;
        if entry.session_id != auth.session_id {
            return Err(Status::permission_denied(
                "prepared statement belongs to another logical Session",
            ));
        }
        self.register_execution(&auth, entry.statement, request.into_inner())
            .map(Response::new)
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let auth = self.auth(&request)?;
        let execution_id = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("execution ticket is invalid"))?;
        let entry = {
            let mut executions = self
                .executions
                .lock()
                .map_err(|_| Status::internal("Execution registry was poisoned"))?;
            let state = executions
                .get_mut(&execution_id)
                .ok_or_else(|| Status::not_found("execution does not exist or already attached"))?;
            if state.session_id() != auth.session_id {
                return Err(Status::permission_denied(
                    "execution belongs to another logical Session",
                ));
            }
            let ExecutionState::Pending(entry) = state else {
                return Err(Status::not_found(
                    "execution does not exist or already attached",
                ));
            };
            if entry.created_at.elapsed() > self.config.attach_timeout() {
                executions.remove(&execution_id);
                return Err(Status::not_found(
                    "execution does not exist or its attach interval expired",
                ));
            }
            let session_id = entry.session_id.clone();
            let ExecutionState::Pending(entry) = std::mem::replace(
                state,
                ExecutionState::Starting {
                    session_id,
                    cancelled: false,
                },
            ) else {
                unreachable!("pending execution was checked while holding registry lock")
            };
            *entry
        };
        let result = self.execute_entry(execution_id.clone(), entry).await;
        if result.is_err()
            && let Ok(mut executions) = self.executions.lock()
            && matches!(
                executions.get(&execution_id),
                Some(ExecutionState::Starting { .. })
            )
        {
            executions.remove(&execution_id);
        }
        result
    }

    async fn do_put_statement_update(
        &self,
        query: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let auth = self.auth(&request)?;
        if query.transaction_id.is_some() {
            return Err(Status::unimplemented("transactions are not supported"));
        }
        let statement = self.prepare_statement(&auth, &query.query).await?;
        if statement.statement_info().kind != StatementKind::Update {
            return Err(Status::invalid_argument(
                "query-shaped statements must be executed with Flight SQL Execute",
            ));
        }
        tokio::task::spawn_blocking(move || statement.execute_update())
            .await
            .map_err(|error| Status::internal(format!("Kernel update task failed: {error}")))?
            .map_err(|error| status_from_vql(&error))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let auth = self.auth(&request)?;
        if query.transaction_id.is_some() {
            return Err(Status::unimplemented("transactions are not supported"));
        }
        let statement = self.prepare_statement(&auth, &query.query).await?;
        let handle = uuid::Uuid::new_v4().to_string();
        self.prepared
            .lock()
            .map_err(|_| Status::internal("Prepared statement registry was poisoned"))?
            .insert(
                handle.clone(),
                PreparedEntry {
                    session_id: auth.session_id,
                    statement: statement.clone(),
                },
            );
        let message: IpcMessage = SchemaAsIpc::new(
            statement.result_schema().as_ref(),
            &IpcWriteOptions::default(),
        )
        .try_into()
        .map_err(|error: arrow::error::ArrowError| Status::internal(error.to_string()))?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: message.0,
            parameter_schema: Bytes::new(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        let auth = self.auth(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("prepared statement handle is invalid"))?;
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| Status::internal("Prepared statement registry was poisoned"))?;
        if let Some(entry) = prepared.get(&handle)
            && entry.session_id != auth.session_id
        {
            return Err(Status::permission_denied(
                "prepared statement belongs to another logical Session",
            ));
        }
        prepared.remove(&handle);
        Ok(())
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let auth = self.auth(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("prepared statement handle is invalid"))?;
        let entry = self
            .prepared
            .lock()
            .map_err(|_| Status::internal("Prepared statement registry was poisoned"))?
            .get(&handle)
            .cloned()
            .ok_or_else(|| Status::not_found("prepared statement does not exist"))?;
        if entry.session_id != auth.session_id {
            return Err(Status::permission_denied(
                "prepared statement belongs to another logical Session",
            ));
        }
        tokio::task::spawn_blocking(move || entry.statement.execute_update())
            .await
            .map_err(|error| Status::internal(format!("Kernel update task failed: {error}")))?
            .map_err(|error| status_from_vql(&error))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "prepared statement parameters are not supported",
        ))
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        let auth = self.auth(&request)?;
        let info = FlightInfo::decode(&*query.info)
            .map_err(|_| Status::invalid_argument("cancel request FlightInfo is invalid"))?;
        let metadata: serde_json::Value = serde_json::from_slice(&info.app_metadata)
            .map_err(|_| Status::invalid_argument("cancel request has no execution ID"))?;
        let execution_id = metadata
            .get("execution_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Status::invalid_argument("cancel request has no execution ID"))?;
        if !self.cancel_execution(&auth.session_id, execution_id)? {
            return Err(Status::not_found("execution does not exist"));
        }
        Ok(ActionCancelQueryResult {
            // `CANCELLED` in the Flight SQL ActionCancelQueryResult enum.
            result: 1,
        })
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        if request.get_ref().r#type != "CancelFlightInfo" {
            return Err(Status::unimplemented("Flight action is not supported"));
        }
        let auth = self.auth(&request)?;
        let cancel = CancelFlightInfoRequest::decode(&*request.get_ref().body)
            .map_err(|_| Status::invalid_argument("CancelFlightInfo request is invalid"))?;
        let info = cancel
            .info
            .ok_or_else(|| Status::invalid_argument("CancelFlightInfo has no FlightInfo"))?;
        let execution_id = execution_id_from_info(&info)?;
        if !self.cancel_execution(&auth.session_id, &execution_id)? {
            return Err(Status::not_found("execution does not exist"));
        }
        let result = CancelFlightInfoResult::new(CancelStatus::Cancelled);
        let output = stream::once(async move {
            Ok(FlightActionResult {
                body: result.encode_to_vec().into(),
            })
        });
        Ok(Response::new(Box::pin(output)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

fn execution_id_from_info(info: &FlightInfo) -> Result<String, Status> {
    let metadata: serde_json::Value = serde_json::from_slice(&info.app_metadata)
        .map_err(|_| Status::invalid_argument("FlightInfo has no execution ID"))?;
    metadata
        .get("execution_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Status::invalid_argument("FlightInfo has no execution ID"))
}

fn encode_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Response<<VqlFlightSqlService as FlightService>::DoGetStream> {
    let input = stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>));
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input)
        .map(|result| result.map_err(Status::from));
    Response::new(Box::pin(stream))
}

fn status_from_vql(error: &VqlError) -> Status {
    let code = match error.code {
        ErrorCode::FeatureNotAvailable => tonic::Code::Unimplemented,
        ErrorCode::NotFound => tonic::Code::NotFound,
        ErrorCode::InvalidArgument
        | ErrorCode::InvalidSql
        | ErrorCode::InvalidOption
        | ErrorCode::InvalidLocation => tonic::Code::InvalidArgument,
        ErrorCode::AlreadyExists | ErrorCode::NameConflict => tonic::Code::AlreadyExists,
        ErrorCode::FailedPrecondition | ErrorCode::PythonHostRequired => {
            tonic::Code::FailedPrecondition
        }
        ErrorCode::ResourceExhausted => tonic::Code::ResourceExhausted,
        ErrorCode::QueryCancelled => tonic::Code::Cancelled,
        ErrorCode::Catalog | ErrorCode::Execution => tonic::Code::Unavailable,
        ErrorCode::Internal => tonic::Code::Internal,
        _ => tonic::Code::Unknown,
    };
    let details = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "code": error.code.as_str(),
        "symbol": error.code.symbol(),
        "message": error.message,
        "target_version": error.target_version,
    }))
    .unwrap_or_default();
    Status::with_details(code, error.to_string(), details.into())
}

fn cancelled_status() -> Status {
    status_from_vql(&VqlError::new(ErrorCode::QueryCancelled, "query cancelled"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tempfile::tempdir;
    use vql_kernel::{Engine, EngineConfig};

    #[test]
    fn structured_status_preserves_vql_identity() {
        let status = status_from_vql(&VqlError::new(ErrorCode::InvalidSql, "bad SQL"));
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let details: serde_json::Value = serde_json::from_slice(status.details()).unwrap();
        assert_eq!(details["code"], "VQL-42001");
        assert_eq!(details["symbol"], "INVALID_SQL");
    }

    #[test]
    fn expired_session_removes_prepared_and_pending_execution_state() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let controller = Arc::new(QueryController::new(engine.clone(), 100, 30));
        let config = Arc::new(ServiceConfig {
            session_idle_timeout_seconds: 1,
            ..ServiceConfig::default()
        });
        let service = VqlFlightSqlService::new(
            engine,
            controller,
            Arc::clone(&config),
            Arc::new(AtomicBool::new(true)),
        );
        let auth = service.direct_session("expired").unwrap();
        let statement = auth
            .session
            .prepare(
                "SELECT 1",
                &auth.principal,
                auth.session.semantic_settings(),
            )
            .unwrap();
        service.prepared.lock().unwrap().insert(
            "prepared".to_owned(),
            PreparedEntry {
                session_id: auth.session_id.clone(),
                statement: statement.clone(),
            },
        );
        service.executions.lock().unwrap().insert(
            "execution".to_owned(),
            ExecutionState::Pending(Box::new(ExecutionEntry {
                session_id: auth.session_id,
                statement,
                created_at: Instant::now(),
            })),
        );
        service
            .sessions
            .lock()
            .unwrap()
            .get_mut("expired")
            .unwrap()
            .last_used = Instant::now() - config.session_idle_timeout() - Duration::from_secs(1);

        service.cleanup_expired_sessions();

        assert_eq!(service.session_count(), 0);
        assert!(service.prepared.lock().unwrap().is_empty());
        assert!(service.executions.lock().unwrap().is_empty());
    }

    #[test]
    fn active_attached_execution_keeps_its_session_alive() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let controller = Arc::new(QueryController::new(engine.clone(), 100, 30));
        let config = Arc::new(ServiceConfig {
            session_idle_timeout_seconds: 1,
            ..ServiceConfig::default()
        });
        let service = VqlFlightSqlService::new(
            engine,
            controller,
            Arc::clone(&config),
            Arc::new(AtomicBool::new(true)),
        );
        let auth = service.direct_session("active").unwrap();
        let prepared = auth
            .session
            .prepare(
                "SELECT 1",
                &auth.principal,
                auth.session.semantic_settings(),
            )
            .unwrap();
        let PreparedResult::Query(handle) = prepared.execute_query().unwrap() else {
            panic!("SELECT must produce a QueryHandle")
        };
        service.executions.lock().unwrap().insert(
            "execution".to_owned(),
            ExecutionState::Attached(Box::new(AttachedExecution {
                session_id: auth.session_id,
                handle: *handle,
            })),
        );
        service
            .sessions
            .lock()
            .unwrap()
            .get_mut("active")
            .unwrap()
            .last_used = Instant::now() - config.session_idle_timeout() - Duration::from_secs(1);

        service.cleanup_expired_sessions();
        assert_eq!(service.session_count(), 1);

        service.executions.lock().unwrap().clear();
        service.cleanup_expired_sessions();
        assert_eq!(service.session_count(), 0);
    }

    #[test]
    fn cancellation_is_recorded_while_execution_is_starting() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let controller = Arc::new(QueryController::new(engine.clone(), 100, 30));
        let service = VqlFlightSqlService::new(
            engine,
            controller,
            Arc::new(ServiceConfig::default()),
            Arc::new(AtomicBool::new(true)),
        );
        service.executions.lock().unwrap().insert(
            "execution".to_owned(),
            ExecutionState::Starting {
                session_id: "session".to_owned(),
                cancelled: false,
            },
        );

        assert!(service.cancel_execution("session", "execution").unwrap());
        assert!(matches!(
            service.executions.lock().unwrap().get("execution"),
            Some(ExecutionState::Starting {
                cancelled: true,
                ..
            })
        ));
    }
}
