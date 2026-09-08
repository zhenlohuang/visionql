pub mod config;
mod error;
mod flight;

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow_flight::FlightInfo;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::header::{CONTENT_TYPE, COOKIE, HOST, ORIGIN, SET_COOKIE};
use axum::http::uri::Authority;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::config::WorkbenchConfig;
use crate::error::ApiProblem;
use crate::flight::{FlightClient, ResultMode, StatementInfo, StatementKind};

const SESSION_COOKIE: &str = "vql_workbench_session";

#[derive(Clone)]
pub struct AppState {
    sessions: Arc<RwLock<HashMap<String, Arc<BrowserSession>>>>,
    defaults: Arc<SessionDefaults>,
    session_idle_timeout: Duration,
    listen_addr: std::net::SocketAddr,
}

#[derive(Debug)]
struct SessionDefaults {
    endpoint: String,
    credential: Option<String>,
    tls_ca_pem: Option<Vec<u8>>,
}

struct BrowserSession {
    id: String,
    endpoint: String,
    client: FlightClient,
    active: Mutex<Option<ActiveExecution>>,
    last_outcome: Mutex<Option<ExecutionOutcome>>,
    last_used: Mutex<Instant>,
    start_lock: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
struct ActiveExecution {
    execution_id: String,
    info: FlightInfo,
    schema: SchemaRef,
    result_mode: ResultMode,
    started_at: Instant,
    attached: bool,
    cancelling: bool,
}

#[derive(Debug, Clone)]
struct ExecutionOutcome {
    execution_id: String,
    status: ExecutionStatus,
    elapsed_ms: u64,
    problem: Option<ApiProblem>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionStatus {
    Running,
    Cancelling,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateSessionRequest {
    endpoint: Option<String>,
    credential: Option<String>,
    tls_ca_pem: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionResponse {
    connected: bool,
    endpoint: String,
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartExecutionRequest {
    sql: String,
    #[serde(default)]
    allow_unbounded: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StartExecutionResponse {
    kind: &'static str,
    result_mode: &'static str,
    execution_id: Option<String>,
    affected_rows: Option<i64>,
    elapsed_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionStatusResponse {
    execution_id: String,
    status: ExecutionStatus,
    result_mode: Option<&'static str>,
    elapsed_ms: u64,
    problem: Option<ApiProblem>,
}

impl BrowserSession {
    fn touch(&self) {
        if let Ok(mut last_used) = self.last_used.lock() {
            *last_used = Instant::now();
        }
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        let attached = self
            .active
            .lock()
            .map(|active| active.as_ref().is_some_and(|execution| execution.attached))
            .unwrap_or(true);
        if attached {
            return false;
        }
        self.last_used
            .lock()
            .map(|last_used| last_used.elapsed() > timeout)
            .unwrap_or(false)
    }

    fn status(&self, execution_id: &str) -> Result<ExecutionStatusResponse, ApiProblem> {
        if let Ok(active) = self.active.lock()
            && let Some(active) = active.as_ref()
            && active.execution_id == execution_id
        {
            return Ok(ExecutionStatusResponse {
                execution_id: execution_id.to_owned(),
                status: if active.cancelling {
                    ExecutionStatus::Cancelling
                } else {
                    ExecutionStatus::Running
                },
                result_mode: Some(active.result_mode.as_str()),
                elapsed_ms: millis(active.started_at.elapsed()),
                problem: None,
            });
        }
        if let Ok(last) = self.last_outcome.lock()
            && let Some(last) = last.as_ref()
            && last.execution_id == execution_id
        {
            return Ok(ExecutionStatusResponse {
                execution_id: execution_id.to_owned(),
                status: last.status,
                result_mode: None,
                elapsed_ms: last.elapsed_ms,
                problem: last.problem.clone(),
            });
        }
        Err(ApiProblem::not_found(
            "execution does not exist in this Session",
        ))
    }

    fn finish(&self, execution_id: &str, problem: Option<ApiProblem>) {
        let active = self.active.lock().ok().and_then(|mut active| {
            if active
                .as_ref()
                .is_some_and(|active| active.execution_id == execution_id)
            {
                active.take()
            } else {
                None
            }
        });
        let Some(active) = active else {
            return;
        };
        let status = match problem.as_ref() {
            None => ExecutionStatus::Completed,
            Some(problem) if problem.is_cancelled() || active.cancelling => {
                ExecutionStatus::Cancelled
            }
            Some(_) => ExecutionStatus::Failed,
        };
        if let Ok(mut last) = self.last_outcome.lock() {
            *last = Some(ExecutionOutcome {
                execution_id: execution_id.to_owned(),
                status,
                elapsed_ms: millis(active.started_at.elapsed()),
                problem,
            });
        }
        self.touch();
    }

    fn mark_cancelling(&self, execution_id: &str) {
        if let Ok(mut active) = self.active.lock()
            && let Some(active) = active.as_mut()
            && active.execution_id == execution_id
        {
            active.cancelling = true;
        }
    }
}

impl AppState {
    pub fn from_config(config: &WorkbenchConfig) -> Result<Self, String> {
        let tls_ca_pem = config
            .tls_ca
            .as_ref()
            .map(std::fs::read)
            .transpose()
            .map_err(|error| format!("failed to read default vqld TLS CA: {error}"))?;
        Ok(Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            defaults: Arc::new(SessionDefaults {
                endpoint: config.default_vqld_endpoint.clone(),
                credential: config.service_token.clone(),
                tls_ca_pem,
            }),
            session_idle_timeout: config.session_idle_timeout(),
            listen_addr: config.listen_addr,
        })
    }

    fn cleanup(&self) {
        let mut expired = Vec::new();
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.retain(|_, session| {
                if session.is_expired(self.session_idle_timeout) {
                    expired.push(Arc::clone(session));
                    false
                } else {
                    true
                }
            });
        }
        for session in expired {
            tokio::spawn(async move {
                cancel_active(&session).await;
            });
        }
    }

    fn session(&self, headers: &HeaderMap) -> Result<Arc<BrowserSession>, ApiProblem> {
        self.cleanup();
        let id = cookie_value(headers, SESSION_COOKIE).ok_or_else(|| {
            let mut problem = ApiProblem::connectivity(
                "Workbench is disconnected",
                "Connect to a vqld endpoint before running SQL.",
            );
            problem.status = StatusCode::UNAUTHORIZED;
            problem
        })?;
        let session = self
            .sessions
            .read()
            .map_err(|_| ApiProblem::backend("Session unavailable", "Session state was poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| {
                let mut problem = ApiProblem::connectivity(
                    "Workbench Session expired",
                    "Reconnect to vqld to continue.",
                );
                problem.status = StatusCode::UNAUTHORIZED;
                problem
            })?;
        session.touch();
        Ok(session)
    }
}

pub async fn run(config: WorkbenchConfig) -> Result<(), String> {
    config.validate()?;
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|error| format!("failed to bind Workbench listener: {error}"))?;
    let state = AppState::from_config(&config)?;
    let app = router(state.clone(), config.static_dir.clone());
    let reaper = tokio::spawn(reap_sessions(state));
    info!(address = %config.listen_addr, "VisionQL Workbench is ready");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("Workbench HTTP server failed: {error}"));
    reaper.abort();
    result
}

pub fn router(state: AppState, static_dir: PathBuf) -> Router {
    let index = static_dir.join("index.html");
    let static_files = ServeDir::new(static_dir).not_found_service(ServeFile::new(index));
    Router::new()
        .route(
            "/api/session",
            get(get_session).post(create_session).delete(close_session),
        )
        .route("/api/executions", post(start_execution))
        .route(
            "/api/executions/{execution_id}",
            get(execution_status).delete(cancel_execution),
        )
        .route(
            "/api/executions/{execution_id}/results",
            get(stream_results),
        )
        .fallback_service(static_files)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_same_origin,
        ))
        .with_state(state)
}

async fn enforce_same_origin(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiProblem> {
    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| allowed_authority(value, state.listen_addr))
        .ok_or_else(|| {
            ApiProblem::forbidden(
                "The HTTP Host must be localhost or a numeric loopback address on the configured Workbench port.",
            )
        })?;
    if is_mutating(request.method()) {
        let origin = request
            .headers()
            .get(ORIGIN)
            .and_then(|value| value.to_str().ok())
            .filter(|value| allowed_origin(value, state.listen_addr, host))
            .ok_or_else(|| {
                ApiProblem::forbidden(
                    "State-changing Workbench requests require the same loopback Origin as the request Host.",
                )
            })?;
        let _ = origin;
    }
    Ok(next.run(request).await)
}

fn is_mutating(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn allowed_authority(value: &str, listen_addr: std::net::SocketAddr) -> bool {
    let Ok(authority) = value.parse::<Authority>() else {
        return false;
    };
    let port = authority.port_u16().unwrap_or(80);
    port == listen_addr.port() && allowed_loopback_host(authority.host())
}

fn allowed_origin(value: &str, listen_addr: std::net::SocketAddr, host: &str) -> bool {
    let Ok(origin) = url::Url::parse(value) else {
        return false;
    };
    if origin.scheme() != "http"
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
        || origin.port_or_known_default() != Some(listen_addr.port())
        || !origin.host_str().is_some_and(allowed_loopback_host)
    {
        return false;
    }
    let Some(origin_host) = origin.host_str() else {
        return false;
    };
    let Ok(authority) = host.parse::<Authority>() else {
        return false;
    };
    origin_host.eq_ignore_ascii_case(authority.host())
}

fn allowed_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

async fn reap_sessions(state: AppState) {
    let period = Duration::from_secs(state.session_idle_timeout.as_secs().clamp(1, 30));
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        state.cleanup();
    }
}

async fn get_session(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    match state.session(&headers) {
        Ok(session) => Json(SessionResponse {
            connected: true,
            endpoint: session.endpoint.clone(),
            session_id: Some(session.id.clone()),
        })
        .into_response(),
        Err(_) => Json(SessionResponse {
            connected: false,
            endpoint: state.defaults.endpoint.clone(),
            session_id: None,
        })
        .into_response(),
    }
}

async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateSessionRequest>,
) -> Result<Response, ApiProblem> {
    if let Some(existing_id) = cookie_value(&headers, SESSION_COOKIE) {
        let existing = state
            .sessions
            .write()
            .ok()
            .and_then(|mut sessions| sessions.remove(existing_id));
        if let Some(existing) = existing {
            cancel_active(&existing).await;
        }
    }
    let endpoint = request
        .endpoint
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.defaults.endpoint.clone());
    let credential = request
        .credential
        .filter(|value| !value.is_empty())
        .or_else(|| state.defaults.credential.clone())
        .unwrap_or_default();
    let tls_ca_pem = request
        .tls_ca_pem
        .filter(|value| !value.trim().is_empty())
        .map(String::into_bytes)
        .or_else(|| state.defaults.tls_ca_pem.clone());
    let client = flight::connect(&endpoint, &credential, tls_ca_pem).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let session = Arc::new(BrowserSession {
        id: id.clone(),
        endpoint: endpoint.clone(),
        client,
        active: Mutex::new(None),
        last_outcome: Mutex::new(None),
        last_used: Mutex::new(Instant::now()),
        start_lock: tokio::sync::Mutex::new(()),
    });
    state
        .sessions
        .write()
        .map_err(|_| ApiProblem::backend("Session unavailable", "Session state was poisoned"))?
        .insert(id.clone(), session);
    let max_age = state.session_idle_timeout.as_secs();
    let cookie =
        format!("{SESSION_COOKIE}={id}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}");
    let mut response = Json(SessionResponse {
        connected: true,
        endpoint,
        session_id: Some(id),
    })
    .into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&cookie)
            .map_err(|_| ApiProblem::backend("Session failed", "Session cookie was invalid"))?,
    );
    Ok(response)
}

async fn close_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiProblem> {
    if let Some(id) = cookie_value(&headers, SESSION_COOKIE) {
        let session = state
            .sessions
            .write()
            .map_err(|_| ApiProblem::backend("Session unavailable", "Session state was poisoned"))?
            .remove(id);
        if let Some(session) = session {
            cancel_active(&session).await;
        }
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_static(
            "vql_workbench_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0",
        ),
    );
    Ok(response)
}

async fn start_execution(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StartExecutionRequest>,
) -> Result<Response, ApiProblem> {
    if request.sql.trim().is_empty() {
        return Err(ApiProblem::invalid("SQL cannot be empty"));
    }
    let session = state.session(&headers)?;
    let _start = session.start_lock.lock().await;
    if session
        .active
        .lock()
        .map_err(|_| ApiProblem::backend("Execution unavailable", "Execution state was poisoned"))?
        .is_some()
    {
        return Err(ApiProblem::conflict(
            "Cancel or wait for the active execution before starting another statement.",
        ));
    }
    let started_at = Instant::now();
    let mut client = session.client.clone();
    let mut prepared = client
        .prepare(request.sql, None)
        .await
        .map_err(|error| ApiProblem::from_flight(&error))?;
    let schema = Arc::new(
        prepared
            .dataset_schema()
            .map_err(|error| ApiProblem::from_flight(&error))?
            .clone(),
    );
    let statement_info = StatementInfo::from_schema(schema.as_ref())?;
    if statement_info.result_mode == ResultMode::Unbounded && !request.allow_unbounded {
        let _ = prepared.close().await;
        return Err(ApiProblem::policy(
            "This statement has an unbounded result. Use Run as stream to open an attached rolling preview.",
        ));
    }
    if statement_info.kind == StatementKind::Update {
        let affected_rows = prepared
            .execute_update()
            .await
            .map_err(|error| ApiProblem::from_flight(&error))?;
        prepared
            .close()
            .await
            .map_err(|error| ApiProblem::from_flight(&error))?;
        return Ok(Json(StartExecutionResponse {
            kind: "update",
            result_mode: "none",
            execution_id: None,
            affected_rows: Some(affected_rows),
            elapsed_ms: Some(millis(started_at.elapsed())),
        })
        .into_response());
    }
    let info = prepared
        .execute()
        .await
        .map_err(|error| ApiProblem::from_flight(&error))?;
    let execution_id = flight::execution_id(&info)?;
    if let Err(error) = prepared.close().await {
        let mut cancel_client = session.client.clone();
        let _ = flight::cancel(&mut cancel_client, info).await;
        return Err(ApiProblem::from_flight(&error));
    }
    *session.active.lock().map_err(|_| {
        ApiProblem::backend("Execution unavailable", "Execution state was poisoned")
    })? = Some(ActiveExecution {
        execution_id: execution_id.clone(),
        info,
        schema,
        result_mode: statement_info.result_mode,
        started_at,
        attached: false,
        cancelling: false,
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(StartExecutionResponse {
            kind: "query",
            result_mode: statement_info.result_mode.as_str(),
            execution_id: Some(execution_id),
            affected_rows: None,
            elapsed_ms: None,
        }),
    )
        .into_response())
}

async fn stream_results(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(execution_id): Path<String>,
) -> Result<Response, ApiProblem> {
    let session = state.session(&headers)?;
    let (active, ticket) = {
        let mut current = session.active.lock().map_err(|_| {
            ApiProblem::backend("Execution unavailable", "Execution state was poisoned")
        })?;
        let active = current
            .as_mut()
            .ok_or_else(|| ApiProblem::not_found("there is no active execution in this Session"))?;
        if active.execution_id != execution_id {
            return Err(ApiProblem::not_found(
                "execution does not belong to this browser Session",
            ));
        }
        if active.attached {
            return Err(ApiProblem::conflict(
                "the active result stream is already attached",
            ));
        }
        let ticket = flight::only_ticket(&active.info)?;
        active.attached = true;
        (active.clone(), ticket)
    };
    let flight::IpcBridge {
        batch_tx,
        byte_rx,
        encoder,
    } = flight::spawn_ipc_bridge(active.schema);
    let (body_signal_tx, body_signal_rx) = oneshot::channel();
    let worker_session = Arc::clone(&session);
    let worker_execution_id = execution_id.clone();
    tokio::spawn(async move {
        let mut copy = tokio::spawn(flight::copy_flight_to_ipc(
            worker_session.client.clone(),
            ticket,
            batch_tx,
        ));
        let copy_result = tokio::select! {
            result = &mut copy => joined_copy_result(result),
            signal = body_signal_rx => {
                if matches!(signal, Ok(BodyTermination::Dropped)) {
                    worker_session.mark_cancelling(&worker_execution_id);
                    copy.abort();
                    let mut client = worker_session.client.clone();
                    let problem = flight::cancel(&mut client, active.info.clone())
                        .await
                        .err()
                        .unwrap_or_else(|| {
                            ApiProblem::backend(
                                "Browser result stream closed",
                                "The browser stopped consuming the active Arrow result stream.",
                            )
                        });
                    let _ = copy.await;
                    let _ = encoder.await;
                    worker_session.finish(&worker_execution_id, Some(problem));
                    return;
                }
                joined_copy_result(copy.await)
            }
        };
        let encode_result = encoder.await;
        let mut problem = copy_result.err();
        if problem.is_none() && !matches!(encode_result, Ok(Ok(()))) {
            problem = Some(ApiProblem::backend(
                "Arrow result stream closed",
                "The browser stopped consuming the Arrow result stream.",
            ));
        }
        if problem
            .as_ref()
            .is_some_and(|problem| matches!(problem.source, crate::error::ProblemSource::Backend))
        {
            let mut client = worker_session.client.clone();
            let _ = flight::cancel(&mut client, active.info).await;
        }
        worker_session.finish(&worker_execution_id, problem);
    });
    let body = Body::from_stream(BrowserBodyStream::new(
        ReceiverStream::new(byte_rx),
        body_signal_tx,
    ));
    let mut response = Response::new(body);
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    response.headers_mut().insert(
        "x-vql-execution-id",
        HeaderValue::from_str(&execution_id).map_err(|_| {
            ApiProblem::backend(
                "Execution unavailable",
                "Execution ID was not a valid header",
            )
        })?,
    );
    Ok(response)
}

async fn execution_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(execution_id): Path<String>,
) -> Result<Json<ExecutionStatusResponse>, ApiProblem> {
    let session = state.session(&headers)?;
    session.status(&execution_id).map(Json)
}

async fn cancel_execution(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(execution_id): Path<String>,
) -> Result<Response, ApiProblem> {
    let session = state.session(&headers)?;
    let (info, attached, started_at) = {
        let mut active = session.active.lock().map_err(|_| {
            ApiProblem::backend("Execution unavailable", "Execution state was poisoned")
        })?;
        let active = active
            .as_mut()
            .filter(|active| active.execution_id == execution_id)
            .ok_or_else(|| ApiProblem::not_found("execution is not active in this Session"))?;
        active.cancelling = true;
        (active.info.clone(), active.attached, active.started_at)
    };
    let mut client = session.client.clone();
    if let Err(problem) = flight::cancel(&mut client, info).await {
        if let Ok(mut active) = session.active.lock()
            && let Some(active) = active.as_mut()
            && active.execution_id == execution_id
        {
            active.cancelling = false;
        }
        return Err(problem);
    }
    if !attached {
        if let Ok(mut active) = session.active.lock() {
            *active = None;
        }
        if let Ok(mut last) = session.last_outcome.lock() {
            *last = Some(ExecutionOutcome {
                execution_id: execution_id.clone(),
                status: ExecutionStatus::Cancelled,
                elapsed_ms: millis(started_at.elapsed()),
                problem: Some(ApiProblem::cancelled()),
            });
        }
    }
    Ok((StatusCode::ACCEPTED, Json(session.status(&execution_id)?)).into_response())
}

async fn cancel_active(session: &BrowserSession) {
    let active = session
        .active
        .lock()
        .ok()
        .and_then(|mut active| active.take());
    if let Some(active) = active {
        let mut client = session.client.clone();
        let _ = flight::cancel(&mut client, active.info).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyTermination {
    Completed,
    Dropped,
}

struct BrowserBodyStream<S> {
    inner: S,
    signal: Option<oneshot::Sender<BodyTermination>>,
}

impl<S> BrowserBodyStream<S> {
    fn new(inner: S, signal: oneshot::Sender<BodyTermination>) -> Self {
        Self {
            inner,
            signal: Some(signal),
        }
    }

    fn signal(&mut self, termination: BodyTermination) {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(termination);
        }
    }
}

impl<S: Stream + Unpin> Stream for BrowserBodyStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            self.signal(BodyTermination::Completed);
        }
        result
    }
}

impl<S> Drop for BrowserBodyStream<S> {
    fn drop(&mut self) {
        self.signal(BodyTermination::Dropped);
    }
}

fn joined_copy_result(
    result: Result<Result<(), ApiProblem>, tokio::task::JoinError>,
) -> Result<(), ApiProblem> {
    result.unwrap_or_else(|error| {
        Err(ApiProblem::backend(
            "Arrow result bridge failed",
            format!("The result bridge task stopped unexpectedly: {error}"),
        ))
    })
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (key, value) = cookie.trim().split_once('=')?;
                (key == name).then_some(value)
            })
        })
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use axum::body::Body;
    use axum::http::Request;
    use futures::StreamExt;
    use tower::ServiceExt;

    use super::*;

    fn test_router() -> Router {
        let config = WorkbenchConfig::default();
        router(
            AppState::from_config(&config).expect("test AppState"),
            PathBuf::from("frontend/dist"),
        )
    }

    fn test_session(attached: bool, last_used: Instant) -> Arc<BrowserSession> {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        Arc::new(BrowserSession {
            id: "session-1".to_owned(),
            endpoint: "http://127.0.0.1:9".to_owned(),
            client: FlightSqlServiceClient::new(channel),
            active: Mutex::new(Some(ActiveExecution {
                execution_id: "execution-1".to_owned(),
                info: FlightInfo::default(),
                schema: Arc::new(Schema::empty()),
                result_mode: ResultMode::Unbounded,
                started_at: Instant::now(),
                attached,
                cancelling: false,
            })),
            last_outcome: Mutex::new(None),
            last_used: Mutex::new(last_used),
            start_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[tokio::test]
    async fn rejects_dns_rebinding_host() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/api/session")
                    .header(HOST, "attacker.example:6040")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn mutating_routes_require_same_origin() {
        for origin in [None, Some("http://attacker.example:6040")] {
            let mut request = Request::builder()
                .method(Method::DELETE)
                .uri("/api/session")
                .header(HOST, "127.0.0.1:6040");
            if let Some(origin) = origin {
                request = request.header(ORIGIN, origin);
            }
            let response = test_router()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        let response = test_router()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/api/session")
                    .header(HOST, "127.0.0.1:6040")
                    .header(ORIGIN, "http://127.0.0.1:6040")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn response_body_reports_disconnect_before_another_batch() {
        let (batch_tx, batch_rx) =
            tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
        let (signal_tx, signal_rx) = oneshot::channel();
        let stream = BrowserBodyStream::new(ReceiverStream::new(batch_rx), signal_tx);

        drop(stream);
        assert_eq!(signal_rx.await.unwrap(), BodyTermination::Dropped);
        assert!(batch_tx.send(Ok(bytes::Bytes::new())).await.is_err());
    }

    #[tokio::test]
    async fn response_body_distinguishes_normal_completion() {
        let (batch_tx, batch_rx) =
            tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
        let (signal_tx, signal_rx) = oneshot::channel();
        let mut stream = BrowserBodyStream::new(ReceiverStream::new(batch_rx), signal_tx);

        drop(batch_tx);
        assert!(stream.next().await.is_none());
        assert_eq!(signal_rx.await.unwrap(), BodyTermination::Completed);
    }

    #[tokio::test]
    async fn cleanup_reaps_expired_unattached_executions_only() {
        let config = WorkbenchConfig {
            session_idle_timeout_seconds: 1,
            ..WorkbenchConfig::default()
        };
        let state = AppState::from_config(&config).unwrap();
        let expired = test_session(false, Instant::now() - Duration::from_secs(2));
        let attached = test_session(true, Instant::now() - Duration::from_secs(2));
        state.sessions.write().unwrap().extend([
            ("expired".to_owned(), expired),
            ("attached".to_owned(), attached),
        ]);

        state.cleanup();

        let sessions = state.sessions.read().unwrap();
        assert!(!sessions.contains_key("expired"));
        assert!(sessions.contains_key("attached"));
    }

    #[test]
    fn accepts_ipv4_ipv6_and_localhost_loopback_authorities() {
        let address = "127.0.0.1:6040".parse().unwrap();
        for authority in ["127.0.0.1:6040", "[::1]:6040", "localhost:6040"] {
            assert!(allowed_authority(authority, address), "{authority}");
        }
        assert!(allowed_origin("http://[::1]:6040", address, "[::1]:6040"));
    }
}
