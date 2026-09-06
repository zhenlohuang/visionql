use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::{Action, CancelFlightInfoRequest, CancelFlightInfoResult, FlightInfo, Ticket};
use futures::StreamExt;
use prost::Message;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use vql_kernel::{ErrorCode, QueryInterruptAction, Result, Session, VqlError};

const STATEMENT_INFO_VERSION: &str = "vql.statement_info.version";
const STATEMENT_INFO_KIND: &str = "vql.statement_info.kind";
const STATEMENT_INFO_RESULT_MODE: &str = "vql.statement_info.result_mode";

pub(crate) enum ExecutionOutput {
    Batches(Vec<RecordBatch>),
    Update { affected_rows: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptAction {
    NoActiveQuery,
    GracefulStopRequested,
    CancellationRequested,
}

pub(crate) trait ShellBackend: Send + Sync {
    fn description(&self) -> String;

    fn execute(&self, sql: &str, emit: &mut dyn FnMut(ExecutionOutput) -> Result<()>)
    -> Result<()>;

    fn interrupt_active_query(&self) -> InterruptAction;
}

#[derive(Debug, Clone)]
pub(crate) struct EmbeddedBackend {
    session: Session,
}

impl EmbeddedBackend {
    pub(crate) fn new(session: Session) -> Self {
        Self { session }
    }
}

impl ShellBackend for EmbeddedBackend {
    fn description(&self) -> String {
        "embedded".to_owned()
    }

    fn execute(
        &self,
        sql: &str,
        emit: &mut dyn FnMut(ExecutionOutput) -> Result<()>,
    ) -> Result<()> {
        let statement = self.session.sql(sql)?;
        if statement.is_unbounded() {
            statement.for_each_batch(|batch| emit(ExecutionOutput::Batches(vec![batch.clone()])))
        } else {
            emit(ExecutionOutput::Batches(statement.collect()?))
        }
    }

    fn interrupt_active_query(&self) -> InterruptAction {
        match self.session.interrupt_active_query() {
            QueryInterruptAction::NoActiveQuery => InterruptAction::NoActiveQuery,
            QueryInterruptAction::GracefulStopRequested => InterruptAction::GracefulStopRequested,
            QueryInterruptAction::ImmediateCancellationRequested => {
                InterruptAction::CancellationRequested
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveFlightExecution {
    info: FlightInfo,
}

#[derive(Debug)]
pub(crate) struct FlightBackend {
    endpoint: String,
    runtime: tokio::runtime::Runtime,
    client: FlightSqlServiceClient<Channel>,
    active: Arc<Mutex<Option<ActiveFlightExecution>>>,
}

impl FlightBackend {
    pub(crate) fn connect(endpoint: String, token: String, tls_ca: Option<&Path>) -> Result<Self> {
        let parsed_endpoint = url::Url::parse(&endpoint).map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidArgument,
                format!("invalid vqld endpoint '{endpoint}'"),
            )
            .with_source(error)
        })?;
        let is_https = match parsed_endpoint.scheme() {
            "http" => false,
            "https" => true,
            scheme => {
                return Err(VqlError::new(
                    ErrorCode::InvalidArgument,
                    format!("invalid vqld endpoint scheme '{scheme}'; expected http or https"),
                ));
            }
        };
        if parsed_endpoint.host_str().is_none()
            || !parsed_endpoint.username().is_empty()
            || parsed_endpoint.password().is_some()
            || parsed_endpoint.query().is_some()
            || parsed_endpoint.fragment().is_some()
            || !matches!(parsed_endpoint.path(), "" | "/")
        {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "vqld endpoint must contain only an http(s) scheme, host, and optional port",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to create the Flight SQL client runtime",
                )
                .with_source(error)
            })?;
        let mut transport = Endpoint::from_shared(endpoint.clone()).map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidArgument,
                format!("invalid vqld endpoint '{endpoint}'"),
            )
            .with_source(error)
        })?;
        if tls_ca.is_some() && !is_https {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "--tls-ca requires an https:// vqld endpoint",
            ));
        }
        if is_https {
            let mut tls = ClientTlsConfig::new().with_enabled_roots();
            if let Some(path) = tls_ca {
                let pem = std::fs::read(path).map_err(|error| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        format!("failed to read vqld TLS CA '{}': {error}", path.display()),
                    )
                    .with_source(error)
                })?;
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            transport = transport.tls_config(tls).map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("invalid TLS configuration for vqld endpoint '{endpoint}'"),
                )
                .with_source(error)
            })?;
        }
        let channel = runtime.block_on(transport.connect()).map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                format!("failed to connect to vqld at '{endpoint}'"),
            )
            .with_source(error)
        })?;
        let mut client = FlightSqlServiceClient::new(channel);
        runtime
            .block_on(client.handshake("", &token))
            .map_err(flight_error)?;
        Ok(Self {
            endpoint,
            runtime,
            client,
            active: Arc::new(Mutex::new(None)),
        })
    }

    async fn execute_remote(
        mut client: FlightSqlServiceClient<Channel>,
        active: Arc<Mutex<Option<ActiveFlightExecution>>>,
        sql: String,
        emit: &mut dyn FnMut(ExecutionOutput) -> Result<()>,
    ) -> Result<()> {
        let mut prepared = client.prepare(sql, None).await.map_err(flight_error)?;
        let result = async {
            let schema = prepared.dataset_schema().map_err(flight_error)?;
            let info = RemoteStatementInfo::from_schema(schema)?;
            match info.kind {
                RemoteStatementKind::Update => {
                    let affected_rows = prepared.execute_update().await.map_err(flight_error)?;
                    emit(ExecutionOutput::Update { affected_rows })
                }
                RemoteStatementKind::Query => {
                    let flight_info = prepared.execute().await.map_err(flight_error)?;
                    let ticket = only_ticket(&flight_info)?;
                    set_active(&active, flight_info)?;
                    let _guard = ActiveFlightGuard {
                        active: Arc::clone(&active),
                    };
                    let mut stream = client.do_get(ticket).await.map_err(flight_error)?;
                    if info.result_mode == RemoteResultMode::Bounded {
                        let mut batches = Vec::new();
                        while let Some(batch) = stream.next().await {
                            batches.push(batch.map_err(flight_error)?);
                        }
                        emit(ExecutionOutput::Batches(batches))
                    } else {
                        while let Some(batch) = stream.next().await {
                            emit(ExecutionOutput::Batches(vec![batch.map_err(flight_error)?]))?;
                        }
                        Ok(())
                    }
                }
            }
        }
        .await;
        let close = prepared.close().await.map_err(flight_error);
        result.and(close)
    }
}

impl ShellBackend for FlightBackend {
    fn description(&self) -> String {
        format!("vqld {}", self.endpoint)
    }

    fn execute(
        &self,
        sql: &str,
        emit: &mut dyn FnMut(ExecutionOutput) -> Result<()>,
    ) -> Result<()> {
        self.runtime.block_on(Self::execute_remote(
            self.client.clone(),
            Arc::clone(&self.active),
            sql.to_owned(),
            emit,
        ))
    }

    fn interrupt_active_query(&self) -> InterruptAction {
        let info = {
            let Ok(active) = self.active.lock() else {
                return InterruptAction::NoActiveQuery;
            };
            let Some(execution) = active.as_ref() else {
                return InterruptAction::NoActiveQuery;
            };
            execution.info.clone()
        };
        let mut client = self.client.clone();
        self.runtime.handle().spawn(async move {
            let action = Action::new(
                "CancelFlightInfo",
                CancelFlightInfoRequest::new(info).encode_to_vec(),
            );
            if let Ok(mut responses) = client.do_action(action).await
                && let Ok(Some(response)) = responses.message().await
            {
                let _ = CancelFlightInfoResult::decode(&*response.body);
            }
        });
        InterruptAction::CancellationRequested
    }
}

struct ActiveFlightGuard {
    active: Arc<Mutex<Option<ActiveFlightExecution>>>,
}

impl Drop for ActiveFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            *active = None;
        }
    }
}

fn set_active(active: &Mutex<Option<ActiveFlightExecution>>, info: FlightInfo) -> Result<()> {
    let mut active = active.lock().map_err(|_| {
        VqlError::new(
            ErrorCode::Internal,
            "Flight shell execution state was poisoned",
        )
    })?;
    *active = Some(ActiveFlightExecution { info });
    Ok(())
}

fn only_ticket(info: &FlightInfo) -> Result<Ticket> {
    match info.endpoint.as_slice() {
        [endpoint] => endpoint.ticket.clone().ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                "vqld returned a Flight endpoint without a ticket",
            )
        }),
        endpoints => Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "vqld returned {} Flight endpoints; the shell requires exactly one",
                endpoints.len()
            ),
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteStatementKind {
    Query,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteResultMode {
    Bounded,
    Unbounded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RemoteStatementInfo {
    kind: RemoteStatementKind,
    result_mode: RemoteResultMode,
}

impl RemoteStatementInfo {
    fn from_schema(schema: &arrow::datatypes::Schema) -> Result<Self> {
        let metadata = schema.metadata();
        if metadata.get(STATEMENT_INFO_VERSION).map(String::as_str) != Some("1") {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "vqld returned unsupported or missing statement metadata version",
            ));
        }
        let (kind, expected_result_mode) =
            match metadata.get(STATEMENT_INFO_KIND).map(String::as_str) {
                Some("query" | "persistent_submission") => (RemoteStatementKind::Query, None),
                Some("update") => (RemoteStatementKind::Update, Some("none")),
                Some(value) => {
                    return Err(VqlError::new(
                        ErrorCode::Execution,
                        format!("vqld returned unsupported statement kind '{value}'"),
                    ));
                }
                None => {
                    return Err(VqlError::new(
                        ErrorCode::Execution,
                        "vqld returned no statement kind",
                    ));
                }
            };
        let result_mode = metadata.get(STATEMENT_INFO_RESULT_MODE).map(String::as_str);
        if let Some(expected) = expected_result_mode {
            if result_mode != Some(expected) {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    "vqld returned inconsistent update statement metadata",
                ));
            }
            return Ok(Self {
                kind,
                result_mode: RemoteResultMode::Bounded,
            });
        }
        let result_mode = match result_mode {
            Some("bounded") => RemoteResultMode::Bounded,
            Some("unbounded") => RemoteResultMode::Unbounded,
            Some(value) => {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    format!("vqld returned unsupported result mode '{value}'"),
                ));
            }
            None => {
                return Err(VqlError::new(
                    ErrorCode::Execution,
                    "vqld returned no statement result mode",
                ));
            }
        };
        Ok(Self { kind, result_mode })
    }
}

fn flight_error(error: FlightError) -> VqlError {
    let remote = structured_error(&error);
    if let Some(remote) = remote
        && let (Some(code), Some(message)) = (
            remote.get("code").and_then(serde_json::Value::as_str),
            remote.get("message").and_then(serde_json::Value::as_str),
        )
        && let Some(code) = parse_error_code(code)
    {
        let mut result = VqlError::new(code, message);
        result.target_version = remote
            .get("target_version")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        return result.with_source(error);
    }
    VqlError::new(
        ErrorCode::Execution,
        format!("vqld Flight SQL request failed: {error}"),
    )
    .with_source(error)
}

fn structured_error(error: &(dyn std::error::Error + 'static)) -> Option<serde_json::Value> {
    let mut current = Some(error);
    while let Some(error) = current {
        let status = error
            .downcast_ref::<FlightError>()
            .and_then(|error| match error {
                FlightError::Tonic(status) => Some(status.as_ref()),
                _ => None,
            })
            .or_else(|| error.downcast_ref::<tonic::Status>());
        if let Some(status) = status {
            let value = serde_json::from_slice::<serde_json::Value>(status.details()).ok()?;
            if value.get("version").and_then(serde_json::Value::as_u64) == Some(1) {
                return Some(value);
            }
        }
        current = error.source();
    }
    None
}

fn parse_error_code(value: &str) -> Option<ErrorCode> {
    Some(match value {
        "VQL-0A001" => ErrorCode::FeatureNotAvailable,
        "VQL-02001" => ErrorCode::NotFound,
        "VQL-22001" => ErrorCode::InvalidArgument,
        "VQL-22002" => ErrorCode::InvalidOption,
        "VQL-22003" => ErrorCode::InvalidLocation,
        "VQL-23001" => ErrorCode::AlreadyExists,
        "VQL-23002" => ErrorCode::NameConflict,
        "VQL-23003" => ErrorCode::FailedPrecondition,
        "VQL-42001" => ErrorCode::InvalidSql,
        "VQL-53001" => ErrorCode::ResourceExhausted,
        "VQL-55001" => ErrorCode::PythonHostRequired,
        "VQL-57001" => ErrorCode::QueryCancelled,
        "VQL-58001" => ErrorCode::Catalog,
        "VQL-58002" => ErrorCode::Execution,
        "VQL-XX001" => ErrorCode::Internal,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use vql_kernel::{Engine, EngineConfig};

    use super::*;

    #[test]
    fn embedded_backend_executes_with_one_session() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let backend = EmbeddedBackend::new(engine.session().build().unwrap());
        let mut outputs = Vec::new();

        backend
            .execute("SELECT 42 AS answer", &mut |output| {
                outputs.push(output);
                Ok(())
            })
            .unwrap();

        let ExecutionOutput::Batches(batches) = &outputs[0] else {
            panic!("SELECT must return batches");
        };
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
    }

    #[test]
    fn flight_endpoint_rejects_unsupported_or_credential_bearing_uris() {
        for endpoint in [
            "grpc://127.0.0.1:6031",
            "http://user:secret@127.0.0.1:6031",
            "http://127.0.0.1:6031/flights",
        ] {
            let error =
                FlightBackend::connect(endpoint.to_owned(), String::new(), None).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidArgument, "{endpoint}");
        }
    }

    #[test]
    fn nested_flight_status_preserves_structured_vql_error() {
        let details = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "code": "VQL-42001",
            "symbol": "INVALID_SQL",
            "message": "invalid statement",
            "target_version": null,
        }))
        .unwrap();
        let status = tonic::Status::with_details(
            tonic::Code::InvalidArgument,
            "remote failure",
            details.into(),
        );
        let error = FlightError::ExternalError(Box::new(FlightError::from(status)));

        let error = flight_error(error);

        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert_eq!(error.message, "invalid statement");
    }
}
