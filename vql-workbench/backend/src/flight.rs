use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::{Action, CancelFlightInfoRequest, CancelFlightInfoResult, FlightInfo, Ticket};
use bytes::Bytes;
use futures::StreamExt;
use prost::Message;
use std::io::{self, Write};
use tokio::sync::mpsc;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use url::Url;

use crate::error::ApiProblem;

const STATEMENT_INFO_VERSION: &str = "vql.statement_info.version";
const STATEMENT_INFO_KIND: &str = "vql.statement_info.kind";
const STATEMENT_INFO_RESULT_MODE: &str = "vql.statement_info.result_mode";

pub type FlightClient = FlightSqlServiceClient<Channel>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Query,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultMode {
    Bounded,
    Unbounded,
    None,
}

impl ResultMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementInfo {
    pub kind: StatementKind,
    pub result_mode: ResultMode,
}

impl StatementInfo {
    pub fn from_schema(schema: &Schema) -> Result<Self, ApiProblem> {
        let metadata = schema.metadata();
        if metadata.get(STATEMENT_INFO_VERSION).map(String::as_str) != Some("1") {
            return Err(ApiProblem::connectivity(
                "Unsupported vqld protocol",
                "vqld returned an unsupported or missing statement metadata version",
            ));
        }
        let (kind, expected_mode) = match metadata.get(STATEMENT_INFO_KIND).map(String::as_str) {
            Some("query" | "persistent_submission") => (StatementKind::Query, None),
            Some("update") => (StatementKind::Update, Some("none")),
            Some(value) => {
                return Err(ApiProblem::connectivity(
                    "Unsupported vqld protocol",
                    format!("vqld returned unsupported statement kind '{value}'"),
                ));
            }
            None => {
                return Err(ApiProblem::connectivity(
                    "Unsupported vqld protocol",
                    "vqld returned no statement kind",
                ));
            }
        };
        let raw_mode = metadata.get(STATEMENT_INFO_RESULT_MODE).map(String::as_str);
        if let Some(expected) = expected_mode
            && raw_mode != Some(expected)
        {
            return Err(ApiProblem::connectivity(
                "Inconsistent vqld protocol",
                "vqld returned inconsistent update statement metadata",
            ));
        }
        let result_mode = match raw_mode {
            Some("bounded") => ResultMode::Bounded,
            Some("unbounded") => ResultMode::Unbounded,
            Some("none") => ResultMode::None,
            Some(value) => {
                return Err(ApiProblem::connectivity(
                    "Unsupported vqld protocol",
                    format!("vqld returned unsupported result mode '{value}'"),
                ));
            }
            None => {
                return Err(ApiProblem::connectivity(
                    "Unsupported vqld protocol",
                    "vqld returned no statement result mode",
                ));
            }
        };
        Ok(Self { kind, result_mode })
    }
}

pub async fn connect(
    endpoint: &str,
    credential: &str,
    tls_ca_pem: Option<Vec<u8>>,
) -> Result<FlightClient, ApiProblem> {
    let parsed = validate_endpoint(endpoint)?;
    let is_https = parsed.scheme() == "https";
    if tls_ca_pem.is_some() && !is_https {
        return Err(ApiProblem::invalid(
            "TLS CA material requires an https:// vqld endpoint",
        ));
    }
    let mut transport = Endpoint::from_shared(endpoint.to_owned()).map_err(|error| {
        ApiProblem::invalid(format!("invalid vqld endpoint '{endpoint}': {error}"))
    })?;
    if is_https {
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        if let Some(pem) = tls_ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        transport = transport.tls_config(tls).map_err(|error| {
            ApiProblem::invalid(format!("invalid TLS configuration for vqld: {error}"))
        })?;
    }
    let channel = transport.connect().await.map_err(|error| {
        ApiProblem::connectivity(
            "Unable to connect to vqld",
            format!("The endpoint '{endpoint}' could not be reached: {error}"),
        )
    })?;
    let mut client = FlightSqlServiceClient::new(channel);
    client
        .handshake("vql-workbench", credential)
        .await
        .map_err(|error| ApiProblem::from_flight(&error))?;
    Ok(client)
}

fn validate_endpoint(endpoint: &str) -> Result<Url, ApiProblem> {
    let parsed = Url::parse(endpoint)
        .map_err(|error| ApiProblem::invalid(format!("invalid vqld endpoint: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ApiProblem::invalid(
            "vqld endpoint scheme must be http or https",
        ));
    }
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(ApiProblem::invalid(
            "vqld endpoint must contain only an http(s) scheme, host, and optional port",
        ));
    }
    Ok(parsed)
}

pub fn execution_id(info: &FlightInfo) -> Result<String, ApiProblem> {
    let metadata: serde_json::Value = serde_json::from_slice(&info.app_metadata).map_err(|_| {
        ApiProblem::connectivity(
            "Invalid vqld execution metadata",
            "vqld returned malformed execution metadata",
        )
    })?;
    metadata
        .get("execution_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            ApiProblem::connectivity(
                "Invalid vqld execution metadata",
                "vqld returned no public execution ID",
            )
        })
}

pub fn only_ticket(info: &FlightInfo) -> Result<Ticket, ApiProblem> {
    match info.endpoint.as_slice() {
        [endpoint] => endpoint.ticket.clone().ok_or_else(|| {
            ApiProblem::connectivity(
                "Invalid vqld execution",
                "vqld returned a Flight endpoint without a ticket",
            )
        }),
        endpoints => Err(ApiProblem::connectivity(
            "Invalid vqld execution",
            format!(
                "vqld returned {} Flight endpoints; Workbench requires exactly one",
                endpoints.len()
            ),
        )),
    }
}

pub async fn cancel(client: &mut FlightClient, info: FlightInfo) -> Result<(), ApiProblem> {
    let action = Action::new(
        "CancelFlightInfo",
        CancelFlightInfoRequest::new(info).encode_to_vec(),
    );
    let mut responses = client
        .do_action(action)
        .await
        .map_err(|error| ApiProblem::from_flight(&error))?;
    if let Some(response) = responses
        .message()
        .await
        .map_err(|error| ApiProblem::from_flight(&arrow_flight::error::FlightError::from(error)))?
    {
        CancelFlightInfoResult::decode(&*response.body).map_err(|error| {
            ApiProblem::connectivity(
                "Invalid cancellation response",
                format!("vqld returned an invalid cancellation response: {error}"),
            )
        })?;
    }
    Ok(())
}

pub struct IpcBridge {
    pub batch_tx: mpsc::Sender<RecordBatch>,
    pub byte_rx: mpsc::Receiver<Result<Bytes, io::Error>>,
    pub encoder: tokio::task::JoinHandle<Result<(), io::Error>>,
}

pub fn spawn_ipc_bridge(schema: SchemaRef) -> IpcBridge {
    let (batch_tx, mut batch_rx) = mpsc::channel::<RecordBatch>(2);
    let (byte_tx, byte_rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    let task = tokio::task::spawn_blocking(move || {
        let sink = ChannelWriter { sender: byte_tx };
        let mut writer = StreamWriter::try_new(sink, schema.as_ref()).map_err(io::Error::other)?;
        while let Some(batch) = batch_rx.blocking_recv() {
            writer.write(&batch).map_err(io::Error::other)?;
        }
        writer.finish().map_err(io::Error::other)?;
        Ok(())
    });
    IpcBridge {
        batch_tx,
        byte_rx,
        encoder: task,
    }
}

pub async fn copy_flight_to_ipc(
    mut client: FlightClient,
    ticket: Ticket,
    batch_tx: mpsc::Sender<RecordBatch>,
) -> Result<(), ApiProblem> {
    let mut stream = client
        .do_get(ticket)
        .await
        .map_err(|error| ApiProblem::from_flight(&error))?;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| ApiProblem::from_flight(&error))?;
        batch_tx.send(batch).await.map_err(|_| {
            ApiProblem::backend(
                "Browser result stream closed",
                "The browser stopped consuming the active Arrow result stream.",
            )
        })?;
    }
    Ok(())
}

struct ChannelWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.sender
            .blocking_send(Ok(Bytes::copy_from_slice(buffer)))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "browser stream closed"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::Field;

    use super::*;

    fn schema(kind: &str, mode: &str) -> Schema {
        Schema::new_with_metadata(
            vec![Field::new("value", arrow::datatypes::DataType::Int64, true)],
            HashMap::from([
                (STATEMENT_INFO_VERSION.to_owned(), "1".to_owned()),
                (STATEMENT_INFO_KIND.to_owned(), kind.to_owned()),
                (STATEMENT_INFO_RESULT_MODE.to_owned(), mode.to_owned()),
            ]),
        )
    }

    #[test]
    fn prepared_metadata_controls_execution_mode() {
        assert_eq!(
            StatementInfo::from_schema(&schema("query", "bounded"))
                .unwrap()
                .result_mode,
            ResultMode::Bounded
        );
        assert_eq!(
            StatementInfo::from_schema(&schema("query", "unbounded"))
                .unwrap()
                .result_mode,
            ResultMode::Unbounded
        );
        assert_eq!(
            StatementInfo::from_schema(&schema("update", "none"))
                .unwrap()
                .kind,
            StatementKind::Update
        );
    }

    #[test]
    fn endpoint_rejects_credentials_and_paths() {
        for endpoint in [
            "grpc://127.0.0.1:6031",
            "http://user:secret@127.0.0.1:6031",
            "http://127.0.0.1:6031/flights",
        ] {
            assert!(validate_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }
}
