use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::Int64Array;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::{Action, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus};
use futures::TryStreamExt;
use prost::Message;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use vql_kernel::{Engine, EngineConfig};
use vql_server::config::ServiceConfig;
use vql_server::controller::QueryController;
use vql_server::flight::VqlFlightSqlService;

async fn start_server() -> (
    FlightSqlServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.keep();
    let engine = Engine::new(EngineConfig::from_home(home)).unwrap();
    let config = Arc::new(ServiceConfig::default());
    let controller = Arc::new(QueryController::new(engine.clone(), 100, 30));
    let service =
        VqlFlightSqlService::new(engine, controller, config, Arc::new(AtomicBool::new(true)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel);
    client
        .handshake("client-name-is-not-an-identity", "")
        .await
        .unwrap();
    (client, task)
}

#[tokio::test]
async fn arrow_flight_sql_client_runs_direct_and_prepared_queries() {
    let (mut client, task) = start_server().await;

    let info = client
        .execute("SELECT 42 AS answer".to_owned(), None)
        .await
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&info.app_metadata).unwrap();
    assert!(metadata["execution_id"].as_str().is_some());
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let answer = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(answer.value(0), 42);

    let mut prepared = client
        .prepare("SELECT 7 AS value".to_owned(), None)
        .await
        .unwrap();
    assert_eq!(
        prepared
            .dataset_schema()
            .unwrap()
            .metadata()
            .get("vql.statement_info.kind")
            .map(String::as_str),
        Some("query")
    );
    let info = prepared.execute().await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value.value(0), 7);

    task.abort();
}

#[tokio::test]
async fn show_jobs_lists_persistent_jobs_through_direct_and_prepared_flight_sql() {
    let (mut client, task) = start_server().await;
    let mut prepared = client.prepare("SHOW JOBS".to_owned(), None).await.unwrap();
    assert_eq!(
        prepared.dataset_schema().unwrap().field(0).name(),
        "query_id"
    );
    assert_eq!(
        prepared
            .dataset_schema()
            .unwrap()
            .metadata()
            .get("vql.statement_info.kind")
            .map(String::as_str),
        Some("query")
    );
    for info in [
        client.execute("SHOW JOBS;".to_owned(), None).await.unwrap(),
        prepared.execute().await.unwrap(),
    ] {
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let mut stream = client.do_get(ticket).await.unwrap();
        let batches = (&mut stream).try_collect::<Vec<_>>().await.unwrap();
        assert!(batches.iter().all(|batch| batch.num_rows() == 0));
        assert_eq!(stream.schema().unwrap().field(0).name(), "query_id");
    }
    task.abort();
}

#[tokio::test]
async fn query_and_update_paths_are_not_interchangeable() {
    let (mut client, task) = start_server().await;

    client
        .execute_update("SET vql.on_error = 'fail'".to_owned(), None)
        .await
        .unwrap();
    let error = client
        .execute("SET vql.on_error = 'null'".to_owned(), None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ExecuteUpdate"));

    task.abort();
}

#[tokio::test]
async fn cancel_flight_info_targets_one_server_execution_id() {
    let (mut client, task) = start_server().await;
    let info = client.execute("SELECT 1".to_owned(), None).await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let action = Action::new(
        "CancelFlightInfo",
        CancelFlightInfoRequest::new(info).encode_to_vec(),
    );
    let result = client
        .do_action(action)
        .await
        .unwrap()
        .message()
        .await
        .unwrap()
        .unwrap();
    let result = CancelFlightInfoResult::decode(&*result.body).unwrap();
    assert_eq!(result.status, CancelStatus::Cancelled as i32);
    assert!(client.do_get(ticket).await.is_err());

    task.abort();
}
