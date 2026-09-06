use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::{Action, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus};
use futures::TryStreamExt;
use libtest_mimic::{Arguments, Completion, Failed, Trial};
use prost::Message as ProstMessage;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use vql_kernel::{Engine, EngineConfig};
use vql_server::config::ServiceConfig;
use vql_server::controller::QueryController;
use vql_server::flight::VqlFlightSqlService;

#[path = "support/mod.rs"]
mod support;

const RTSP_URL_ENV: &str = "VQL_TEST_RTSP_URL";
const KAFKA_BOOTSTRAP_SERVERS_ENV: &str = "VQL_TEST_KAFKA_BOOTSTRAP_SERVERS";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ServiceRun {
    controller: Arc<QueryController>,
    flight: VqlFlightSqlService,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    address: std::net::SocketAddr,
}

impl ServiceRun {
    async fn stop(self) -> Result<(), String> {
        self.controller.shutdown();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.controller.active_count() != 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if self.controller.active_count() != 0 {
            return Err("vqld persistent Query did not stop during daemon shutdown".to_owned());
        }
        self.task.abort();
        let _ = self.task.await;
        Ok(())
    }
}

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }
    let fixtures = support::FixturePaths::from_workspace(&support::workspace_root());
    let video = fixtures.videos.join("people-detection.mp4");
    let rtsp = std::env::var(RTSP_URL_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let kafka = std::env::var(KAFKA_BOOTSTRAP_SERVERS_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let mut missing = Vec::new();
    if rtsp.is_none() {
        missing.push(format!("missing {RTSP_URL_ENV}"));
    }
    if kafka.is_none() {
        missing.push(format!("missing {KAFKA_BOOTSTRAP_SERVERS_ENV}"));
    }
    if !command_available("ffmpeg", "-version") {
        missing.push("ffmpeg executable".to_owned());
    }
    if !video.is_file() {
        missing.push(format!(
            "{} (run: python scripts/fetch_datasets.py)",
            video.display()
        ));
    }

    let trial = Trial::ignorable_test(
        "vqld/persistent_query_survives_client_and_daemon",
        move || {
            if let Some(result) = support::prerequisite_result("vqld", &missing) {
                return result;
            }
            let suffix = unique_suffix()?;
            let endpoint = format!(
                "{}-vqld-{suffix}",
                rtsp.as_deref().expect("RTSP endpoint was checked")
            );
            run_case(
                &endpoint,
                kafka.as_deref().expect("Kafka endpoint was checked"),
                &video,
                &suffix,
            )
            .map(|()| Completion::Completed)
            .map_err(Failed::from)
        },
    );
    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_case(endpoint: &str, kafka: &str, video: &Path, suffix: &str) -> Result<(), String> {
    let _publisher = ChildGuard(
        Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-re", "-stream_loop", "-1", "-i"])
            .arg(video)
            .args([
                "-an",
                "-c:v",
                "copy",
                "-f",
                "rtsp",
                "-rtsp_transport",
                "tcp",
            ])
            .arg(endpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("start FFmpeg publisher: {error}"))?,
    );
    let home = tempfile::tempdir().map_err(|error| error.to_string())?;
    let topic = format!("vql-vqld-{suffix}");
    let group = format!("vql-vqld-{suffix}");
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    runtime.block_on(async {
        create_topic(kafka, &topic).await?;
        let first = start_service(home.path()).await?;
        eprintln!("vqld scenario: first daemon started");
        let mut client = connect(first.address).await?;
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE camera USING RTSP OPTIONS (\
                 url = '{}', fps = 5, event_time = 'capture_time', \
                 watermark = '1 second', transport = 'tcp')",
                escape_sql_literal(endpoint)
            ),
        )
        .await?;
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE events (ts TIMESTAMP NOT NULL, frame_id BIGINT NOT NULL) \
                 USING KAFKA OPTIONS (bootstrap_servers = '{}', topic = '{}')",
                escape_sql_literal(kafka),
                topic
            ),
        )
        .await?;
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE counts (window_start TIMESTAMP, frames BIGINT) \
                 USING KAFKA OPTIONS (bootstrap_servers = '{}', topic = '{}')",
                escape_sql_literal(kafka),
                topic
            ),
        )
        .await?;
        exercise_attached_query(&mut client, &first.flight).await?;

        let submitted = execute_query(
            &mut client,
            "SUBMIT QUERY disconnect_case AS \
             INSERT INTO events SELECT ts, frame_id FROM camera",
        )
        .await?;
        let disconnect_id = string_value(&submitted, 0, 0)?;
        drop(client);

        let mut client = connect(first.address).await?;
        wait_for_running(&mut client, &disconnect_id, 0).await?;
        eprintln!("vqld scenario: Query survived client disconnect");
        wait_for_kafka_message(kafka, &topic, &group).await?;
        let stopped = execute_query(&mut client, &format!("STOP QUERY '{disconnect_id}'")).await?;
        if string_value(&stopped, 2, 0)? != "STOPPED" {
            return Err("STOP QUERY did not return STOPPED".to_owned());
        }
        eprintln!("vqld scenario: disconnected Query stopped");

        let submitted = execute_query(
            &mut client,
            "SUBMIT QUERY restart_case AS INSERT INTO counts \
             SELECT TUMBLE(ts, INTERVAL '2' SECOND) AS window_start, COUNT(*) AS frames \
             FROM camera GROUP BY 1",
        )
        .await?;
        let restart_id = string_value(&submitted, 0, 0)?;
        wait_for_running(&mut client, &restart_id, 0).await?;
        eprintln!("vqld scenario: restart Query running before daemon shutdown");
        drop(client);
        first.stop().await?;
        eprintln!("vqld scenario: first daemon stopped");

        let second = start_service(home.path()).await?;
        eprintln!("vqld scenario: second daemon started");
        let mut client = connect(second.address).await?;
        wait_for_running(&mut client, &restart_id, 1).await?;
        eprintln!("vqld scenario: restarted Query reported source progress");
        let described =
            execute_query(&mut client, &format!("DESCRIBE QUERY '{restart_id}'")).await?;
        let gap_start = timestamp_value(&described, 8, 0)?;
        let gap_end = timestamp_value(&described, 9, 0)?;
        if gap_start.is_none() || gap_end.is_none() {
            return Err("restarted Query did not report a closed restart gap".to_owned());
        }
        let reset = described[0]
            .column(10)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "restart reset flag is not Boolean".to_owned())?;
        if !reset.value(0) {
            return Err("restarted TUMBLE Query did not report discarded window state".to_owned());
        }
        eprintln!("vqld scenario: restart gap and window reset verified");
        let stopped = execute_query(&mut client, &format!("STOP QUERY '{restart_id}'")).await?;
        if string_value(&stopped, 2, 0)? != "STOPPED" {
            return Err("restarted Query did not stop".to_owned());
        }
        eprintln!("vqld scenario: restarted Query stopped");
        second.stop().await
    })
}

async fn start_service(home: &Path) -> Result<ServiceRun, String> {
    let engine = Engine::new(EngineConfig::from_home(home)).map_err(|error| error.to_string())?;
    let controller = Arc::new(QueryController::new(engine.clone(), 100, 30));
    controller
        .recover()
        .await
        .map_err(|error| error.to_string())?;
    let config = Arc::new(ServiceConfig::default());
    let service = VqlFlightSqlService::new(
        engine,
        Arc::clone(&controller),
        config,
        Arc::new(AtomicBool::new(true)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let flight = service.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    Ok(ServiceRun {
        controller,
        flight,
        task,
        address,
    })
}

async fn exercise_attached_query(
    client: &mut FlightSqlServiceClient<Channel>,
    service: &VqlFlightSqlService,
) -> Result<(), String> {
    let info = client
        .execute("SELECT ts, frame_id FROM camera".to_owned(), None)
        .await
        .map_err(|error| format!("prepare attached RTSP query: {error}"))?;
    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.clone())
        .ok_or_else(|| "attached RTSP query returned no ticket".to_owned())?;
    let mut stream = client
        .do_get(ticket)
        .await
        .map_err(|error| format!("attach RTSP query: {error}"))?;
    let first = tokio::time::timeout(Duration::from_secs(10), stream.try_next())
        .await
        .map_err(|_| "attached RTSP query produced no batch".to_owned())?
        .map_err(|error| format!("read attached RTSP query: {error}"))?;
    if first.is_none() || service.attached_execution_count() != 1 {
        return Err("attached RTSP execution was not registered".to_owned());
    }
    let action = Action::new(
        "CancelFlightInfo",
        CancelFlightInfoRequest::new(info).encode_to_vec(),
    );
    let result = client
        .do_action(action)
        .await
        .map_err(|error| format!("cancel attached RTSP query: {error}"))?
        .message()
        .await
        .map_err(|error| format!("read attached cancellation result: {error}"))?
        .ok_or_else(|| "attached cancellation returned no result".to_owned())?;
    let result = CancelFlightInfoResult::decode(&*result.body)
        .map_err(|error| format!("decode attached cancellation result: {error}"))?;
    if result.status != CancelStatus::Cancelled as i32 {
        return Err("attached cancellation did not return CANCELLED".to_owned());
    }
    drop(stream);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while service.attached_execution_count() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if service.attached_execution_count() != 0 {
        return Err("cancelled attached RTSP execution remained registered".to_owned());
    }
    Ok(())
}

async fn connect(address: std::net::SocketAddr) -> Result<FlightSqlServiceClient<Channel>, String> {
    let channel = Channel::from_shared(format!("http://{address}"))
        .map_err(|error| error.to_string())?
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    let mut client = FlightSqlServiceClient::new(channel);
    client
        .handshake("service", "")
        .await
        .map_err(|error| format!("Flight handshake: {error}"))?;
    Ok(client)
}

async fn execute_update(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<(), String> {
    client
        .execute_update(sql.to_owned(), None)
        .await
        .map(|_| ())
        .map_err(|error| format!("execute update {sql:?}: {error}"))
}

async fn execute_query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let info = client
        .execute(sql.to_owned(), None)
        .await
        .map_err(|error| format!("prepare query {sql:?}: {error}"))?;
    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.clone())
        .ok_or_else(|| "Flight query returned no ticket".to_owned())?;
    client
        .do_get(ticket)
        .await
        .map_err(|error| format!("attach query {sql:?}: {error}"))?
        .try_collect()
        .await
        .map_err(|error| format!("collect query {sql:?}: {error}"))
}

async fn wait_for_running(
    client: &mut FlightSqlServiceClient<Channel>,
    query_id: &str,
    restart_gap_count: i64,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let batches = execute_query(client, "SHOW QUERIES").await?;
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW QUERIES query_id is not Utf8".to_owned())?;
            let states = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW QUERIES state is not Utf8".to_owned())?;
            let health = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW QUERIES source_health is not Utf8".to_owned())?;
            let event_time = batch
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "SHOW QUERIES last_event_time is not Timestamp".to_owned())?;
            let gaps = batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "SHOW QUERIES restart_gap_count is not Int64".to_owned())?;
            for row in 0..batch.num_rows() {
                if ids.value(row) == query_id
                    && states.value(row) == "RUNNING"
                    && !health.is_null(row)
                    && health.value(row) == "connected"
                    && !event_time.is_null(row)
                    && gaps.value(row) == restart_gap_count
                {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "Query {query_id} did not become RUNNING with restart_gap_count={restart_gap_count}"
    ))
}

async fn create_topic(bootstrap_servers: &str, topic: &str) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "vql-vqld-integration")
        .create()
        .map_err(|error| format!("create Kafka admin client: {error}"))?;
    let options = AdminOptions::new()
        .request_timeout(Some(Duration::from_secs(10)))
        .operation_timeout(Some(Duration::from_secs(10)));
    for result in admin
        .create_topics(
            [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &options,
        )
        .await
        .map_err(|error| format!("create Kafka topic: {error}"))?
    {
        if let Err((name, error)) = result {
            return Err(format!("create Kafka topic '{name}': {error}"));
        }
    }
    Ok(())
}

async fn wait_for_kafka_message(
    bootstrap_servers: &str,
    topic: &str,
    group: &str,
) -> Result<(), String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|error| format!("create Kafka consumer: {error}"))?;
    consumer
        .subscribe(&[topic])
        .map_err(|error| format!("subscribe to Kafka topic: {error}"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await
            && message.payload().is_some()
        {
            consumer.unsubscribe();
            return Ok(());
        }
    }
    Err("persistent Query produced no Kafka record after client disconnect".to_owned())
}

fn string_value(batches: &[RecordBatch], column: usize, row: usize) -> Result<String, String> {
    batches
        .first()
        .and_then(|batch| batch.column(column).as_any().downcast_ref::<StringArray>())
        .filter(|values| !values.is_null(row))
        .map(|values| values.value(row).to_owned())
        .ok_or_else(|| format!("result column {column}, row {row} is not a non-NULL Utf8 value"))
}

fn timestamp_value(
    batches: &[RecordBatch],
    column: usize,
    row: usize,
) -> Result<Option<i64>, String> {
    let values = batches
        .first()
        .and_then(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
        })
        .ok_or_else(|| format!("result column {column} is not Timestamp(Millisecond)"))?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn command_available(name: &str, version_arg: &str) -> bool {
    Command::new(name)
        .arg(version_arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn unique_suffix() -> Result<String, Failed> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| format!("{}-{}", std::process::id(), duration.as_nanos()))
        .map_err(|error| Failed::from(error.to_string()))
}
