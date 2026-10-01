use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
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
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[path = "support/system.rs"]
mod system_support;

const CLIENT_RTSP_URL_ENV: &str = "VQL_TEST_RTSP_URL";
const SERVER_RTSP_URL_ENV: &str = "VQL_TEST_VQLD_RTSP_URL";
const CLIENT_KAFKA_ENV: &str = "VQL_TEST_KAFKA_BOOTSTRAP_SERVERS";
const SERVER_KAFKA_ENV: &str = "VQL_TEST_VQLD_KAFKA_BOOTSTRAP_SERVERS";
const VQLD_ENDPOINT_ENV: &str = "VQL_TEST_VQLD_ENDPOINT";
const VQLD_TOKEN_ENV: &str = "VQL_TEST_VQLD_TOKEN";
const VQLD_TLS_CA_ENV: &str = "VQL_TEST_VQLD_TLS_CA";
const COMPOSE_FILE_ENV: &str = "VQL_TEST_VQLD_COMPOSE_FILE";
const COMPOSE_PROJECT_ENV: &str = "VQL_TEST_VQLD_COMPOSE_PROJECT";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct SystemConfig {
    client_rtsp_url: String,
    server_rtsp_url: String,
    client_kafka: String,
    server_kafka: String,
    vqld_endpoint: String,
    vqld_token: String,
    vqld_tls_ca: PathBuf,
    compose_file: PathBuf,
    compose_project: String,
}

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }
    let fixtures = system_support::FixturePaths::from_workspace(&system_support::workspace_root());
    let video = fixtures.videos.join("people-detection.mp4");
    let mut missing = Vec::new();
    let config = SystemConfig {
        client_rtsp_url: required_env(CLIENT_RTSP_URL_ENV, &mut missing),
        server_rtsp_url: required_env(SERVER_RTSP_URL_ENV, &mut missing),
        client_kafka: required_env(CLIENT_KAFKA_ENV, &mut missing),
        server_kafka: required_env(SERVER_KAFKA_ENV, &mut missing),
        vqld_endpoint: required_env(VQLD_ENDPOINT_ENV, &mut missing),
        vqld_token: required_env(VQLD_TOKEN_ENV, &mut missing),
        vqld_tls_ca: PathBuf::from(required_env(VQLD_TLS_CA_ENV, &mut missing)),
        compose_file: PathBuf::from(required_env(COMPOSE_FILE_ENV, &mut missing)),
        compose_project: required_env(COMPOSE_PROJECT_ENV, &mut missing),
    };
    if !config.vqld_tls_ca.as_os_str().is_empty() && !config.vqld_tls_ca.is_file() {
        missing.push(format!(
            "{} from {VQLD_TLS_CA_ENV}",
            config.vqld_tls_ca.display()
        ));
    }
    if !config.compose_file.as_os_str().is_empty() && !config.compose_file.is_file() {
        missing.push(format!(
            "{} from {COMPOSE_FILE_ENV}",
            config.compose_file.display()
        ));
    }
    if !command_available("ffmpeg", &["-version"]) {
        missing.push("ffmpeg executable".to_owned());
    }
    if !command_available("docker", &["compose", "version"]) {
        missing.push("docker compose executable".to_owned());
    }
    if !video.is_file() {
        missing.push(format!(
            "{} (run: python scripts/fetch_datasets.py)",
            video.display()
        ));
    }

    let trial = Trial::ignorable_test(
        "vqld/persistent_job_survives_client_and_container_restart",
        move || {
            if let Some(result) = system_support::prerequisite_result("vqld", &missing) {
                return result;
            }
            run_case(&config, &video)
                .map(|()| Completion::Completed)
                .map_err(Failed::from)
        },
    );
    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_case(config: &SystemConfig, video: &Path) -> Result<(), String> {
    exercise_packaged_cli(config)?;

    let suffix = unique_suffix()?;
    let publisher_endpoint = format!("{}-vqld-{suffix}", config.client_rtsp_url);
    let source_endpoint = format!("{}-vqld-{suffix}", config.server_rtsp_url);
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
            .arg(&publisher_endpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("start FFmpeg publisher: {error}"))?,
    );
    let topic = format!("vql-vqld-{suffix}");
    let group = format!("vql-vqld-{suffix}");
    let ca_pem = fs::read(&config.vqld_tls_ca).map_err(|error| {
        format!(
            "read vqld test CA '{}': {error}",
            config.vqld_tls_ca.display()
        )
    })?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    runtime.block_on(async {
        create_topic(&config.client_kafka, &topic).await?;
        let mut client = connect_with_retry(
            &config.vqld_endpoint,
            &config.vqld_token,
            &ca_pem,
            Duration::from_secs(30),
        )
        .await?;
        eprintln!("vqld scenario: packaged daemon and CLI are reachable");
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE camera USING RTSP OPTIONS (\
                 url = '{}', fps = 5, event_time = 'capture_time', \
                 watermark = '1 second', transport = 'tcp')",
                escape_sql_literal(&source_endpoint)
            ),
        )
        .await?;
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE events (ts TIMESTAMP NOT NULL, frame_id BIGINT NOT NULL) \
                 USING KAFKA OPTIONS (bootstrap_servers = '{}', topic = '{}')",
                escape_sql_literal(&config.server_kafka),
                topic
            ),
        )
        .await?;
        execute_update(
            &mut client,
            &format!(
                "CREATE TABLE counts (window_start TIMESTAMP, frames BIGINT) \
                 USING KAFKA OPTIONS (bootstrap_servers = '{}', topic = '{}')",
                escape_sql_literal(&config.server_kafka),
                topic
            ),
        )
        .await?;
        exercise_attached_query(&mut client).await?;

        let submitted = execute_query(
            &mut client,
            "SUBMIT JOB disconnect_case AS \
             INSERT INTO events SELECT ts, frame_id FROM camera",
        )
        .await?;
        let disconnect_id = string_value(&submitted, 0, 0)?;
        drop(client);

        let mut client = connect_with_retry(
            &config.vqld_endpoint,
            &config.vqld_token,
            &ca_pem,
            Duration::from_secs(10),
        )
        .await?;
        wait_for_running(&mut client, &disconnect_id, 0).await?;
        eprintln!("vqld scenario: Job survived client disconnect");
        wait_for_kafka_message(&config.client_kafka, &topic, &group).await?;
        let stopped = execute_query(&mut client, &format!("STOP JOB '{disconnect_id}'")).await?;
        if string_value(&stopped, 2, 0)? != "STOPPED" {
            return Err("STOP JOB did not return STOPPED".to_owned());
        }

        let submitted = execute_query(
            &mut client,
            "SUBMIT JOB restart_case AS INSERT INTO counts \
             SELECT TUMBLE(ts, INTERVAL '2' SECOND) AS window_start, COUNT(*) AS frames \
             FROM camera GROUP BY 1",
        )
        .await?;
        let restart_id = string_value(&submitted, 0, 0)?;
        wait_for_running(&mut client, &restart_id, 0).await?;
        drop(client);
        restart_vqld(config)?;
        eprintln!("vqld scenario: Compose restarted the packaged daemon");

        let mut client = connect_with_retry(
            &config.vqld_endpoint,
            &config.vqld_token,
            &ca_pem,
            Duration::from_secs(30),
        )
        .await?;
        wait_for_running(&mut client, &restart_id, 1).await?;
        let described = wait_for_closed_restart_gap(&mut client, &restart_id).await?;
        let reset = described[0]
            .column(10)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "restart reset flag is not Boolean".to_owned())?;
        if !reset.value(0) {
            return Err("restarted TUMBLE Job did not report discarded window state".to_owned());
        }
        let stopped = execute_query(&mut client, &format!("STOP JOB '{restart_id}'")).await?;
        if string_value(&stopped, 2, 0)? != "STOPPED" {
            return Err("restarted Job did not stop".to_owned());
        }
        eprintln!("vqld scenario: stable Job identity and restart gap verified");
        Ok(())
    })
}

async fn exercise_attached_query(
    client: &mut FlightSqlServiceClient<Channel>,
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
    if first.is_none() {
        return Err("attached RTSP execution ended before producing a batch".to_owned());
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
    Ok(())
}

async fn connect_with_retry(
    endpoint: &str,
    token: &str,
    ca_pem: &[u8],
    timeout: Duration,
) -> Result<FlightSqlServiceClient<Channel>, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        match connect(endpoint, token, ca_pem).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "vqld did not accept authenticated Flight SQL connections at {endpoint}: {}",
        last_error.unwrap_or_else(|| "connection deadline elapsed".to_owned())
    ))
}

async fn connect(
    endpoint: &str,
    token: &str,
    ca_pem: &[u8],
) -> Result<FlightSqlServiceClient<Channel>, String> {
    let transport = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| error.to_string())?
        .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem)))
        .map_err(|error| format!("configure Flight TLS: {error}"))?;
    let channel = transport
        .connect()
        .await
        .map_err(|error| format!("connect to Flight endpoint: {error}"))?;
    let mut client = FlightSqlServiceClient::new(channel);
    client
        .handshake("", token)
        .await
        .map_err(|error| format!("Flight handshake: {error}"))?;
    Ok(client)
}

fn exercise_packaged_cli(config: &SystemConfig) -> Result<(), String> {
    let mut child = compose_command(config)
        .args([
            "exec",
            "-T",
            "vqld",
            "vql",
            "shell",
            "--endpoint",
            "https://127.0.0.1:6031",
            "--token",
            &config.vqld_token,
            "--tls-ca",
            "/run/vqld-tls/server.crt",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("start packaged vql shell: {error}"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| "packaged vql shell stdin was unavailable".to_owned())?
        .write_all(
            b"SELECT 42 AS answer;\nSET vql.on_error = 'fail';\n\
              CREATE FUNCTION detect USING MODEL detector;\n\\q\n",
        )
        .map_err(|error| format!("write packaged vql shell input: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for packaged vql shell: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "packaged vql shell failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.contains("answer")
        || !stdout.contains("42")
        || !stdout.contains("OK (0 rows affected)")
    {
        return Err(format!(
            "packaged vql shell returned unexpected output: {stdout}"
        ));
    }
    if !stderr.contains("[VQL-42001] INVALID_SQL") {
        return Err(format!(
            "packaged vql shell did not preserve the structured error: {stderr}"
        ));
    }
    Ok(())
}

fn restart_vqld(config: &SystemConfig) -> Result<(), String> {
    let output = compose_command(config)
        .args(["restart", "vqld"])
        .output()
        .map_err(|error| format!("restart vqld container: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "restart vqld container: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn compose_command(config: &SystemConfig) -> Command {
    let mut command = Command::new("docker");
    command
        .args(["compose", "-f"])
        .arg(&config.compose_file)
        .args(["-p", &config.compose_project]);
    command
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
    job_id: &str,
    restart_gap_count: i64,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let batches = execute_query(client, "SHOW JOBS").await?;
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW JOBS job_id is not Utf8".to_owned())?;
            let states = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW JOBS state is not Utf8".to_owned())?;
            let health = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "SHOW JOBS source_health is not Utf8".to_owned())?;
            let event_time = batch
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "SHOW JOBS last_event_time is not Timestamp".to_owned())?;
            let gaps = batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "SHOW JOBS restart_gap_count is not Int64".to_owned())?;
            for row in 0..batch.num_rows() {
                if ids.value(row) == job_id
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
        "Job {job_id} did not become RUNNING with restart_gap_count={restart_gap_count}"
    ))
}

async fn wait_for_closed_restart_gap(
    client: &mut FlightSqlServiceClient<Channel>,
    job_id: &str,
) -> Result<Vec<RecordBatch>, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let described = execute_query(client, &format!("DESCRIBE JOB '{job_id}'")).await?;
        if timestamp_value(&described, 8, 0)?.is_some()
            && timestamp_value(&described, 9, 0)?.is_some()
        {
            return Ok(described);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("restarted Job did not report a closed restart gap".to_owned())
}

async fn create_topic(bootstrap_servers: &str, topic: &str) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "vql-vqld-system-test")
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
    Err("persistent Job produced no Kafka record after client disconnect".to_owned())
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

fn required_env(name: &str, missing: &mut Vec<String>) -> String {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        Some(value) => value,
        None => {
            missing.push(format!("missing {name}"));
            String::new()
        }
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn command_available(name: &str, args: &[&str]) -> bool {
    Command::new(name)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn unique_suffix() -> Result<String, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| format!("{}-{}", std::process::id(), duration.as_nanos()))
        .map_err(|error| error.to_string())
}
