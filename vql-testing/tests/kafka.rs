use std::time::{Duration, SystemTime, UNIX_EPOCH};

use libtest_mimic::{Arguments, Completion, Failed, Trial};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tempfile::tempdir;
use vql_kernel::{Engine, EngineConfig};
use vql_testing::REQUIRE_ENV;

const KAFKA_BOOTSTRAP_SERVERS_ENV: &str = "VQL_TEST_KAFKA_BOOTSTRAP_SERVERS";

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }
    let bootstrap_servers = std::env::var(KAFKA_BOOTSTRAP_SERVERS_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let require_dependencies = std::env::var_os(REQUIRE_ENV).is_some();
    let trial = Trial::ignorable_test(
        "kafka/bounded_query_publishes_acknowledged_json",
        move || {
            let Some(bootstrap_servers) = bootstrap_servers.as_deref() else {
                let message = format!(
                    "missing {KAFKA_BOOTSTRAP_SERVERS_ENV} \
                 (start Kafka with docker compose --profile kafka up -d)"
                );
                return if require_dependencies {
                    Err(Failed::from(message))
                } else {
                    Ok(Completion::ignored_with(message))
                };
            };
            run_kafka_case(bootstrap_servers)
                .map(|()| Completion::Completed)
                .map_err(Failed::from)
        },
    );

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_kafka_case(bootstrap_servers: &str) -> Result<(), String> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let topic = format!("visionql-kafka-sink-{}-{suffix}", std::process::id());
    let group = format!("visionql-kafka-test-{}-{suffix}", std::process::id());
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    runtime.block_on(create_topic(bootstrap_servers, &topic))?;
    drop(runtime);
    let temp = tempdir().map_err(|error| error.to_string())?;
    {
        let engine = Engine::new(EngineConfig::from_home(temp.path().join("vql-home")))
            .map_err(|error| error.to_string())?;
        let session = engine
            .session()
            .build()
            .map_err(|error| error.to_string())?;
        session
            .sql(&format!(
                "CREATE TABLE events USING KAFKA OPTIONS (\
                 bootstrap_servers = '{}', topic = '{topic}', \
                 delivery_timeout_ms = 15000, buffer_capacity = 2)",
                escape_sql_literal(bootstrap_servers)
            ))
            .map_err(|error| format!("create Kafka table: {error}"))?;
        let statement = session
            .sql(
                "INSERT INTO events \
                 SELECT * FROM (VALUES \
                 (CAST(42 AS BIGINT), CAST(NULL AS VARCHAR)), \
                 (CAST(7 AS BIGINT), 'seven')) AS rows(answer, note)",
            )
            .map_err(|error| format!("plan Kafka table write: {error}"))?;
        let batches = statement
            .collect()
            .map_err(|error| format!("publish Kafka table write: {error}"))?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if rows != 2 {
            return Err(format!("expected 2 acknowledged query rows, found {rows}"));
        }
    }

    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let mut messages =
        runtime.block_on(async {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("group.id", group)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .set("allow.auto.create.topics", "false")
                .create()
                .map_err(|error| format!("create Kafka consumer: {error}"))?;
            consumer
                .subscribe(&[&topic])
                .map_err(|error| format!("subscribe to Kafka topic: {error}"))?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            let mut messages = Vec::new();
            while messages.len() < 2 && tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
                    Ok(Ok(message)) => {
                        if let Some(value) = message.payload() {
                            messages.push(String::from_utf8(value.to_vec()).map_err(|error| {
                                format!("Kafka value is not UTF-8 JSON: {error}")
                            })?);
                        }
                    }
                    Ok(Err(error)) => return Err(format!("poll Kafka topic: {error}")),
                    Err(_) => {}
                }
            }
            consumer.unsubscribe();
            Ok::<_, String>(messages)
        })?;
    messages.sort();
    let mut expected = vec![
        r#"{"answer":42,"note":null}"#.to_owned(),
        r#"{"answer":7,"note":"seven"}"#.to_owned(),
    ];
    expected.sort();
    if messages != expected {
        return Err(format!(
            "unexpected Kafka JSON messages: expected {expected:?}, found {messages:?}"
        ));
    }
    Ok(())
}

async fn create_topic(bootstrap_servers: &str, topic: &str) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "visionql-kafka-sink-integration")
        .create()
        .map_err(|error| format!("create Kafka admin client: {error}"))?;
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let options = AdminOptions::new()
        .request_timeout(Some(Duration::from_secs(10)))
        .operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .create_topics([&new_topic], &options)
        .await
        .map_err(|error| format!("create Kafka topic: {error}"))?;
    for result in results {
        if let Err((name, error)) = result {
            return Err(format!("create Kafka topic '{name}': {error}"));
        }
    }
    Ok(())
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
