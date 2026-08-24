use std::time::{Duration, SystemTime, UNIX_EPOCH};

use libtest_mimic::{Arguments, Completion, Failed, Trial};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};

#[path = "support/mod.rs"]
mod support;

use support::{FixturePaths, SystemSession};

const KAFKA_BOOTSTRAP_SERVERS_ENV: &str = "VQL_TEST_KAFKA_BOOTSTRAP_SERVERS";
const KAFKA_SETUP_SQL: &str = include_str!("kafka/setup.sql");
const KAFKA_PUBLISH_SQL: &str = include_str!("kafka/publish.sql");

fn main() {
    let mut arguments = Arguments::from_args();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }
    let bootstrap_servers = std::env::var(KAFKA_BOOTSTRAP_SERVERS_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let fixtures = FixturePaths::from_workspace(&support::workspace_root());
    let mut missing = Vec::new();
    if bootstrap_servers.is_none() {
        missing.push(format!(
            "missing {KAFKA_BOOTSTRAP_SERVERS_ENV} \
             (start Kafka with docker compose --profile kafka up -d)"
        ));
    }
    missing.extend(
        [
            support::missing_path(&fixtures.images, "python scripts/fetch_datasets.py"),
            support::missing_path(
                &fixtures.detector,
                "python scripts/export_yolo26.py --task detect --size n",
            ),
        ]
        .into_iter()
        .flatten(),
    );
    let trial = Trial::ignorable_test(
        "kafka/vql_detect_result_is_acknowledged_and_consumable",
        move || {
            if let Some(result) = support::prerequisite_result("Kafka", &missing) {
                return result;
            }
            run_kafka_case(
                bootstrap_servers.as_deref().expect("endpoint was checked"),
                &fixtures,
            )
            .map(|()| Completion::Completed)
            .map_err(Failed::from)
        },
    );

    libtest_mimic::run(&arguments, vec![trial]).exit();
}

fn run_kafka_case(bootstrap_servers: &str, fixtures: &FixturePaths) -> Result<(), String> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let topic = format!("visionql-kafka-sink-{}-{suffix}", std::process::id());
    let group = format!("visionql-kafka-test-{}-{suffix}", std::process::id());
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    runtime.block_on(create_topic(bootstrap_servers, &topic))?;
    drop(runtime);
    {
        let system = SystemSession::isolated(Some(&fixtures.detector), None)?;
        let session = &system.session;
        let setup_sql = KAFKA_SETUP_SQL
            .replace(
                "${KAFKA_BOOTSTRAP_SERVERS}",
                &escape_sql_literal(bootstrap_servers),
            )
            .replace("${KAFKA_TOPIC}", &escape_sql_literal(&topic))
            .replace(
                "${IMAGES_LOCATION}",
                &escape_sql_literal(&fixtures.images.to_string_lossy()),
            );
        session
            .run_script(&setup_sql)
            .map_err(|error| format!("create image and Kafka tables: {error}"))?;
        let statement = session
            .sql(KAFKA_PUBLISH_SQL)
            .map_err(|error| format!("plan Kafka table write: {error}"))?;
        let batches = statement
            .collect()
            .map_err(|error| format!("publish Kafka table write: {error}"))?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if rows != 1 {
            return Err(format!("expected 1 acknowledged query row, found {rows}"));
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
            while messages.is_empty() && tokio::time::Instant::now() < deadline {
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
    let mut expected = vec![r#"{"image":"000000000049.jpg","detected":true}"#.to_owned()];
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
