use std::time::{Duration, SystemTime, UNIX_EPOCH};

use krafka::admin::{AdminClient, NewTopic};
use krafka::consumer::{AutoOffsetReset, Consumer};
use libtest_mimic::{Arguments, Completion, Failed, Trial};
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
                "CREATE SINK events TYPE KAFKA WITH (\
                 bootstrap_servers='{}', topic='{topic}', \
                 delivery_timeout_ms=15000, buffer_capacity=2)",
                escape_sql_literal(bootstrap_servers)
            ))
            .map_err(|error| format!("create Kafka Sink: {error}"))?;
        let statement = session
            .sql(
                "INSERT INTO events \
                 SELECT * FROM (VALUES \
                 (CAST(42 AS BIGINT), CAST(NULL AS VARCHAR)), \
                 (CAST(7 AS BIGINT), 'seven')) AS rows(answer, note)",
            )
            .map_err(|error| format!("plan Kafka Sink query: {error}"))?;
        let batches = statement
            .collect()
            .map_err(|error| format!("publish Kafka Sink query: {error}"))?;
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if rows != 2 {
            return Err(format!("expected 2 acknowledged query rows, found {rows}"));
        }
    }

    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let mut messages = runtime.block_on(async {
        let consumer = Consumer::builder()
            .bootstrap_servers(bootstrap_servers)
            .group_id(group)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .map_err(|error| format!("create Kafka consumer: {error}"))?;
        let subscribe_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match consumer.subscribe(&[&topic]).await {
                Ok(()) => break,
                Err(_) if tokio::time::Instant::now() < subscribe_deadline => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(error) => return Err(format!("subscribe to Kafka topic: {error}")),
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut messages = Vec::new();
        while messages.len() < 2 && tokio::time::Instant::now() < deadline {
            let records = consumer
                .poll(Duration::from_millis(500))
                .await
                .map_err(|error| format!("poll Kafka topic: {error}"))?;
            for record in records {
                if let Some(value) = record.value {
                    messages.push(
                        String::from_utf8(value.to_vec())
                            .map_err(|error| format!("Kafka value is not UTF-8 JSON: {error}"))?,
                    );
                }
            }
        }
        consumer
            .close()
            .await
            .map_err(|error| format!("close Kafka consumer: {error}"))?;
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
    let admin = AdminClient::builder()
        .bootstrap_servers(bootstrap_servers)
        .client_id("visionql-kafka-sink-integration")
        .build()
        .await
        .map_err(|error| format!("create Kafka admin client: {error}"))?;
    let new_topic = NewTopic::new(topic, 1, 1)
        .map_err(|error| format!("build Kafka topic definition: {error}"))?;
    let results = admin
        .create_topics(vec![new_topic], Duration::from_secs(10), false)
        .await
        .map_err(|error| format!("create Kafka topic: {error}"))?;
    let topic_error = results.into_iter().find_map(|result| result.error);
    admin.close().await;
    if let Some(error) = topic_error {
        return Err(format!("create Kafka topic '{topic}': {error}"));
    }
    Ok(())
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
