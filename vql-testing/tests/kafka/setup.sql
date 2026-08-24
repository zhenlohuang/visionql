CREATE TABLE events USING KAFKA OPTIONS (
  bootstrap_servers = '${KAFKA_BOOTSTRAP_SERVERS}',
  topic = '${KAFKA_TOPIC}',
  delivery_timeout_ms = 15000,
  buffer_capacity = 2
)
