use std::fmt::Write;

use chrono::{DateTime, Utc};

use crate::catalog::{
    EventTimePolicy, FunctionDef, FunctionImplementation, KafkaSinkConfig, ModelDef, ModelType,
    RtspTransport, SinkDef, SinkKind, StreamDef, TableDef, TableProviderKind,
};
use crate::{ErrorCode, Result, VqlError};

pub(crate) trait RenderCreate {
    fn render_create(&self) -> Result<String>;
}

pub(crate) fn render_create(definition: &impl RenderCreate) -> Result<String> {
    definition.render_create()
}

impl RenderCreate for TableDef {
    fn render_create(&self) -> Result<String> {
        let provider = match self.provider {
            TableProviderKind::Images => "IMAGES",
            TableProviderKind::Videos => "VIDEOS",
        };
        let mut options = vec![format!("recursive = {}", self.recursive)];
        if let Some(fps) = self.fps {
            options.push(format!("fps = {fps}"));
        }
        if let Some(start_time_ms) = self.start_time_ms {
            let start_time =
                DateTime::<Utc>::from_timestamp_millis(start_time_ms).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Catalog,
                        format!("table '{}' has an invalid start_time", self.name),
                    )
                })?;
            options.push(format!(
                "start_time = {}",
                quote_string(&start_time.to_rfc3339())
            ));
        }
        Ok(format!(
            "CREATE TABLE {} USING {provider} LOCATION {} WITH ({})",
            quote_identifier(&self.name),
            quote_sanitized_string(&self.location),
            options.join(", ")
        ))
    }
}

impl RenderCreate for StreamDef {
    fn render_create(&self) -> Result<String> {
        let event_time = match self.event_time {
            EventTimePolicy::CaptureTime => "capture_time",
            EventTimePolicy::IngestTime => "ingest_time",
        };
        let transport = match self.transport {
            RtspTransport::Tcp => "tcp",
            RtspTransport::Udp => "udp",
        };
        Ok(format!(
            "CREATE STREAM {} FROM {} WITH (fps = {}, event_time = {}, watermark = INTERVAL '{}' MILLISECOND, transport = {})",
            quote_identifier(&self.name),
            quote_sanitized_string(&self.endpoint),
            self.fps,
            quote_string(event_time),
            self.watermark_delay_ms,
            quote_string(transport),
        ))
    }
}

impl RenderCreate for ModelDef {
    fn render_create(&self) -> Result<String> {
        let model_type = match self.model_type {
            ModelType::ObjectDetection => "OBJECT_DETECTION",
        };
        let runtime = self.runtime_kind.to_ascii_uppercase().replace('-', "_");
        let mut sql = format!(
            "CREATE MODEL {} TYPE {model_type} FROM {} USING {runtime}",
            quote_identifier(&self.name),
            quote_sanitized_string(&self.source),
        );
        if !self.options.is_empty() {
            let options = self
                .options
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{} = {}",
                        render_dotted_identifier(key),
                        render_json_value(value, key)
                    )
                })
                .collect::<Vec<_>>();
            write!(sql, " WITH ({})", options.join(", ")).expect("writing to String cannot fail");
        }
        Ok(sql)
    }
}

impl RenderCreate for FunctionDef {
    fn render_create(&self) -> Result<String> {
        let parameters = self
            .parameters
            .iter()
            .map(|(name, data_type)| {
                if name.starts_with('$') {
                    data_type.to_owned()
                } else {
                    format!("{} {data_type}", quote_identifier(name))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let prefix = format!(
            "CREATE FUNCTION {}({parameters}) RETURNS {}",
            quote_identifier(&self.name),
            self.return_type,
        );
        Ok(match &self.implementation {
            FunctionImplementation::SqlMacro { expression } => {
                format!("{prefix} RETURN {}", sanitize_sql_literals(expression))
            }
            FunctionImplementation::Python { entry } => format!(
                "{prefix} LANGUAGE PYTHON AS {}",
                quote_sanitized_string(entry)
            ),
        })
    }
}

impl RenderCreate for SinkDef {
    fn render_create(&self) -> Result<String> {
        match self.kind {
            SinkKind::Console => Ok(format!(
                "CREATE SINK {} TYPE CONSOLE",
                quote_identifier(&self.name)
            )),
            SinkKind::Kafka => {
                let kafka = self.kafka.as_ref().ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Catalog,
                        format!("Kafka Sink '{}' has no configuration", self.name),
                    )
                })?;
                Ok(render_kafka_sink(&self.name, kafka))
            }
        }
    }
}

fn render_kafka_sink(name: &str, kafka: &KafkaSinkConfig) -> String {
    let mut options = vec![
        format!(
            "bootstrap_servers = {}",
            quote_sanitized_string(&kafka.bootstrap_servers)
        ),
        format!("topic = {}", quote_sanitized_string(&kafka.topic)),
        "format = 'json'".to_owned(),
    ];
    if kafka.credential_ref.is_some() {
        options.push(format!(
            "credential_ref = {}",
            quote_string(REDACTED_SECRET_REFERENCE)
        ));
    }
    options.push(format!(
        "delivery_timeout_ms = {}",
        kafka.delivery_timeout_ms
    ));
    options.push(format!("buffer_capacity = {}", kafka.buffer_capacity));
    format!(
        "CREATE SINK {} TYPE KAFKA WITH ({})",
        quote_identifier(name),
        options.join(", ")
    )
}

const REDACTED_SECRET_REFERENCE: &str = "[REDACTED_SECRET_REF]";

fn render_json_value(value: &serde_json::Value, key: &str) -> String {
    if is_sensitive_key(key) {
        return quote_string(REDACTED_SECRET_REFERENCE);
    }
    match value {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => quote_sanitized_string(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| render_json_value(value, key))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        serde_json::Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(child_key, value)| format!(
                    "{} = {}",
                    quote_identifier(child_key),
                    render_json_value(value, child_key)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn render_dotted_identifier(value: &str) -> String {
    value
        .split('.')
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_sanitized_string(value: &str) -> String {
    quote_string(&sanitize_display_string(value))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["credential", "password", "secret", "token"]
        .iter()
        .any(|part| key.contains(part))
}

fn sanitize_display_string(value: &str) -> String {
    if value.to_ascii_lowercase().starts_with("secret://") {
        return REDACTED_SECRET_REFERENCE.to_owned();
    }
    let Ok(mut uri) = url::Url::parse(value) else {
        return value.to_owned();
    };
    if !uri.username().is_empty() || uri.password().is_some() {
        let _ = uri.set_username("");
        let _ = uri.set_password(None);
    }
    uri.set_query(None);
    uri.set_fragment(None);
    uri.to_string()
}

fn sanitize_sql_literals(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '\'' {
            output.push(character);
            continue;
        }
        let mut value = String::new();
        let mut closed = false;
        while let Some(character) = characters.next() {
            if character == '\'' {
                if characters.peek() == Some(&'\'') {
                    characters.next();
                    value.push('\'');
                } else {
                    closed = true;
                    break;
                }
            } else {
                value.push(character);
            }
        }
        output.push_str(&quote_sanitized_string(&value));
        if !closed {
            break;
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_uri_credentials_and_secret_references() {
        assert_eq!(
            sanitize_display_string("https://user:password@example.com/model.onnx?token=x#part"),
            "https://example.com/model.onnx"
        );
        assert_eq!(
            sanitize_display_string("secret://kafka/producer"),
            REDACTED_SECRET_REFERENCE
        );
        assert_eq!(
            sanitize_sql_literals("'https://user:pass@example.com/a?signature=x' || 'safe'"),
            "'https://example.com/a' || 'safe'"
        );
    }
}
