use std::fmt::Write;

use arrow::datatypes::{DataType, Schema};
use chrono::{DateTime, Utc};

use crate::catalog::{
    EventTimePolicy, FunctionDef, FunctionImplementation, KafkaTableConfig, ModelDef, ModelType,
    RtspTransport, TableDef, TableProvider,
};
use crate::{ErrorCode, Result, VqlError};

pub(crate) trait RenderCreate {
    fn render_create(&self) -> Result<String>;
}

pub(crate) fn render_create(definition: &impl RenderCreate) -> Result<String> {
    definition.render_create()
}

pub(crate) fn render_create_table(definition: &TableDef, schema: &Schema) -> Result<String> {
    let name = quote_identifier(&definition.name);
    Ok(match &definition.provider {
        TableProvider::Kafka(config) => {
            render_kafka_table(&name, config, &render_table_columns(schema)?)
        }
        _ => definition.render_create()?,
    })
}

impl RenderCreate for TableDef {
    fn render_create(&self) -> Result<String> {
        let name = quote_identifier(&self.name);
        Ok(match &self.provider {
            TableProvider::Images {
                location,
                recursive,
            } => format!(
                "CREATE TABLE {name} USING IMAGES OPTIONS (recursive = {}) LOCATION {}",
                quote_string(&recursive.to_string()),
                quote_sanitized_string(location),
            ),
            TableProvider::Videos {
                location,
                recursive,
                fps,
                start_time_ms,
            } => {
                let mut options = vec![format!(
                    "recursive = {}",
                    quote_string(&recursive.to_string())
                )];
                if let Some(fps) = fps {
                    options.push(format!("fps = {}", quote_string(&fps.to_string())));
                }
                if let Some(start_time_ms) = start_time_ms {
                    let start_time = DateTime::<Utc>::from_timestamp_millis(*start_time_ms)
                        .ok_or_else(|| {
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
                format!(
                    "CREATE TABLE {name} USING VIDEOS OPTIONS ({}) LOCATION {}",
                    options.join(", "),
                    quote_sanitized_string(location),
                )
            }
            TableProvider::Rtsp(config) => {
                let event_time = match config.event_time {
                    EventTimePolicy::CaptureTime => "capture_time",
                    EventTimePolicy::IngestTime => "ingest_time",
                };
                let transport = match config.transport {
                    RtspTransport::Tcp => "tcp",
                    RtspTransport::Udp => "udp",
                };
                format!(
                    "CREATE TABLE {name} USING RTSP OPTIONS (url = {}, fps = {}, event_time = {}, watermark = {}, transport = {})",
                    quote_sanitized_string(&config.endpoint),
                    quote_string(&config.fps.to_string()),
                    quote_string(event_time),
                    quote_string(&format!("{} milliseconds", config.watermark_delay_ms)),
                    quote_string(transport),
                )
            }
            TableProvider::Kafka(config) => render_kafka_table(&name, config, ""),
            TableProvider::External {
                data_source_format,
                storage_location,
            } => {
                let provider = data_source_format.as_deref().unwrap_or("EXTERNAL");
                match storage_location {
                    Some(location) => format!(
                        "CREATE TABLE {name} USING {provider} LOCATION {}",
                        quote_sanitized_string(location)
                    ),
                    None => format!("CREATE TABLE {name} USING {provider}"),
                }
            }
        })
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

fn render_kafka_table(name: &str, kafka: &KafkaTableConfig, columns: &str) -> String {
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
        quote_string(&kafka.delivery_timeout_ms.to_string())
    ));
    options.push(format!(
        "buffer_capacity = {}",
        quote_string(&kafka.buffer_capacity.to_string())
    ));
    format!(
        "CREATE TABLE {name}{columns} USING KAFKA OPTIONS ({})",
        options.join(", ")
    )
}

fn render_table_columns(schema: &Schema) -> Result<String> {
    if schema.fields().is_empty() {
        return Ok(String::new());
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING",
                DataType::Int64 => "BIGINT",
                DataType::Int32 => "INT",
                DataType::Boolean => "BOOLEAN",
                DataType::Float32 => "FLOAT",
                DataType::Float64 => "DOUBLE",
                DataType::Timestamp(_, _) => "TIMESTAMP",
                data_type => {
                    return Err(VqlError::new(
                        ErrorCode::Catalog,
                        format!(
                            "table column '{}' has unsupported SQL type '{data_type}'",
                            field.name()
                        ),
                    ));
                }
            };
            Ok(format!(
                "{} {data_type}{}",
                quote_identifier(field.name()),
                if field.is_nullable() { "" } else { " NOT NULL" }
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(" ({})", columns.join(", ")))
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
