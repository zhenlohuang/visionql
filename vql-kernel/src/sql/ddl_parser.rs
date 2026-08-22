use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use std::collections::BTreeMap;

use super::ast::{CreateModel, CreateTable, ShowKind, TableColumn, VqlStatement};
use crate::catalog::{
    EventTimePolicy, KafkaTableConfig, ModelType, RtspTableConfig, RtspTransport, TableProvider,
};
use crate::{ErrorCode, Result, VqlError};

pub(crate) fn parse_statement(sql: &str) -> Result<VqlStatement> {
    if sql.contains("<->") {
        return Err(VqlError::feature(
            "vector distance operator is not available",
            "v0.3",
        ));
    }
    let tokens = significant_tokens(sql)?;
    let first = word(tokens.first()).unwrap_or_default();
    match first.as_str() {
        "CREATE" if token_is(tokens.get(1), "FUNCTION") => Ok(VqlStatement::CreateFunction {
            sql: sql.to_owned(),
        }),
        "CREATE" => parse_create(&tokens),
        "RESOLVE" => parse_resolve(&tokens),
        "ALTER" => invalid("ALTER is not supported; DROP and recreate the object"),
        "DROP" => parse_drop(&tokens),
        "SHOW" => parse_show(&tokens),
        "DESCRIBE" | "DESC" => parse_describe(&tokens),
        "EXPLAIN" => Ok(VqlStatement::Explain {
            sql: sql.to_owned(),
        }),
        "SET" => Ok(VqlStatement::Set {
            sql: sql.to_owned(),
        }),
        "SELECT" | "WITH" | "VALUES" | "INSERT" => Ok(VqlStatement::Query {
            sql: sql.to_owned(),
        }),
        "SUBMIT" | "PAUSE" | "RESUME" | "STOP" => Err(VqlError::feature(
            format!("{first} requires a durable-query service and is not available"),
            "v0.2",
        )),
        _ => Err(VqlError::new(
            ErrorCode::InvalidSql,
            format!("unsupported or empty statement starting with '{first}'"),
        )),
    }
}

fn parse_resolve(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() != 3 {
        return invalid("expected RESOLVE MODEL <name>");
    }
    expect_word(tokens.get(1), "MODEL")?;
    Ok(VqlStatement::ResolveModel {
        name: identifier(tokens.get(2), "model name")?,
    })
}

fn parse_create(tokens: &[Token]) -> Result<VqlStatement> {
    match word(tokens.get(1)).as_deref() {
        Some("TABLE") if token_is(tokens.get(2), "FUNCTION") => Err(VqlError::feature(
            "table functions are not available",
            "未排期",
        )),
        Some("TABLE") => parse_create_table(tokens),
        Some("MODEL") => parse_create_model(tokens),
        Some("INDEX") => Err(VqlError::feature(
            "vector indexes are not available",
            "v0.3",
        )),
        Some("AGGREGATE") | Some("TABLE_FUNCTION") => Err(VqlError::feature(
            "aggregate and table functions are not available",
            "未排期",
        )),
        _ => invalid("expected CREATE TABLE, MODEL, or FUNCTION"),
    }
}

fn parse_create_table(tokens: &[Token]) -> Result<VqlStatement> {
    let name = identifier(tokens.get(2), "table name")?;
    let mut index = 3;
    let columns = if tokens.get(index) == Some(&Token::LParen) {
        parse_table_columns(tokens, &mut index)?
    } else {
        Vec::new()
    };
    if token_is(tokens.get(index), "AS") {
        return Err(VqlError::feature(
            "CREATE TABLE AS SELECT is not available",
            "v0.3",
        ));
    }
    expect_word(tokens.get(index), "USING")?;
    index += 1;
    let provider_name = word(tokens.get(index))
        .ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "expected provider after USING"))?;
    index += 1;
    let mut location = None;
    let mut options = BTreeMap::new();
    while index < tokens.len() {
        match word(tokens.get(index)).as_deref() {
            Some("OPTIONS") => parse_table_options(tokens, &mut index, &mut options)?,
            Some("LOCATION") => {
                if location.is_some() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        "duplicate LOCATION clause",
                    ));
                }
                index += 1;
                location = Some(string_literal(tokens.get(index))?);
                index += 1;
            }
            _ => return invalid("expected OPTIONS (...) or LOCATION '<path>'"),
        }
    }
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE TABLE");
    }
    let provider = build_table_provider(&provider_name, location, options)?;
    if !columns.is_empty()
        && !matches!(
            provider,
            TableProvider::Kafka(_) | TableProvider::External { .. }
        )
    {
        return invalid("an explicit column list is supported only for writable table providers");
    }
    Ok(VqlStatement::CreateTable(CreateTable {
        name,
        provider,
        columns,
    }))
}

fn parse_table_columns(tokens: &[Token], index: &mut usize) -> Result<Vec<TableColumn>> {
    *index += 1;
    let mut columns = Vec::new();
    while tokens.get(*index) != Some(&Token::RParen) {
        let name = identifier(tokens.get(*index), "column name")?;
        *index += 1;
        let data_type = identifier(tokens.get(*index), "column type")?.to_ascii_uppercase();
        *index += 1;
        let nullable = if token_is(tokens.get(*index), "NOT") {
            *index += 1;
            expect_word(tokens.get(*index), "NULL")?;
            *index += 1;
            false
        } else {
            true
        };
        columns.push(TableColumn {
            name,
            data_type,
            nullable,
        });
        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::RParen) => {}
            _ => return invalid("expected ',' or ')' after column definition"),
        }
    }
    *index += 1;
    Ok(columns)
}

fn parse_table_options(
    tokens: &[Token],
    index: &mut usize,
    options: &mut BTreeMap<String, String>,
) -> Result<()> {
    *index += 1;
    expect_token(tokens.get(*index), Token::LParen, "'(' after OPTIONS")?;
    *index += 1;
    while tokens.get(*index) != Some(&Token::RParen) {
        let key = match tokens.get(*index) {
            Some(Token::Word(value)) => value.value.to_ascii_lowercase(),
            Some(Token::SingleQuotedString(value)) | Some(Token::DoubleQuotedString(value)) => {
                value.to_ascii_lowercase()
            }
            _ => return invalid("expected option name"),
        };
        *index += 1;
        expect_token(tokens.get(*index), Token::Eq, "'=' after option name")?;
        *index += 1;
        let value = option_string(tokens.get(*index))?;
        *index += 1;
        if options.insert(key.clone(), value).is_some() {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("duplicate table option '{key}'"),
            ));
        }
        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::RParen) => {}
            _ => return invalid("expected ',' or ')' after table option"),
        }
    }
    *index += 1;
    Ok(())
}

fn build_table_provider(
    provider: &str,
    location: Option<String>,
    mut options: BTreeMap<String, String>,
) -> Result<TableProvider> {
    let provider = match provider {
        "IMAGES" => TableProvider::Images {
            location: required_location(location, "IMAGES")?,
            recursive: take_bool(&mut options, "recursive", false)?,
        },
        "VIDEOS" => {
            let fps = take_number(&mut options, "fps")?;
            if fps.is_some_and(|fps| !fps.is_finite() || fps <= 0.0 || fps > 120.0) {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "fps must be greater than 0 and at most 120",
                ));
            }
            let start_time_ms = options
                .remove("start_time")
                .map(|value| {
                    chrono::DateTime::parse_from_rfc3339(&value)
                        .map(|value| value.timestamp_millis())
                        .map_err(|error| {
                            VqlError::new(
                                ErrorCode::InvalidOption,
                                "start_time must be an RFC 3339 timestamp",
                            )
                            .with_source(error)
                        })
                })
                .transpose()?;
            TableProvider::Videos {
                location: required_location(location, "VIDEOS")?,
                recursive: take_bool(&mut options, "recursive", false)?,
                fps,
                start_time_ms,
            }
        }
        "RTSP" => {
            if location.is_some() {
                return invalid("USING RTSP uses OPTIONS (url = 'rtsp://...'), not LOCATION");
            }
            let endpoint = required_option(&mut options, "url", "RTSP")?;
            let fps = take_number(&mut options, "fps")?.unwrap_or(5.0);
            if !fps.is_finite() || fps <= 0.0 || fps > 120.0 {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "fps must be greater than 0 and at most 120",
                ));
            }
            let event_time = match options
                .remove("event_time")
                .unwrap_or_else(|| "capture_time".to_owned())
                .to_ascii_lowercase()
                .as_str()
            {
                "capture_time" => EventTimePolicy::CaptureTime,
                "ingest_time" => EventTimePolicy::IngestTime,
                _ => {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        "event_time must be 'capture_time' or 'ingest_time'",
                    ));
                }
            };
            let watermark_delay_ms = options
                .remove("watermark")
                .map(|value| parse_duration_ms(&value))
                .transpose()?
                .unwrap_or(2_000);
            let transport = match options
                .remove("transport")
                .unwrap_or_else(|| "tcp".to_owned())
                .to_ascii_lowercase()
                .as_str()
            {
                "tcp" => RtspTransport::Tcp,
                "udp" => RtspTransport::Udp,
                _ => {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        "transport must be 'tcp' or 'udp'",
                    ));
                }
            };
            TableProvider::Rtsp(RtspTableConfig {
                name: String::new(),
                endpoint,
                fps,
                event_time,
                watermark_delay_ms,
                transport,
            })
        }
        "KAFKA" => {
            if location.is_some() {
                return invalid("USING KAFKA uses OPTIONS, not LOCATION");
            }
            let bootstrap_servers = required_option(&mut options, "bootstrap_servers", "KAFKA")?;
            validate_kafka_bootstrap_servers(&bootstrap_servers)?;
            let topic = required_option(&mut options, "topic", "KAFKA")?;
            validate_kafka_topic(&topic)?;
            let format = options
                .remove("format")
                .unwrap_or_else(|| "json".to_owned());
            if !format.eq_ignore_ascii_case("json") {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "KAFKA format must be 'json'",
                ));
            }
            let credential_ref = options.remove("credential_ref");
            if let Some(reference) = credential_ref.as_deref()
                && (reference.is_empty()
                    || reference.trim() != reference
                    || reference.len() > 1_024
                    || reference.chars().any(char::is_control))
            {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "credential_ref must be a non-empty opaque reference of at most 1024 characters",
                ));
            }
            let delivery_timeout_ms =
                take_unsigned(&mut options, "delivery_timeout_ms")?.unwrap_or(30_000);
            let buffer_capacity = take_unsigned(&mut options, "buffer_capacity")?
                .map(usize::try_from)
                .transpose()
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        "buffer_capacity is too large for this platform",
                    )
                    .with_source(error)
                })?
                .unwrap_or(1_024);
            if !(1..=3_600_000).contains(&delivery_timeout_ms) {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "delivery_timeout_ms must be between 1 and 3600000",
                ));
            }
            if !(1..=100_000).contains(&buffer_capacity) {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "buffer_capacity must be between 1 and 100000",
                ));
            }
            TableProvider::Kafka(KafkaTableConfig {
                bootstrap_servers,
                topic,
                credential_ref,
                delivery_timeout_ms,
                buffer_capacity,
            })
        }
        "PARQUET" | "LANCE" | "HNSW" => {
            return Err(VqlError::feature(
                "columnar/vector providers are not available",
                "v0.3",
            ));
        }
        _ => {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("unsupported table provider '{provider}'"),
            ));
        }
    };
    if let Some(option) = options.keys().next() {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("unknown {provider:?} option '{option}'"),
        ));
    }
    Ok(provider)
}

fn required_location(location: Option<String>, provider: &str) -> Result<String> {
    location.ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            format!("USING {provider} requires LOCATION"),
        )
    })
}

fn required_option(
    options: &mut BTreeMap<String, String>,
    name: &str,
    provider: &str,
) -> Result<String> {
    options.remove(name).ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            format!("{provider} requires option '{name}'"),
        )
    })
}

fn take_bool(options: &mut BTreeMap<String, String>, name: &str, default: bool) -> Result<bool> {
    options
        .remove(name)
        .map(|value| {
            value.parse::<bool>().map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("{name} must be true or false"),
                )
                .with_source(error)
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn take_number(options: &mut BTreeMap<String, String>, name: &str) -> Result<Option<f64>> {
    options
        .remove(name)
        .map(|value| {
            value.parse::<f64>().map_err(|error| {
                VqlError::new(ErrorCode::InvalidOption, format!("{name} must be numeric"))
                    .with_source(error)
            })
        })
        .transpose()
}

fn take_unsigned(options: &mut BTreeMap<String, String>, name: &str) -> Result<Option<u64>> {
    options
        .remove(name)
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("{name} must be a non-negative integer"),
                )
                .with_source(error)
            })
        })
        .transpose()
}

fn option_string(token: Option<&Token>) -> Result<String> {
    match token {
        Some(Token::SingleQuotedString(value)) | Some(Token::DoubleQuotedString(value)) => {
            Ok(value.clone())
        }
        Some(Token::Number(value, _)) => Ok(value.clone()),
        Some(Token::Word(value)) => Ok(value.value.clone()),
        _ => invalid("table option values must be strings, numbers, or booleans"),
    }
}

fn parse_duration_ms(value: &str) -> Result<i64> {
    let mut parts = value.split_whitespace();
    let number = parts
        .next()
        .ok_or_else(|| VqlError::new(ErrorCode::InvalidOption, "watermark is empty"))?
        .parse::<f64>()
        .map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "watermark must start with a number",
            )
            .with_source(error)
        })?;
    let multiplier = match parts.next().map(str::to_ascii_lowercase).as_deref() {
        Some("millisecond" | "milliseconds" | "ms") => 1.0,
        Some("second" | "seconds" | "s") => 1_000.0,
        Some("minute" | "minutes" | "m") => 60_000.0,
        _ => {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "watermark must use milliseconds, seconds, or minutes",
            ));
        }
    };
    if parts.next().is_some() || !number.is_finite() || number < 0.0 {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "watermark must be a finite non-negative duration",
        ));
    }
    Ok((number * multiplier).round() as i64)
}

fn parse_create_model(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() < 9 {
        return invalid(
            "expected CREATE MODEL <name> TYPE OBJECT_DETECTION FROM '<source>' USING <runtime>",
        );
    }
    let name = identifier(tokens.get(2), "model name")?;
    expect_word(tokens.get(3), "TYPE")?;
    let model_type = match word(tokens.get(4)).as_deref() {
        Some("OBJECT_DETECTION") => ModelType::ObjectDetection,
        Some("IMAGE_EMBEDDING") | Some("TEXT_EMBEDDING") => {
            return Err(VqlError::feature(
                "embedding Model types are not available",
                "v0.3",
            ));
        }
        Some("IMAGE_CLASSIFICATION") | Some("TEXT_GENERATION") => {
            return Err(VqlError::feature(
                "this Model type is not available",
                "未排期",
            ));
        }
        _ => return invalid("the only supported Model TYPE is OBJECT_DETECTION"),
    };
    expect_word(tokens.get(5), "FROM")?;
    let source = string_literal(tokens.get(6))?;
    expect_word(tokens.get(7), "USING")?;
    let runtime_kind = identifier(tokens.get(8), "Runtime")?
        .to_ascii_lowercase()
        .replace('_', "-");
    let mut index = 9;
    let mut options = BTreeMap::new();
    if index < tokens.len() {
        expect_word(tokens.get(index), "WITH")?;
        parse_model_options(tokens, &mut index, &mut options)?;
    }
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE MODEL");
    }
    Ok(VqlStatement::CreateModel(CreateModel {
        name,
        model_type,
        source,
        runtime_kind,
        options,
    }))
}

fn parse_model_options(
    tokens: &[Token],
    index: &mut usize,
    options: &mut BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    *index += 1;
    expect_token(tokens.get(*index), Token::LParen, "'(' after WITH")?;
    *index += 1;
    while tokens.get(*index) != Some(&Token::RParen) {
        let key = dotted_identifier(tokens, index, "Model option")?;
        expect_token(tokens.get(*index), Token::Eq, "'=' after Model option")?;
        *index += 1;
        let value = parse_json_value(tokens, index)?;
        if options.insert(key.clone(), value).is_some() {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("duplicate Model option '{key}'"),
            ));
        }
        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::RParen) => {}
            _ => return invalid("expected ',' or ')' after Model option"),
        }
    }
    *index += 1;
    Ok(())
}

fn dotted_identifier(tokens: &[Token], index: &mut usize, label: &str) -> Result<String> {
    let mut parts = vec![identifier(tokens.get(*index), label)?.to_ascii_lowercase()];
    *index += 1;
    while tokens.get(*index) == Some(&Token::Period) {
        *index += 1;
        parts.push(identifier(tokens.get(*index), label)?.to_ascii_lowercase());
        *index += 1;
    }
    Ok(parts.join("."))
}

fn parse_json_value(tokens: &[Token], index: &mut usize) -> Result<serde_json::Value> {
    match tokens.get(*index) {
        Some(Token::SingleQuotedString(value)) | Some(Token::DoubleQuotedString(value)) => {
            *index += 1;
            Ok(serde_json::Value::String(value.clone()))
        }
        Some(Token::Number(value, _)) => {
            *index += 1;
            if !value.contains(['.', 'e', 'E']) {
                let value = value.parse::<u64>().map_err(|error| {
                    VqlError::new(ErrorCode::InvalidOption, "option must be numeric")
                        .with_source(error)
                })?;
                return Ok(serde_json::json!(value));
            }
            let value = value.parse::<f64>().map_err(|error| {
                VqlError::new(ErrorCode::InvalidOption, "option must be numeric").with_source(error)
            })?;
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| VqlError::new(ErrorCode::InvalidOption, "option must be finite"))
        }
        Some(Token::LBracket) => {
            *index += 1;
            let mut values = Vec::new();
            while tokens.get(*index) != Some(&Token::RBracket) {
                values.push(parse_json_value(tokens, index)?);
                match tokens.get(*index) {
                    Some(Token::Comma) => *index += 1,
                    Some(Token::RBracket) => {}
                    _ => return invalid("expected ',' or ']' in option array"),
                }
            }
            *index += 1;
            Ok(serde_json::Value::Array(values))
        }
        Some(Token::LBrace) => {
            *index += 1;
            let mut values = serde_json::Map::new();
            while tokens.get(*index) != Some(&Token::RBrace) {
                let key = identifier(tokens.get(*index), "option object key")?.to_ascii_lowercase();
                *index += 1;
                expect_token(tokens.get(*index), Token::Eq, "'=' after option object key")?;
                *index += 1;
                let value = parse_json_value(tokens, index)?;
                if values.insert(key.clone(), value).is_some() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!("duplicate option object key '{key}'"),
                    ));
                }
                match tokens.get(*index) {
                    Some(Token::Comma) => *index += 1,
                    Some(Token::RBrace) => {}
                    _ => return invalid("expected ',' or '}' in option object"),
                }
            }
            *index += 1;
            Ok(serde_json::Value::Object(values))
        }
        Some(Token::Word(value)) if value.value.eq_ignore_ascii_case("true") => {
            *index += 1;
            Ok(serde_json::Value::Bool(true))
        }
        Some(Token::Word(value)) if value.value.eq_ignore_ascii_case("false") => {
            *index += 1;
            Ok(serde_json::Value::Bool(false))
        }
        Some(Token::Word(value)) if value.value.eq_ignore_ascii_case("null") => {
            *index += 1;
            Ok(serde_json::Value::Null)
        }
        _ => invalid("unsupported Model option value"),
    }
}

fn validate_kafka_bootstrap_servers(value: &str) -> Result<()> {
    let invalid = || {
        VqlError::new(
            ErrorCode::InvalidOption,
            "bootstrap_servers must be a comma-separated list of host:port endpoints without URI schemes or credentials",
        )
    };
    let endpoints = value.split(',').collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(invalid());
    }
    for endpoint in endpoints {
        let endpoint = endpoint.trim();
        if endpoint.is_empty()
            || endpoint.chars().any(|character| {
                character.is_whitespace() || matches!(character, '@' | '/' | '?' | '#')
            })
        {
            return Err(invalid());
        }
        let (host, port) = if let Some(bracketed) = endpoint.strip_prefix('[') {
            let (host, port) = bracketed.split_once("]:").ok_or_else(&invalid)?;
            if host.is_empty() || host.contains('[') || host.contains(']') {
                return Err(invalid());
            }
            (host, port)
        } else {
            let (host, port) = endpoint.rsplit_once(':').ok_or_else(&invalid)?;
            if host.is_empty() || host.contains(':') || host.contains('[') || host.contains(']') {
                return Err(invalid());
            }
            (host, port)
        };
        if host.is_empty() || port.parse::<u16>().ok().filter(|port| *port > 0).is_none() {
            return Err(invalid());
        }
    }
    Ok(())
}

fn validate_kafka_topic(topic: &str) -> Result<()> {
    if topic.is_empty()
        || topic.len() > 249
        || matches!(topic, "." | "..")
        || !topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "topic must be 1-249 ASCII letters, digits, '.', '_', or '-' and cannot be '.' or '..'",
        ));
    }
    Ok(())
}

fn parse_drop(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() != 3 {
        return invalid("expected DROP <object kind> <name>");
    }
    let kind = singular_kind(tokens.get(1))?;
    Ok(VqlStatement::Drop {
        kind,
        name: identifier(tokens.get(2), "object name")?,
    })
}

fn parse_show(tokens: &[Token]) -> Result<VqlStatement> {
    if token_is(tokens.get(1), "CREATE") {
        if tokens.len() != 4 {
            return invalid("expected SHOW CREATE <object kind> <name>");
        }
        return Ok(VqlStatement::ShowCreate {
            kind: singular_kind(tokens.get(2))?,
            name: identifier(tokens.get(3), "object name")?,
        });
    }
    if tokens.len() != 2 {
        return invalid("expected SHOW TABLES, MODELS, FUNCTIONS, or SHOW CREATE <kind> <name>");
    }
    let kind = match word(tokens.get(1)).as_deref() {
        Some("TABLES") => ShowKind::Tables,
        Some("MODELS") => ShowKind::Models,
        Some("FUNCTIONS") => ShowKind::Functions,
        _ => return invalid("expected SHOW TABLES, MODELS, or FUNCTIONS"),
    };
    Ok(VqlStatement::Show(kind))
}

fn singular_kind(token: Option<&Token>) -> Result<ShowKind> {
    match word(token).as_deref() {
        Some("TABLE") => Ok(ShowKind::Tables),
        Some("MODEL") => Ok(ShowKind::Models),
        Some("FUNCTION") => Ok(ShowKind::Functions),
        _ => invalid("unsupported object kind"),
    }
}

fn parse_describe(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() == 2 {
        Ok(VqlStatement::Describe {
            name: identifier(tokens.get(1), "table name")?,
        })
    } else {
        invalid("expected DESCRIBE <table>")
    }
}

fn significant_tokens(sql: &str) -> Result<Vec<Token>> {
    let dialect = GenericDialect {};
    Tokenizer::new(&dialect, sql)
        .tokenize()
        .map(|tokens| {
            tokens
                .into_iter()
                .filter(|token| !matches!(token, Token::Whitespace(_)))
                .collect()
        })
        .map_err(|error| VqlError::new(ErrorCode::InvalidSql, error.to_string()))
}

fn word(token: Option<&Token>) -> Option<String> {
    match token {
        Some(Token::Word(word)) => Some(word.value.to_ascii_uppercase()),
        _ => None,
    }
}

fn token_is(token: Option<&Token>, expected: &str) -> bool {
    word(token).as_deref() == Some(expected)
}

fn expect_word(token: Option<&Token>, expected: &str) -> Result<()> {
    if token_is(token, expected) {
        Ok(())
    } else {
        invalid(format!("expected {expected}"))
    }
}

fn identifier(token: Option<&Token>, label: &str) -> Result<String> {
    match token {
        Some(Token::Word(word)) => Ok(word.value.clone()),
        _ => invalid(format!("expected {label}")),
    }
}

fn string_literal(token: Option<&Token>) -> Result<String> {
    match token {
        Some(Token::SingleQuotedString(value)) | Some(Token::DoubleQuotedString(value)) => {
            Ok(value.clone())
        }
        _ => invalid("expected quoted string literal"),
    }
}

fn expect_token(token: Option<&Token>, expected: Token, label: &str) -> Result<()> {
    if token == Some(&expected) {
        Ok(())
    } else {
        invalid(format!("expected {label}"))
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(VqlError::new(ErrorCode::InvalidSql, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_images_ddl() {
        let parsed = parse_statement(
            "CREATE TABLE photos USING IMAGES OPTIONS (recursive = 'true') LOCATION './photos'",
        )
        .unwrap();
        assert_eq!(
            parsed,
            VqlStatement::CreateTable(CreateTable {
                name: "photos".to_owned(),
                provider: TableProvider::Images {
                    location: "./photos".to_owned(),
                    recursive: true,
                },
                columns: Vec::new(),
            })
        );
    }

    #[test]
    fn parses_videos_options() {
        let parsed = parse_statement(
            "CREATE TABLE clips USING VIDEOS OPTIONS \
             (fps = '5', recursive = 'true', start_time = '2026-08-08T00:00:00Z') LOCATION './clips'",
        )
        .unwrap();
        let VqlStatement::CreateTable(create) = parsed else {
            panic!("expected CREATE TABLE")
        };
        assert_eq!(
            create.provider,
            TableProvider::Videos {
                location: "./clips".to_owned(),
                recursive: true,
                fps: Some(5.0),
                start_time_ms: Some(1_786_147_200_000),
            }
        );
    }

    #[test]
    fn parses_rtsp_table_options() {
        let parsed = parse_statement(
            "CREATE TABLE cam_entrance USING RTSP OPTIONS (\
             url = 'rtsp://10.0.0.15:554/main', fps = '5', event_time = 'capture_time', \
             watermark = '2 seconds', transport = 'tcp')",
        )
        .unwrap();
        assert_eq!(
            parsed,
            VqlStatement::CreateTable(CreateTable {
                name: "cam_entrance".to_owned(),
                provider: TableProvider::Rtsp(RtspTableConfig {
                    name: String::new(),
                    endpoint: "rtsp://10.0.0.15:554/main".to_owned(),
                    fps: 5.0,
                    event_time: EventTimePolicy::CaptureTime,
                    watermark_delay_ms: 2_000,
                    transport: RtspTransport::Tcp,
                }),
                columns: Vec::new(),
            })
        );
    }

    #[test]
    fn parses_kafka_table_options() {
        let parsed = parse_statement(
            "CREATE TABLE people_per_minute (people BIGINT) USING KAFKA OPTIONS (\
             bootstrap_servers = 'broker-1:9092,broker-2:9092', \
             topic = 'people-per-minute', format = 'json', \
             credential_ref = 'secret://kafka/producer', \
             delivery_timeout_ms = '45000', buffer_capacity = '256')",
        )
        .unwrap();

        assert_eq!(
            parsed,
            VqlStatement::CreateTable(CreateTable {
                name: "people_per_minute".to_owned(),
                provider: TableProvider::Kafka(KafkaTableConfig {
                    bootstrap_servers: "broker-1:9092,broker-2:9092".to_owned(),
                    topic: "people-per-minute".to_owned(),
                    credential_ref: Some("secret://kafka/producer".to_owned()),
                    delivery_timeout_ms: 45_000,
                    buffer_capacity: 256,
                }),
                columns: vec![TableColumn {
                    name: "people".to_owned(),
                    data_type: "BIGINT".to_owned(),
                    nullable: true,
                }],
            })
        );
    }

    #[test]
    fn rejects_invalid_kafka_table_options() {
        for sql in [
            "CREATE TABLE out USING KAFKA OPTIONS (topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'http://broker:9092', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'user:secret@broker:9092', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:0', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:70000', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092,', topic = 'events')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'bad topic')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', format = 'avro')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', delivery_timeout_ms = '0')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', buffer_capacity = '100001')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', topic = 'other')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', retries = '3')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', credential_ref = '')",
            "CREATE TABLE out USING KAFKA OPTIONS (bootstrap_servers = 'broker:9092', topic = 'events', username = 'user')",
        ] {
            assert_eq!(
                parse_statement(sql).unwrap_err().code,
                ErrorCode::InvalidOption,
                "{sql}"
            );
        }
    }

    #[test]
    fn rejects_invalid_rtsp_table_options() {
        for sql in [
            "CREATE TABLE cam USING RTSP OPTIONS (url = 'rtsp://camera/live', fps = '0')",
            "CREATE TABLE cam USING RTSP OPTIONS (url = 'rtsp://camera/live', event_time = 'wall_time')",
            "CREATE TABLE cam USING RTSP OPTIONS (url = 'rtsp://camera/live', watermark = '-1 second')",
            "CREATE TABLE cam USING RTSP OPTIONS (url = 'rtsp://camera/live', transport = 'quic')",
        ] {
            assert_eq!(
                parse_statement(sql).unwrap_err().code,
                ErrorCode::InvalidOption
            );
        }
    }

    #[test]
    fn rejects_unknown_images_option() {
        let error = parse_statement(
            "CREATE TABLE photos USING IMAGES OPTIONS (magic = 'true') LOCATION './photos'",
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
    }

    #[test]
    fn rejects_removed_stream_and_sink_object_syntax() {
        for sql in [
            "CREATE STREAM camera FROM 'rtsp://camera/live'",
            "CREATE SINK events TYPE KAFKA",
            "SHOW STREAMS",
            "SHOW SINKS",
        ] {
            assert_eq!(
                parse_statement(sql).unwrap_err().code,
                ErrorCode::InvalidSql,
                "{sql}"
            );
        }
    }

    #[test]
    fn parses_runtime_scoped_model_options() {
        let parsed = parse_statement(
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'file:///model.onnx' \
             USING ONNX_RUNTIME \
             WITH (input={name='images', width=32, height=24}, \
                   output={name='output0', format='yolo_e2e', labels=['person']})",
        )
        .unwrap();
        let VqlStatement::CreateModel(create) = parsed else {
            panic!("expected CREATE MODEL")
        };

        assert_eq!(create.runtime_kind, "onnx-runtime");
        assert_eq!(
            create.options["input"],
            serde_json::json!({"name": "images", "width": 32, "height": 24})
        );
        assert_eq!(
            create.options["output"],
            serde_json::json!({"name": "output0", "format": "yolo_e2e", "labels": ["person"]})
        );
    }

    #[test]
    fn parses_resolve_model() {
        assert_eq!(
            parse_statement("RESOLVE MODEL detector").unwrap(),
            VqlStatement::ResolveModel {
                name: "detector".to_owned(),
            }
        );
    }

    #[test]
    fn parses_show_create() {
        assert_eq!(
            parse_statement("SHOW CREATE MODEL detector").unwrap(),
            VqlStatement::ShowCreate {
                kind: ShowKind::Models,
                name: "detector".to_owned(),
            }
        );
        assert!(parse_statement("SHOW CREATE MODEL").is_err());
        assert!(parse_statement("SHOW CREATE VIEW example").is_err());
    }

    #[test]
    fn parses_positional_sql_expression_function() {
        let sql = "CREATE FUNCTION plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1";
        let parsed = parse_statement(sql).unwrap();
        let VqlStatement::CreateFunction { sql: parsed_sql } = parsed else {
            panic!("expected CREATE FUNCTION")
        };
        assert_eq!(parsed_sql, sql);
    }

    #[test]
    fn future_capabilities_have_stable_target_versions() {
        let cases = [
            ("CREATE TABLE out USING PARQUET LOCATION './out'", "v0.3"),
            ("CREATE TABLE out AS SELECT 1", "v0.3"),
            ("CREATE INDEX idx USING HNSW", "v0.3"),
            (
                "CREATE MODEL clip TYPE IMAGE_EMBEDDING(512) FROM 'model.safetensors' USING TRANSFORMERS",
                "v0.3",
            ),
            (
                "CREATE MODEL classifier TYPE IMAGE_CLASSIFICATION FROM 'model.onnx' USING ONNX_RUNTIME",
                "未排期",
            ),
            (
                "CREATE MODEL generator TYPE TEXT_GENERATION FROM 'model.gguf' USING LLAMA_CPP",
                "未排期",
            ),
            ("SELECT embedding <-> other FROM values", "v0.3"),
            ("SUBMIT QUERY q AS SELECT 1", "v0.2"),
            ("CREATE AGGREGATE FUNCTION f", "未排期"),
            ("CREATE TABLE FUNCTION f", "未排期"),
        ];
        for (sql, target) in cases {
            let error = parse_statement(sql).unwrap_err();
            assert_eq!(error.code, ErrorCode::FeatureNotAvailable, "{sql}");
            assert_eq!(error.target_version.as_deref(), Some(target), "{sql}");
        }
    }
}
