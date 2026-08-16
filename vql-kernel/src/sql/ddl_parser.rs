use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use std::collections::BTreeMap;

use super::ast::{CreateModel, CreateStream, CreateTable, ShowKind, VqlStatement};
use crate::catalog::{
    EventTimePolicy, KafkaSinkConfig, ModelType, RtspTransport, SinkKind, TableProviderKind,
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
            format!("{first} is not available in v0.1"),
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
        Some("SINK") => parse_create_sink(tokens),
        Some("STREAM") => parse_create_stream(tokens),
        Some("INDEX") => Err(VqlError::feature(
            "vector indexes are not available",
            "v0.3",
        )),
        Some("AGGREGATE") | Some("TABLE_FUNCTION") => Err(VqlError::feature(
            "aggregate and table functions are not available",
            "未排期",
        )),
        _ => invalid("expected CREATE TABLE, MODEL, FUNCTION, or SINK"),
    }
}

fn parse_create_stream(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() < 5 {
        return invalid("expected CREATE STREAM <name> FROM 'rtsp://...'");
    }
    let name = identifier(tokens.get(2), "stream name")?;
    expect_word(tokens.get(3), "FROM")?;
    let endpoint = string_literal(tokens.get(4))?;
    let mut fps = 5.0;
    let mut event_time = EventTimePolicy::CaptureTime;
    let mut watermark_delay_ms = 2_000;
    let mut transport = RtspTransport::Tcp;
    let mut index = 5;
    if index < tokens.len() {
        expect_word(tokens.get(index), "WITH")?;
        index += 1;
        expect_token(tokens.get(index), Token::LParen, "'(' after WITH")?;
        index += 1;
        while tokens.get(index) != Some(&Token::RParen) {
            let option = identifier(tokens.get(index), "stream option")?.to_ascii_lowercase();
            index += 1;
            expect_token(tokens.get(index), Token::Eq, "'=' after stream option")?;
            index += 1;
            match option.as_str() {
                "fps" => {
                    fps = parse_number(tokens.get(index), "fps")?;
                    if !fps.is_finite() || fps <= 0.0 || fps > 120.0 {
                        return Err(VqlError::new(
                            ErrorCode::InvalidOption,
                            "fps must be greater than 0 and at most 120",
                        ));
                    }
                    index += 1;
                }
                "event_time" => {
                    event_time = match string_literal(tokens.get(index))?
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
                    index += 1;
                }
                "watermark" => {
                    let (value, consumed) = parse_fixed_interval_ms(&tokens[index..])?;
                    watermark_delay_ms = value;
                    index += consumed;
                }
                "transport" => {
                    transport = match string_literal(tokens.get(index))?
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
                    index += 1;
                }
                _ => {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!("unknown RTSP option '{option}'"),
                    ));
                }
            }
            match tokens.get(index) {
                Some(Token::Comma) => index += 1,
                Some(Token::RParen) => {}
                _ => return invalid("expected ',' or ')' after stream option"),
            }
        }
        index += 1;
    }
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE STREAM");
    }
    Ok(VqlStatement::CreateStream(CreateStream {
        name,
        endpoint,
        fps,
        event_time,
        watermark_delay_ms,
        transport,
    }))
}

fn parse_fixed_interval_ms(tokens: &[Token]) -> Result<(i64, usize)> {
    expect_word(tokens.first(), "INTERVAL")?;
    let raw = string_literal(tokens.get(1))?;
    let value = raw.parse::<f64>().map_err(|error| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "watermark interval must be numeric",
        )
        .with_source(error)
    })?;
    if !value.is_finite() || value < 0.0 {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "watermark interval must be finite and non-negative",
        ));
    }
    let multiplier = match word(tokens.get(2)).as_deref() {
        Some("MILLISECOND") | Some("MILLISECONDS") => 1.0,
        Some("SECOND") | Some("SECONDS") => 1_000.0,
        Some("MINUTE") | Some("MINUTES") => 60_000.0,
        _ => return invalid("watermark must use MILLISECOND, SECOND, or MINUTE"),
    };
    let millis = value * multiplier;
    if millis > i64::MAX as f64 {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "watermark interval is too large",
        ));
    }
    Ok((millis.round() as i64, 3))
}

fn parse_create_table(tokens: &[Token]) -> Result<VqlStatement> {
    if token_is(tokens.get(3), "AS") {
        return Err(VqlError::feature(
            "CREATE TABLE AS SELECT is not available",
            "v0.3",
        ));
    }
    let name = identifier(tokens.get(2), "table name")?;
    expect_word(tokens.get(3), "USING")?;
    let provider = match word(tokens.get(4)).as_deref() {
        Some("IMAGES") => TableProviderKind::Images,
        Some("VIDEOS") => TableProviderKind::Videos,
        Some("KAFKA") => {
            return Err(VqlError::feature(
                "Kafka table provider is not available",
                "v0.1",
            ));
        }
        Some("PARQUET") | Some("LANCE") | Some("HNSW") => {
            return Err(VqlError::feature(
                "columnar/vector providers are not available",
                "v0.3",
            ));
        }
        Some(provider) => {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("unsupported table provider '{provider}'"),
            ));
        }
        None => return invalid("expected IMAGES or VIDEOS after USING"),
    };
    expect_word(tokens.get(5), "LOCATION")?;
    let location = string_literal(tokens.get(6))?;
    let mut recursive = false;
    let mut fps = None;
    let mut start_time_ms = None;
    let mut index = 7;
    if index < tokens.len() {
        expect_word(tokens.get(index), "WITH")?;
        index += 1;
        expect_token(tokens.get(index), Token::LParen, "'(' after WITH")?;
        index += 1;
        while index < tokens.len() {
            if tokens.get(index) == Some(&Token::RParen) {
                index += 1;
                break;
            }
            let option = identifier(tokens.get(index), "option name")?.to_ascii_lowercase();
            index += 1;
            expect_token(tokens.get(index), Token::Eq, "'=' after option name")?;
            index += 1;
            match (provider, option.as_str()) {
                (TableProviderKind::Images | TableProviderKind::Videos, "recursive") => {
                    recursive = parse_boolean(tokens.get(index))?;
                }
                (TableProviderKind::Videos, "fps") => {
                    let value = parse_number(tokens.get(index), "fps")?;
                    if !value.is_finite() || value <= 0.0 || value > 120.0 {
                        return Err(VqlError::new(
                            ErrorCode::InvalidOption,
                            "fps must be greater than 0 and at most 120",
                        ));
                    }
                    fps = Some(value);
                }
                (TableProviderKind::Videos, "start_time") => {
                    let value = string_literal(tokens.get(index))?;
                    let parsed = chrono::DateTime::parse_from_rfc3339(&value).map_err(|error| {
                        VqlError::new(
                            ErrorCode::InvalidOption,
                            "start_time must be an RFC 3339 timestamp",
                        )
                        .with_source(error)
                    })?;
                    start_time_ms = Some(parsed.timestamp_millis());
                }
                _ => {
                    return Err(VqlError::new(
                        ErrorCode::InvalidOption,
                        format!(
                            "unknown {} option '{option}'",
                            format!("{provider:?}").to_ascii_uppercase()
                        ),
                    ));
                }
            }
            index += 1;
            match tokens.get(index) {
                Some(Token::Comma) => index += 1,
                Some(Token::RParen) => {
                    index += 1;
                    break;
                }
                _ => return invalid("expected ',' or ')' after WITH option"),
            }
        }
    }
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE TABLE");
    }
    Ok(VqlStatement::CreateTable(CreateTable {
        name,
        provider,
        location,
        recursive,
        fps,
        start_time_ms,
    }))
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
        _ => return invalid("v0.1 supports TYPE OBJECT_DETECTION"),
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

fn parse_create_sink(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() < 5 {
        return invalid("expected CREATE SINK <name> TYPE console|kafka");
    }
    let name = identifier(tokens.get(2), "sink name")?;
    expect_word(tokens.get(3), "TYPE")?;
    match word(tokens.get(4)).as_deref() {
        Some("CONSOLE") if tokens.len() == 5 => Ok(VqlStatement::CreateSink {
            name,
            kind: SinkKind::Console,
            kafka: None,
        }),
        Some("CONSOLE") => invalid("Console Sink does not accept WITH options"),
        Some("KAFKA") => parse_kafka_sink(tokens, name),
        Some("PARQUET") | Some("LANCE") => {
            Err(VqlError::feature("file Sinks are not available", "v0.3"))
        }
        _ => invalid("v0.1 supports TYPE console or kafka"),
    }
}

fn parse_kafka_sink(tokens: &[Token], name: String) -> Result<VqlStatement> {
    let mut bootstrap_servers = None;
    let mut topic = None;
    let mut credential_ref = None;
    let mut delivery_timeout_ms = 30_000_u64;
    let mut buffer_capacity = 1_024_usize;
    let mut format = "json".to_owned();
    let mut seen = std::collections::BTreeSet::new();
    let mut index = 5;
    expect_word(tokens.get(index), "WITH")?;
    index += 1;
    expect_token(tokens.get(index), Token::LParen, "'(' after WITH")?;
    index += 1;
    while tokens.get(index) != Some(&Token::RParen) {
        let option = identifier(tokens.get(index), "Kafka Sink option")?.to_ascii_lowercase();
        if !seen.insert(option.clone()) {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("duplicate Kafka Sink option '{option}'"),
            ));
        }
        index += 1;
        expect_token(tokens.get(index), Token::Eq, "'=' after Kafka Sink option")?;
        index += 1;
        match option.as_str() {
            "bootstrap_servers" => {
                bootstrap_servers = Some(string_literal(tokens.get(index))?);
                index += 1;
            }
            "topic" => {
                topic = Some(string_literal(tokens.get(index))?);
                index += 1;
            }
            "credential_ref" => {
                credential_ref = Some(string_literal(tokens.get(index))?);
                index += 1;
            }
            "format" => {
                format = string_literal(tokens.get(index))?.to_ascii_lowercase();
                index += 1;
            }
            "delivery_timeout_ms" => {
                delivery_timeout_ms =
                    parse_unsigned_integer(tokens.get(index), "delivery_timeout_ms")?;
                index += 1;
            }
            "buffer_capacity" => {
                let value = parse_unsigned_integer(tokens.get(index), "buffer_capacity")?;
                buffer_capacity = usize::try_from(value).map_err(|error| {
                    VqlError::new(
                        ErrorCode::InvalidOption,
                        "buffer_capacity is too large for this platform",
                    )
                    .with_source(error)
                })?;
                index += 1;
            }
            _ => {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("unknown Kafka Sink option '{option}'"),
                ));
            }
        }
        match tokens.get(index) {
            Some(Token::Comma) => index += 1,
            Some(Token::RParen) => {}
            _ => return invalid("expected ',' or ')' after Kafka Sink option"),
        }
    }
    index += 1;
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE SINK");
    }

    let bootstrap_servers = bootstrap_servers.ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "Kafka Sink requires bootstrap_servers",
        )
    })?;
    validate_kafka_bootstrap_servers(&bootstrap_servers)?;
    let topic = topic
        .ok_or_else(|| VqlError::new(ErrorCode::InvalidOption, "Kafka Sink requires topic"))?;
    validate_kafka_topic(&topic)?;
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
    if format != "json" {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "v0.1 Kafka Sink format must be 'json'",
        ));
    }
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

    Ok(VqlStatement::CreateSink {
        name,
        kind: SinkKind::Kafka,
        kafka: Some(KafkaSinkConfig {
            bootstrap_servers,
            topic,
            credential_ref,
            delivery_timeout_ms,
            buffer_capacity,
        }),
    })
}

fn parse_unsigned_integer(token: Option<&Token>, label: &str) -> Result<u64> {
    match token {
        Some(Token::Number(value, false)) if value.bytes().all(|byte| byte.is_ascii_digit()) => {
            value.parse::<u64>().map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    format!("{label} must be a non-negative integer"),
                )
                .with_source(error)
            })
        }
        _ => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("{label} must be a non-negative integer"),
        )),
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
    if tokens.len() != 2 {
        return invalid("expected SHOW TABLES, STREAMS, MODELS, FUNCTIONS, or SINKS");
    }
    let kind = match word(tokens.get(1)).as_deref() {
        Some("TABLES") => ShowKind::Tables,
        Some("STREAMS") => ShowKind::Streams,
        Some("MODELS") => ShowKind::Models,
        Some("FUNCTIONS") => ShowKind::Functions,
        Some("SINKS") => ShowKind::Sinks,
        _ => return invalid("expected SHOW TABLES, STREAMS, MODELS, FUNCTIONS, or SINKS"),
    };
    Ok(VqlStatement::Show(kind))
}

fn singular_kind(token: Option<&Token>) -> Result<ShowKind> {
    match word(token).as_deref() {
        Some("TABLE") => Ok(ShowKind::Tables),
        Some("STREAM") => Ok(ShowKind::Streams),
        Some("MODEL") => Ok(ShowKind::Models),
        Some("FUNCTION") => Ok(ShowKind::Functions),
        Some("SINK") => Ok(ShowKind::Sinks),
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

fn parse_boolean(token: Option<&Token>) -> Result<bool> {
    match word(token).as_deref() {
        Some("TRUE") => Ok(true),
        Some("FALSE") => Ok(false),
        _ => invalid("recursive must be true or false"),
    }
}

fn parse_number(token: Option<&Token>, label: &str) -> Result<f64> {
    match token {
        Some(Token::Number(value, _)) => value.parse::<f64>().map_err(|error| {
            VqlError::new(ErrorCode::InvalidOption, format!("{label} must be numeric"))
                .with_source(error)
        }),
        _ => invalid(format!("{label} must be numeric")),
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
            "CREATE TABLE photos USING IMAGES LOCATION './photos' WITH (recursive=true)",
        )
        .unwrap();
        assert_eq!(
            parsed,
            VqlStatement::CreateTable(CreateTable {
                name: "photos".to_owned(),
                provider: TableProviderKind::Images,
                location: "./photos".to_owned(),
                recursive: true,
                fps: None,
                start_time_ms: None,
            })
        );
    }

    #[test]
    fn parses_videos_options() {
        let parsed = parse_statement(
            "CREATE TABLE clips USING VIDEOS LOCATION './clips' WITH \
             (fps=5, recursive=true, start_time='2026-08-08T00:00:00Z')",
        )
        .unwrap();
        let VqlStatement::CreateTable(create) = parsed else {
            panic!("expected CREATE TABLE")
        };
        assert_eq!(create.provider, TableProviderKind::Videos);
        assert_eq!(create.fps, Some(5.0));
        assert!(create.recursive);
        assert_eq!(create.start_time_ms, Some(1_786_147_200_000));
    }

    #[test]
    fn parses_rtsp_stream_options() {
        let parsed = parse_statement(
            "CREATE STREAM cam_entrance FROM 'rtsp://10.0.0.15:554/main' WITH (\
             fps=5, event_time='capture_time', watermark=INTERVAL '2' SECOND, transport='tcp')",
        )
        .unwrap();
        assert_eq!(
            parsed,
            VqlStatement::CreateStream(CreateStream {
                name: "cam_entrance".to_owned(),
                endpoint: "rtsp://10.0.0.15:554/main".to_owned(),
                fps: 5.0,
                event_time: EventTimePolicy::CaptureTime,
                watermark_delay_ms: 2_000,
                transport: RtspTransport::Tcp,
            })
        );
    }

    #[test]
    fn parses_kafka_sink_options() {
        let parsed = parse_statement(
            "CREATE SINK people_per_minute TYPE KAFKA WITH (\
             bootstrap_servers='broker-1:9092,broker-2:9092', \
             topic='people-per-minute', format='json', \
             credential_ref='secret://kafka/producer', \
             delivery_timeout_ms=45000, buffer_capacity=256)",
        )
        .unwrap();

        assert_eq!(
            parsed,
            VqlStatement::CreateSink {
                name: "people_per_minute".to_owned(),
                kind: SinkKind::Kafka,
                kafka: Some(KafkaSinkConfig {
                    bootstrap_servers: "broker-1:9092,broker-2:9092".to_owned(),
                    topic: "people-per-minute".to_owned(),
                    credential_ref: Some("secret://kafka/producer".to_owned()),
                    delivery_timeout_ms: 45_000,
                    buffer_capacity: 256,
                }),
            }
        );
    }

    #[test]
    fn rejects_invalid_kafka_sink_options() {
        for sql in [
            "CREATE SINK out TYPE KAFKA WITH (topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='http://broker:9092', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='user:secret@broker:9092', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:0', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:70000', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092,', topic='events')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='bad topic')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', format='avro')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', delivery_timeout_ms=0)",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', buffer_capacity=100001)",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', topic='other')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', retries=3)",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', credential_ref='')",
            "CREATE SINK out TYPE KAFKA WITH (bootstrap_servers='broker:9092', topic='events', username='user')",
        ] {
            assert_eq!(
                parse_statement(sql).unwrap_err().code,
                ErrorCode::InvalidOption,
                "{sql}"
            );
        }
    }

    #[test]
    fn rejects_invalid_rtsp_stream_options() {
        for sql in [
            "CREATE STREAM cam FROM 'rtsp://camera/live' WITH (fps=0)",
            "CREATE STREAM cam FROM 'rtsp://camera/live' WITH (event_time='wall_time')",
            "CREATE STREAM cam FROM 'rtsp://camera/live' WITH (watermark=INTERVAL '-1' SECOND)",
            "CREATE STREAM cam FROM 'rtsp://camera/live' WITH (transport='quic')",
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
            "CREATE TABLE photos USING IMAGES LOCATION './photos' WITH (magic=true)",
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
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
            ("CREATE TABLE events USING KAFKA LOCATION 'topic'", "v0.1"),
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
