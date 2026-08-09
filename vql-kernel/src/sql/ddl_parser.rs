use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use std::collections::BTreeMap;

use super::ast::{CreateFunction, CreateModel, CreateTable, ShowKind, VqlStatement};
use crate::catalog::{FunctionImplementation, ModelType, SinkKind, TableProviderKind};
use crate::{ErrorCode, Result, VqlError};

pub(crate) fn parse_statement(sql: &str) -> Result<VqlStatement> {
    if sql.contains("<->") {
        return Err(VqlError::feature(
            "vector distance operator is not available",
            "v0.4",
        ));
    }
    let tokens = significant_tokens(sql)?;
    let first = word(tokens.first()).unwrap_or_default();
    match first.as_str() {
        "CREATE" => parse_create(&tokens),
        "ALTER" => parse_alter(&tokens),
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

fn parse_create(tokens: &[Token]) -> Result<VqlStatement> {
    match word(tokens.get(1)).as_deref() {
        Some("TABLE") if token_is(tokens.get(2), "FUNCTION") => Err(VqlError::feature(
            "table functions are not available",
            "未排期",
        )),
        Some("TABLE") => parse_create_table(tokens),
        Some("MODEL") => parse_create_model(tokens),
        Some("FUNCTION") => parse_create_function(tokens),
        Some("SINK") => parse_create_sink(tokens),
        Some("STREAM") => Err(VqlError::feature(
            "CREATE STREAM is not available in v0.1",
            "v0.2",
        )),
        Some("INDEX") => Err(VqlError::feature(
            "vector indexes are not available",
            "v0.4",
        )),
        Some("AGGREGATE") | Some("TABLE_FUNCTION") => Err(VqlError::feature(
            "aggregate and table functions are not available",
            "未排期",
        )),
        _ => invalid("expected CREATE TABLE, MODEL, FUNCTION, or SINK"),
    }
}

fn parse_create_table(tokens: &[Token]) -> Result<VqlStatement> {
    if token_is(tokens.get(3), "AS") {
        return Err(VqlError::feature(
            "CREATE TABLE AS SELECT is not available",
            "v0.4",
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
                "v0.2",
            ));
        }
        Some("PARQUET") | Some("LANCE") | Some("HNSW") => {
            return Err(VqlError::feature(
                "columnar/vector providers are not available",
                "v0.4",
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
    if tokens.len() < 7 {
        return invalid("expected CREATE MODEL <name> TYPE OBJECT_DETECTION FROM '<source>'");
    }
    let name = identifier(tokens.get(2), "model name")?;
    expect_word(tokens.get(3), "TYPE")?;
    let model_type = match word(tokens.get(4)).as_deref() {
        Some("OBJECT_DETECTION") => ModelType::ObjectDetection,
        Some("EMBEDDING") => {
            return Err(VqlError::feature(
                "EMBEDDING models are not available",
                "v0.4",
            ));
        }
        _ => return invalid("v0.1 supports TYPE OBJECT_DETECTION"),
    };
    expect_word(tokens.get(5), "FROM")?;
    let source = string_literal(tokens.get(6))?;
    let mut index = 7;
    let mut defaults = BTreeMap::new();
    let mut has_defaults = false;
    let mut function = None;
    while index < tokens.len() {
        if token_is(tokens.get(index), "WITH") {
            if has_defaults {
                return invalid("CREATE MODEL has more than one WITH clause");
            }
            parse_model_parameters(tokens, &mut index, &mut defaults, "model parameter")?;
            has_defaults = true;
        } else if token_is(tokens.get(index), "FUNCTION") {
            if function.is_some() {
                return invalid("CREATE MODEL has more than one FUNCTION clause");
            }
            function = Some(identifier(tokens.get(index + 1), "function name")?);
            index += 2;
        } else {
            return invalid("unexpected tokens after CREATE MODEL");
        }
    }
    Ok(VqlStatement::CreateModel(CreateModel {
        name,
        model_type,
        source,
        defaults,
        function,
    }))
}

fn parse_create_function(tokens: &[Token]) -> Result<VqlStatement> {
    let name = identifier(tokens.get(2), "function name")?;
    let mut index = 3;
    let mut parameters = Vec::new();
    if tokens.get(index) == Some(&Token::LParen) {
        index += 1;
        while tokens.get(index) != Some(&Token::RParen) {
            let parameter = identifier(tokens.get(index), "parameter name")?;
            index += 1;
            let data_type = identifier(tokens.get(index), "parameter type")?.to_ascii_uppercase();
            index += 1;
            parameters.push((parameter, data_type));
            match tokens.get(index) {
                Some(Token::Comma) => index += 1,
                Some(Token::RParen) => {}
                _ => return invalid("expected ',' or ')' in function parameters"),
            }
        }
        index += 1;
    }
    let mut return_type = None;
    if token_is(tokens.get(index), "RETURNS") {
        index += 1;
        return_type = Some(identifier(tokens.get(index), "return type")?.to_ascii_uppercase());
        index += 1;
    }
    let implementation = if token_is(tokens.get(index), "USING") {
        expect_word(tokens.get(index + 1), "MODEL")?;
        let model = identifier(tokens.get(index + 2), "model name")?;
        index += 3;
        FunctionImplementation::Model { model }
    } else if token_is(tokens.get(index), "LANGUAGE") {
        expect_word(tokens.get(index + 1), "PYTHON")?;
        expect_word(tokens.get(index + 2), "AS")?;
        let entry = string_literal(tokens.get(index + 3))?;
        index += 4;
        FunctionImplementation::Python { entry }
    } else if token_is(tokens.get(index), "AS") {
        index += 1;
        expect_token(tokens.get(index), Token::LParen, "'(' after AS")?;
        index += 1;
        let start = index;
        let mut depth = 1usize;
        while index < tokens.len() && depth > 0 {
            match tokens.get(index) {
                Some(Token::LParen) => depth += 1,
                Some(Token::RParen) => depth -= 1,
                _ => {}
            }
            if depth > 0 {
                index += 1;
            }
        }
        if depth != 0 {
            return invalid("unterminated SQL macro expression");
        }
        let expression = tokens[start..index]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        index += 1;
        FunctionImplementation::SqlMacro { expression }
    } else {
        return invalid("expected USING MODEL, LANGUAGE PYTHON AS, or AS (<expression>)");
    };
    let mut bindings = BTreeMap::new();
    if token_is(tokens.get(index), "WITH") {
        parse_model_parameters(tokens, &mut index, &mut bindings, "function binding")?;
    }
    if index != tokens.len() {
        return invalid("unexpected tokens after CREATE FUNCTION");
    }
    Ok(VqlStatement::CreateFunction(CreateFunction {
        name,
        parameters,
        return_type,
        implementation,
        bindings,
    }))
}

fn parse_model_parameters(
    tokens: &[Token],
    index: &mut usize,
    parameters: &mut BTreeMap<String, serde_json::Value>,
    label: &str,
) -> Result<()> {
    *index += 1;
    expect_token(tokens.get(*index), Token::LParen, "'(' after WITH")?;
    *index += 1;
    while tokens.get(*index) != Some(&Token::RParen) {
        let key = identifier(tokens.get(*index), label)?.to_ascii_lowercase();
        *index += 1;
        expect_token(tokens.get(*index), Token::Eq, "'=' after parameter name")?;
        *index += 1;
        let value = parse_json_value(tokens, index)?;
        let key = canonical_model_parameter(&key).ok_or_else(|| {
            VqlError::new(ErrorCode::InvalidOption, format!("unknown {label} '{key}'"))
        })?;
        if parameters.insert(key.to_owned(), value).is_some() {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("duplicate {label} '{key}'"),
            ));
        }
        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::RParen) => {}
            _ => return invalid(format!("expected ',' or ')' after {label}")),
        }
    }
    *index += 1;
    Ok(())
}

fn canonical_model_parameter(name: &str) -> Option<&str> {
    match name {
        "processor" | "input_name" | "output_name" | "input_width" | "input_height"
        | "output_format" | "labels" | "classes" | "min_confidence" | "nms_iou_threshold" => {
            Some(name)
        }
        "nms_threshold" => Some("nms_iou_threshold"),
        _ => None,
    }
}

fn parse_json_value(tokens: &[Token], index: &mut usize) -> Result<serde_json::Value> {
    match tokens.get(*index) {
        Some(Token::SingleQuotedString(value)) | Some(Token::DoubleQuotedString(value)) => {
            *index += 1;
            Ok(serde_json::Value::String(value.clone()))
        }
        Some(Token::Number(value, _)) => {
            *index += 1;
            let value = value.parse::<f64>().map_err(|error| {
                VqlError::new(ErrorCode::InvalidOption, "binding must be numeric")
                    .with_source(error)
            })?;
            Ok(serde_json::json!(value))
        }
        Some(Token::LBracket) => {
            *index += 1;
            let mut values = Vec::new();
            while tokens.get(*index) != Some(&Token::RBracket) {
                values.push(serde_json::Value::String(string_literal(
                    tokens.get(*index),
                )?));
                *index += 1;
                match tokens.get(*index) {
                    Some(Token::Comma) => *index += 1,
                    Some(Token::RBracket) => {}
                    _ => return invalid("parameter must be an array of strings"),
                }
            }
            *index += 1;
            Ok(serde_json::Value::Array(values))
        }
        _ => invalid("unsupported binding value"),
    }
}

fn parse_create_sink(tokens: &[Token]) -> Result<VqlStatement> {
    if tokens.len() != 5 {
        return invalid("expected CREATE SINK <name> TYPE console");
    }
    let name = identifier(tokens.get(2), "sink name")?;
    expect_word(tokens.get(3), "TYPE")?;
    let kind = match word(tokens.get(4)).as_deref() {
        Some("CONSOLE") => SinkKind::Console,
        Some("KAFKA") => return Err(VqlError::feature("Kafka Sink is not available", "v0.2")),
        Some("PARQUET") | Some("LANCE") => {
            return Err(VqlError::feature("file Sinks are not available", "v0.4"));
        }
        _ => return invalid("v0.1 supports TYPE console"),
    };
    Ok(VqlStatement::CreateSink { name, kind })
}

fn parse_alter(tokens: &[Token]) -> Result<VqlStatement> {
    match word(tokens.get(1)).as_deref() {
        Some("MODEL") if tokens.len() == 6 && token_is(tokens.get(3), "SET") => {
            let name = identifier(tokens.get(2), "model name")?;
            if !token_is(tokens.get(4), "FROM") && !token_is(tokens.get(4), "SOURCE") {
                return invalid("expected ALTER MODEL <name> SET FROM '<source>'");
            }
            Ok(VqlStatement::AlterModel {
                name,
                source: string_literal(tokens.get(5))?,
            })
        }
        Some("FUNCTION") if tokens.len() == 6 && token_is(tokens.get(3), "SET") => {
            expect_word(tokens.get(4), "MODEL")?;
            Ok(VqlStatement::AlterFunction {
                name: identifier(tokens.get(2), "function name")?,
                model: identifier(tokens.get(5), "model name")?,
            })
        }
        _ => invalid("unsupported ALTER statement"),
    }
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
        return invalid("expected SHOW TABLES, MODELS, FUNCTIONS, or SINKS");
    }
    let kind = match word(tokens.get(1)).as_deref() {
        Some("TABLES") => ShowKind::Tables,
        Some("MODELS") => ShowKind::Models,
        Some("FUNCTIONS") => ShowKind::Functions,
        Some("SINKS") => ShowKind::Sinks,
        _ => return invalid("expected SHOW TABLES, MODELS, FUNCTIONS, or SINKS"),
    };
    Ok(VqlStatement::Show(kind))
}

fn singular_kind(token: Option<&Token>) -> Result<ShowKind> {
    match word(token).as_deref() {
        Some("TABLE") => Ok(ShowKind::Tables),
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
    fn rejects_unknown_images_option() {
        let error = parse_statement(
            "CREATE TABLE photos USING IMAGES LOCATION './photos' WITH (magic=true)",
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
    }

    #[test]
    fn parses_model_defaults_and_function_name() {
        let parsed = parse_statement(
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'file:///model.onnx' \
             WITH (input_width=32, labels=['person'], min_confidence=0.4) FUNCTION detect",
        )
        .unwrap();
        let VqlStatement::CreateModel(create) = parsed else {
            panic!("expected CREATE MODEL")
        };

        assert_eq!(create.function.as_deref(), Some("detect"));
        assert_eq!(create.defaults["input_width"], serde_json::json!(32.0));
        assert_eq!(create.defaults["labels"], serde_json::json!(["person"]));
        assert_eq!(create.defaults["min_confidence"], serde_json::json!(0.4));
    }

    #[test]
    fn parses_function_overrides_and_canonicalizes_legacy_nms_name() {
        let parsed = parse_statement(
            "CREATE FUNCTION people USING MODEL detector \
             WITH (classes=['person'], nms_threshold=0.4)",
        )
        .unwrap();
        let VqlStatement::CreateFunction(create) = parsed else {
            panic!("expected CREATE FUNCTION")
        };

        assert_eq!(create.bindings["classes"], serde_json::json!(["person"]));
        assert_eq!(create.bindings["nms_iou_threshold"], serde_json::json!(0.4));
        assert!(!create.bindings.contains_key("nms_threshold"));
    }

    #[test]
    fn future_capabilities_have_stable_target_versions() {
        let cases = [
            ("CREATE STREAM cam FROM 'rtsp://example'", "v0.2"),
            ("CREATE TABLE events USING KAFKA LOCATION 'topic'", "v0.2"),
            ("CREATE TABLE out USING PARQUET LOCATION './out'", "v0.4"),
            ("CREATE TABLE out AS SELECT 1", "v0.4"),
            ("CREATE INDEX idx USING HNSW", "v0.4"),
            ("SELECT embedding <-> other FROM values", "v0.4"),
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
