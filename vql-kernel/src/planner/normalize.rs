use crate::functions::BuiltinAiFunction;
use crate::{ErrorCode, Result, VqlError};

pub(super) fn normalize_query(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<String> {
    reject_internal_builtin_ai_calls(sql)?;
    let sql = expand_macros(sql, snapshot)?;
    let sql = normalize_builtin_ai_calls(&sql)?;
    let sql = normalize_model_calls(&sql, snapshot)?;
    let sql = rewrite_center(&sql);
    rewrite_correlated_unnest(&sql)
}

pub(super) fn normalize_function_ddl(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<String> {
    let Some(return_start) = function_return_start(sql) else {
        return Ok(sql.to_owned());
    };
    let body_start = return_start + "RETURN".len();
    // The declaration's name and parameter types are not callable expressions.
    // Rewriting them can hide Catalog conflicts behind unrelated SQL errors.
    let body = expand_macros_inner(&sql[body_start..], snapshot, false)?;
    let sql = normalize_function_parameter_references(&format!("{}{}", &sql[..body_start], body))?;
    reject_internal_builtin_ai_calls(&sql[body_start..])?;
    let body = normalize_builtin_ai_calls(&sql[body_start..])?;
    let body = normalize_model_calls(&body, snapshot)?;
    Ok(format!("{}{}", &sql[..body_start], body))
}

fn reject_internal_builtin_ai_calls(sql: &str) -> Result<()> {
    for function in [
        BuiltinAiFunction::Classify,
        BuiltinAiFunction::Extract,
        BuiltinAiFunction::Detect,
    ] {
        if find_function_call(sql, function.marker_name())?.is_some() {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                format!(
                    "{} is an internal planning marker; call {} instead",
                    function.marker_name(),
                    function.name().to_ascii_uppercase()
                ),
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct BuiltinAiSignature {
    function: BuiltinAiFunction,
    parameters: &'static [&'static str],
    required: usize,
    positional: usize,
}

const BUILTIN_AI_SIGNATURES: [BuiltinAiSignature; 2] = [
    BuiltinAiSignature {
        function: BuiltinAiFunction::Classify,
        parameters: &["input", "categories", "output_mode", "min_score"],
        required: 2,
        positional: 2,
    },
    BuiltinAiSignature {
        function: BuiltinAiFunction::Detect,
        parameters: &["input", "classes", "min_score"],
        required: 1,
        positional: 1,
    },
];

fn normalize_builtin_ai_calls(sql: &str) -> Result<String> {
    let mut output = normalize_extract_calls(sql)?;
    for signature in BUILTIN_AI_SIGNATURES {
        let mut cursor = 0usize;
        while let Some((start, open, close)) =
            find_function_call_from(&output, signature.function.name(), cursor)?
        {
            let args = split_args(&output[open + 1..close])?;
            let mut ordered = vec![None; signature.parameters.len()];
            let mut positional_index = 0usize;
            let mut seen_named = false;
            for argument in args {
                if let Some(arrow) = top_level_arrow(argument) {
                    seen_named = true;
                    let name = argument[..arrow].trim().to_ascii_lowercase();
                    let value = argument[arrow + 2..].trim();
                    let position = signature
                        .parameters
                        .iter()
                        .position(|parameter| *parameter == name)
                        .ok_or_else(|| {
                            VqlError::new(
                                ErrorCode::InvalidSql,
                                format!(
                                    "unknown {} argument '{name}'",
                                    signature.function.name().to_ascii_uppercase()
                                ),
                            )
                        })?;
                    if value.is_empty() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "{} argument '{name}' requires a value",
                                signature.function.name().to_ascii_uppercase()
                            ),
                        ));
                    }
                    if ordered[position].replace(value.to_owned()).is_some() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "duplicate {} argument '{name}'",
                                signature.function.name().to_ascii_uppercase()
                            ),
                        ));
                    }
                } else {
                    if seen_named {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "{} positional arguments must precede named arguments",
                                signature.function.name().to_ascii_uppercase()
                            ),
                        ));
                    }
                    if positional_index >= signature.positional {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "{} optional arguments must use name => value",
                                signature.function.name().to_ascii_uppercase()
                            ),
                        ));
                    }
                    ordered[positional_index] = Some(argument.trim().to_owned());
                    positional_index += 1;
                }
            }
            for (index, parameter) in signature.parameters[..signature.required]
                .iter()
                .enumerate()
            {
                if ordered[index].is_none() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!(
                            "{} requires argument '{parameter}'",
                            signature.function.name().to_ascii_uppercase()
                        ),
                    ));
                }
            }
            let replacement = ordered
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "NULL".to_owned()))
                .collect::<Vec<_>>()
                .join(", ");
            let replacement = format!("{}({replacement})", signature.function.marker_name());
            output.replace_range(start..=close, &replacement);
            cursor = start + replacement.len();
        }
    }
    Ok(output)
}

fn normalize_extract_calls(sql: &str) -> Result<String> {
    let function = BuiltinAiFunction::Extract;
    let mut output = sql.to_owned();
    let mut cursor = 0usize;
    while let Some((start, open, close)) =
        find_function_call_from(&output, function.name(), cursor)?
    {
        let args = split_args(&output[open + 1..close])?;
        let mut input = None;
        let mut fields = None;
        let mut positional = 0usize;
        let mut seen_named = false;
        for argument in args {
            if let Some(arrow) = top_level_arrow(argument) {
                seen_named = true;
                let name = argument[..arrow].trim().to_ascii_lowercase();
                let value = argument[arrow + 2..].trim();
                if matches!(name.as_str(), "classes" | "min_confidence") {
                    return Err(extract_detection_migration_error());
                }
                let target = match name.as_str() {
                    "input" => &mut input,
                    "fields" => &mut fields,
                    _ => {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!("unknown VQL_EXTRACT argument '{name}'"),
                        ));
                    }
                };
                if value.is_empty() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!("VQL_EXTRACT argument '{name}' requires a value"),
                    ));
                }
                if target.replace(value.to_owned()).is_some() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!("duplicate VQL_EXTRACT argument '{name}'"),
                    ));
                }
            } else {
                if seen_named {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        "VQL_EXTRACT positional arguments must precede named arguments",
                    ));
                }
                match positional {
                    0 => input = Some(argument.trim().to_owned()),
                    1 => fields = Some(argument.trim().to_owned()),
                    _ => return Err(extract_detection_migration_error()),
                }
                positional += 1;
            }
        }
        let input = input.ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidSql,
                "VQL_EXTRACT requires argument 'input'",
            )
        })?;
        let fields = fields.ok_or_else(extract_detection_migration_error)?;
        let fields = parse_extract_fields(&fields)?;
        let mut normalized = Vec::with_capacity(1 + fields.len() * 3);
        normalized.push(input);
        for field in fields {
            normalized.push(quote_sql_string(&field.name));
            normalized.push(quote_sql_string(&field.question));
            normalized.push(if field.list { "TRUE" } else { "FALSE" }.to_owned());
        }
        let replacement = format!("{}({})", function.marker_name(), normalized.join(", "));
        output.replace_range(start..=close, &replacement);
        cursor = start + replacement.len();
    }
    Ok(output)
}

fn extract_detection_migration_error() -> VqlError {
    VqlError::new(
        ErrorCode::InvalidArgument,
        "VQL_EXTRACT now requires fields; rewrite detection calls as VQL_DETECT(input, classes => ..., min_score => ...)",
    )
}

fn parse_extract_fields(value: &str) -> Result<Vec<crate::models::ExtractFieldSpec>> {
    let value = strip_outer_parentheses(value);
    let Some(after_map) = value
        .get(3..)
        .filter(|_| value[..3].eq_ignore_ascii_case("MAP"))
    else {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT fields must be a non-empty constant MAP literal",
        ));
    };
    let open = 3 + after_map.len() - after_map.trim_start().len();
    if value.as_bytes().get(open) != Some(&b'{') {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT fields must use MAP { 'field': descriptor } syntax",
        ));
    }
    let close = matching_delimiter(value, open, b'{', b'}')?;
    if !value[close + 1..].trim().is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT fields must be a constant MAP literal",
        ));
    }
    let entries = split_args(&value[open + 1..close])?;
    if entries.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT fields must not be empty",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    entries
        .into_iter()
        .map(|entry| {
            let colon = find_top_level_map_colon(entry).ok_or_else(|| {
                VqlError::new(
                    ErrorCode::InvalidArgument,
                    "VQL_EXTRACT MAP entries must use 'field': descriptor",
                )
            })?;
            let name = parse_sql_string(&entry[..colon], "field name")?.to_ascii_lowercase();
            if !is_valid_extract_field_name(&name) {
                return Err(VqlError::new(
                    ErrorCode::InvalidArgument,
                    format!("VQL_EXTRACT field '{name}' is not a valid identifier"),
                ));
            }
            if !seen.insert(name.clone()) {
                return Err(VqlError::new(
                    ErrorCode::InvalidArgument,
                    format!("VQL_EXTRACT field '{name}' is duplicated after normalization"),
                ));
            }
            let (question, list) = parse_extract_descriptor(&entry[colon + 1..])?;
            Ok(crate::models::ExtractFieldSpec {
                name,
                question,
                list,
            })
        })
        .collect()
}

fn parse_extract_descriptor(value: &str) -> Result<(String, bool)> {
    let value = strip_outer_parentheses(value);
    if value.starts_with('\'') {
        return Ok((parse_sql_string(value, "question")?, false));
    }
    let Some(after_struct) = value
        .get(6..)
        .filter(|_| value[..6].eq_ignore_ascii_case("STRUCT"))
    else {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT field descriptors must be STRING or STRUCT(question, list)",
        ));
    };
    let open = 6 + after_struct.len() - after_struct.trim_start().len();
    if value.as_bytes().get(open) != Some(&b'(') {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT STRUCT descriptor requires parentheses",
        ));
    }
    let close = matching_paren(value, open)?;
    if !value[close + 1..].trim().is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT STRUCT descriptor must be a constant literal",
        ));
    }
    let mut question = None;
    let mut list = None;
    for member in split_args(&value[open + 1..close])? {
        let (name, value) = if let Some(arrow) = top_level_arrow(member) {
            (member[..arrow].trim(), member[arrow + 2..].trim())
        } else if let Some(as_position) = find_top_level_keyword(member, "AS", 0) {
            (
                member[as_position + 2..].trim(),
                member[..as_position].trim(),
            )
        } else {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                "VQL_EXTRACT STRUCT members must name question and list",
            ));
        };
        match name.to_ascii_lowercase().as_str() {
            "question" if question.is_none() => {
                question = Some(parse_sql_string(value, "question")?)
            }
            "list" if list.is_none() => list = Some(parse_boolean(value)?),
            "question" | "list" => {
                return Err(VqlError::new(
                    ErrorCode::InvalidArgument,
                    format!("duplicate VQL_EXTRACT descriptor member '{name}'"),
                ));
            }
            _ => {
                return Err(VqlError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown VQL_EXTRACT descriptor member '{name}'"),
                ));
            }
        }
    }
    Ok((
        question.ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidArgument,
                "VQL_EXTRACT descriptor requires question",
            )
        })?,
        list.ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidArgument,
                "VQL_EXTRACT descriptor requires list",
            )
        })?,
    ))
}

fn parse_sql_string(value: &str, role: &str) -> Result<String> {
    let value = value.trim();
    if value.len() < 2 || !value.starts_with('\'') || !value.ends_with('\'') {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            format!("VQL_EXTRACT {role} must be a constant STRING"),
        ));
    }
    let mut parsed = String::new();
    let mut characters = value[1..value.len() - 1].chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\'' && characters.next_if_eq(&'\'').is_none() {
            return Err(VqlError::new(
                ErrorCode::InvalidArgument,
                format!("VQL_EXTRACT {role} contains an unescaped quote"),
            ));
        }
        parsed.push(character);
    }
    if parsed.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidArgument,
            format!("VQL_EXTRACT {role} must not be empty"),
        ));
    }
    Ok(parsed)
}

fn parse_boolean(value: &str) -> Result<bool> {
    match value.trim().to_ascii_uppercase().as_str() {
        "TRUE" => Ok(true),
        "FALSE" => Ok(false),
        _ => Err(VqlError::new(
            ErrorCode::InvalidArgument,
            "VQL_EXTRACT descriptor list must be a constant BOOLEAN",
        )),
    }
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_valid_extract_field_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn find_top_level_map_colon(value: &str) -> Option<usize> {
    let code = sql_code_mask(value);
    let mut parens = 0usize;
    let mut brackets = 0usize;
    let mut braces = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        if !code[index] {
            continue;
        }
        match byte {
            b'(' => parens += 1,
            b')' => parens = parens.saturating_sub(1),
            b'[' => brackets += 1,
            b']' => brackets = brackets.saturating_sub(1),
            b'{' => braces += 1,
            b'}' => braces = braces.saturating_sub(1),
            b':' if parens == 0 && brackets == 0 && braces == 0 => return Some(index),
            _ => {}
        }
    }
    None
}

fn normalize_function_parameter_references(sql: &str) -> Result<String> {
    let Some(return_start) = function_return_start(sql) else {
        return Ok(sql.to_owned());
    };
    let open = sql[..return_start].find('(').ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidSql,
            "CREATE FUNCTION requires a parameter list",
        )
    })?;
    let close = matching_paren(sql, open)?;
    if close >= return_start {
        return Ok(sql.to_owned());
    }
    let mut body = sql[return_start + "RETURN".len()..].to_owned();
    for (index, parameter) in split_args(&sql[open + 1..close])?.into_iter().enumerate() {
        let Some((name, _data_type)) = parameter.trim().split_once(char::is_whitespace) else {
            continue;
        };
        body = replace_identifier(&body, name, &format!("${}", index + 1));
    }
    Ok(format!("{}RETURN{}", &sql[..return_start], body))
}

fn function_return_start(sql: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let code = sql_code_mask(sql);
    upper.match_indices("RETURN").find_map(|(position, _)| {
        let end = position + "RETURN".len();
        (code[position..end].iter().all(|value| *value)
            && (position == 0 || !is_identifier_byte(sql.as_bytes()[position - 1]))
            && sql
                .as_bytes()
                .get(end)
                .is_none_or(|byte| !is_identifier_byte(*byte)))
        .then_some(position)
    })
}

pub(super) fn infer_constant_parameters(
    expression: &str,
    parameters: &[(String, String)],
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<Vec<String>> {
    let mut constants = Vec::new();
    for (model_name, model) in snapshot.models() {
        let arguments = model.definition.interface.arguments().collect::<Vec<_>>();
        let mut cursor = 0usize;
        while let Some((_start, open, close)) =
            find_function_call_from(expression, model_name, cursor)?
        {
            let values = split_args(&expression[open + 1..close])?;
            for (index, parameter) in arguments.iter().enumerate() {
                if !parameter.constant {
                    continue;
                }
                let Some(value) = values.get(index) else {
                    continue;
                };
                let value = strip_outer_parentheses(value);
                if let Some((name, _)) =
                    parameters
                        .iter()
                        .enumerate()
                        .find_map(|(index, parameter)| {
                            (value.eq_ignore_ascii_case(&parameter.0)
                                || value == format!("${}", index + 1))
                            .then_some(parameter)
                        })
                    && !constants.iter().any(|candidate: &String| candidate == name)
                {
                    constants.push(name.clone());
                }
            }
            if let Some(value) = values.get(arguments.len()) {
                let value = strip_outer_parentheses(value);
                if !value.eq_ignore_ascii_case("null")
                    && let Some((name, _)) =
                        parameters
                            .iter()
                            .enumerate()
                            .find_map(|(index, parameter)| {
                                (value.eq_ignore_ascii_case(&parameter.0)
                                    || value == format!("${}", index + 1))
                                .then_some(parameter)
                            })
                    && !constants.iter().any(|candidate: &String| candidate == name)
                {
                    constants.push(name.clone());
                }
            }
            cursor = close + 1;
        }
    }
    for signature in BUILTIN_AI_SIGNATURES {
        let mut cursor = 0usize;
        while let Some((_start, open, close)) =
            find_function_call_from(expression, signature.function.marker_name(), cursor)?
        {
            let values = split_args(&expression[open + 1..close])?;
            for position in 1..signature.parameters.len() {
                let Some(value) = values.get(position) else {
                    continue;
                };
                let value = strip_outer_parentheses(value);
                if value.eq_ignore_ascii_case("null") {
                    continue;
                }
                if let Some((name, _)) =
                    parameters
                        .iter()
                        .enumerate()
                        .find_map(|(index, parameter)| {
                            (value.eq_ignore_ascii_case(&parameter.0)
                                || value == format!("${}", index + 1))
                            .then_some(parameter)
                        })
                    && !constants.iter().any(|candidate| candidate == name)
                {
                    constants.push(name.clone());
                }
            }
            cursor = close + 1;
        }
    }
    Ok(constants)
}

fn normalize_model_calls(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<String> {
    let mut output = sql.to_owned();
    for (model_name, model) in snapshot.models() {
        let mut cursor = 0usize;
        while let Some((_start, open, close)) =
            find_function_call_from(&output, model_name, cursor)?
        {
            let arguments = model.definition.interface.arguments().collect::<Vec<_>>();
            let args = split_args(&output[open + 1..close])?;
            let mut ordered = vec![None; arguments.len() + 1];
            let canonical_positional = args.len() == ordered.len()
                && !args
                    .iter()
                    .any(|argument| top_level_arrow(argument).is_some());
            let mut positional_index = 0usize;
            let mut seen_named = false;
            for argument in args {
                if let Some(arrow) = top_level_arrow(argument) {
                    seen_named = true;
                    let name = argument[..arrow].trim().to_ascii_lowercase();
                    let value = argument[arrow + 2..].trim();
                    if value.is_empty() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!("model '{model_name}' argument '{name}' requires a value"),
                        ));
                    }
                    let position = if name == "version" {
                        arguments.len()
                    } else {
                        arguments
                            .iter()
                            .position(|parameter| parameter.name == name)
                            .ok_or_else(|| {
                                VqlError::new(
                                    ErrorCode::InvalidSql,
                                    format!("unknown model '{model_name}' argument '{name}'"),
                                )
                            })?
                    };
                    if ordered[position].replace(value.to_owned()).is_some() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!("duplicate model '{model_name}' argument '{name}'"),
                        ));
                    }
                } else {
                    if seen_named {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "model '{model_name}' positional arguments must precede named arguments"
                            ),
                        ));
                    }
                    if positional_index >= model.definition.interface.parameters.len()
                        && !canonical_positional
                    {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "model '{model_name}' semantic and version arguments must use name => value"
                            ),
                        ));
                    }
                    if positional_index >= ordered.len() {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!("model '{model_name}' received too many arguments"),
                        ));
                    }
                    ordered[positional_index] = Some(argument.trim().to_owned());
                    positional_index += 1;
                }
            }
            for (index, parameter) in arguments.iter().enumerate() {
                if ordered[index].is_none() && !parameter.optional {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!(
                            "model '{model_name}' requires argument '{}'",
                            parameter.name
                        ),
                    ));
                }
            }
            let replacement = ordered
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "NULL".to_owned()))
                .collect::<Vec<_>>()
                .join(", ");
            if replacement.is_empty() {
                return Err(VqlError::new(
                    ErrorCode::InvalidSql,
                    format!("model '{model_name}' has no callable parameters"),
                ));
            }
            output.replace_range(open + 1..close, &replacement);
            cursor = open + replacement.len() + 2;
        }
    }
    Ok(output)
}

pub(super) fn expand_macros(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<String> {
    expand_macros_inner(sql, snapshot, true)
}

fn expand_macros_inner(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
    enforce_constants: bool,
) -> Result<String> {
    let mut output = sql.to_owned();
    for _ in 0..16 {
        let mut changed = false;
        for (name, function) in snapshot.functions() {
            let crate::catalog::FunctionImplementation::SqlMacro { expression } =
                &function.definition.implementation
            else {
                continue;
            };
            if let Some((start, open, close)) = find_function_call(&output, name)? {
                let args = split_args(&output[open + 1..close])?;
                if args.len() != function.definition.parameters.len() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!(
                            "function '{name}' expects {} arguments, got {}",
                            function.definition.parameters.len(),
                            args.len()
                        ),
                    ));
                }
                for constant in function
                    .definition
                    .constant_parameters
                    .iter()
                    .filter(|_| enforce_constants)
                {
                    let position = function
                        .definition
                        .parameters
                        .iter()
                        .position(|(parameter, _)| parameter.eq_ignore_ascii_case(constant))
                        .expect("constant function parameter exists in the signature");
                    if !is_constant_argument(args[position]) {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!(
                                "function '{name}' argument {} ('{constant}') must be constant",
                                position + 1
                            ),
                        ));
                    }
                }
                let mut expanded = expression.clone();
                for (index, ((parameter, _), argument)) in
                    function.definition.parameters.iter().zip(args).enumerate()
                {
                    expanded = replace_identifier(&expanded, parameter, &format!("({argument})"));
                    expanded = replace_identifier(
                        &expanded,
                        &format!("${}", index + 1),
                        &format!("({argument})"),
                    );
                }
                output.replace_range(start..=close, &format!("({expanded})"));
                changed = true;
                break;
            }
        }
        if !changed {
            return Ok(output);
        }
    }
    Err(VqlError::new(
        ErrorCode::InvalidSql,
        "SQL macro expansion exceeded maximum depth 16",
    ))
}

fn strip_outer_parentheses(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim();
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let Ok(close) = matching_paren(trimmed, 0) else {
            return trimmed;
        };
        if close != trimmed.len() - 1 {
            return trimmed;
        }
        value = &trimmed[1..trimmed.len() - 1];
    }
}

fn is_constant_argument(value: &str) -> bool {
    let value = strip_outer_parentheses(value);
    if value.eq_ignore_ascii_case("null")
        || value.eq_ignore_ascii_case("true")
        || value.eq_ignore_ascii_case("false")
        || value.parse::<f64>().is_ok()
    {
        return true;
    }
    if value.starts_with('\'') && value.ends_with('\'') {
        let code = sql_code_mask(value);
        return code.iter().all(|in_code| !*in_code);
    }
    if value.starts_with('[') && value.ends_with(']') {
        return split_args(&value[1..value.len() - 1])
            .is_ok_and(|values| values.into_iter().all(is_constant_argument));
    }
    false
}

fn find_function_call(sql: &str, name: &str) -> Result<Option<(usize, usize, usize)>> {
    find_function_call_from(sql, name, 0)
}

fn find_function_call_from(
    sql: &str,
    name: &str,
    mut start: usize,
) -> Result<Option<(usize, usize, usize)>> {
    let upper = sql.to_ascii_uppercase();
    let needle = name.to_ascii_uppercase();
    let code = sql_code_mask(sql);
    while let Some(offset) = upper[start..].find(&needle) {
        let position = start + offset;
        let after_name = position + needle.len();
        let in_code = code[position..after_name].iter().all(|value| *value);
        let before_ok = position == 0 || !is_identifier_byte(upper.as_bytes()[position - 1]);
        let after_ok = upper
            .as_bytes()
            .get(after_name)
            .is_none_or(|byte| !is_identifier_byte(*byte));
        let mut open = after_name;
        while sql
            .as_bytes()
            .get(open)
            .is_some_and(u8::is_ascii_whitespace)
        {
            open += 1;
        }
        if in_code
            && before_ok
            && after_ok
            && code.get(open) == Some(&true)
            && sql.as_bytes().get(open) == Some(&b'(')
        {
            return Ok(Some((position, open, matching_paren(sql, open)?)));
        }
        start = after_name;
    }
    Ok(None)
}

fn split_args(value: &str) -> Result<Vec<&str>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let code = sql_code_mask(value);
    for (index, byte) in value.bytes().enumerate() {
        if !code[index] {
            continue;
        }
        match byte {
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'[' => bracket_depth += 1,
            b']' => bracket_depth = bracket_depth.saturating_sub(1),
            b'{' => brace_depth += 1,
            b'}' => brace_depth = brace_depth.saturating_sub(1),
            b',' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                values.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    if paren_depth != 0 || bracket_depth != 0 || brace_depth != 0 {
        return Err(VqlError::new(
            ErrorCode::InvalidSql,
            "invalid SQL macro arguments",
        ));
    }
    values.push(value[start..].trim());
    Ok(values)
}

fn top_level_arrow(value: &str) -> Option<usize> {
    let code = sql_code_mask(value);
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        if !code[index] {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'[' => bracket_depth += 1,
            b']' => bracket_depth = bracket_depth.saturating_sub(1),
            b'{' => brace_depth += 1,
            b'}' => brace_depth = brace_depth.saturating_sub(1),
            b'=' if bytes[index + 1] == b'>'
                && code[index + 1]
                && paren_depth == 0
                && bracket_depth == 0
                && brace_depth == 0 =>
            {
                return Some(index);
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn replace_identifier(expression: &str, identifier: &str, replacement: &str) -> String {
    let upper = expression.to_ascii_uppercase();
    let needle = identifier.to_ascii_uppercase();
    let code = sql_code_mask(expression);
    let mut output = String::new();
    let mut cursor = 0usize;
    while let Some(offset) = upper[cursor..].find(&needle) {
        let position = cursor + offset;
        let end = position + needle.len();
        let boundary = code[position..end].iter().all(|value| *value)
            && (position == 0 || !is_identifier_byte(upper.as_bytes()[position - 1]))
            && upper
                .as_bytes()
                .get(end)
                .is_none_or(|byte| !is_identifier_byte(*byte));
        if boundary {
            output.push_str(&expression[cursor..position]);
            output.push_str(replacement);
            cursor = end;
        } else {
            output.push_str(&expression[cursor..end]);
            cursor = end;
        }
    }
    output.push_str(&expression[cursor..]);
    output
}

fn rewrite_center(sql: &str) -> String {
    let mut output = sql.to_owned();
    loop {
        let lower = output.to_ascii_lowercase();
        let code = sql_code_mask(&output);
        let Some(end) = lower
            .match_indices(".center")
            .map(|(position, _)| position)
            .find(|position| {
                code[*position..*position + ".center".len()]
                    .iter()
                    .all(|v| *v)
            })
        else {
            break;
        };
        let mut start = end;
        while start > 0
            && code[start - 1]
            && matches!(output.as_bytes()[start - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'.')
        {
            start -= 1;
        }
        if start == end {
            break;
        }
        let operand = output[start..end].to_owned();
        output.replace_range(
            start..end + ".center".len(),
            &format!("BOX_CENTER({operand})"),
        );
    }
    output
}

fn rewrite_correlated_unnest(sql: &str) -> Result<String> {
    let Some(from) = find_top_level_keyword(sql, "FROM", 0) else {
        return Ok(sql.to_owned());
    };
    let Some(comma) = find_top_level_char(sql, ',', from + 4) else {
        return Ok(sql.to_owned());
    };
    let after_comma = sql[comma + 1..].trim_start();
    if !after_comma.to_ascii_uppercase().starts_with("UNNEST") {
        return Ok(sql.to_owned());
    }
    let unnest_start = comma + 1 + (sql[comma + 1..].len() - after_comma.len());
    let open = sql[unnest_start..]
        .find('(')
        .map(|offset| unnest_start + offset)
        .ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "UNNEST requires parentheses"))?;
    let close = matching_paren(sql, open)?;
    let expression = sql[open + 1..close].trim();
    let remainder = &sql[close + 1..];
    let clause_offset = ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT"]
        .into_iter()
        .filter_map(|keyword| find_top_level_keyword(remainder, keyword, 0))
        .min()
        .unwrap_or(remainder.len());
    let alias = remainder[..clause_offset].trim();
    let clause = remainder[clause_offset..].trim();
    let output_name = parse_unnest_alias(alias)?;
    let select = sql[..from].trim_end();
    let base = sql[from + 4..comma].trim();
    if base.is_empty() {
        return Err(VqlError::new(
            ErrorCode::InvalidSql,
            "UNNEST requires a left input",
        ));
    }
    let relation_alias = relation_alias(base).unwrap_or_else(|| "__vql_unnest".to_owned());
    Ok(format!(
        "{select} FROM (SELECT *, UNNEST({expression}) AS {output_name} FROM {base}) AS {relation_alias}{}{}",
        if clause.is_empty() { "" } else { " " },
        clause
    ))
}

fn relation_alias(base: &str) -> Option<String> {
    let base = base.trim();
    if let Some(as_position) = find_top_level_keyword(base, "AS", 0) {
        return valid_alias(base[as_position + 2..].trim());
    }
    if let Some(split) = last_top_level_whitespace(base) {
        let candidate = base[split..].trim();
        if let Some(alias) = valid_alias(candidate) {
            return Some(alias);
        }
    }
    let candidate = base.rsplit('.').next()?.trim();
    valid_alias(candidate)
}

fn valid_alias(value: &str) -> Option<String> {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('`') && value.ends_with('`')))
    {
        return Some(value.to_owned());
    }
    let mut bytes = value.bytes();
    let first = bytes.next()?;
    (matches!(first, b'a'..=b'z' | b'A'..=b'Z' | b'_') && bytes.all(is_identifier_byte))
        .then(|| value.to_owned())
}

fn last_top_level_whitespace(sql: &str) -> Option<usize> {
    let code = sql_code_mask(sql);
    let mut depth = 0usize;
    let mut last = None;
    for (index, byte) in sql.bytes().enumerate() {
        if !code[index] {
            continue;
        }
        match byte {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && byte.is_ascii_whitespace() => last = Some(index),
            _ => {}
        }
    }
    last
}

fn parse_unnest_alias(alias: &str) -> Result<String> {
    let alias = alias
        .strip_prefix("AS ")
        .or_else(|| alias.strip_prefix("as "))
        .unwrap_or(alias)
        .trim();
    if alias.is_empty() {
        return Ok("unnest".to_owned());
    }
    if let Some(open) = alias.find('(') {
        let close = alias
            .rfind(')')
            .ok_or_else(|| VqlError::new(ErrorCode::InvalidSql, "invalid UNNEST column alias"))?;
        return Ok(alias[open + 1..close].trim().to_owned());
    }
    Ok(alias.to_owned())
}

fn matching_paren(sql: &str, open: usize) -> Result<usize> {
    matching_delimiter(sql, open, b'(', b')')
}

fn matching_delimiter(sql: &str, open: usize, opening: u8, closing: u8) -> Result<usize> {
    let mut depth = 0usize;
    let code = sql_code_mask(sql);
    for (offset, byte) in sql.as_bytes()[open..].iter().enumerate() {
        let index = open + offset;
        if !code[index] {
            continue;
        }
        if *byte == opening {
            depth += 1;
        } else if *byte == closing {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Ok(index);
            }
        }
    }
    Err(VqlError::new(
        ErrorCode::InvalidSql,
        "unterminated SQL expression",
    ))
}

fn find_top_level_keyword(sql: &str, keyword: &str, start: usize) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let target = keyword.as_bytes();
    let code = sql_code_mask(sql);
    scan_top_level(sql, start, |index| {
        bytes.get(index..index + target.len()) == Some(target)
            && code[index..index + target.len()].iter().all(|value| *value)
            && (index == 0 || !is_identifier_byte(bytes[index - 1]))
            && bytes
                .get(index + target.len())
                .is_none_or(|byte| !is_identifier_byte(*byte))
    })
}

fn find_top_level_char(sql: &str, target: char, start: usize) -> Option<usize> {
    scan_top_level(sql, start, |index| sql.as_bytes()[index] == target as u8)
}

fn scan_top_level(sql: &str, start: usize, predicate: impl Fn(usize) -> bool) -> Option<usize> {
    let bytes = sql.as_bytes();
    let code = sql_code_mask(sql);
    let mut depth = 0usize;
    let mut index = start;
    while index < bytes.len() {
        if !code[index] {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && predicate(index) {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

fn sql_code_mask(sql: &str) -> Vec<bool> {
    #[derive(Clone, Copy)]
    enum State {
        Code,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment(usize),
    }

    let bytes = sql.as_bytes();
    let mut mask = vec![false; bytes.len()];
    let mut state = State::Code;
    let mut index = 0usize;
    while index < bytes.len() {
        match state {
            State::Code => match bytes[index] {
                b'\'' => {
                    state = State::SingleQuote;
                    index += 1;
                }
                b'"' => {
                    state = State::DoubleQuote;
                    index += 1;
                }
                b'`' => {
                    state = State::Backtick;
                    index += 1;
                }
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    state = State::LineComment;
                    index += 2;
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    state = State::BlockComment(1);
                    index += 2;
                }
                _ => {
                    mask[index] = true;
                    index += 1;
                }
            },
            State::SingleQuote => {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        state = State::Code;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::DoubleQuote => {
                if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        index += 2;
                    } else {
                        state = State::Code;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::Backtick => {
                if bytes[index] == b'`' {
                    if bytes.get(index + 1) == Some(&b'`') {
                        index += 2;
                    } else {
                        state = State::Code;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::LineComment => {
                if bytes[index] == b'\n' {
                    state = State::Code;
                }
                index += 1;
            }
            State::BlockComment(depth) => {
                if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    state = State::BlockComment(depth + 1);
                    index += 2;
                } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    state = if depth == 1 {
                        State::Code
                    } else {
                        State::BlockComment(depth - 1)
                    };
                    index += 2;
                } else {
                    index += 1;
                }
            }
        }
    }
    mask
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::catalog::{
        CatalogStore, ModelDef, ModelInterface, ModelParameter, ModelType, ModelVersion,
    };

    fn model_snapshot() -> crate::catalog::DefinitionSnapshot {
        let directory = tempfile::tempdir().unwrap().keep();
        let catalog = CatalogStore::open(&directory.join("catalog.db")).unwrap();
        catalog
            .create_model(&ModelDef {
                name: "yolo".to_owned(),
                interface: ModelInterface {
                    capability: Some(ModelType::ObjectDetection),
                    parameters: vec![ModelParameter {
                        name: "image".to_owned(),
                        data_type: "IMAGE".to_owned(),
                        constant: false,
                        optional: false,
                    }],
                    semantic_arguments: vec![
                        ModelParameter {
                            name: "classes".to_owned(),
                            data_type: "ARRAY<STRING>".to_owned(),
                            constant: true,
                            optional: true,
                        },
                        ModelParameter {
                            name: "min_confidence".to_owned(),
                            data_type: "FLOAT".to_owned(),
                            constant: true,
                            optional: true,
                        },
                    ],
                    return_type: "detections".to_owned(),
                    processing_family: "vision.object_detection".to_owned(),
                    deterministic: true,
                },
                versions: vec![ModelVersion {
                    name: "v1".to_owned(),
                    source: "mock://person".to_owned(),
                    runtime_kind: "onnx-runtime".to_owned(),
                    options: BTreeMap::new(),
                    declaration_fingerprint: "declaration".to_owned(),
                    resolved: None,
                    created_at: 0,
                }],
                initial_version_fingerprint: "declaration".to_owned(),
                default_version: None,
                comment: None,
                builtin: false,
            })
            .unwrap();
        catalog.snapshot().unwrap()
    }

    #[test]
    fn normalizes_named_inference_options() {
        let snapshot = model_snapshot();
        assert_eq!(
            normalize_model_calls(
                "SELECT yolo(image, min_confidence => 0.5, classes => ['person', 'car'])",
                &snapshot,
            )
            .unwrap(),
            "SELECT yolo(image, ['person', 'car'], 0.5, NULL)"
        );
        assert_eq!(
            normalize_model_calls("SELECT yolo(image, min_confidence => 0.5)", &snapshot).unwrap(),
            "SELECT yolo(image, NULL, 0.5, NULL)"
        );
    }

    #[test]
    fn normalizes_builtin_ai_arguments() {
        assert_eq!(
            normalize_builtin_ai_calls(
                "SELECT VQL_CLASSIFY(image, ['person'], output_mode => 'multi', min_score => 0.4)"
            )
            .unwrap(),
            "SELECT __vql_classify(image, ['person'], 'multi', 0.4)"
        );
        assert_eq!(
            normalize_builtin_ai_calls(
                "SELECT VQL_DETECT(image, min_score => 0.5, classes => ['car'])"
            )
            .unwrap(),
            "SELECT __vql_detect(image, ['car'], 0.5)"
        );
        assert_eq!(
            normalize_builtin_ai_calls(
                "SELECT VQL_EXTRACT(image, MAP {\
                   'Total_Amount': 'What is the total?',\
                   'items': STRUCT('List the items' AS question, TRUE AS list)\
                 })"
            )
            .unwrap(),
            "SELECT __vql_extract(image, 'total_amount', 'What is the total?', FALSE, 'items', 'List the items', TRUE)"
        );
    }

    #[test]
    fn rejects_invalid_builtin_ai_arguments() {
        let positional = normalize_builtin_ai_calls(
            "SELECT VQL_EXTRACT(image, classes => ['person'], min_confidence => 0.5)",
        )
        .unwrap_err();
        assert_eq!(positional.code, ErrorCode::InvalidArgument);
        assert!(positional.message.contains("VQL_DETECT"));

        let detect_positional =
            normalize_builtin_ai_calls("SELECT VQL_DETECT(image, ['person'])").unwrap_err();
        assert!(
            detect_positional
                .message
                .contains("optional arguments must use")
        );

        let missing = normalize_builtin_ai_calls("SELECT VQL_CLASSIFY(image)").unwrap_err();
        assert!(missing.message.contains("requires argument 'categories'"));

        let duplicate = normalize_builtin_ai_calls(
            "SELECT VQL_EXTRACT(image, MAP {'Name': 'first', 'name': 'second'})",
        )
        .unwrap_err();
        assert_eq!(duplicate.code, ErrorCode::InvalidArgument);
        assert!(duplicate.message.contains("after normalization"));

        let marker = normalize_query("SELECT __vql_detect(image, NULL, 0.5)", &model_snapshot())
            .unwrap_err();
        assert_eq!(marker.code, ErrorCode::InvalidSql);
        assert!(marker.message.contains("internal planning marker"));
    }

    #[test]
    fn rejects_invalid_inference_option_shapes() {
        let snapshot = model_snapshot();
        let positional =
            normalize_model_calls("SELECT yolo(image, ['person'])", &snapshot).unwrap_err();
        assert!(positional.message.contains("must use name => value"));

        let duplicate = normalize_model_calls(
            "SELECT yolo(image, classes => ['person'], classes => ['car'])",
            &snapshot,
        )
        .unwrap_err();
        assert!(
            duplicate
                .message
                .contains("duplicate model 'yolo' argument 'classes'")
        );

        let unknown =
            normalize_model_calls("SELECT yolo(image, threshold => 0.5)", &snapshot).unwrap_err();
        assert!(
            unknown
                .message
                .contains("unknown model 'yolo' argument 'threshold'")
        );
    }

    #[test]
    fn rewrites_correlated_unnest_to_projection_unnest() {
        assert_eq!(
            rewrite_correlated_unnest("SELECT det.label FROM photos, UNNEST(yolo(image)) AS u(det) WHERE det.confidence > 0.5").unwrap(),
            "SELECT det.label FROM (SELECT *, UNNEST(yolo(image)) AS det FROM photos) AS photos WHERE det.confidence > 0.5"
        );
    }

    #[test]
    fn preserves_the_left_relation_alias_after_unnest_rewrite() {
        assert_eq!(
            rewrite_correlated_unnest("SELECT f.uri, det.box FROM traffic_videos AS f, UNNEST(yolo(f.frame)) AS det WHERE det.label = 'person'").unwrap(),
            "SELECT f.uri, det.box FROM (SELECT *, UNNEST(yolo(f.frame)) AS det FROM traffic_videos AS f) AS f WHERE det.label = 'person'"
        );
    }

    #[test]
    fn textual_rewrites_skip_literals_and_comments() {
        assert_eq!(
            find_function_call("SELECT 'plus_one(1)'", "plus_one").unwrap(),
            None
        );
        assert_eq!(
            find_function_call("SELECT 1 -- plus_one(1)\n", "plus_one").unwrap(),
            None
        );
        assert_eq!(replace_identifier("'x' || x", "x", "(1)"), "'x' || (1)");
        assert_eq!(rewrite_center("SELECT 'box.center'"), "SELECT 'box.center'");
    }
}
