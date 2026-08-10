use crate::{ErrorCode, Result, VqlError};

pub(super) fn normalize_query(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> Result<String> {
    let sql = normalize_inference_calls(sql, false)?;
    let sql = expand_macros(&sql, snapshot)?;
    let sql = normalize_inference_calls(&sql, true)?;
    let sql = rewrite_center(&sql);
    rewrite_correlated_unnest(&sql)
}

fn normalize_inference_calls(sql: &str, allow_canonical_positional: bool) -> Result<String> {
    let mut output = sql.to_owned();
    let mut cursor = 0usize;
    while let Some((_start, open, close)) =
        find_function_call_from(&output, "DETECT_OBJECTS", cursor)?
    {
        let args = split_args(&output[open + 1..close])?;
        let has_named = args.iter().any(|arg| top_level_arrow(arg).is_some());
        if !has_named {
            if args.len() > 2 && !allow_canonical_positional {
                return Err(VqlError::new(
                    ErrorCode::InvalidSql,
                    "DETECT_OBJECTS optional arguments must use name => value",
                ));
            }
            cursor = close + 1;
            continue;
        }

        let mut positional = Vec::new();
        let mut classes = None;
        let mut min_confidence = None;
        let mut seen_named = false;
        for arg in args {
            if let Some(arrow) = top_level_arrow(arg) {
                seen_named = true;
                let name = arg[..arrow].trim().to_ascii_lowercase();
                let value = arg[arrow + 2..].trim();
                if value.is_empty() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!("DETECT_OBJECTS argument '{name}' requires a value"),
                    ));
                }
                let target = match name.as_str() {
                    "classes" => &mut classes,
                    "min_confidence" => &mut min_confidence,
                    _ => {
                        return Err(VqlError::new(
                            ErrorCode::InvalidSql,
                            format!("unknown DETECT_OBJECTS argument '{name}'"),
                        ));
                    }
                };
                if target.replace(value.to_owned()).is_some() {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        format!("duplicate DETECT_OBJECTS argument '{name}'"),
                    ));
                }
            } else {
                if seen_named {
                    return Err(VqlError::new(
                        ErrorCode::InvalidSql,
                        "DETECT_OBJECTS positional arguments must precede named arguments",
                    ));
                }
                positional.push(arg.trim().to_owned());
            }
        }
        if positional.len() != 2 {
            return Err(VqlError::new(
                ErrorCode::InvalidSql,
                "DETECT_OBJECTS requires positional model and image arguments",
            ));
        }
        let mut ordered = positional;
        if classes.is_some() || min_confidence.is_some() {
            ordered.push(classes.unwrap_or_else(|| "NULL".to_owned()));
        }
        if let Some(min_confidence) = min_confidence {
            ordered.push(min_confidence);
        }
        let replacement = ordered.join(", ");
        output.replace_range(open + 1..close, &replacement);
        cursor = open + replacement.len() + 2;
    }
    Ok(output)
}

fn expand_macros(sql: &str, snapshot: &crate::catalog::DefinitionSnapshot) -> Result<String> {
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
                let mut expanded = expression.clone();
                for ((parameter, _), argument) in function.definition.parameters.iter().zip(args) {
                    expanded = replace_identifier(&expanded, parameter, &format!("({argument})"));
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
    let mut depth = 0usize;
    let code = sql_code_mask(sql);
    for (offset, byte) in sql.as_bytes()[open..].iter().enumerate() {
        let index = open + offset;
        if !code[index] {
            continue;
        }
        if *byte == b'(' {
            depth += 1;
        } else if *byte == b')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Ok(index);
            }
        }
    }
    Err(VqlError::new(
        ErrorCode::InvalidSql,
        "unterminated UNNEST expression",
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

    #[test]
    fn normalizes_named_inference_options() {
        assert_eq!(
            normalize_inference_calls(
                "SELECT DETECT_OBJECTS('yolo', image, min_confidence => 0.5, classes => ['person', 'car'])",
                false,
            )
            .unwrap(),
            "SELECT DETECT_OBJECTS('yolo', image, ['person', 'car'], 0.5)"
        );
        assert_eq!(
            normalize_inference_calls(
                "SELECT DETECT_OBJECTS('yolo', image, min_confidence => 0.5)",
                false,
            )
            .unwrap(),
            "SELECT DETECT_OBJECTS('yolo', image, NULL, 0.5)"
        );
    }

    #[test]
    fn rejects_invalid_inference_option_shapes() {
        let positional =
            normalize_inference_calls("SELECT DETECT_OBJECTS('yolo', image, ['person'])", false)
                .unwrap_err();
        assert!(positional.message.contains("must use name => value"));

        let duplicate = normalize_inference_calls(
            "SELECT DETECT_OBJECTS('yolo', image, classes => ['person'], classes => ['car'])",
            false,
        )
        .unwrap_err();
        assert!(
            duplicate
                .message
                .contains("duplicate DETECT_OBJECTS argument 'classes'")
        );

        let unknown = normalize_inference_calls(
            "SELECT DETECT_OBJECTS('yolo', image, threshold => 0.5)",
            false,
        )
        .unwrap_err();
        assert!(
            unknown
                .message
                .contains("unknown DETECT_OBJECTS argument 'threshold'")
        );
    }

    #[test]
    fn rewrites_correlated_unnest_to_projection_unnest() {
        assert_eq!(
            rewrite_correlated_unnest("SELECT det.label FROM photos, UNNEST(DETECT_OBJECTS('yolo', image)) AS u(det) WHERE det.confidence > 0.5").unwrap(),
            "SELECT det.label FROM (SELECT *, UNNEST(DETECT_OBJECTS('yolo', image)) AS det FROM photos) AS photos WHERE det.confidence > 0.5"
        );
    }

    #[test]
    fn preserves_the_left_relation_alias_after_unnest_rewrite() {
        assert_eq!(
            rewrite_correlated_unnest("SELECT f.uri, det.box FROM traffic_videos AS f, UNNEST(DETECT_OBJECTS('yolo', f.frame)) AS det WHERE det.label = 'person'").unwrap(),
            "SELECT f.uri, det.box FROM (SELECT *, UNNEST(DETECT_OBJECTS('yolo', f.frame)) AS det FROM traffic_videos AS f) AS f WHERE det.label = 'person'"
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
