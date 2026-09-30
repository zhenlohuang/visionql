use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::{ErrorCode, Result, VqlError};

/// Split a script on semicolon tokens, never on semicolons inside literals or comments.
pub fn split_statements(script: &str) -> Result<Vec<String>> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, script)
        .with_unescape(false)
        .tokenize()
        .map_err(|error| VqlError::new(ErrorCode::InvalidSql, error.to_string()))?;
    let mut result = Vec::new();
    let mut current = String::new();
    let mut significant = false;
    for token in tokens {
        if token == Token::SemiColon {
            push_statement(&mut result, &mut current, &mut significant);
        } else {
            significant |= !matches!(token, Token::Whitespace(_));
            current.push_str(&token.to_string());
        }
    }
    push_statement(&mut result, &mut current, &mut significant);
    Ok(result)
}

pub fn ends_with_statement_terminator(script: &str) -> Result<bool> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, script)
        .tokenize()
        .map_err(|error| VqlError::new(ErrorCode::InvalidSql, error.to_string()))?;
    Ok(tokens
        .iter()
        .rev()
        .find(|token| !matches!(token, Token::Whitespace(_)))
        == Some(&Token::SemiColon))
}

fn push_statement(result: &mut Vec<String>, current: &mut String, significant: &mut bool) {
    let trimmed = current.trim();
    if *significant && !trimmed.is_empty() {
        result.push(trimmed.to_owned());
    }
    current.clear();
    *significant = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_semicolons_inside_strings_and_comments() {
        let statements = split_statements(
            "SELECT ';' AS value; -- ignored ; here\nSELECT \"semi;colon\" FROM t;",
        )
        .unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("';'"));
        assert!(statements[1].contains("\"semi;colon\""));
    }

    #[test]
    fn ignores_comment_only_statements_and_detects_trailing_comments() {
        assert!(
            split_statements("/* no statement; */ -- still none\n")
                .unwrap()
                .is_empty()
        );
        assert!(ends_with_statement_terminator("SELECT 1; -- done").unwrap());
        assert!(!ends_with_statement_terminator("SELECT ';'").unwrap());
    }

    #[test]
    fn preserves_escaped_literals_and_identifiers_when_splitting() {
        let statements = split_statements(
            "SHOW CREATE MODEL \"odd\"\"name\" VERSION 'One''s;蓝色'; SELECT 'Vision''s model';",
        )
        .unwrap();
        assert_eq!(
            statements,
            [
                "SHOW CREATE MODEL \"odd\"\"name\" VERSION 'One''s;蓝色'",
                "SELECT 'Vision''s model'",
            ]
        );
    }
}
