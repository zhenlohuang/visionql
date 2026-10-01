//! Public SQL documentation for the release-owned VQL functions.
//!
//! Metadata comes from DataFusion `#[user_doc]` attributes. It can be rendered as
//! Markdown, HTML, or another format without parsing Rust source. Catalog Models
//! and user-defined Functions are not part of this static reference.

use datafusion::logical_expr::ScalarUDF;
use datafusion_doc::Documentation;

use crate::functions::{BuiltinAiFunction, builtin_udfs};
use crate::{ErrorCode, Result, VqlError};

/// A public SQL function name and its DataFusion documentation metadata.
#[derive(Debug, Clone)]
pub struct FunctionDocumentation {
    pub name: String,
    pub documentation: Documentation,
}

/// Return documented built-ins in public-name order, without loading a Catalog
/// or resolving model artifacts. Missing metadata is an error so new registered
/// functions cannot silently disappear from the generated reference.
pub fn builtin_functions() -> Result<Vec<FunctionDocumentation>> {
    let mut functions = builtin_udfs()?
        .iter()
        .map(function_documentation)
        .collect::<Result<Vec<_>>>()?;
    functions.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(functions)
}

fn function_documentation(function: &ScalarUDF) -> Result<FunctionDocumentation> {
    let name = match BuiltinAiFunction::from_name(function.name()) {
        Some(function) => function.name(),
        None => function.name(),
    };
    let documentation = function.documentation().ok_or_else(|| {
        VqlError::new(
            ErrorCode::Internal,
            format!("built-in function '{name}' is missing #[user_doc] metadata"),
        )
    })?;
    Ok(FunctionDocumentation {
        name: name.to_owned(),
        documentation: documentation.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{Volatility, create_udf};
    use std::sync::Arc;

    #[test]
    fn registered_functions_publish_public_syntax_and_arguments() {
        let functions = builtin_functions().unwrap();
        let names = functions
            .iter()
            .map(|function| function.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "box_center",
                "polygon",
                "st_contains",
                "st_polygon",
                "tumble",
                "vql_classify",
                "vql_detect",
                "vql_extract"
            ]
        );
        for function in &functions {
            let doc = &function.documentation;
            assert!(!doc.description.is_empty());
            assert!(!doc.syntax_example.contains("__vql_"));
            assert!(doc.arguments.as_ref().is_some_and(|args| !args.is_empty()));
            assert!(
                doc.sql_example
                    .as_ref()
                    .is_some_and(|example| example.contains("```sql"))
            );
        }
        let extract = functions
            .iter()
            .find(|function| function.name == "vql_extract")
            .unwrap();
        let arguments = extract.documentation.arguments.as_ref().unwrap();
        assert_eq!(
            arguments
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["input", "fields"]
        );
        assert!(
            extract
                .documentation
                .description
                .contains("FEATURE_NOT_AVAILABLE")
        );
    }

    #[test]
    fn missing_function_metadata_fails_generation() {
        let function = create_udf(
            "undocumented",
            vec![DataType::Int64],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|args| Ok(args[0].clone())),
        );
        let error = function_documentation(&function).unwrap_err();
        assert!(error.to_string().contains("undocumented"));
        assert!(error.to_string().contains("missing #[user_doc] metadata"));
    }
}
