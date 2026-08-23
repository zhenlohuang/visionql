use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, FieldRef};
use async_trait::async_trait;
use datafusion::common::{DFSchema, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::execution::context::{FunctionFactory, RegisterFunction};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::{CreateFunction, Expr, ExprSchemable, Volatility, create_udf};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use datafusion::sql::unparser::expr_to_sql;

use crate::catalog::{FunctionDef, FunctionImplementation};
use crate::models::semantic_fingerprint;
use crate::{ErrorCode, VqlError};

use super::python_udf::parse_data_type as parse_python_data_type;

#[derive(Debug, Default)]
pub(crate) struct VqlTypePlanner;

impl TypePlanner for VqlTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> DataFusionResult<Option<FieldRef>> {
        match sql_type {
            SqlDataType::Custom(name, arguments)
                if arguments.is_empty() && name.to_string().eq_ignore_ascii_case("IMAGE") =>
            {
                Ok(Some(Arc::new(crate::types::image_field("", true))))
            }
            SqlDataType::Custom(name, arguments)
                if name.to_string().eq_ignore_ascii_case("VECTOR")
                    || name.to_string().eq_ignore_ascii_case("TENSOR") =>
            {
                let rendered = format!("{}({})", name, arguments.join(", "));
                crate::models::parse_boundary_field("", &rendered, true)
                    .map(Some)
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct VqlFunctionFactory {
    definition: Mutex<Option<FunctionDef>>,
}

impl VqlFunctionFactory {
    pub(crate) fn take_definition(&self) -> crate::Result<FunctionDef> {
        self.definition
            .lock()
            .map_err(|_| {
                VqlError::new(ErrorCode::Internal, "function factory result was poisoned")
            })?
            .take()
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Internal,
                    "DataFusion FunctionFactory did not produce a function definition",
                )
            })
    }
}

#[async_trait]
impl FunctionFactory for VqlFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> DataFusionResult<RegisterFunction> {
        let (function, return_type) = build_definition(&statement).map_err(external_error)?;
        let argument_types = statement
            .args
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|argument| argument.data_type.clone())
            .collect::<Vec<_>>();
        let udf_name = function.name.clone();
        let execution_name = udf_name.clone();
        let udf = create_udf(
            &udf_name,
            argument_types,
            return_type,
            statement.params.behavior.unwrap_or(Volatility::Volatile),
            Arc::new(move |_| {
                Err(DataFusionError::Execution(format!(
                    "function '{execution_name}' is registered from the durable Catalog"
                )))
            }),
        );
        let mut output = self.definition.lock().map_err(|_| {
            DataFusionError::Internal("function factory result was poisoned".to_owned())
        })?;
        *output = Some(function);
        Ok(RegisterFunction::Scalar(Arc::new(udf)))
    }
}

fn build_definition(statement: &CreateFunction) -> crate::Result<(FunctionDef, DataType)> {
    if statement.or_replace || statement.temporary {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "OR REPLACE and TEMPORARY functions are not supported",
        ));
    }
    let function_name = statement.name.to_ascii_lowercase();
    if function_name.to_ascii_uppercase().starts_with("VQL_") {
        return Err(VqlError::new(
            ErrorCode::NameConflict,
            format!("function name '{function_name}' uses the reserved VQL_* prefix"),
        ));
    }
    if function_name.starts_with("vql.builtin.") || function_name.starts_with("builtin.") {
        return Err(VqlError::new(
            ErrorCode::NameConflict,
            "the vql.builtin schema is release-managed",
        ));
    }
    let arguments = statement.args.as_deref().unwrap_or_default();
    if arguments
        .iter()
        .any(|argument| argument.default_expr.is_some())
    {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "default function arguments are not supported",
        ));
    }
    let body = statement.params.function_body.as_ref().ok_or_else(|| {
        VqlError::new(
            ErrorCode::InvalidOption,
            "CREATE FUNCTION requires RETURN <expression> or a Python entry point",
        )
    })?;
    let language = statement
        .params
        .language
        .as_ref()
        .map(|language| language.value.to_ascii_lowercase());
    let is_python = language.as_deref() == Some("python");
    let parameters = arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            let data_type = data_type_name(&argument.data_type, !is_python)?;
            if data_type.eq_ignore_ascii_case("MODEL") {
                return Err(VqlError::new(
                    ErrorCode::InvalidOption,
                    "MODEL is not a function parameter type; use one wrapper function per model",
                ));
            }
            if is_python {
                parse_python_data_type(&data_type)?;
            }
            Ok((
                argument
                    .name
                    .as_ref()
                    .map(|name| name.value.to_ascii_lowercase())
                    .unwrap_or_else(|| format!("${}", index + 1)),
                data_type,
            ))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let resolved_return_type = match &statement.return_type {
        Some(return_type) => return_type.clone(),
        None if is_python => {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                "Python functions require RETURNS",
            ));
        }
        None => body.get_type(&DFSchema::empty()).map_err(|error| {
            VqlError::new(
                ErrorCode::InvalidSql,
                "cannot infer SQL expression function return type; add RETURNS",
            )
            .with_source(error)
        })?,
    };
    let return_type = data_type_name(&resolved_return_type, !is_python)?;
    if is_python {
        parse_python_data_type(&return_type)?;
    }
    let implementation = match language.as_deref() {
        None | Some("sql") => FunctionImplementation::SqlMacro {
            expression: expr_to_sql(body)
                .map_err(|error| {
                    VqlError::new(
                        ErrorCode::InvalidSql,
                        "failed to normalize SQL function expression",
                    )
                    .with_source(error)
                })?
                .to_string(),
        },
        Some("python") => FunctionImplementation::Python {
            entry: python_entry(body)?,
        },
        Some(language) => {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!("unsupported function language '{language}'"),
            ));
        }
    };
    let mut function = FunctionDef {
        name: function_name,
        implementation,
        parameters,
        constant_parameters: Vec::new(),
        return_type,
        semantic_fingerprint: String::new(),
    };
    function.semantic_fingerprint = semantic_fingerprint(&function);
    Ok((function, resolved_return_type))
}

fn python_entry(body: &Expr) -> crate::Result<String> {
    let Expr::Literal(value, _) = body else {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "LANGUAGE PYTHON requires AS 'module:function'",
        ));
    };
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
            if !value.is_empty() =>
        {
            Ok(value.clone())
        }
        _ => Err(VqlError::new(
            ErrorCode::InvalidOption,
            "LANGUAGE PYTHON requires AS 'module:function'",
        )),
    }
}

fn data_type_name(data_type: &DataType, allow_complex: bool) -> crate::Result<String> {
    if crate::types::is_image_storage(data_type) {
        return Ok("IMAGE".to_owned());
    }
    match data_type {
        DataType::Float32 => Ok("FLOAT".to_owned()),
        DataType::Float64 => Ok("DOUBLE".to_owned()),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok("STRING".to_owned()),
        DataType::Boolean => Ok("BOOLEAN".to_owned()),
        DataType::Int8 => Ok("TINYINT".to_owned()),
        DataType::Int16 => Ok("SMALLINT".to_owned()),
        DataType::Int32 => Ok("INT".to_owned()),
        DataType::Int64 => Ok("BIGINT".to_owned()),
        DataType::UInt8 => Ok("TINYINT UNSIGNED".to_owned()),
        DataType::UInt16 => Ok("SMALLINT UNSIGNED".to_owned()),
        DataType::UInt32 => Ok("INT UNSIGNED".to_owned()),
        DataType::UInt64 => Ok("BIGINT UNSIGNED".to_owned()),
        DataType::Binary => Ok("BINARY".to_owned()),
        other if allow_complex => Ok(other.to_string()),
        other => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("unsupported function type '{other}'"),
        )),
    }
}

fn external_error(error: VqlError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
