use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::catalog::CatalogStore;
use crate::catalog::FunctionDef;
use crate::functions::materialize_encoded_images;
use crate::media::MediaRuntime;
use crate::python::{PyUdfHandle, PythonUdfHostRef};
use crate::resources::{QueryBudget, QueryReservation};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug)]
struct PythonFunction {
    function: FunctionDef,
    signature: Signature,
    return_type: DataType,
    host: Option<PythonUdfHostRef>,
    handle: Option<PyUdfHandle>,
    fail_on_error: Arc<AtomicBool>,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    budget: Option<QueryBudget>,
}

impl PartialEq for PythonFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function.semantic_fingerprint == other.function.semantic_fingerprint
    }
}

impl Eq for PythonFunction {}

impl Hash for PythonFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.semantic_fingerprint.hash(state);
    }
}

impl ScalarUDFImpl for PythonFunction {
    fn name(&self) -> &str {
        &self.function.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let fail_on_error = self.fail_on_error.load(Ordering::Relaxed);
        let mut materialization_reservations = Vec::<QueryReservation>::new();
        let arrays = arrays
            .into_iter()
            .map(|array| {
                if crate::types::is_image_storage(array.data_type()) {
                    let images = array
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .expect("IMAGE storage is a StructArray");
                    let materialized = materialize_encoded_images(
                        Arc::clone(&self.catalog),
                        Arc::clone(&self.media),
                        images,
                        fail_on_error,
                        self.budget.clone(),
                    )
                    .map_err(|error| {
                        datafusion::common::DataFusionError::Execution(error.to_string())
                    })?;
                    materialization_reservations.extend(materialized.reservations);
                    Ok(materialized.array)
                } else {
                    Ok(array)
                }
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let row_count = arrays.first().map_or(args.number_rows, |array| array.len());
        let Some(host) = &self.host else {
            return Err(datafusion::common::DataFusionError::Execution(
                VqlError::new(
                    ErrorCode::PythonHostRequired,
                    format!(
                        "function '{}' requires the visionql Python host",
                        self.function.name
                    ),
                )
                .to_string(),
            ));
        };
        let handle = self
            .handle
            .as_ref()
            .expect("registered Python host has a resolved handle");
        let expected = Arc::new(Field::new("result", self.return_type.clone(), true));
        match host.invoke(
            handle,
            &arrays,
            &expected,
            &tokio_util::sync::CancellationToken::new(),
        ) {
            Ok(array) if array.len() == row_count && array.data_type() == &self.return_type => {
                Ok(ColumnarValue::Array(array))
            }
            Ok(array) => Err(datafusion::common::DataFusionError::Execution(format!(
                "Python UDF '{}' returned length/type {}/{}; expected {row_count}/{}",
                self.function.name,
                array.len(),
                array.data_type(),
                self.return_type
            ))),
            Err(error) if fail_on_error => Err(datafusion::common::DataFusionError::Execution(
                error.to_string(),
            )),
            Err(_) => Ok(ColumnarValue::Array(new_null_array(
                &self.return_type,
                row_count,
            ))),
        }
    }
}

pub(crate) fn python_function_udf(
    function: FunctionDef,
    host: Option<PythonUdfHostRef>,
    fail_on_error: Arc<AtomicBool>,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    budget: Option<QueryBudget>,
) -> Result<ScalarUDF> {
    let crate::catalog::FunctionImplementation::Python { entry } = &function.implementation else {
        return Err(VqlError::new(
            ErrorCode::Internal,
            "python_function_udf received a non-Python definition",
        ));
    };
    let argument_types = function
        .parameters
        .iter()
        .map(|(_, data_type)| parse_data_type(data_type))
        .collect::<Result<Vec<_>>>()?;
    let return_type = parse_data_type(&function.return_type)?;
    let handle = host.as_ref().map(|host| host.resolve(entry)).transpose()?;
    let mut signature = Signature::exact(argument_types, Volatility::Volatile);
    if function
        .parameters
        .iter()
        .all(|(name, _)| !name.starts_with('$'))
    {
        signature = signature
            .with_parameter_names(
                function
                    .parameters
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect(),
            )
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    "invalid Python function parameters",
                )
                .with_source(error)
            })?;
    }
    Ok(ScalarUDF::new_from_impl(PythonFunction {
        function,
        signature,
        return_type,
        host,
        handle,
        fail_on_error,
        catalog,
        media,
        budget,
    }))
}

pub(super) fn parse_data_type(value: &str) -> Result<DataType> {
    match value.to_ascii_uppercase().as_str() {
        "IMAGE" => Ok(DataType::Struct(crate::types::image_storage_fields())),
        "FLOAT" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "STRING" | "VARCHAR" => Ok(DataType::Utf8),
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "TINYINT UNSIGNED" => Ok(DataType::UInt8),
        "SMALLINT UNSIGNED" => Ok(DataType::UInt16),
        "INT UNSIGNED" => Ok(DataType::UInt32),
        "BIGINT UNSIGNED" => Ok(DataType::UInt64),
        "BINARY" => Ok(DataType::Binary),
        data_type => Err(VqlError::new(
            ErrorCode::InvalidOption,
            format!("unsupported Python UDF type '{data_type}'"),
        )),
    }
}
