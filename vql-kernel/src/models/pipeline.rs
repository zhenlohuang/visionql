use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, FixedShapeTensor};
use async_trait::async_trait;
use image::DynamicImage;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::backend::ModelBackend;
use crate::resources::{QueryBudget, QueryReservation};
use crate::{ErrorCode, Result, VqlError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TensorContract {
    pub(super) name: String,
    pub(super) dtype: DataType,
    /// `-1` is a wildcard dimension.
    pub(super) shape: Vec<i64>,
}

impl TensorContract {
    pub(super) fn validate_batch(&self, role: &str, batch: &TensorBatch) -> Result<()> {
        if batch.name() != self.name {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "{role} tensor name '{}' does not match contract '{}'",
                    batch.name(),
                    self.name
                ),
            ));
        }
        if batch.value_type() != &self.dtype {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "{role} tensor '{}' has dtype {}, expected {}",
                    batch.name(),
                    batch.value_type(),
                    self.dtype
                ),
            ));
        }
        validate_shape(role, &self.name, &self.shape, &batch.shape())
    }
}

pub(super) fn validate_shape(
    role: &str,
    name: &str,
    expected: &[i64],
    actual: &[i64],
) -> Result<()> {
    if expected.len() != actual.len()
        || expected
            .iter()
            .zip(actual)
            .any(|(expected, actual)| *expected >= 0 && expected != actual)
    {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "{role} tensor '{name}' shape {actual:?} is incompatible with contract {expected:?}"
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct FixedShapeMetadata {
    shape: Vec<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dim_names: Option<Vec<String>>,
}

/// A batch of equal-shape tensors represented by Arrow's canonical
/// `arrow.fixed_shape_tensor` extension type. The outer array length is the
/// batch dimension; the extension metadata stores the remaining dimensions.
#[derive(Debug, Clone)]
pub(super) struct TensorBatch {
    field: FieldRef,
    values: FixedSizeListArray,
}

impl TensorBatch {
    pub(super) fn try_new(
        name: impl Into<String>,
        shape: Vec<i64>,
        values: ArrayRef,
        dimension_names: Option<Vec<String>>,
    ) -> Result<Self> {
        let (batch_size, tensor_shape) = shape.split_first().ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                "fixed-shape tensor requires a batch dimension",
            )
        })?;
        let batch_size = usize::try_from(*batch_size).map_err(|_| {
            VqlError::new(
                ErrorCode::Execution,
                "fixed-shape tensor batch dimension must be non-negative",
            )
        })?;
        let tensor_shape = tensor_shape
            .iter()
            .map(|dimension| {
                if *dimension <= 0 {
                    return Err(VqlError::new(
                        ErrorCode::Execution,
                        "fixed-shape tensor dimensions must be positive",
                    ));
                }
                usize::try_from(*dimension).map_err(|_| {
                    VqlError::new(
                        ErrorCode::Execution,
                        "fixed-shape tensor dimensions exceed platform limits",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let metadata = FixedShapeMetadata {
            shape: tensor_shape.clone(),
            dim_names: dimension_names.clone(),
        };
        let tensor_type = FixedShapeTensor::try_new(
            values.data_type().clone(),
            tensor_shape,
            dimension_names,
            None,
        )
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "failed to define Arrow fixed-shape tensor type",
            )
            .with_source(error)
        })?;
        let list_size = i32::try_from(tensor_type.list_size()).map_err(|_| {
            VqlError::new(
                ErrorCode::Execution,
                "fixed-shape tensor element count exceeds Arrow limits",
            )
        })?;
        let expected_values = batch_size
            .checked_mul(tensor_type.list_size())
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    "fixed-shape tensor value count overflow",
                )
            })?;
        if values.len() != expected_values {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "fixed-shape tensor has {} values, expected {expected_values} for shape {shape:?}",
                    values.len()
                ),
            ));
        }
        if values.null_count() != 0 {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "runtime tensor values must not contain NULLs",
            ));
        }

        let value_field = Arc::new(Field::new("item", values.data_type().clone(), false));
        let array =
            FixedSizeListArray::try_new(value_field, list_size, values, None).map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to build Arrow fixed-shape tensor array",
                )
                .with_source(error)
            })?;
        let mut field = Field::new(name.into(), array.data_type().clone(), false);
        field
            .try_with_extension_type(tensor_type)
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::Execution,
                    "failed to attach Arrow fixed-shape tensor metadata",
                )
                .with_source(error)
            })?;
        // arrow-schema 58.4 serializes absent optional members as JSON null,
        // while its deserializer expects those keys to be omitted. Normalize
        // the canonical metadata so DataFusion and Arrow can read it back.
        field.metadata_mut().insert(
            EXTENSION_TYPE_METADATA_KEY.to_owned(),
            serde_json::to_string(&metadata).map_err(|error| {
                VqlError::new(
                    ErrorCode::Internal,
                    "failed to serialize Arrow fixed-shape tensor metadata",
                )
                .with_source(error)
            })?,
        );
        Ok(Self {
            field: Arc::new(field),
            values: array,
        })
    }

    pub(super) fn from_f32(
        name: impl Into<String>,
        shape: Vec<i64>,
        values: Vec<f32>,
        dimension_names: Option<Vec<String>>,
    ) -> Result<Self> {
        Self::try_new(
            name,
            shape,
            Arc::new(Float32Array::from(values)),
            dimension_names,
        )
    }

    pub(super) fn name(&self) -> &str {
        self.field.name()
    }

    pub(super) fn value_type(&self) -> &DataType {
        self.values.values().data_type()
    }

    pub(super) fn shape(&self) -> Vec<i64> {
        let metadata = self
            .field
            .metadata()
            .get(EXTENSION_TYPE_METADATA_KEY)
            .expect("FixedShapeTensor field has extension metadata");
        let metadata: FixedShapeMetadata = serde_json::from_str(metadata)
            .expect("FixedShapeTensor metadata created by Arrow is valid JSON");
        let mut shape = Vec::with_capacity(metadata.shape.len() + 1);
        shape.push(i64::try_from(self.values.len()).expect("Arrow array length fits i64"));
        shape.extend(
            metadata
                .shape
                .into_iter()
                .map(|value| i64::try_from(value).expect("FixedShapeTensor dimension fits i64")),
        );
        shape
    }

    pub(super) fn as_f32(&self, role: &str) -> Result<&[f32]> {
        let values = self
            .values
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                VqlError::new(
                    ErrorCode::Execution,
                    format!(
                        "{role} tensor '{}' must contain float32 values",
                        self.name()
                    ),
                )
            })?;
        let start = self.values.offset() * self.values.value_length() as usize;
        let length = self.values.len() * self.values.value_length() as usize;
        Ok(&values.values()[start..start + length])
    }
}

#[derive(Debug)]
pub(super) struct RuntimeRequestBatch {
    pub(super) input: TensorBatch,
    pub(super) output_names: Vec<String>,
}

#[derive(Debug)]
pub(super) struct RuntimeResponseBatch {
    pub(super) outputs: BTreeMap<String, TensorBatch>,
    pub(super) _reservations: Vec<QueryReservation>,
}

impl RuntimeResponseBatch {
    pub(super) fn output(&self, name: &str) -> Result<&TensorBatch> {
        self.outputs.get(name).ok_or_else(|| {
            VqlError::new(
                ErrorCode::Execution,
                format!("Runtime output '{name}' is missing"),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ImageTransform {
    pub(super) original_width: f32,
    pub(super) original_height: f32,
    pub(super) input_width: f32,
    pub(super) input_height: f32,
    pub(super) scale: f32,
    pub(super) pad_x: f32,
    pub(super) pad_y: f32,
}

#[derive(Debug)]
pub(super) struct PreProcessContext {
    pub(super) transforms: Vec<ImageTransform>,
}

#[derive(Debug)]
pub(super) struct PreprocessedBatch {
    pub(super) input: TensorBatch,
    pub(super) context: PreProcessContext,
}

pub(super) trait PreProcessor: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn runtime_output(&self) -> &TensorContract;
    fn output_bytes(&self, batch_size: usize) -> Result<usize>;
    fn process(&self, images: &[DynamicImage]) -> Result<PreprocessedBatch>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchingOwner {
    VisionQl,
    Service,
}

#[async_trait]
pub(super) trait RuntimeSession: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn input_contract(&self) -> &TensorContract;
    fn output_contract(&self) -> &TensorContract;
    fn batching_owner(&self) -> BatchingOwner;
    async fn infer(
        &self,
        batch: RuntimeRequestBatch,
        cancel: CancellationToken,
        budget: QueryBudget,
    ) -> Result<RuntimeResponseBatch>;
}

pub(super) trait PostProcessor: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn runtime_input(&self) -> &TensorContract;
    fn process(
        &self,
        response: RuntimeResponseBatch,
        context: &PreProcessContext,
    ) -> Result<ArrayRef>;
}

pub(super) struct CompiledPipeline {
    pre_processor: Arc<dyn PreProcessor>,
    runtime: Arc<dyn RuntimeSession>,
    post_processor: Arc<dyn PostProcessor>,
}

impl CompiledPipeline {
    pub(super) fn try_new(
        pre_processor: Arc<dyn PreProcessor>,
        runtime: Arc<dyn RuntimeSession>,
        post_processor: Arc<dyn PostProcessor>,
    ) -> Result<Self> {
        validate_contracts(
            "PreProcessor output",
            pre_processor.runtime_output(),
            "Runtime input",
            runtime.input_contract(),
        )?;
        validate_contracts(
            "Runtime output",
            runtime.output_contract(),
            "PostProcessor input",
            post_processor.runtime_input(),
        )?;
        Ok(Self {
            pre_processor,
            runtime,
            post_processor,
        })
    }

    pub(super) fn batching_owner(&self) -> BatchingOwner {
        self.runtime.batching_owner()
    }
}

fn validate_contracts(
    left_role: &str,
    left: &TensorContract,
    right_role: &str,
    right: &TensorContract,
) -> Result<()> {
    if left.name != right.name || left.dtype != right.dtype || left.shape != right.shape {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "{left_role} contract {left:?} is incompatible with {right_role} contract {right:?}"
            ),
        ));
    }
    Ok(())
}

impl Debug for CompiledPipeline {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledPipeline")
            .field("pre_processor", &self.pre_processor.kind())
            .field("runtime", &self.runtime.kind())
            .field("post_processor", &self.post_processor.kind())
            .field("batching_owner", &self.batching_owner())
            .finish()
    }
}

#[async_trait]
impl ModelBackend for CompiledPipeline {
    async fn infer(
        &self,
        images: Vec<DynamicImage>,
        cancel: CancellationToken,
        budget: &QueryBudget,
    ) -> Result<ArrayRef> {
        let expected_rows = images.len();
        let tensor_bytes = self.pre_processor.output_bytes(expected_rows)?;
        let _tensor_reservation =
            budget.reserve(crate::QueryResource::ModelTensor, tensor_bytes)?;
        let preprocessed = self.pre_processor.process(&images)?;
        let request = RuntimeRequestBatch {
            input: preprocessed.input,
            output_names: vec![self.post_processor.runtime_input().name.clone()],
        };
        let response = self.runtime.infer(request, cancel, budget.clone()).await?;
        let output = self
            .post_processor
            .process(response, &preprocessed.context)?;
        if output.len() != expected_rows {
            return Err(VqlError::new(
                ErrorCode::Execution,
                format!(
                    "compiled model pipeline returned {} rows for {} inputs",
                    output.len(),
                    expected_rows
                ),
            ));
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tensor_batch_uses_canonical_fixed_shape_tensor_metadata() {
        let batch = TensorBatch::from_f32(
            "images",
            vec![2, 3, 2, 4],
            vec![0.0; 48],
            Some(vec!["C".to_owned(), "H".to_owned(), "W".to_owned()]),
        )
        .unwrap();

        let tensor_type = batch
            .field
            .try_extension_type::<FixedShapeTensor>()
            .unwrap();
        assert_eq!(tensor_type.value_type(), &DataType::Float32);
        assert_eq!(tensor_type.list_size(), 24);
        assert_eq!(
            tensor_type.dimension_names(),
            Some(["C".to_owned(), "H".to_owned(), "W".to_owned()].as_slice())
        );
        assert_eq!(batch.shape(), vec![2, 3, 2, 4]);
        assert_eq!(batch.as_f32("input").unwrap().len(), 48);
    }

    #[test]
    fn tensor_batch_rejects_shape_value_count_mismatch() {
        let error =
            TensorBatch::from_f32("images", vec![2, 3, 2, 4], vec![0.0; 47], None).unwrap_err();

        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("expected 48"));
    }
}
