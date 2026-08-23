use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, ListArray, ListBuilder, StringArray,
    StringBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::DataType;
use serde::Deserialize;

use super::super::pipeline::{
    PostProcessor, PreProcessContext, RuntimeResponseBatch, TensorContract,
};
use super::super::registry::{PostProcessorFactory, deserialize_processor_options, invalid_option};
use super::super::{BoundInferenceParams, classification_fields};
use crate::catalog::{ModelType, ProcessorSpec};
use crate::{ErrorCode, Result, VqlError};

const KIND: &str = "vision.image_classification@1";
const SUPPORTED_TYPES: &[ModelType] = &[ModelType::ImageClassification];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassificationOptions {
    #[serde(default = "default_output_name")]
    output_name: String,
    labels: Vec<String>,
}

impl ClassificationOptions {
    fn parse(spec: &ProcessorSpec) -> Result<Self> {
        let options: Self = deserialize_processor_options("post_processor.options", &spec.options)?;
        if options.output_name.is_empty() {
            return invalid_option(
                "post_processor.options.output_name",
                "must be a non-empty string",
            );
        }
        if options.labels.is_empty() || options.labels.iter().any(String::is_empty) {
            return invalid_option(
                "post_processor.options.labels",
                "must be a non-empty array of non-empty strings",
            );
        }
        Ok(options)
    }
}

fn default_output_name() -> String {
    "output0".to_owned()
}

#[derive(Debug)]
pub(in crate::models) struct ClassificationPostProcessorFactory;

impl PostProcessorFactory for ClassificationPostProcessorFactory {
    fn kind(&self) -> &str {
        KIND
    }

    fn supported_types(&self) -> &[ModelType] {
        SUPPORTED_TYPES
    }

    fn validate(&self, spec: &ProcessorSpec) -> Result<()> {
        ClassificationOptions::parse(spec).map(|_| ())
    }

    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PostProcessor>> {
        let options = ClassificationOptions::parse(spec)?;
        let class_count = i64::try_from(options.labels.len()).map_err(|_| {
            VqlError::new(
                ErrorCode::InvalidOption,
                "post_processor.options.labels contains too many labels",
            )
        })?;
        Ok(Arc::new(ClassificationPostProcessor {
            output_contract: TensorContract {
                name: options.output_name,
                dtype: DataType::Float32,
                shape: vec![-1, class_count],
            },
            labels: options.labels,
        }))
    }
}

#[derive(Debug)]
struct ClassificationPostProcessor {
    output_contract: TensorContract,
    labels: Vec<String>,
}

impl PostProcessor for ClassificationPostProcessor {
    fn kind(&self) -> &str {
        KIND
    }

    fn runtime_input(&self) -> &TensorContract {
        &self.output_contract
    }

    fn process(
        &self,
        response: RuntimeResponseBatch,
        context: &PreProcessContext,
    ) -> Result<ArrayRef> {
        let output = response.output(&self.output_contract.name)?;
        self.output_contract
            .validate_batch("Runtime output", output)?;
        let scores = output.as_f32("Runtime output")?;
        let mut rows = Vec::with_capacity(context.transforms.len());
        for values in scores.chunks_exact(self.labels.len()) {
            let mut classifications = self
                .labels
                .iter()
                .cloned()
                .zip(values.iter().copied())
                .map(|(label, score)| {
                    if !score.is_finite() {
                        return Err(VqlError::new(
                            ErrorCode::Execution,
                            "classification Runtime returned a non-finite score",
                        ));
                    }
                    Ok(Classification { label, score })
                })
                .collect::<Result<Vec<_>>>()?;
            classifications.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.label.cmp(&right.label))
            });
            rows.push(Some(classifications));
        }
        if rows.len() != context.transforms.len() {
            return Err(VqlError::new(
                ErrorCode::Execution,
                "classification Runtime batch size does not match the input batch",
            ));
        }
        Ok(build_classification_array(rows, context.transforms.len()))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Classification {
    label: String,
    score: f32,
}

pub(in crate::models) fn empty_classification_output() -> ArrayRef {
    build_classification_array(std::iter::empty(), 0)
}

pub(in crate::models) fn filter_and_scatter_classifications(
    input: &ArrayRef,
    positions: &[usize],
    total_rows: usize,
    invocation: &BoundInferenceParams,
) -> Result<ArrayRef> {
    let input = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        VqlError::new(
            ErrorCode::Internal,
            "IMAGE_CLASSIFICATION pipeline returned a non-classification Arrow array",
        )
    })?;
    if input.len() != positions.len() {
        return Err(VqlError::new(
            ErrorCode::Execution,
            format!(
                "model backend returned {} rows for {} inputs",
                input.len(),
                positions.len()
            ),
        ));
    }
    let mut rows = vec![None; total_rows];
    for (source_row, target_row) in positions.iter().copied().enumerate() {
        let classifications = read_classification_row(input, source_row)?
            .into_iter()
            .filter(|classification| {
                classification.score >= invocation.min_confidence
                    && invocation
                        .classes
                        .as_ref()
                        .is_none_or(|classes| classes.contains(&classification.label))
            })
            .collect();
        rows[target_row] = Some(classifications);
    }
    Ok(build_classification_array(rows, total_rows))
}

fn build_classification_array(
    rows: impl IntoIterator<Item = Option<Vec<Classification>>>,
    capacity: usize,
) -> ArrayRef {
    let item_builder = StructBuilder::new(
        classification_fields(),
        vec![
            Box::new(StringBuilder::with_capacity(capacity, capacity * 12)),
            Box::new(Float32Builder::with_capacity(capacity)),
        ],
    );
    let mut builder = ListBuilder::new(item_builder);
    for row in rows {
        let Some(classifications) = row else {
            builder.append(false);
            continue;
        };
        for classification in classifications {
            let values = builder.values();
            values
                .field_builder::<StringBuilder>(0)
                .expect("classification label builder")
                .append_value(classification.label);
            values
                .field_builder::<Float32Builder>(1)
                .expect("classification score builder")
                .append_value(classification.score);
            values.append(true);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn read_classification_row(array: &ListArray, row: usize) -> Result<Vec<Classification>> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "classification item is not a struct"))?;
    let labels = values
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "classification label is not STRING"))?;
    let scores = values
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VqlError::new(ErrorCode::Internal, "classification score is not FLOAT"))?;
    Ok((0..values.len())
        .map(|index| Classification {
            label: labels.value(index).to_owned(),
            score: scores.value(index),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_categories_and_scatters_rows() {
        let input = build_classification_array(
            [Some(vec![
                Classification {
                    label: "tabby".to_owned(),
                    score: 0.8,
                },
                Classification {
                    label: "tiger cat".to_owned(),
                    score: 0.2,
                },
            ])],
            1,
        );
        let output = filter_and_scatter_classifications(
            &input,
            &[1],
            3,
            &BoundInferenceParams {
                classes: Some(vec!["tabby".to_owned()]),
                min_confidence: 0.5,
            },
        )
        .unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(output.is_null(0));
        assert_eq!(read_classification_row(output, 1).unwrap().len(), 1);
        assert!(output.is_null(2));
    }
}
