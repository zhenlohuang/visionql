use serde::{Deserialize, Serialize};

use super::registry::invalid_option;
use crate::Result;

pub(crate) const DEFAULT_MIN_SCORE: f32 = 0.25;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ClassificationOutputMode {
    Single,
    Multi,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExtractFieldSpec {
    pub(crate) name: String,
    pub(crate) question: String,
    pub(crate) list: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BoundInferenceParams {
    #[serde(default)]
    pub(crate) classes: Option<Vec<String>>,
    pub(crate) min_score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) output_mode: Option<ClassificationOutputMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) extract_fields: Vec<ExtractFieldSpec>,
}

impl Default for BoundInferenceParams {
    fn default() -> Self {
        Self {
            classes: None,
            min_score: DEFAULT_MIN_SCORE,
            output_mode: None,
            extract_fields: Vec::new(),
        }
    }
}

pub(crate) fn bind_inference_params(
    classes: Option<Vec<String>>,
    min_confidence: Option<f32>,
) -> Result<BoundInferenceParams> {
    if classes
        .as_ref()
        .is_some_and(|values| values.iter().any(String::is_empty))
    {
        return invalid_option("classes", "must not contain empty labels");
    }
    let min_confidence = min_confidence.unwrap_or(DEFAULT_MIN_SCORE);
    if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
        return invalid_option("min_confidence", "must be between 0 and 1");
    }
    Ok(BoundInferenceParams {
        classes,
        min_score: min_confidence,
        output_mode: None,
        extract_fields: Vec::new(),
    })
}

pub(crate) fn bind_detection_params(
    classes: Option<Vec<String>>,
    min_score: Option<f32>,
) -> Result<BoundInferenceParams> {
    let mut params = bind_inference_params(classes, min_score).map_err(|error| {
        crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            error.message.replace("min_confidence", "min_score"),
        )
    })?;
    params.output_mode = None;
    Ok(params)
}

pub(crate) fn bind_classification_params(
    categories: Option<Vec<String>>,
    output_mode: Option<String>,
    min_score: Option<f32>,
) -> Result<BoundInferenceParams> {
    let categories = categories.ok_or_else(|| {
        crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            "VQL_CLASSIFY categories must be a non-empty constant array of distinct strings",
        )
    })?;
    if categories.is_empty() || categories.iter().any(String::is_empty) {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            "VQL_CLASSIFY categories must be a non-empty constant array of distinct strings",
        ));
    }
    let mut distinct = std::collections::HashSet::new();
    if categories.iter().any(|category| !distinct.insert(category)) {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            "VQL_CLASSIFY categories must be distinct",
        ));
    }
    let output_mode = match output_mode.as_deref().unwrap_or("single") {
        "single" => ClassificationOutputMode::Single,
        "multi" => ClassificationOutputMode::Multi,
        value => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidArgument,
                format!("VQL_CLASSIFY output_mode must be 'single' or 'multi'; got '{value}'"),
            ));
        }
    };
    if output_mode == ClassificationOutputMode::Single && min_score.is_some() {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            "VQL_CLASSIFY min_score is only valid when output_mode => 'multi'",
        ));
    }
    let min_score = min_score.unwrap_or(DEFAULT_MIN_SCORE);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidArgument,
            "min_score must be between 0 and 1",
        ));
    }
    Ok(BoundInferenceParams {
        classes: Some(categories),
        min_score,
        output_mode: Some(output_mode),
        extract_fields: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    #[test]
    fn invocation_options_are_validated() {
        let params = bind_inference_params(Some(vec!["person".to_owned()]), Some(0.8)).unwrap();
        assert_eq!(
            params.classes.as_deref(),
            Some(["person".to_owned()].as_slice())
        );
        assert_eq!(params.min_score, 0.8);

        for confidence in [-0.1, 1.1, f32::NAN] {
            let error = bind_inference_params(None, Some(confidence)).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidOption);
        }
    }

    #[test]
    fn classification_contract_validates_mode_and_categories() {
        let params =
            bind_classification_params(Some(vec!["cat".to_owned(), "dog".to_owned()]), None, None)
                .unwrap();
        assert_eq!(params.output_mode, Some(ClassificationOutputMode::Single));
        assert!(bind_classification_params(Some(vec![]), None, None).is_err());
        assert!(
            bind_classification_params(Some(vec!["cat".to_owned(), "cat".to_owned()]), None, None,)
                .is_err()
        );
        assert!(
            bind_classification_params(Some(vec!["cat".to_owned()]), None, Some(0.5),).is_err()
        );
    }
}
