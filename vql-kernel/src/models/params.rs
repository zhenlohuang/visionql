use serde::{Deserialize, Serialize};

use super::registry::invalid_option;
use crate::Result;

pub(crate) const DEFAULT_MIN_CONFIDENCE: f32 = 0.25;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BoundInferenceParams {
    #[serde(default)]
    pub(crate) classes: Option<Vec<String>>,
    pub(crate) min_confidence: f32,
}

impl Default for BoundInferenceParams {
    fn default() -> Self {
        Self {
            classes: None,
            min_confidence: DEFAULT_MIN_CONFIDENCE,
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
    let min_confidence = min_confidence.unwrap_or(DEFAULT_MIN_CONFIDENCE);
    if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
        return invalid_option("min_confidence", "must be between 0 and 1");
    }
    Ok(BoundInferenceParams {
        classes,
        min_confidence,
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
        assert_eq!(params.min_confidence, 0.8);

        for confidence in [-0.1, 1.1, f32::NAN] {
            let error = bind_inference_params(None, Some(confidence)).unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidOption);
        }
    }
}
