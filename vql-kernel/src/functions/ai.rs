use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BuiltinAiFunction {
    Classify,
    Extract,
}

impl BuiltinAiFunction {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Classify => "vql_classify",
            Self::Extract => "vql_extract",
        }
    }

    pub(crate) fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case(Self::Classify.name()) {
            Some(Self::Classify)
        } else if name.eq_ignore_ascii_case(Self::Extract.name()) {
            Some(Self::Extract)
        } else {
            None
        }
    }

    pub(crate) fn output_field(self, name: impl Into<String>, nullable: bool) -> FieldRef {
        match self {
            Self::Classify => crate::models::classification_field(name, nullable),
            Self::Extract => crate::models::detection_field(name, nullable),
        }
    }

    fn parameter_names(self) -> Vec<String> {
        match self {
            Self::Classify => ["input", "categories", "min_score"],
            Self::Extract => ["input", "classes", "min_confidence"],
        }
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}

#[derive(Debug)]
struct BuiltinAiMarker {
    function: BuiltinAiFunction,
    signature: Signature,
    return_field: FieldRef,
}

impl PartialEq for BuiltinAiMarker {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
    }
}

impl Eq for BuiltinAiMarker {}

impl Hash for BuiltinAiMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
    }
}

impl ScalarUDFImpl for BuiltinAiMarker {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        exec_err!(
            "built-in AI function '{}' reached scalar execution without InferenceNode extraction",
            self.function.name().to_ascii_uppercase()
        )
    }
}

pub(crate) fn builtin_ai_udf(function: BuiltinAiFunction) -> crate::Result<ScalarUDF> {
    let signature = Signature::one_of(vec![TypeSignature::Any(3)], Volatility::Volatile)
        .with_parameter_names(function.parameter_names())
        .map_err(|error| {
            crate::VqlError::new(
                crate::ErrorCode::Internal,
                "invalid built-in AI function signature",
            )
            .with_source(error)
        })?;
    Ok(ScalarUDF::new_from_impl(BuiltinAiMarker {
        function,
        signature,
        return_field: function.output_field("", true),
    }))
}
