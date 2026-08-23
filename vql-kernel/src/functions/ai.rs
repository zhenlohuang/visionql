use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{ScalarValue, exec_err, internal_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use std::hash::{Hash, Hasher};

use crate::models::ExtractFieldSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BuiltinAiFunction {
    Classify,
    Extract,
    Detect,
}

impl BuiltinAiFunction {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Classify => "vql_classify",
            Self::Extract => "vql_extract",
            Self::Detect => "vql_detect",
        }
    }

    pub(crate) const fn marker_name(self) -> &'static str {
        match self {
            Self::Classify => "__vql_classify",
            Self::Extract => "__vql_extract",
            Self::Detect => "__vql_detect",
        }
    }

    pub(crate) fn from_name(name: &str) -> Option<Self> {
        [Self::Classify, Self::Extract, Self::Detect]
            .into_iter()
            .find(|function| name.eq_ignore_ascii_case(function.marker_name()))
    }

    pub(crate) fn normalized_argument_count(self, actual: usize) -> bool {
        match self {
            Self::Classify => actual == 4,
            Self::Detect => actual == 3,
            Self::Extract => actual >= 4 && (actual - 1).is_multiple_of(3),
        }
    }

    fn static_output_field(self, name: impl Into<String>, nullable: bool) -> Option<FieldRef> {
        match self {
            Self::Classify => Some(crate::models::classification_field(name, nullable)),
            Self::Detect => Some(crate::models::task_detection_field(name, nullable)),
            Self::Extract => None,
        }
    }

    fn signature(self) -> crate::Result<Signature> {
        let signature = match self {
            Self::Classify => Signature::one_of(vec![TypeSignature::Any(4)], Volatility::Volatile)
                .with_parameter_names(
                    ["input", "categories", "output_mode", "min_score"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                ),
            Self::Detect => Signature::one_of(vec![TypeSignature::Any(3)], Volatility::Volatile)
                .with_parameter_names(
                    ["input", "classes", "min_score"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                ),
            Self::Extract => Ok(Signature::variadic_any(Volatility::Volatile)),
        }
        .map_err(|error| {
            crate::VqlError::new(
                crate::ErrorCode::Internal,
                "invalid built-in AI function signature",
            )
            .with_source(error)
        })?;
        Ok(signature)
    }
}

#[derive(Debug)]
struct BuiltinAiMarker {
    function: BuiltinAiFunction,
    signature: Signature,
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
        self.function.marker_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        let Some(field) = self.function.static_output_field("", true) else {
            return internal_err!(
                "VQL_EXTRACT return type requires planning-time field descriptors"
            );
        };
        Ok(field.data_type().clone())
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        if let Some(field) = self.function.static_output_field("", true) {
            return Ok(field);
        }
        let fields = extract_field_specs_from_scalar_args(args.scalar_arguments)?;
        Ok(crate::models::extraction_field("", &fields, true))
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

pub(crate) fn extract_field_specs_from_scalar_args(
    args: &[Option<&ScalarValue>],
) -> datafusion::common::Result<Vec<ExtractFieldSpec>> {
    if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
        return plan_err!(
            "VQL_EXTRACT marker requires input followed by one or more normalized field descriptors"
        );
    }
    args[1..]
        .chunks_exact(3)
        .map(|descriptor| {
            let name = scalar_string(descriptor[0], "field name")?;
            let question = scalar_string(descriptor[1], "question")?;
            let list = match descriptor[2] {
                Some(ScalarValue::Boolean(Some(value))) => *value,
                _ => return plan_err!("VQL_EXTRACT field list flag must be a constant BOOLEAN"),
            };
            Ok(ExtractFieldSpec {
                name,
                question,
                list,
            })
        })
        .collect()
}

fn scalar_string(value: Option<&ScalarValue>, role: &str) -> datafusion::common::Result<String> {
    match value {
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value)))
            if !value.is_empty() =>
        {
            Ok(value.clone())
        }
        _ => plan_err!("VQL_EXTRACT {role} must be a non-empty constant STRING"),
    }
}

pub(crate) fn builtin_ai_udf(function: BuiltinAiFunction) -> crate::Result<ScalarUDF> {
    Ok(ScalarUDF::new_from_impl(BuiltinAiMarker {
        function,
        signature: function.signature()?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};
    use std::sync::Arc;

    #[test]
    fn task_markers_publish_exact_static_schemas() {
        let classify = BuiltinAiFunction::Classify
            .static_output_field("result", true)
            .unwrap();
        assert_eq!(
            classify.data_type(),
            crate::models::classification_field("result", true).data_type()
        );

        let detect = BuiltinAiFunction::Detect
            .static_output_field("result", true)
            .unwrap();
        let DataType::List(item) = detect.data_type() else {
            panic!("VQL_DETECT must return an array");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("VQL_DETECT array items must be structs");
        };
        assert_eq!(field_names(fields), ["label", "score", "locator"]);
        assert!(!fields[0].is_nullable());
        assert!(!fields[1].is_nullable());
        assert!(fields[2].is_nullable());
        assert_locator(fields[2].data_type());
    }

    #[test]
    fn extract_schema_is_derived_from_field_order_and_cardinality() {
        let marker = BuiltinAiMarker {
            function: BuiltinAiFunction::Extract,
            signature: BuiltinAiFunction::Extract.signature().unwrap(),
        };
        let values = [
            ScalarValue::Utf8(Some("total_amount".to_owned())),
            ScalarValue::Utf8(Some("What is the total?".to_owned())),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Utf8(Some("items".to_owned())),
            ScalarValue::Utf8(Some("List the items".to_owned())),
            ScalarValue::Boolean(Some(true)),
        ];
        let mut scalar_arguments = vec![None];
        scalar_arguments.extend(values.iter().map(Some));
        let arg_fields = std::iter::once(Arc::new(Field::new("input", DataType::Null, true)))
            .chain(values.iter().enumerate().map(|(index, value)| {
                Arc::new(Field::new(
                    format!("argument_{index}"),
                    value.data_type(),
                    value.is_null(),
                ))
            }))
            .collect::<Vec<_>>();
        let field = marker
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        assert!(field.is_nullable());
        let DataType::Struct(fields) = field.data_type() else {
            panic!("VQL_EXTRACT must return a struct");
        };
        assert_eq!(field_names(fields), ["total_amount", "items"]);
        assert!(!fields[0].is_nullable());
        assert_answer(fields[0].data_type());
        let DataType::List(item) = fields[1].data_type() else {
            panic!("list field must return an array");
        };
        assert!(item.is_nullable());
        assert_answer(item.data_type());
    }

    fn assert_answer(data_type: &DataType) {
        let DataType::Struct(fields) = data_type else {
            panic!("extracted answer must be a struct");
        };
        assert_eq!(field_names(fields), ["value", "score", "locator"]);
        assert!(fields.iter().all(|field| field.is_nullable()));
        assert_locator(fields[2].data_type());
    }

    fn assert_locator(data_type: &DataType) {
        let DataType::Struct(fields) = data_type else {
            panic!("LOCATOR must be a struct");
        };
        assert_eq!(field_names(fields), ["char_span", "box"]);
        assert!(fields.iter().all(|field| field.is_nullable()));
        let DataType::Struct(span) = fields[0].data_type() else {
            panic!("char_span must be a struct");
        };
        assert_eq!(field_names(span), ["start", "end"]);
    }

    fn field_names(fields: &Fields) -> Vec<&str> {
        fields.iter().map(|field| field.name().as_str()).collect()
    }
}
