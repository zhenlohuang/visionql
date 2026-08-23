use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, StringArray, StructArray};
use arrow::datatypes::{Field, FieldRef, Fields, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion::execution::context::{QueryPlanner, SessionState, TaskContext};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::catalog::{
    DefinitionSnapshot, ModelType, ResolvedExecutionSpec, ResolvedModelDef, RuntimeSpec,
};
use crate::functions::BuiltinAiFunction;
use crate::models::{
    BoundInferenceParams, BuiltinModels, ClassificationOutputMode, ExtractFieldSpec, ModelRuntime,
    bind_classification_params, bind_detection_params, bind_inference_params, semantic_fingerprint,
};
use crate::planner::sink::SinkExtensionPlanner;
use crate::resources::QueryBudget;

#[derive(Clone)]
struct InferenceNode {
    input: LogicalPlan,
    input_exprs: Vec<Expr>,
    output_name: String,
    output_field: FieldRef,
    schema: DFSchemaRef,
    operation: String,
    model: ResolvedModelDef,
    invocation: BoundInferenceParams,
    invocation_fingerprint: String,
    invocation_volatile: bool,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    budget: QueryBudget,
}

impl InferenceNode {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        input: LogicalPlan,
        input_exprs: Vec<Expr>,
        output_name: String,
        output_field: FieldRef,
        operation: String,
        model: ResolvedModelDef,
        invocation: BoundInferenceParams,
        invocation_fingerprint: String,
        invocation_volatile: bool,
        runtime: Arc<ModelRuntime>,
        fail_on_error: Arc<AtomicBool>,
        cancellation: CancellationToken,
        budget: QueryBudget,
    ) -> DataFusionResult<Self> {
        let output_field = Arc::new(output_field.as_ref().clone().with_name(&output_name));
        let result = DFSchema::from_unqualified_fields(
            Fields::from(vec![Arc::clone(&output_field)]),
            HashMap::new(),
        )?;
        let schema = Arc::new(input.schema().join(&result)?);
        Ok(Self {
            input,
            input_exprs,
            output_name,
            output_field,
            schema,
            operation,
            model,
            invocation,
            invocation_fingerprint,
            invocation_volatile,
            runtime,
            fail_on_error,
            cancellation,
            budget,
        })
    }

    fn comparison_key(&self) -> (&str, &str, &str, &[Expr]) {
        (
            &self.output_name,
            &self.invocation_fingerprint,
            &self.model.semantic_fingerprint,
            &self.input_exprs,
        )
    }
}

fn execution_summary(model: &ResolvedModelDef) -> (&RuntimeSpec, &str, &str) {
    match &model.execution {
        ResolvedExecutionSpec::Embedded {
            runtime,
            pre_processor,
            post_processor,
        } => (runtime, &pre_processor.kind, &post_processor.kind),
        ResolvedExecutionSpec::Service { runtime } => (runtime, "service", "service"),
        ResolvedExecutionSpec::Generic { runtime, .. } => (runtime, "generic", "generic"),
    }
}

fn model_identity(model: &ResolvedModelDef) -> String {
    model
        .artifact_hash
        .as_ref()
        .map(|hash| format!("{}@{}:artifact:{hash}", model.name, model.version))
        .unwrap_or_else(|| {
            format!(
                "{}@{}:semantic:{}",
                model.name, model.version, model.semantic_fingerprint
            )
        })
}

fn batching_owner(model: &ResolvedModelDef) -> &'static str {
    match model.execution {
        ResolvedExecutionSpec::Embedded { .. } => "visionql",
        ResolvedExecutionSpec::Service { .. } => "service",
        ResolvedExecutionSpec::Generic { .. } => "visionql",
    }
}

fn decode_summary(model: &ResolvedModelDef) -> &'static str {
    if model.source.starts_with("mock://") {
        "skipped(mock)"
    } else {
        "required"
    }
}

impl Debug for InferenceNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InferenceNode")
            .field("operation", &self.operation)
            .field("model", &self.model.name)
            .field("output", &self.output_name)
            .field("input_exprs", &self.input_exprs)
            .finish()
    }
}

impl PartialEq for InferenceNode {
    fn eq(&self, other: &Self) -> bool {
        self.comparison_key() == other.comparison_key() && self.input == other.input
    }
}

impl Eq for InferenceNode {}

impl Hash for InferenceNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.comparison_key().hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for InferenceNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.comparison_key(), format!("{:?}", self.input))
            .partial_cmp(&(other.comparison_key(), format!("{:?}", other.input)))
    }
}

impl UserDefinedLogicalNodeCore for InferenceNode {
    fn name(&self) -> &str {
        "InferenceNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.input_exprs.clone()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::from([self.output_name.clone()])
    }

    fn fmt_for_explain(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let (runtime, pre_processor, post_processor) = execution_summary(&self.model);
        write!(
            formatter,
            "InferenceNode: operation={}, model={}, identity={}, runtime={}, protocol={}, pre_processor={}, post_processor={}, batching_owner={}, volatile={}, dedup={}, decode={}, output={}",
            self.operation,
            self.model.name,
            model_identity(&self.model),
            runtime.kind,
            runtime.protocol.as_deref().unwrap_or("embedded"),
            pre_processor,
            post_processor,
            batching_owner(&self.model),
            self.invocation_volatile,
            if self.invocation_volatile {
                "disabled"
            } else {
                "enabled"
            },
            decode_summary(&self.model),
            self.output_name,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "InferenceNode requires at least one expression and one input".to_owned(),
            ));
        }
        Self::try_new(
            inputs.remove(0),
            exprs,
            self.output_name.clone(),
            Arc::clone(&self.output_field),
            self.operation.clone(),
            self.model.clone(),
            self.invocation.clone(),
            self.invocation_fingerprint.clone(),
            self.invocation_volatile,
            Arc::clone(&self.runtime),
            Arc::clone(&self.fail_on_error),
            self.cancellation.clone(),
            self.budget.clone(),
        )
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let input_columns = self.input.schema().fields().len();
        Some(vec![
            output_columns
                .iter()
                .copied()
                .filter(|index| *index < input_columns)
                .collect(),
        ])
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

pub(crate) fn explain_annotations(plan: &LogicalPlan, image_payload: &str) -> Vec<String> {
    let mut annotations = Vec::new();
    collect_explain_annotations(plan, image_payload, &mut annotations);
    annotations
}

fn collect_explain_annotations(
    plan: &LogicalPlan,
    image_payload: &str,
    annotations: &mut Vec<String>,
) {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(node) = extension.node.as_any().downcast_ref::<InferenceNode>()
    {
        let (runtime, pre_processor, post_processor) = execution_summary(&node.model);
        annotations.push(format!(
            "Inference model={} identity={} runtime={} protocol={} pipeline={} -> {} -> {} batching_owner={} volatile={} dedup={} decode={} image_payload={}",
            node.model.name,
            model_identity(&node.model),
            runtime.kind,
            runtime.protocol.as_deref().unwrap_or("embedded"),
            pre_processor,
            runtime.kind,
            post_processor,
            batching_owner(&node.model),
            node.invocation_volatile,
            if node.invocation_volatile { "disabled" } else { "enabled" },
            decode_summary(&node.model),
            image_payload,
        ));
    }
    for input in plan.inputs() {
        collect_explain_annotations(input, image_payload, annotations);
    }
}

pub(crate) async fn extract_inference(
    plan: LogicalPlan,
    snapshot: &DefinitionSnapshot,
    builtins: Arc<BuiltinModels>,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    budget: QueryBudget,
) -> crate::Result<LogicalPlan> {
    let requirements = validate_builtin_ai_inputs(&plan)?;
    let mut builtin_models = ResolvedBuiltinModels::default();
    if requirements.contains(&BuiltinAiFunction::Classify) {
        let model = builtins.classifier(cancellation.clone()).await?;
        runtime.register_builtin(&model)?;
        builtin_models.classifier = Some(model);
    }
    if requirements.contains(&BuiltinAiFunction::Detect) {
        let model = builtins.detector(cancellation.clone()).await?;
        runtime.register_builtin(&model)?;
        builtin_models.detector = Some(model);
    }
    let mut volatile_id = 0_u64;
    plan.transform_up(|plan| {
        rewrite_plan_node(
            plan,
            snapshot,
            &builtin_models,
            Arc::clone(&runtime),
            Arc::clone(&fail_on_error),
            cancellation.clone(),
            budget.clone(),
            &mut volatile_id,
        )
    })
    .data()
    .map_err(Into::into)
}

#[derive(Default)]
struct ResolvedBuiltinModels {
    classifier: Option<ResolvedModelDef>,
    detector: Option<ResolvedModelDef>,
}

impl ResolvedBuiltinModels {
    fn get(&self, function: BuiltinAiFunction) -> Option<&ResolvedModelDef> {
        match function {
            BuiltinAiFunction::Classify => self.classifier.as_ref(),
            BuiltinAiFunction::Detect => self.detector.as_ref(),
            BuiltinAiFunction::Extract => None,
        }
    }
}

fn validate_builtin_ai_inputs(plan: &LogicalPlan) -> crate::Result<HashSet<BuiltinAiFunction>> {
    let mut requirements = HashSet::new();
    validate_builtin_ai_node(plan, &mut requirements)?;
    Ok(requirements)
}

fn validate_builtin_ai_node(
    plan: &LogicalPlan,
    requirements: &mut HashSet<BuiltinAiFunction>,
) -> crate::Result<()> {
    for input in plan.inputs() {
        validate_builtin_ai_node(input, requirements)?;
    }
    let inputs = plan.inputs();
    for expression in plan.expressions() {
        let mut validation_error = None;
        expression
            .transform_up(|expression| {
                let Expr::ScalarFunction(call) = &expression else {
                    return Ok(Transformed::no(expression));
                };
                let Some(function) = BuiltinAiFunction::from_name(call.name()) else {
                    return Ok(Transformed::no(expression));
                };
                if !function.normalized_argument_count(call.args.len()) {
                    validation_error = Some(crate::VqlError::new(
                        crate::ErrorCode::Internal,
                        format!(
                            "{} has an invalid normalized argument list",
                            function.name().to_ascii_uppercase()
                        ),
                    ));
                    return Ok(Transformed::no(expression));
                }
                if let Err(error) = validate_builtin_arguments(function, &call.args) {
                    validation_error = Some(error);
                    return Ok(Transformed::no(expression));
                }
                let Some(input_plan) = inputs.first().filter(|_| inputs.len() == 1) else {
                    validation_error = Some(crate::VqlError::new(
                        crate::ErrorCode::InvalidSql,
                        format!(
                            "{} is only supported on a single-input plan",
                            function.name().to_ascii_uppercase()
                        ),
                    ));
                    return Ok(Transformed::no(expression));
                };
                let data_type = call.args[0].get_type(input_plan.schema())?;
                if is_null_literal(&call.args[0]) {
                    return Ok(Transformed::no(expression));
                }
                let is_image = crate::types::is_image_storage(&data_type);
                let logical_type = builtin_input_type(&data_type);
                match function {
                    BuiltinAiFunction::Classify if is_image => {
                        requirements.insert(function);
                    }
                    BuiltinAiFunction::Classify if logical_type == Some("STRING") => {
                        validation_error = Some(crate::VqlError::feature(
                            "VQL_CLASSIFY accepts STRING input, but execution for that type is not implemented",
                            "未排期",
                        ));
                    }
                    BuiltinAiFunction::Detect if is_image => {
                        requirements.insert(function);
                    }
                    BuiltinAiFunction::Extract
                        if is_image || logical_type == Some("STRING") =>
                    {
                        validation_error = Some(crate::VqlError::feature(
                            format!(
                                "VQL_EXTRACT accepts {} input, but execution for that type is not implemented",
                                if is_image { "IMAGE" } else { "STRING" }
                            ),
                            "未排期",
                        ));
                    }
                    BuiltinAiFunction::Classify => {
                        validation_error = Some(invalid_builtin_input(
                            function,
                            "IMAGE or STRING",
                            &data_type,
                        ));
                    }
                    BuiltinAiFunction::Detect => {
                        validation_error = Some(invalid_builtin_input(
                            function,
                            "IMAGE",
                            &data_type,
                        ));
                    }
                    BuiltinAiFunction::Extract => {
                        validation_error = Some(invalid_builtin_input(
                            function,
                            "IMAGE or STRING",
                            &data_type,
                        ));
                    }
                }
                Ok(Transformed::no(expression))
            })
            .data()
            .map_err(crate::VqlError::from)?;
        if let Some(error) = validation_error {
            return Err(error);
        }
    }
    Ok(())
}

fn validate_builtin_arguments(function: BuiltinAiFunction, args: &[Expr]) -> crate::Result<()> {
    let invalid_constant = |error: DataFusionError| {
        crate::VqlError::new(crate::ErrorCode::InvalidArgument, error.to_string())
    };
    match function {
        BuiltinAiFunction::Classify => {
            let categories = classes_literal(args.get(1)).map_err(invalid_constant)?;
            let output_mode = optional_string_literal(args.get(2)).map_err(invalid_constant)?;
            let min_score = probability_literal(args.get(3)).map_err(invalid_constant)?;
            bind_classification_params(categories, output_mode, min_score).map(|_| ())
        }
        BuiltinAiFunction::Detect => {
            let classes = classes_literal(args.get(1)).map_err(invalid_constant)?;
            let min_score = probability_literal(args.get(2)).map_err(invalid_constant)?;
            bind_detection_params(classes, min_score).map(|_| ())
        }
        BuiltinAiFunction::Extract => extract_fields_literal(&args[1..])
            .map(|_| ())
            .map_err(invalid_constant),
    }
}

fn invalid_builtin_input(
    function: BuiltinAiFunction,
    expected: &str,
    actual: &arrow::datatypes::DataType,
) -> crate::VqlError {
    crate::VqlError::new(
        crate::ErrorCode::InvalidSql,
        format!(
            "{} input must be {expected}; got {actual}",
            function.name().to_ascii_uppercase()
        ),
    )
}

fn builtin_input_type(data_type: &arrow::datatypes::DataType) -> Option<&'static str> {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some("STRING"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Some("BINARY"),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn rewrite_plan_node(
    plan: LogicalPlan,
    snapshot: &DefinitionSnapshot,
    builtin_models: &ResolvedBuiltinModels,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    budget: QueryBudget,
    volatile_id: &mut u64,
) -> DataFusionResult<Transformed<LogicalPlan>> {
    let mut inputs = plan
        .inputs()
        .into_iter()
        .cloned()
        .collect::<Vec<LogicalPlan>>();
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(plan.expressions().len());
    let name_preserver = NamePreserver::new(&plan);
    for expr in plan.expressions() {
        let original_name = name_preserver.save(&expr);
        let transformed = expr.transform_up(|expr| {
            let Expr::ScalarFunction(call) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let builtin_function = BuiltinAiFunction::from_name(call.name());
            if builtin_function.is_none() && snapshot.model(call.name()).is_none() {
                return Ok(Transformed::no(expr));
            }
            if inputs.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "callable '{}' is only supported on single-input plans",
                    call.name()
                )));
            }
            let expression_output_type = expr.get_type(inputs[0].schema())?;
            if builtin_function.is_some() && is_null_literal(&call.args[0]) {
                let value = ScalarValue::try_new_null(&expression_output_type)?;
                return Ok(Transformed::yes(Expr::Literal(value, None)));
            }
            let (operation, model, input_exprs, invocation) =
                if let Some(function) = builtin_function {
                    if !function.normalized_argument_count(call.args.len()) {
                        return Err(DataFusionError::Plan(format!(
                            "{} marker has an invalid normalized argument count {}; got {}",
                            function.name().to_ascii_uppercase(),
                            match function {
                                BuiltinAiFunction::Classify => "4",
                                BuiltinAiFunction::Detect => "3",
                                BuiltinAiFunction::Extract => "1 + 3n",
                            },
                            call.args.len()
                        )));
                    }
                    let model = builtin_models.get(function).cloned().ok_or_else(|| {
                        DataFusionError::Internal(
                            "built-in AI call was not resolved after input validation".to_owned(),
                        )
                    })?;
                    let invocation = match function {
                        BuiltinAiFunction::Classify => {
                            let categories = classes_literal(call.args.get(1))?;
                            let output_mode = optional_string_literal(call.args.get(2))?;
                            let min_score = probability_literal(call.args.get(3))?;
                            let params = bind_classification_params(
                                categories,
                                output_mode,
                                min_score,
                            )
                            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
                            validate_classification_vocabulary(&model, &params)?;
                            params
                        }
                        BuiltinAiFunction::Detect => {
                            let classes = classes_literal(call.args.get(1))?;
                            let min_score = probability_literal(call.args.get(2))?;
                            bind_detection_params(classes, min_score)
                                .map_err(|error| DataFusionError::Plan(error.to_string()))?
                        }
                        BuiltinAiFunction::Extract => BoundInferenceParams {
                            extract_fields: extract_fields_literal(&call.args[1..])?,
                            ..BoundInferenceParams::default()
                        },
                    };
                    (
                        function.name().to_ascii_uppercase(),
                        model,
                        vec![call.args[0].clone()],
                        invocation,
                    )
                } else {
                    let Some(model_object) = snapshot.model(call.name()) else {
                        return Ok(Transformed::no(expr));
                    };
                    let expected_arguments = model_object.definition.interface.parameters.len()
                        + model_object.definition.interface.semantic_arguments.len()
                        + 1;
                    if call.args.len() != expected_arguments {
                        return Err(DataFusionError::Plan(format!(
                            "model '{}' marker expects {expected_arguments} normalized arguments; got {}",
                            call.name(),
                            call.args.len()
                        )));
                    }
                    let input_exprs = call.args
                        [..model_object.definition.interface.parameters.len()]
                        .to_vec();
                    for (expression, parameter) in input_exprs
                        .iter()
                        .zip(&model_object.definition.interface.parameters)
                    {
                        let actual = expression.get_type(inputs[0].schema())?;
                        let expected = crate::models::parse_boundary_type(&parameter.data_type)
                            .map_err(|error| DataFusionError::External(Box::new(error)))?;
                        let compatible = if parameter.data_type.eq_ignore_ascii_case("IMAGE") {
                            crate::types::is_image_storage(&actual)
                        } else {
                            actual == expected
                        };
                        if !compatible {
                            return Err(DataFusionError::Plan(format!(
                                "model '{}' argument '{}' expects {}, got {actual}",
                                model_object.definition.name, parameter.name, parameter.data_type
                            )));
                        }
                    }
                    let invocation = match model_object.definition.interface.capability {
                        None => BoundInferenceParams::default(),
                        Some(ModelType::ObjectDetection | ModelType::ImageClassification) => {
                            let classes = classes_literal(call.args.get(1))?;
                            let min_confidence = probability_literal(call.args.get(2))?;
                            bind_inference_params(classes, min_confidence)
                                .map_err(|error| DataFusionError::Plan(error.to_string()))?
                        }
                    };
                    let version =
                        version_literal(call.args.last().expect("version marker argument"))?
                            .or_else(|| model_object.definition.default_version.clone())
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "model '{}' has no published default; run RESOLVE MODEL {}",
                                    model_object.definition.name, model_object.definition.name
                                ))
                            })?;
                    if model_object.definition.version(&version).is_none() {
                        return Err(DataFusionError::Plan(format!(
                            "unknown version '{version}' for model '{}'; known versions: {}",
                            model_object.definition.name,
                            model_object
                                .definition
                                .versions
                                .iter()
                                .map(|version| version.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                    let model = model_object
                        .definition
                        .resolved_definition(&version)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "model '{}:{}' is not resolved; run RESOLVE MODEL {} VERSION '{}'",
                                model_object.definition.name,
                                version,
                                model_object.definition.name,
                                version
                            ))
                        })?;
                    (
                        model.name.clone(),
                        model,
                        input_exprs,
                        invocation,
                    )
                };
            let output_field = if builtin_function.is_some() {
                Arc::new(Field::new("", expression_output_type, true))
            } else {
                crate::models::interface_output_field("", &model.interface, true)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
            };
            let invocation_fingerprint = semantic_fingerprint(&invocation);
            let invocation_volatile = model.volatile
                || !model.interface.deterministic
                || input_exprs.iter().any(Expr::is_volatile);
            let output_name = inference_output_name(
                &operation,
                &model,
                &invocation_fingerprint,
                &input_exprs,
                invocation_volatile,
                volatile_id,
            );
            let already_extracted = !invocation_volatile
                && inputs[0]
                    .schema()
                    .field_with_unqualified_name(&output_name)
                    .is_ok();
            if !already_extracted {
                let input = inputs.remove(0);
                inputs.push(LogicalPlan::Extension(Extension {
                    node: Arc::new(InferenceNode::try_new(
                        input,
                        input_exprs,
                        output_name.clone(),
                        Arc::clone(&output_field),
                        operation,
                        model,
                        invocation,
                        invocation_fingerprint,
                        invocation_volatile,
                        Arc::clone(&runtime),
                        Arc::clone(&fail_on_error),
                        cancellation.clone(),
                        budget.clone(),
                    )?),
                }));
            }
            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                output_name,
            ))))
        })?;
        changed |= transformed.transformed;
        rewritten.push(original_name.restore(transformed.data));
    }
    if changed {
        Ok(Transformed::yes(plan.with_new_exprs(rewritten, inputs)?))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn inference_output_name(
    operation: &str,
    model: &ResolvedModelDef,
    invocation_fingerprint: &str,
    inputs: &[Expr],
    invocation_volatile: bool,
    volatile_id: &mut u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(operation.as_bytes());
    digest.update(model.semantic_fingerprint.as_bytes());
    digest.update(invocation_fingerprint.as_bytes());
    digest.update(format!("{inputs:?}").as_bytes());
    if invocation_volatile {
        digest.update(volatile_id.to_le_bytes());
        *volatile_id += 1;
    }
    let hash = digest.finalize();
    let suffix = hash[..10]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("__vql_inference_{suffix}")
}

fn version_literal(expr: &Expr) -> DataFusionResult<Option<String>> {
    let Expr::Literal(value, _) = strip_cast(expr) else {
        return Err(DataFusionError::Plan(
            "model version must be a constant string".to_owned(),
        ));
    };
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
            if !value.is_empty() =>
        {
            Ok(Some(value.clone()))
        }
        _ => Err(DataFusionError::Plan(
            "model version must be a non-NULL constant string".to_owned(),
        )),
    }
}

fn classes_literal(expr: Option<&Expr>) -> DataFusionResult<Option<Vec<String>>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    match strip_cast(expr) {
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        Expr::Literal(ScalarValue::List(value), _) if value.is_null(0) => Ok(None),
        Expr::Literal(ScalarValue::List(value), _) => {
            let values = value.value(0);
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Model classes/categories must be a constant array of strings".to_owned(),
                    )
                })?;
            Ok(Some(
                (0..values.len())
                    .map(|index| values.value(index).to_owned())
                    .collect(),
            ))
        }
        Expr::ScalarFunction(call) if call.name().eq_ignore_ascii_case("make_array") => call
            .args
            .iter()
            .map(string_literal)
            .collect::<DataFusionResult<Vec<_>>>()
            .map(Some),
        _ => Err(DataFusionError::Plan(
            "Model classes/categories must be a constant array of strings".to_owned(),
        )),
    }
}

fn string_literal(expr: &Expr) -> DataFusionResult<String> {
    match strip_cast(expr) {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(
            "Model classes/categories must be a constant array of strings".to_owned(),
        )),
    }
}

fn optional_string_literal(expr: Option<&Expr>) -> DataFusionResult<Option<String>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    match strip_cast(expr) {
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Ok(Some(value.clone())),
        _ => Err(DataFusionError::Plan(
            "AI function option must be a constant string".to_owned(),
        )),
    }
}

fn extract_fields_literal(args: &[Expr]) -> DataFusionResult<Vec<ExtractFieldSpec>> {
    if args.is_empty() || !args.len().is_multiple_of(3) {
        return Err(DataFusionError::Plan(
            "VQL_EXTRACT requires one or more normalized field descriptors".to_owned(),
        ));
    }
    args.chunks_exact(3)
        .map(|descriptor| {
            let name = extract_string_literal(&descriptor[0], "field name")?;
            let question = extract_string_literal(&descriptor[1], "question")?;
            let list = match strip_cast(&descriptor[2]) {
                Expr::Literal(ScalarValue::Boolean(Some(value)), _) => *value,
                _ => {
                    return Err(DataFusionError::Plan(
                        "VQL_EXTRACT field list flag must be a constant BOOLEAN".to_owned(),
                    ));
                }
            };
            Ok(ExtractFieldSpec {
                name,
                question,
                list,
            })
        })
        .collect()
}

fn extract_string_literal(expr: &Expr, role: &str) -> DataFusionResult<String> {
    match strip_cast(expr) {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
            if !value.is_empty() =>
        {
            Ok(value.clone())
        }
        _ => Err(DataFusionError::Plan(format!(
            "VQL_EXTRACT {role} must be a non-empty constant STRING"
        ))),
    }
}

fn probability_literal(expr: Option<&Expr>) -> DataFusionResult<Option<f32>> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    let Expr::Literal(value, _) = strip_cast(expr) else {
        return Err(DataFusionError::Plan(
            "Model score threshold must be a constant number".to_owned(),
        ));
    };
    let value = match value {
        ScalarValue::Null => return Ok(None),
        ScalarValue::Float32(Some(value)) => f64::from(*value),
        ScalarValue::Float64(Some(value)) => *value,
        ScalarValue::Decimal32(Some(value), _, scale) => {
            f64::from(*value) * 10_f64.powi(-i32::from(*scale))
        }
        ScalarValue::Decimal64(Some(value), _, scale) => {
            *value as f64 * 10_f64.powi(-i32::from(*scale))
        }
        ScalarValue::Decimal128(Some(value), _, scale) => {
            *value as f64 * 10_f64.powi(-i32::from(*scale))
        }
        ScalarValue::Int8(Some(value)) => f64::from(*value),
        ScalarValue::Int16(Some(value)) => f64::from(*value),
        ScalarValue::Int32(Some(value)) => f64::from(*value),
        ScalarValue::Int64(Some(value)) => *value as f64,
        ScalarValue::UInt8(Some(value)) => f64::from(*value),
        ScalarValue::UInt16(Some(value)) => f64::from(*value),
        ScalarValue::UInt32(Some(value)) => f64::from(*value),
        ScalarValue::UInt64(Some(value)) => *value as f64,
        _ => {
            return Err(DataFusionError::Plan(
                "Model score threshold must be a constant number".to_owned(),
            ));
        }
    };
    Ok(Some(value as f32))
}

fn strip_cast(mut expr: &Expr) -> &Expr {
    loop {
        match expr {
            Expr::Cast(value) => expr = &value.expr,
            Expr::TryCast(value) => expr = &value.expr,
            _ => return expr,
        }
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(strip_cast(expr), Expr::Literal(value, _) if value.is_null())
}

fn validate_classification_vocabulary(
    model: &ResolvedModelDef,
    invocation: &BoundInferenceParams,
) -> DataFusionResult<()> {
    if invocation.output_mode != Some(ClassificationOutputMode::Single) {
        return Ok(());
    }
    let ResolvedExecutionSpec::Embedded { post_processor, .. } = &model.execution else {
        return Err(DataFusionError::Internal(
            "built-in VQL_CLASSIFY requires an embedded enumerable vocabulary".to_owned(),
        ));
    };
    let labels = post_processor
        .options
        .get("labels")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            DataFusionError::Internal(
                "built-in VQL_CLASSIFY implementation has no enumerable vocabulary".to_owned(),
            )
        })?;
    let labels = labels
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<HashSet<_>>();
    let categories = invocation
        .classes
        .as_ref()
        .expect("classification binding always has categories");
    let unknown = categories
        .iter()
        .filter(|category| !labels.contains(category.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "VQL_CLASSIFY categories are outside the bound implementation vocabulary: {}",
            unknown.join(", ")
        )))
    }
}

#[derive(Debug)]
pub(crate) struct VqlQueryPlanner;

#[async_trait]
impl QueryPlanner for VqlQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(InferenceExtensionPlanner),
            Arc::new(SinkExtensionPlanner),
        ])
        .create_physical_plan(logical_plan, session_state)
        .await
    }
}

#[derive(Debug)]
struct InferenceExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for InferenceExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<InferenceNode>() else {
            return Ok(None);
        };
        if logical_inputs.len() != 1 || physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "InferenceNode planner requires one input".to_owned(),
            ));
        }
        let input_exprs = node
            .input_exprs
            .iter()
            .map(|expression| {
                planner.create_physical_expr(expression, logical_inputs[0].schema(), session_state)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Some(Arc::new(InferenceExec::new(
            Arc::clone(&physical_inputs[0]),
            input_exprs,
            node.schema.inner().clone(),
            node.output_name.clone(),
            node.operation.clone(),
            node.model.clone(),
            node.invocation.clone(),
            node.invocation_volatile,
            Arc::clone(&node.runtime),
            Arc::clone(&node.fail_on_error),
            node.cancellation.clone(),
            node.budget.clone(),
        ))))
    }
}

struct InferenceExec {
    input: Arc<dyn ExecutionPlan>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    output_name: String,
    operation: String,
    model: ResolvedModelDef,
    invocation: BoundInferenceParams,
    invocation_volatile: bool,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    budget: QueryBudget,
    properties: Arc<PlanProperties>,
}

impl InferenceExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Arc<dyn ExecutionPlan>,
        input_exprs: Vec<Arc<dyn PhysicalExpr>>,
        schema: SchemaRef,
        output_name: String,
        operation: String,
        model: ResolvedModelDef,
        invocation: BoundInferenceParams,
        invocation_volatile: bool,
        runtime: Arc<ModelRuntime>,
        fail_on_error: Arc<AtomicBool>,
        cancellation: CancellationToken,
        budget: QueryBudget,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self {
            input,
            input_exprs,
            schema,
            output_name,
            operation,
            model,
            invocation,
            invocation_volatile,
            runtime,
            fail_on_error,
            cancellation,
            budget,
            properties,
        }
    }
}

impl Debug for InferenceExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InferenceExec")
            .field("operation", &self.operation)
            .field("model", &self.model.name)
            .field("output", &self.output_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for InferenceExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        let (runtime, _, _) = execution_summary(&self.model);
        write!(
            formatter,
            "InferenceExec: operation={}, model={}, identity={}, runtime={}, batching_owner={}, volatile={}, dedup={}, decode={}, output={}",
            self.operation,
            self.model.name,
            model_identity(&self.model),
            runtime.kind,
            batching_owner(&self.model),
            self.invocation_volatile,
            if self.invocation_volatile {
                "disabled"
            } else {
                "enabled"
            },
            decode_summary(&self.model),
            self.output_name
        )
    }
}

impl ExecutionPlan for InferenceExec {
    fn name(&self) -> &str {
        "InferenceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "InferenceExec requires one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.input_exprs.clone(),
            Arc::clone(&self.schema),
            self.output_name.clone(),
            self.operation.clone(),
            self.model.clone(),
            self.invocation.clone(),
            self.invocation_volatile,
            Arc::clone(&self.runtime),
            Arc::clone(&self.fail_on_error),
            self.cancellation.clone(),
            self.budget.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let input_exprs = self.input_exprs.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let operation = self.operation.clone();
        let model = self.model.clone();
        let invocation = self.invocation.clone();
        let runtime = Arc::clone(&self.runtime);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let cancellation = self.cancellation.clone();
        let budget = self.budget.clone();
        let stream = async_stream::try_stream! {
            while let Some(batch) = input.next().await {
                if cancellation.is_cancelled() {
                    Err(DataFusionError::External(Box::new(crate::VqlError::new(
                        crate::ErrorCode::QueryCancelled,
                        "query cancelled",
                    ))))?;
                }
                let batch = batch?;
                let values = input_exprs
                    .iter()
                    .map(|expression| expression.evaluate(&batch)?.into_array(batch.num_rows()))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                let output = if model.interface.capability.is_none() {
                    runtime
                        .infer_generic(
                            &model,
                            &values,
                            batch.num_rows(),
                            fail_on_error.load(Ordering::Relaxed),
                            cancellation.clone(),
                            &budget,
                        )
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                } else {
                    let images = values[0]
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| DataFusionError::Execution(format!(
                            "{} expects IMAGE",
                            operation
                        )))?;
                    runtime
                        .infer(
                            &model,
                            &invocation,
                            images,
                            fail_on_error.load(Ordering::Relaxed),
                            cancellation.clone(),
                            &budget,
                        )
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                };
                let output = if operation.eq_ignore_ascii_case("VQL_DETECT") {
                    let images = values[0]
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| DataFusionError::Execution(
                            "VQL_DETECT expects IMAGE".to_owned()
                        ))?;
                    crate::models::task_detection_output(&output, images)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                } else {
                    output
                };
                let mut columns = batch.columns().to_vec();
                columns.push(output);
                yield RecordBatch::try_new(Arc::clone(&schema), columns)?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}
