use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{Field, Fields, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result as DataFusionResult,
};
use datafusion::execution::context::{QueryPlanner, SessionState, TaskContext};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
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

use crate::catalog::{DefinitionSnapshot, FunctionDef, FunctionImplementation, ModelDef};
use crate::models::{ModelRuntime, append_detections, detection_builder, detections_type};
use crate::planner::sink::SinkExtensionPlanner;

#[derive(Clone)]
struct InferenceNode {
    input: LogicalPlan,
    input_expr: Expr,
    output_name: String,
    schema: DFSchemaRef,
    function: FunctionDef,
    model: ModelDef,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
}

impl InferenceNode {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        input: LogicalPlan,
        input_expr: Expr,
        output_name: String,
        function: FunctionDef,
        model: ModelDef,
        runtime: Arc<ModelRuntime>,
        fail_on_error: Arc<AtomicBool>,
        cancellation: CancellationToken,
    ) -> DataFusionResult<Self> {
        let result = DFSchema::from_unqualified_fields(
            Fields::from(vec![Arc::new(Field::new(
                &output_name,
                detections_type(),
                true,
            ))]),
            HashMap::new(),
        )?;
        let schema = Arc::new(input.schema().join(&result)?);
        Ok(Self {
            input,
            input_expr,
            output_name,
            schema,
            function,
            model,
            runtime,
            fail_on_error,
            cancellation,
        })
    }

    fn comparison_key(&self) -> (&str, &str, &str, &Expr) {
        (
            &self.output_name,
            &self.function.semantic_fingerprint,
            &self.model.semantic_fingerprint,
            &self.input_expr,
        )
    }
}

impl Debug for InferenceNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InferenceNode")
            .field("function", &self.function.name)
            .field("model", &self.model.name)
            .field("output", &self.output_name)
            .field("input_expr", &self.input_expr)
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
        vec![self.input_expr.clone()]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::from([self.output_name.clone()])
    }

    fn fmt_for_explain(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "InferenceNode: function={}, model={}, output={}, mutable_endpoint={}",
            self.function.name, self.model.name, self.output_name, self.model.volatile
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if exprs.len() != 1 || inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "InferenceNode requires one expression and one input".to_owned(),
            ));
        }
        Self::try_new(
            inputs.remove(0),
            exprs.remove(0),
            self.output_name.clone(),
            self.function.clone(),
            self.model.clone(),
            Arc::clone(&self.runtime),
            Arc::clone(&self.fail_on_error),
            self.cancellation.clone(),
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

pub(crate) fn extract_inference(
    plan: LogicalPlan,
    snapshot: &DefinitionSnapshot,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
) -> crate::Result<LogicalPlan> {
    let mut functions = HashMap::new();
    for (name, function) in snapshot.functions() {
        let FunctionImplementation::Model { model } = &function.definition.implementation else {
            continue;
        };
        let model = snapshot.model(model).ok_or_else(|| {
            crate::VqlError::new(
                crate::ErrorCode::Catalog,
                format!("function '{name}' references missing model '{model}'"),
            )
        })?;
        functions.insert(
            name.to_ascii_lowercase(),
            (function.definition.clone(), model.definition.clone()),
        );
    }
    let mut volatile_id = 0_u64;
    plan.transform_up(|plan| {
        rewrite_plan_node(
            plan,
            &functions,
            Arc::clone(&runtime),
            Arc::clone(&fail_on_error),
            cancellation.clone(),
            &mut volatile_id,
        )
    })
    .data()
    .map_err(Into::into)
}

fn rewrite_plan_node(
    plan: LogicalPlan,
    functions: &HashMap<String, (FunctionDef, ModelDef)>,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
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
            let Some((function, model)) = functions.get(&call.name().to_ascii_lowercase()) else {
                return Ok(Transformed::no(expr));
            };
            if inputs.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "model function '{}' is only supported on single-input v0.1 plans",
                    function.name
                )));
            }
            let input_expr = call.args.first().cloned().ok_or_else(|| {
                DataFusionError::Plan(format!("model function '{}' requires IMAGE", function.name))
            })?;
            let output_name = inference_output_name(function, model, &input_expr, volatile_id);
            let already_extracted = !model.volatile
                && inputs[0]
                    .schema()
                    .field_with_unqualified_name(&output_name)
                    .is_ok();
            if !already_extracted {
                let input = inputs.remove(0);
                inputs.push(LogicalPlan::Extension(Extension {
                    node: Arc::new(InferenceNode::try_new(
                        input,
                        input_expr,
                        output_name.clone(),
                        function.clone(),
                        model.clone(),
                        Arc::clone(&runtime),
                        Arc::clone(&fail_on_error),
                        cancellation.clone(),
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
    function: &FunctionDef,
    model: &ModelDef,
    input: &Expr,
    volatile_id: &mut u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(function.semantic_fingerprint.as_bytes());
    digest.update(model.semantic_fingerprint.as_bytes());
    digest.update(format!("{input:?}").as_bytes());
    if model.volatile {
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
        let input_expr = planner.create_physical_expr(
            &node.input_expr,
            logical_inputs[0].schema(),
            session_state,
        )?;
        Ok(Some(Arc::new(InferenceExec::new(
            Arc::clone(&physical_inputs[0]),
            input_expr,
            node.schema.inner().clone(),
            node.output_name.clone(),
            node.function.clone(),
            node.model.clone(),
            Arc::clone(&node.runtime),
            Arc::clone(&node.fail_on_error),
            node.cancellation.clone(),
        ))))
    }
}

struct InferenceExec {
    input: Arc<dyn ExecutionPlan>,
    input_expr: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    output_name: String,
    function: FunctionDef,
    model: ModelDef,
    runtime: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    properties: Arc<PlanProperties>,
}

impl InferenceExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Arc<dyn ExecutionPlan>,
        input_expr: Arc<dyn PhysicalExpr>,
        schema: SchemaRef,
        output_name: String,
        function: FunctionDef,
        model: ModelDef,
        runtime: Arc<ModelRuntime>,
        fail_on_error: Arc<AtomicBool>,
        cancellation: CancellationToken,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self {
            input,
            input_expr,
            schema,
            output_name,
            function,
            model,
            runtime,
            fail_on_error,
            cancellation,
            properties,
        }
    }
}

impl Debug for InferenceExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InferenceExec")
            .field("function", &self.function.name)
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
        write!(
            formatter,
            "InferenceExec: function={}, model={}, output={}",
            self.function.name, self.model.name, self.output_name
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
            Arc::clone(&self.input_expr),
            Arc::clone(&self.schema),
            self.output_name.clone(),
            self.function.clone(),
            self.model.clone(),
            Arc::clone(&self.runtime),
            Arc::clone(&self.fail_on_error),
            self.cancellation.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let input_expr = Arc::clone(&self.input_expr);
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let function = self.function.clone();
        let model = self.model.clone();
        let runtime = Arc::clone(&self.runtime);
        let fail_on_error = Arc::clone(&self.fail_on_error);
        let cancellation = self.cancellation.clone();
        let stream = async_stream::try_stream! {
            while let Some(batch) = input.next().await {
                if cancellation.is_cancelled() {
                    Err(DataFusionError::Execution(
                        "[VQL:QUERY_CANCELLED] query cancelled".to_owned(),
                    ))?;
                }
                let batch = batch?;
                let value = input_expr.evaluate(&batch)?.into_array(batch.num_rows())?;
                let images = value
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DataFusionError::Execution(format!(
                        "{} expects IMAGE",
                        function.name
                    )))?;
                let output = runtime
                    .infer(
                        &function,
                        &model,
                        images,
                        fail_on_error.load(Ordering::Relaxed),
                        cancellation.clone(),
                    )
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut builder = detection_builder(images.len());
                for detections in &output {
                    append_detections(&mut builder, detections.as_deref());
                }
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(builder.finish()) as ArrayRef);
                yield RecordBatch::try_new(Arc::clone(&schema), columns)?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}
