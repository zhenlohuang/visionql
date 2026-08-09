use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, DataFusionError, Result as DataFusionResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

pub(crate) fn wrap_console_sink(dataframe: DataFrame, name: String) -> DataFrame {
    let (state, input) = dataframe.into_parts();
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(SinkWriteNode {
            schema: Arc::clone(input.schema()),
            input,
            name,
        }),
    });
    DataFrame::new(state, plan)
}

#[derive(Clone)]
struct SinkWriteNode {
    input: LogicalPlan,
    name: String,
    schema: DFSchemaRef,
}

impl Debug for SinkWriteNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkWrite")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PartialEq for SinkWriteNode {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.input == other.input
    }
}

impl Eq for SinkWriteNode {}

impl Hash for SinkWriteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for SinkWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (&self.name, format!("{:?}", self.input))
            .partial_cmp(&(&other.name, format!("{:?}", other.input)))
    }
}

impl UserDefinedLogicalNodeCore for SinkWriteNode {
    fn name(&self) -> &str {
        "SinkWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn fmt_for_explain(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SinkWrite: name={}, type=console", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "SinkWrite requires no expressions and one input".to_owned(),
            ));
        }
        let input = inputs.remove(0);
        Ok(Self {
            schema: Arc::clone(input.schema()),
            input,
            name: self.name.clone(),
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub(crate) struct SinkExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SinkExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<SinkWriteNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "SinkWrite planner requires one input".to_owned(),
            ));
        }
        Ok(Some(Arc::new(SinkExec::new(
            Arc::clone(&physical_inputs[0]),
            node.name.clone(),
        ))))
    }
}

struct SinkExec {
    input: Arc<dyn ExecutionPlan>,
    name: String,
    properties: Arc<PlanProperties>,
}

impl SinkExec {
    fn new(input: Arc<dyn ExecutionPlan>, name: String) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            name,
            properties,
        }
    }
}

impl Debug for SinkExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkExec")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SinkExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(formatter, "SinkExec: name={}, type=console", self.name)
    }
}

impl ExecutionPlan for SinkExec {
    fn name(&self) -> &str {
        "SinkExec"
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
                "SinkExec requires one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self::new(children.remove(0), self.name.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
