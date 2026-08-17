use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
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
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::catalog::{SinkDef, SinkKind};
use crate::connectors::kafka::{KafkaSink, validate_kafka_schema};
use crate::resources::QueryBudget;
use crate::{ErrorCode, Result, SecretProviderRef, VqlError};

#[derive(Clone)]
pub(crate) struct SinkTarget {
    definition: SinkDef,
    writer: SinkWriter,
    cancellation: CancellationToken,
}

#[derive(Clone)]
enum SinkWriter {
    Console,
    Kafka(Arc<KafkaSink>),
    #[cfg(test)]
    Recording(Arc<std::sync::atomic::AtomicUsize>),
}

impl SinkTarget {
    pub(crate) fn try_new(
        definition: SinkDef,
        schema: &SchemaRef,
        cancellation: CancellationToken,
        secret_provider: Option<SecretProviderRef>,
        budget: QueryBudget,
    ) -> Result<Self> {
        let writer = match (definition.kind, definition.kafka.clone()) {
            (SinkKind::Console, None) => SinkWriter::Console,
            (SinkKind::Kafka, Some(config)) => {
                validate_kafka_schema(schema)?;
                SinkWriter::Kafka(Arc::new(KafkaSink::new(
                    definition.name.clone(),
                    config,
                    secret_provider,
                    budget,
                )))
            }
            _ => {
                return Err(VqlError::new(
                    ErrorCode::Catalog,
                    format!(
                        "Sink '{}' has an inconsistent connector definition",
                        definition.name
                    ),
                ));
            }
        };
        Ok(Self {
            definition,
            writer,
            cancellation,
        })
    }

    fn kind_name(&self) -> &'static str {
        match self.definition.kind {
            SinkKind::Console => "console",
            SinkKind::Kafka => "kafka",
        }
    }

    pub(crate) async fn write(&self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        match &self.writer {
            SinkWriter::Console => Ok(()),
            SinkWriter::Kafka(writer) => writer.write_batch(batch, &self.cancellation).await,
            #[cfg(test)]
            SinkWriter::Recording(rows) => {
                rows.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }
    }

    pub(crate) async fn begin_execution(&self) {
        if let SinkWriter::Kafka(writer) = &self.writer {
            writer.begin_execution().await;
        }
    }

    pub(crate) async fn finish_execution(&self) -> Result<()> {
        match &self.writer {
            SinkWriter::Console => Ok(()),
            SinkWriter::Kafka(writer) => writer.finish_execution(&self.cancellation).await,
            #[cfg(test)]
            SinkWriter::Recording(_) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn recording() -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
        let rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Self {
                definition: SinkDef {
                    name: "recording".to_owned(),
                    kind: SinkKind::Console,
                    kafka: None,
                },
                writer: SinkWriter::Recording(Arc::clone(&rows)),
                cancellation: CancellationToken::new(),
            },
            rows,
        )
    }
}

impl Debug for SinkTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkTarget")
            .field("name", &self.definition.name)
            .field("kind", &self.kind_name())
            .finish_non_exhaustive()
    }
}

pub(crate) fn wrap_sink(dataframe: DataFrame, target: SinkTarget) -> DataFrame {
    let (state, input) = dataframe.into_parts();
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(SinkWriteNode {
            schema: Arc::clone(input.schema()),
            input,
            target,
        }),
    });
    DataFrame::new(state, plan)
}

#[derive(Clone)]
struct SinkWriteNode {
    input: LogicalPlan,
    target: SinkTarget,
    schema: DFSchemaRef,
}

impl Debug for SinkWriteNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkWrite")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl PartialEq for SinkWriteNode {
    fn eq(&self, other: &Self) -> bool {
        self.target.definition == other.target.definition && self.input == other.input
    }
}

impl Eq for SinkWriteNode {}

impl Hash for SinkWriteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target.definition.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for SinkWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            format!("{:?}", self.target.definition),
            format!("{:?}", self.input),
        )
            .partial_cmp(&(
                format!("{:?}", other.target.definition),
                format!("{:?}", other.input),
            ))
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
        write!(
            formatter,
            "SinkWrite: name={}, type={}",
            self.target.definition.name,
            self.target.kind_name()
        )
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
            target: self.target.clone(),
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
            node.target.clone(),
        ))))
    }
}

struct SinkExec {
    input: Arc<dyn ExecutionPlan>,
    target: SinkTarget,
    properties: Arc<PlanProperties>,
}

impl SinkExec {
    fn new(input: Arc<dyn ExecutionPlan>, target: SinkTarget) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            target,
            properties,
        }
    }
}

impl Debug for SinkExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkExec")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SinkExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            formatter,
            "SinkExec: name={}, type={}",
            self.target.definition.name,
            self.target.kind_name()
        )
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
        Ok(Arc::new(Self::new(children.remove(0), self.target.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let target = self.target.clone();
        let stream = async_stream::try_stream! {
            let mut input = input;
            while let Some(batch) = input.next().await {
                let batch = batch?;
                target
                    .write(&batch)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                yield batch;
            }
        };
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        ))
    }
}
