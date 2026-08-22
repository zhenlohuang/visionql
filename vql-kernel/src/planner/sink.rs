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

use crate::catalog::KafkaTableConfig;
use crate::connectors::kafka::{KafkaSink, validate_kafka_schema};
use crate::resources::QueryBudget;
use crate::{ErrorCode, Result, SecretProviderRef, VqlError};

#[derive(Clone)]
pub(crate) struct SinkTarget {
    table: WritableTable,
    write_schema: SchemaRef,
    writer: SinkWriter,
    cancellation: CancellationToken,
}

#[derive(Clone)]
enum SinkWriter {
    Kafka(Arc<KafkaSink>),
    #[cfg(test)]
    Recording(Arc<std::sync::atomic::AtomicUsize>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WritableTable {
    name: String,
    config: KafkaTableConfig,
    schema_fingerprint: String,
}

impl SinkTarget {
    pub(crate) fn try_new(
        name: String,
        config: KafkaTableConfig,
        input_schema: &SchemaRef,
        write_schema: SchemaRef,
        cancellation: CancellationToken,
        secret_provider: Option<SecretProviderRef>,
        budget: QueryBudget,
    ) -> Result<Self> {
        validate_kafka_schema(input_schema)?;
        let writer = SinkWriter::Kafka(Arc::new(KafkaSink::new(
            name.clone(),
            config.clone(),
            secret_provider,
            budget,
        )));
        let schema_fingerprint = format!("{write_schema:?}");
        Ok(Self {
            table: WritableTable {
                name,
                config,
                schema_fingerprint,
            },
            write_schema,
            writer,
            cancellation,
        })
    }

    fn kind_name(&self) -> &'static str {
        "kafka"
    }

    pub(crate) async fn write(&self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        match &self.writer {
            SinkWriter::Kafka(writer) => {
                let batch = self.batch_for_write(batch)?;
                writer.write_batch(&batch, &self.cancellation).await
            }
            #[cfg(test)]
            SinkWriter::Recording(rows) => {
                rows.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }
    }

    fn batch_for_write(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch> {
        arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&self.write_schema),
            batch.columns().to_vec(),
        )
        .map_err(|error| {
            VqlError::new(
                ErrorCode::Execution,
                "query output cannot be encoded with the declared table schema",
            )
            .with_source(error)
        })
    }

    pub(crate) async fn begin_execution(&self) {
        #[allow(irrefutable_let_patterns)]
        if let SinkWriter::Kafka(writer) = &self.writer {
            writer.begin_execution().await;
        }
    }

    pub(crate) async fn finish_execution(&self) -> Result<()> {
        match &self.writer {
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
                table: WritableTable {
                    name: "recording".to_owned(),
                    config: KafkaTableConfig {
                        bootstrap_servers: "localhost:9092".to_owned(),
                        topic: "recording".to_owned(),
                        credential_ref: None,
                        delivery_timeout_ms: 30_000,
                        buffer_capacity: 1,
                    },
                    schema_fingerprint: "recording".to_owned(),
                },
                write_schema: Arc::new(arrow::datatypes::Schema::empty()),
                writer: SinkWriter::Recording(Arc::clone(&rows)),
                cancellation: CancellationToken::new(),
            },
            rows,
        )
    }

    #[cfg(test)]
    pub(crate) fn write_schema(&self) -> &SchemaRef {
        &self.write_schema
    }
}

impl Debug for SinkTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SinkTarget")
            .field("name", &self.table.name)
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
        self.target.table == other.target.table && self.input == other.input
    }
}

impl Eq for SinkWriteNode {}

impl Hash for SinkWriteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target.table.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for SinkWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            format!("{:?}", self.target.table),
            format!("{:?}", self.input),
        )
            .partial_cmp(&(
                format!("{:?}", other.target.table),
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
            self.target.table.name,
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
            self.target.table.name,
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

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn kafka_batch_uses_declared_target_field_names() {
        let (mut target, _) = SinkTarget::recording();
        target.write_schema = Arc::new(Schema::new(vec![Field::new(
            "people",
            DataType::Int64,
            true,
        )]));
        let input = RecordBatch::try_from_iter([(
            "Int64(1)",
            Arc::new(Int64Array::from(vec![1])) as arrow::array::ArrayRef,
        )])
        .unwrap();

        let output = target.batch_for_write(&input).unwrap();

        assert_eq!(output.schema().field(0).name(), "people");
        assert_eq!(output.column(0), input.column(0));
    }
}
