use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, types::IntervalMonthDayNanoType};
use arrow::datatypes::{DataType, FieldRef, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue, exec_err};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    AggregateUDF, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;

use crate::types::is_image_storage;
use crate::{ErrorCode, Result, VqlError};

const WINDOW_INPUT_NAME: &str = "__vql_tumble_output";
const DEFAULT_TUMBLE_STATE_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct TumblePlan {
    input: DataFrame,
    output: DataFrame,
    aggregate_schema: SchemaRef,
    spec: Arc<TumbleSpec>,
    memory_pool: Arc<dyn MemoryPool>,
    state_memory_limit: usize,
}

impl TumblePlan {
    pub(crate) fn try_new(dataframe: &DataFrame) -> Result<Option<Self>> {
        let mut aggregates = Vec::new();
        collect_aggregates(dataframe.logical_plan(), &mut aggregates);
        if aggregates.is_empty() {
            return Ok(None);
        }
        if aggregates.len() != 1 {
            return Err(invalid(
                "an RTSP query may contain at most one TUMBLE aggregate",
            ));
        }
        reject_process_local_image_fields(dataframe.logical_plan())?;
        let aggregate = aggregates[0];
        let (projection_expr, spec) = TumbleSpec::compile(aggregate)?;
        let input_plan = LogicalPlanBuilder::from((*aggregate.input).clone())
            .project(projection_expr)?
            .build()?;

        let aggregate_schema = Arc::new(aggregate.schema.as_arrow().clone());
        let placeholder = Arc::new(MemTable::try_new(
            Arc::clone(&aggregate_schema),
            vec![vec![]],
        )?);
        let placeholder = LogicalPlan::TableScan(datafusion::logical_expr::TableScan::try_new(
            WINDOW_INPUT_NAME,
            provider_as_source(placeholder),
            None,
            vec![],
            None,
        )?);
        let mut replaced = false;
        let output_plan = dataframe
            .logical_plan()
            .clone()
            .transform_up(|plan| {
                if matches!(plan, LogicalPlan::Aggregate(_)) {
                    if replaced {
                        return exec_err!("multiple aggregate nodes in streaming TUMBLE plan");
                    }
                    replaced = true;
                    Ok(Transformed::yes(placeholder.clone()))
                } else {
                    Ok(Transformed::no(plan))
                }
            })
            .data()?;
        let (state, _) = dataframe.clone().into_parts();
        let memory_pool = Arc::clone(&state.runtime_env().memory_pool);
        let state_memory_limit = match memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => limit.min(DEFAULT_TUMBLE_STATE_LIMIT_BYTES),
            MemoryLimit::Infinite | MemoryLimit::Unknown => DEFAULT_TUMBLE_STATE_LIMIT_BYTES,
        };
        Ok(Some(Self {
            input: DataFrame::new(state.clone(), input_plan),
            output: DataFrame::new(state, output_plan),
            aggregate_schema,
            spec: Arc::new(spec),
            memory_pool,
            state_memory_limit,
        }))
    }

    pub(crate) fn input(&self) -> DataFrame {
        self.input.clone()
    }

    pub(crate) fn output(&self) -> DataFrame {
        self.output.clone()
    }

    pub(crate) fn aggregate_schema(&self) -> SchemaRef {
        Arc::clone(&self.aggregate_schema)
    }

    pub(crate) fn output_relation(&self) -> &'static str {
        WINDOW_INPUT_NAME
    }

    pub(crate) fn create_state(&self) -> TumbleState {
        TumbleState::new(
            Arc::clone(&self.spec),
            MemoryConsumer::new("TumbleState").register(&self.memory_pool),
            self.state_memory_limit,
        )
    }
}

#[derive(Debug)]
struct TumbleSpec {
    group_count: usize,
    window_group_index: usize,
    event_time_index: usize,
    width_ms: i64,
    input_schema: SchemaRef,
    aggregate_schema: SchemaRef,
    codecs: Vec<WindowStateCodec>,
}

impl TumbleSpec {
    fn compile(aggregate: &datafusion::logical_expr::Aggregate) -> Result<(Vec<Expr>, Self)> {
        let mut projection = Vec::new();
        let mut tumble = None;
        for (index, expression) in aggregate.group_expr.iter().enumerate() {
            let data_type = expression.get_type(aggregate.input.schema())?;
            if !is_persistable_scalar(&data_type) {
                return Err(invalid(format!(
                    "streaming TUMBLE group key '{}' has unsupported type {data_type}; use a persistent scalar key",
                    expression.schema_name()
                )));
            }
            if let Some((event_time, width_ms)) = parse_tumble(expression)? {
                if tumble.is_some() {
                    return Err(invalid(
                        "an RTSP query may group by exactly one TUMBLE expression",
                    ));
                }
                if !matches!(unalias(&event_time), Expr::Column(column) if column.name == "ts") {
                    return Err(invalid(
                        "streaming TUMBLE must use the RTSP event-time column 'ts' directly",
                    ));
                }
                let (_, field) = event_time.to_field(aggregate.input.schema())?;
                if field.is_nullable() {
                    return Err(invalid(
                        "streaming TUMBLE event time must be non-null; filter NULL timestamps before the aggregate",
                    ));
                }
                if field.data_type()
                    != &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                    && field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
                {
                    return Err(invalid(
                        "streaming TUMBLE event time must use TIMESTAMP(ms)",
                    ));
                }
                tumble = Some((index, event_time.clone(), width_ms));
            }
            projection.push(expression.clone().alias(format!("__vql_group_{index}")));
        }
        let Some((window_group_index, event_time, width_ms)) = tumble else {
            return Err(invalid(
                "streaming aggregates require TUMBLE(event_time, interval) in GROUP BY",
            ));
        };

        let event_time_index = projection.len();
        projection.push(event_time.alias("__vql_event_time"));
        let mut aggregate_functions = Vec::new();
        let mut argument_ranges = Vec::new();
        for expression in &aggregate.aggr_expr {
            let aggregate_function = as_aggregate_function(expression).ok_or_else(|| {
                invalid(format!(
                    "unsupported streaming aggregate '{}'; use COUNT, SUM, AVG, MIN, or MAX",
                    expression.schema_name()
                ))
            })?;
            validate_aggregate(aggregate_function)?;
            let argument_fields = aggregate_function
                .params
                .args
                .iter()
                .map(|argument| {
                    argument
                        .to_field(aggregate.input.schema())
                        .map(|(_, field)| field)
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            let coerced_fields =
                datafusion::logical_expr::type_coercion::functions::fields_with_udf(
                    &argument_fields,
                    aggregate_function.func.as_ref(),
                )?;
            let start = projection.len();
            for (argument, field) in aggregate_function.params.args.iter().zip(coerced_fields) {
                let data_type = field.data_type();
                if !is_persistable_scalar(data_type) {
                    return Err(invalid(format!(
                        "streaming {} input has unsupported type {data_type}; IMAGE, VIDEO, binary, and complex values cannot enter window state",
                        aggregate_function.func.name().to_ascii_uppercase()
                    )));
                }
                projection.push(
                    argument
                        .clone()
                        .cast_to(data_type, aggregate.input.schema())?
                        .alias(format!(
                            "__vql_agg_{}_arg_{}",
                            aggregate_functions.len(),
                            projection.len() - start
                        )),
                );
            }
            let end = projection.len();
            argument_ranges.push(start..end);
            aggregate_functions.push(aggregate_function.clone());
        }

        let projection_plan = LogicalPlanBuilder::from((*aggregate.input).clone())
            .project(projection.clone())?
            .build()?;
        let input_schema = Arc::new(projection_plan.schema().as_arrow().clone());
        let aggregate_schema = Arc::new(aggregate.schema.as_arrow().clone());
        let group_count = aggregate.group_expr.len();
        let codecs = aggregate_functions
            .into_iter()
            .zip(argument_ranges)
            .enumerate()
            .map(|(index, (function, arguments))| {
                WindowStateCodec::try_new(
                    function,
                    arguments,
                    Arc::clone(&input_schema),
                    Arc::clone(&aggregate_schema.fields()[group_count + index]),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((
            projection,
            Self {
                group_count,
                window_group_index,
                event_time_index,
                width_ms,
                input_schema,
                aggregate_schema,
                codecs,
            },
        ))
    }
}

#[derive(Debug)]
struct WindowStateCodec {
    function: Arc<AggregateUDF>,
    argument_indices: std::ops::Range<usize>,
    input_schema: SchemaRef,
    return_field: FieldRef,
    expression_fields: Vec<FieldRef>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    state_fields: Vec<FieldRef>,
}

impl WindowStateCodec {
    fn try_new(
        function: AggregateFunction,
        argument_indices: std::ops::Range<usize>,
        input_schema: SchemaRef,
        return_field: FieldRef,
    ) -> Result<Self> {
        let expression_fields = argument_indices
            .clone()
            .map(|index| Arc::clone(&input_schema.fields()[index]))
            .collect::<Vec<_>>();
        let expressions = argument_indices
            .clone()
            .map(|index| {
                Arc::new(PhysicalColumn::new(input_schema.field(index).name(), index))
                    as Arc<dyn PhysicalExpr>
            })
            .collect::<Vec<_>>();
        let state_fields = function.func.state_fields(StateFieldsArgs {
            name: return_field.name(),
            input_fields: &expression_fields,
            return_field: Arc::clone(&return_field),
            ordering_fields: &[],
            is_distinct: false,
        })?;
        Ok(Self {
            function: function.func,
            argument_indices,
            input_schema,
            return_field,
            expression_fields,
            expressions,
            state_fields,
        })
    }

    fn accumulator(
        &self,
    ) -> datafusion::common::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        self.function.accumulator(AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.input_schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.return_field.name(),
            is_distinct: false,
            exprs: &self.expressions,
            expr_fields: &self.expression_fields,
        })
    }

    fn update(
        &self,
        previous: Option<&[ScalarValue]>,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Vec<ScalarValue>> {
        let mut accumulator = self.accumulator()?;
        if let Some(previous) = previous {
            accumulator.merge_batch(&scalars_to_singleton_arrays(previous)?)?;
        }
        let values = self
            .argument_indices
            .clone()
            .map(|index| {
                ScalarValue::try_from_array(batch.column(index).as_ref(), row)
                    .and_then(|value| value.to_array_of_size(1))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        accumulator.update_batch(&values)?;
        let state = accumulator.state()?;
        self.validate_state(&state)?;
        Ok(state)
    }

    fn evaluate(&self, state: &[ScalarValue]) -> Result<ScalarValue> {
        self.validate_state(state)?;
        let mut accumulator = self.accumulator()?;
        accumulator.merge_batch(&scalars_to_singleton_arrays(state)?)?;
        Ok(accumulator.evaluate()?)
    }

    fn validate_state(&self, state: &[ScalarValue]) -> Result<()> {
        if state.len() != self.state_fields.len()
            || state
                .iter()
                .zip(&self.state_fields)
                .any(|(value, field)| value.data_type() != *field.data_type())
        {
            return Err(VqlError::new(
                ErrorCode::Internal,
                format!(
                    "{} produced a non-deterministic window state schema",
                    self.function.name()
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WindowKey {
    window_start_ms: i64,
    groups: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
struct WindowEntry {
    states: Vec<Vec<ScalarValue>>,
    first_epoch_id: u64,
    last_epoch_id: u64,
}

#[derive(Debug)]
pub(crate) struct TumbleState {
    spec: Arc<TumbleSpec>,
    windows: HashMap<WindowKey, WindowEntry>,
    current_watermark_ms: Option<i64>,
    memory_bytes: usize,
    memory_limit: usize,
    reservation: MemoryReservation,
}

#[derive(Debug)]
pub(crate) struct TumbleEpochOutput {
    pub(crate) closed: Option<RecordBatch>,
    pub(crate) late_rows: u64,
    pub(crate) state_bytes: usize,
}

impl TumbleState {
    fn new(spec: Arc<TumbleSpec>, reservation: MemoryReservation, memory_limit: usize) -> Self {
        Self {
            spec,
            windows: HashMap::new(),
            current_watermark_ms: None,
            memory_bytes: 0,
            memory_limit,
            reservation,
        }
    }

    pub(crate) fn apply_epoch(
        &mut self,
        batches: &[RecordBatch],
        watermark_ms: Option<i64>,
        epoch_id: u64,
    ) -> Result<TumbleEpochOutput> {
        let mut late_rows = 0_u64;
        for batch in batches {
            if batch.schema() != self.spec.input_schema {
                return Err(VqlError::new(
                    ErrorCode::Internal,
                    "streaming TUMBLE input schema changed between epochs",
                ));
            }
            for row in 0..batch.num_rows() {
                let event_time_ms = timestamp_ms(batch.column(self.spec.event_time_index), row)?;
                if self
                    .current_watermark_ms
                    .is_some_and(|watermark| event_time_ms < watermark)
                {
                    late_rows = late_rows.saturating_add(1);
                    continue;
                }
                let groups = (0..self.spec.group_count)
                    .map(|index| ScalarValue::try_from_array(batch.column(index).as_ref(), row))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let window_start_ms =
                    timestamp_scalar_ms(groups.get(self.spec.window_group_index).ok_or_else(
                        || VqlError::new(ErrorCode::Internal, "TUMBLE group key is missing"),
                    )?)?;
                let key = WindowKey {
                    window_start_ms,
                    groups,
                };
                let entry = self.windows.entry(key).or_insert_with(|| WindowEntry {
                    states: vec![Vec::new(); self.spec.codecs.len()],
                    first_epoch_id: epoch_id,
                    last_epoch_id: epoch_id,
                });
                entry.last_epoch_id = epoch_id;
                for (index, codec) in self.spec.codecs.iter().enumerate() {
                    entry.states[index] = codec.update(
                        (!entry.states[index].is_empty()).then_some(entry.states[index].as_slice()),
                        batch,
                        row,
                    )?;
                }
                self.refresh_reservation()?;
            }
        }
        self.current_watermark_ms = match (self.current_watermark_ms, watermark_ms) {
            (Some(current), Some(next)) => Some(current.max(next)),
            (None, next) => next,
            (current, None) => current,
        };
        let closed = self.close_windows()?;
        self.refresh_reservation()?;
        Ok(TumbleEpochOutput {
            closed,
            late_rows,
            state_bytes: self.memory_bytes,
        })
    }

    fn refresh_reservation(&mut self) -> Result<()> {
        let memory_bytes = self.estimated_size();
        if memory_bytes > self.memory_limit {
            return Err(window_state_limit_error(self.memory_limit));
        }
        self.reservation
            .try_resize(memory_bytes)
            .map_err(|error| window_state_limit_error(self.memory_limit).with_source(error))?;
        self.memory_bytes = memory_bytes;
        Ok(())
    }

    fn close_windows(&mut self) -> Result<Option<RecordBatch>> {
        let Some(watermark_ms) = self.current_watermark_ms else {
            return Ok(None);
        };
        let mut keys = self
            .windows
            .keys()
            .filter(|key| key.window_start_ms.saturating_add(self.spec.width_ms) <= watermark_ms)
            .cloned()
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(None);
        }
        keys.sort_by_key(|key| key.window_start_ms);
        let mut columns =
            vec![Vec::with_capacity(keys.len()); self.spec.aggregate_schema.fields().len()];
        for key in keys {
            let entry = self.windows.remove(&key).ok_or_else(|| {
                VqlError::new(ErrorCode::Internal, "closed TUMBLE window disappeared")
            })?;
            debug_assert!(entry.first_epoch_id <= entry.last_epoch_id);
            for (index, value) in key.groups.into_iter().enumerate() {
                columns[index].push(value);
            }
            for (index, (codec, state)) in self.spec.codecs.iter().zip(&entry.states).enumerate() {
                columns[self.spec.group_count + index].push(codec.evaluate(state)?);
            }
        }
        if self.windows.capacity() > self.windows.len().saturating_mul(4) {
            self.windows.shrink_to(self.windows.len().saturating_mul(2));
        }
        let arrays = columns
            .into_iter()
            .map(|values| ScalarValue::iter_to_array(values.into_iter()))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.spec.aggregate_schema), arrays)
            .map(Some)
            .map_err(|error| {
                VqlError::new(ErrorCode::Internal, "failed to build closed TUMBLE windows")
                    .with_source(error)
            })
    }

    fn estimated_size(&self) -> usize {
        self.windows.capacity() * std::mem::size_of::<(WindowKey, WindowEntry)>()
            + self
                .windows
                .iter()
                .map(|(key, entry)| {
                    scalar_vec_allocation(&key.groups, key.groups.capacity())
                        + entry.states.capacity() * std::mem::size_of::<Vec<ScalarValue>>()
                        + entry
                            .states
                            .iter()
                            .map(|state| scalar_vec_allocation(state, state.capacity()))
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

fn reject_process_local_image_fields(plan: &LogicalPlan) -> Result<()> {
    let input_schema = plan
        .inputs()
        .into_iter()
        .next()
        .map(LogicalPlan::schema)
        .unwrap_or_else(|| plan.schema());
    for expression in plan.expressions() {
        let mut process_local_field = None;
        expression.apply(|nested| {
            if let Some(field) = process_local_image_field(nested, input_schema)? {
                process_local_field = Some(field);
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        if let Some(field) = process_local_field {
            return Err(invalid(format!(
                "IMAGE field '{field}' is process-local and cannot enter streaming TUMBLE state"
            )));
        }
    }
    for input in plan.inputs() {
        reject_process_local_image_fields(input)?;
    }
    Ok(())
}

fn process_local_image_field(
    expression: &Expr,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Option<String>> {
    let Expr::ScalarFunction(function) = unalias(expression) else {
        return Ok(None);
    };
    if !function.func.name().eq_ignore_ascii_case("get_field") {
        return Ok(None);
    }
    let Some(Expr::Literal(ScalarValue::Utf8(Some(field)), _)) = function.args.get(1).map(unalias)
    else {
        return Ok(None);
    };
    if !matches!(field.as_str(), "buffer_id" | "buffer_slot") {
        return Ok(None);
    }
    let Some(value) = function.args.first() else {
        return Ok(None);
    };
    Ok(is_image_storage(&value.get_type(input_schema)?).then(|| field.clone()))
}

fn window_state_limit_error(limit: usize) -> VqlError {
    VqlError::new(
        ErrorCode::Execution,
        format!(
            "streaming TUMBLE state exceeded the {limit}-byte query memory budget; reduce group-key cardinality or shorten the window"
        ),
    )
}

fn collect_aggregates<'a>(
    plan: &'a LogicalPlan,
    aggregates: &mut Vec<&'a datafusion::logical_expr::Aggregate>,
) {
    if let LogicalPlan::Aggregate(aggregate) = plan {
        aggregates.push(aggregate);
    }
    for input in plan.inputs() {
        collect_aggregates(input, aggregates);
    }
}

fn parse_tumble(expression: &Expr) -> Result<Option<(Expr, i64)>> {
    let expression = unalias(expression);
    let Expr::ScalarFunction(function) = expression else {
        return Ok(None);
    };
    if !function.func.name().eq_ignore_ascii_case("tumble") {
        return Ok(None);
    }
    if function.args.len() != 2 {
        return Err(invalid("TUMBLE expects an event time and a fixed interval"));
    }
    let Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(interval)), _) = &function.args[1]
    else {
        return Err(invalid(
            "streaming TUMBLE interval must be a constant fixed duration",
        ));
    };
    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*interval);
    if months != 0 {
        return Err(invalid(
            "streaming TUMBLE interval cannot contain calendar months",
        ));
    }
    let width_ms = i64::from(days)
        .checked_mul(86_400_000)
        .and_then(|value| value.checked_add(nanos / 1_000_000))
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid("streaming TUMBLE interval must be positive"))?;
    Ok(Some((function.args[0].clone(), width_ms)))
}

fn as_aggregate_function(expression: &Expr) -> Option<&AggregateFunction> {
    match unalias(expression) {
        Expr::AggregateFunction(function) => Some(function),
        _ => None,
    }
}

fn unalias(mut expression: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expression {
        expression = &alias.expr;
    }
    expression
}

fn validate_aggregate(function: &AggregateFunction) -> Result<()> {
    let name = function.func.name().to_ascii_lowercase();
    if !matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
        return Err(invalid(format!(
            "aggregate {} is not supported for streaming TUMBLE; use COUNT, SUM, AVG, MIN, or MAX",
            function.func.name()
        )));
    }
    if function.params.distinct {
        return Err(invalid(format!(
            "{}(DISTINCT ...) is not supported for streaming TUMBLE in v0.1",
            name.to_ascii_uppercase()
        )));
    }
    if function.params.filter.is_some()
        || !function.params.order_by.is_empty()
        || function.params.null_treatment.is_some()
    {
        return Err(invalid(format!(
            "FILTER, ORDER BY, and NULL treatment are not supported inside streaming {} in v0.1; filter rows before the aggregate",
            name.to_ascii_uppercase()
        )));
    }
    if function.params.args.len() != 1 {
        return Err(invalid(format!(
            "streaming {} expects exactly one input",
            name.to_ascii_uppercase()
        )));
    }
    Ok(())
}

fn is_persistable_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
    )
}

fn timestamp_ms(array: &ArrayRef, row: usize) -> Result<i64> {
    timestamp_scalar_ms(&ScalarValue::try_from_array(array.as_ref(), row)?)
}

fn timestamp_scalar_ms(value: &ScalarValue) -> Result<i64> {
    match value {
        ScalarValue::TimestampMillisecond(Some(value), _) => Ok(*value),
        ScalarValue::TimestampMillisecond(None, _) => Err(VqlError::new(
            ErrorCode::Execution,
            "streaming TUMBLE event time cannot be NULL",
        )),
        _ => Err(VqlError::new(
            ErrorCode::Internal,
            "streaming TUMBLE event time is not TIMESTAMP(ms)",
        )),
    }
}

fn scalars_to_singleton_arrays(
    values: &[ScalarValue],
) -> datafusion::common::Result<Vec<ArrayRef>> {
    values
        .iter()
        .map(|value| value.to_array_of_size(1))
        .collect()
}

fn scalar_vec_allocation(values: &[ScalarValue], capacity: usize) -> usize {
    capacity * std::mem::size_of::<ScalarValue>()
        + values
            .iter()
            .map(|value| {
                value
                    .size()
                    .saturating_sub(std::mem::size_of::<ScalarValue>())
            })
            .sum::<usize>()
}

fn invalid(message: impl Into<String>) -> VqlError {
    VqlError::new(ErrorCode::InvalidSql, message)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::context::SessionContext;

    use super::*;
    use crate::functions::tumble_udf;

    #[tokio::test]
    async fn normalized_state_merges_epochs_nulls_groups_and_late_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("kind", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampMillisecondArray::from(vec![100, 200, 300, 1_100, 1_200, 500])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    None,
                    Some("a"),
                    Some("a"),
                    Some("late"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    None,
                    Some(5),
                    Some(30),
                    Some(40),
                    Some(99),
                ])),
            ],
        )
        .unwrap();
        let context = SessionContext::new();
        context.register_udf(tumble_udf());
        context.register_batch("samples", batch).unwrap();
        let dataframe = context
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start,
                        kind,
                        COUNT(*) AS rows,
                        COUNT(value) AS values,
                        SUM(value) AS total,
                        AVG(value) AS mean,
                        MIN(value) AS minimum,
                        MAX(value) AS maximum
                 FROM samples
                 GROUP BY 1, 2",
            )
            .await
            .unwrap();
        let plan = TumblePlan::try_new(&dataframe).unwrap().unwrap();
        let projected = plan.input().collect().await.unwrap();
        assert_eq!(projected.len(), 1);
        let projected = &projected[0];
        let mut state = plan.create_state();

        let first = state
            .apply_epoch(&[projected.slice(0, 3)], Some(1_000), 0)
            .unwrap();
        assert_eq!(first.late_rows, 0);
        assert_eq!(first.state_bytes, 0);
        let first = first.closed.unwrap();
        assert_window(&first, Some("a"), (2, 1, 10, 10.0, 10, 10));
        assert_window(&first, None, (1, 1, 5, 5.0, 5, 5));

        let late = state
            .apply_epoch(&[projected.slice(5, 1)], None, 1)
            .unwrap();
        assert_eq!(late.late_rows, 1);
        assert!(late.closed.is_none());

        let open = state
            .apply_epoch(&[projected.slice(3, 2)], None, 2)
            .unwrap();
        assert!(open.closed.is_none());
        assert!(open.state_bytes > 0);
        let second = state.apply_epoch(&[], Some(2_000), 3).unwrap();
        assert_eq!(second.state_bytes, 0);
        assert_window(&second.closed.unwrap(), Some("a"), (2, 2, 70, 35.0, 30, 40));
    }

    #[tokio::test]
    async fn sum_overflow_matches_batch_semantics() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![100, 200]).with_timezone("UTC")),
                Arc::new(Int64Array::from(vec![i64::MAX, 1])),
            ],
        )
        .unwrap();
        let context = SessionContext::new();
        context.register_udf(tumble_udf());
        context.register_batch("samples", batch).unwrap();
        let dataframe = context
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start,
                        SUM(value) AS total
                 FROM samples
                 GROUP BY 1",
            )
            .await
            .unwrap();
        let expected = dataframe.clone().collect().await.unwrap();
        let plan = TumblePlan::try_new(&dataframe).unwrap().unwrap();
        let projected = plan.input().collect().await.unwrap();
        let output = plan
            .create_state()
            .apply_epoch(&projected, Some(1_000), 0)
            .unwrap()
            .closed
            .unwrap();
        let sum = |batch: &RecordBatch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(sum(&output), sum(&expected[0]));
    }

    #[tokio::test]
    async fn state_limit_is_enforced_with_an_unbounded_runtime_pool() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("kind", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![100]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let context = SessionContext::new();
        context.register_udf(tumble_udf());
        context.register_batch("samples", batch).unwrap();
        let dataframe = context
            .sql(
                "SELECT TUMBLE(ts, INTERVAL '1' SECOND), kind, COUNT(*)
                 FROM samples
                 GROUP BY 1, 2",
            )
            .await
            .unwrap();
        let plan = TumblePlan::try_new(&dataframe).unwrap().unwrap();
        let projected = plan.input().collect().await.unwrap();
        let mut state = TumbleState::new(
            Arc::clone(&plan.spec),
            MemoryConsumer::new("LimitedTumbleState").register(&plan.memory_pool),
            1,
        );

        let error = state.apply_epoch(&projected, None, 0).unwrap_err();
        assert_eq!(error.code, ErrorCode::Execution);
        assert!(error.message.contains("1-byte query memory budget"));
    }

    fn assert_window(
        batch: &RecordBatch,
        expected_kind: Option<&str>,
        expected: (i64, i64, i64, f64, i64, i64),
    ) {
        let (
            expected_rows,
            expected_values,
            expected_total,
            expected_mean,
            expected_minimum,
            expected_maximum,
        ) = expected;
        let kinds = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row = (0..batch.num_rows())
            .find(|row| {
                if let Some(expected_kind) = expected_kind {
                    !kinds.is_null(*row) && kinds.value(*row) == expected_kind
                } else {
                    kinds.is_null(*row)
                }
            })
            .expect("window group exists");
        let value = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
        };
        assert_eq!(value(2), expected_rows);
        assert_eq!(value(3), expected_values);
        assert_eq!(value(4), expected_total);
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
            expected_mean
        );
        assert_eq!(value(6), expected_minimum);
        assert_eq!(value(7), expected_maximum);
    }
}
