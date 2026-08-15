mod inference;
mod normalize;
mod session_state;
mod sink;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use tokio_util::sync::CancellationToken;

use crate::models::ModelRuntime;

pub(crate) use session_state::{context_for_function_ddl, context_for_snapshot};
pub(crate) use sink::wrap_console_sink;

pub(crate) fn normalize_function_ddl(
    sql: &str,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> crate::Result<String> {
    normalize::expand_macros(sql, snapshot)
}

pub(crate) struct PlannedStatement {
    pub(crate) dataframe: DataFrame,
    pub(crate) stream_name: Option<String>,
    pub(crate) stream_skip: usize,
    pub(crate) stream_fetch: Option<usize>,
}

pub(crate) async fn plan_statement(
    context: &SessionContext,
    snapshot: &crate::catalog::DefinitionSnapshot,
    sql: &str,
    models: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
) -> crate::Result<PlannedStatement> {
    let sql = normalize::normalize_query(sql, snapshot)?;
    let dataframe = context.sql(&sql).await?;
    let (state, plan) = dataframe.into_parts();
    let stream_name = validate_streamability(&plan, snapshot)?;
    let (plan, stream_skip, stream_fetch) = if stream_name.is_some() {
        strip_top_level_limit(plan)?
    } else {
        (plan, 0, None)
    };
    let plan = inference::extract_inference(plan, snapshot, models, fail_on_error, cancellation)?;
    Ok(PlannedStatement {
        dataframe: DataFrame::new(state, plan),
        stream_name,
        stream_skip,
        stream_fetch,
    })
}

pub(crate) fn bind_stream_epoch(
    dataframe: DataFrame,
    stream_name: &str,
    batches: Vec<RecordBatch>,
) -> crate::Result<DataFrame> {
    let schema = crate::connectors::rtsp::rtsp_schema();
    let provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
    let source = provider_as_source(provider);
    let stream_name = stream_name.to_ascii_lowercase();
    let (state, plan) = dataframe.into_parts();
    let plan = plan
        .transform_up(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            if relation_name(&scan.table_name.to_string()) != stream_name {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            }
            scan.source = Arc::clone(&source);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })
        .data()?;
    Ok(DataFrame::new(state, plan))
}

fn validate_streamability(
    plan: &LogicalPlan,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> crate::Result<Option<String>> {
    if matches!(plan, LogicalPlan::Explain(_)) {
        return Ok(None);
    }
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans);
    let stream_scans = scans
        .iter()
        .filter(|name| snapshot.stream(name).is_some())
        .cloned()
        .collect::<Vec<_>>();
    if stream_scans.is_empty() {
        return Ok(None);
    }
    let stream_name = stream_scans[0].clone();
    if stream_scans.iter().any(|name| name != &stream_name) || scans.len() != stream_scans.len() {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidSql,
            "an unbounded query may read exactly one RTSP stream; split multi-source work into independent queries",
        ));
    }
    let limit_count = count_limits(plan);
    if limit_count > usize::from(matches!(plan, LogicalPlan::Limit(_))) {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidSql,
            "LIMIT/OFFSET on an RTSP stream is supported only at the top level",
        ));
    }
    validate_stream_node(plan)?;
    Ok(Some(stream_name))
}

fn collect_scans(plan: &LogicalPlan, scans: &mut Vec<String>) {
    if let LogicalPlan::TableScan(scan) = plan {
        scans.push(relation_name(&scan.table_name.to_string()));
    }
    for input in plan.inputs() {
        collect_scans(input, scans);
    }
}

fn validate_stream_node(plan: &LogicalPlan) -> crate::Result<()> {
    match plan {
        LogicalPlan::Aggregate(_) => {
            return Err(crate::VqlError::feature(
                "streaming aggregates require the unfinished v0.1 TUMBLE state implementation; use a stateless preview for RTSP source validation",
                "v0.1",
            ));
        }
        LogicalPlan::Sort(_) => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                "ORDER BY over an unbounded RTSP stream is not supported; move ordering to a bounded result",
            ));
        }
        LogicalPlan::Distinct(_) => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                "DISTINCT over an unbounded RTSP stream is not supported; use a bounded window or batch query",
            ));
        }
        LogicalPlan::Join(_) | LogicalPlan::Union(_) => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                "JOIN and UNION are not supported for the single-source RTSP runtime",
            ));
        }
        LogicalPlan::Window(_) => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                "analytic OVER windows are not supported on an unbounded RTSP stream; use TUMBLE when streaming window state is enabled",
            ));
        }
        LogicalPlan::Analyze(_) => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                "EXPLAIN ANALYZE is not supported for an unbounded RTSP query; use EXPLAIN without ANALYZE",
            ));
        }
        _ => {}
    }
    for input in plan.inputs() {
        validate_stream_node(input)?;
    }
    Ok(())
}

fn count_limits(plan: &LogicalPlan) -> usize {
    usize::from(matches!(plan, LogicalPlan::Limit(_)))
        + plan.inputs().into_iter().map(count_limits).sum::<usize>()
}

fn strip_top_level_limit(plan: LogicalPlan) -> crate::Result<(LogicalPlan, usize, Option<usize>)> {
    let LogicalPlan::Limit(limit) = plan else {
        return Ok((plan, 0, None));
    };
    let skip = literal_limit_value(limit.skip.as_deref(), "OFFSET")?.unwrap_or(0);
    let fetch = literal_limit_value(limit.fetch.as_deref(), "LIMIT")?;
    Ok(((*limit.input).clone(), skip, fetch))
}

fn literal_limit_value(
    expression: Option<&datafusion::logical_expr::Expr>,
    label: &str,
) -> crate::Result<Option<usize>> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    let datafusion::logical_expr::Expr::Literal(value, _) = expression else {
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidSql,
            format!("{label} on an RTSP stream must be a non-negative integer literal"),
        ));
    };
    let value = match value {
        datafusion::common::ScalarValue::Int64(Some(value)) if *value >= 0 => *value as u64,
        datafusion::common::ScalarValue::UInt64(Some(value)) => *value,
        _ => {
            return Err(crate::VqlError::new(
                crate::ErrorCode::InvalidSql,
                format!("{label} on an RTSP stream must be a non-negative integer literal"),
            ));
        }
    };
    usize::try_from(value).map(Some).map_err(|_| {
        crate::VqlError::new(
            crate::ErrorCode::InvalidSql,
            format!("{label} is too large for this platform"),
        )
    })
}

fn relation_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase()
}
