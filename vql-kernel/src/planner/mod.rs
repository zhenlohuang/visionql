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
use datafusion::logical_expr::{LogicalPlan, PlanType, StringifiedPlan};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, Tokenizer, Whitespace};
use tokio_util::sync::CancellationToken;

use crate::models::ModelRuntime;
use crate::resources::QueryBudget;
use crate::session::QueryMetrics;
use crate::stream::TumblePlan;

pub(crate) use session_state::{context_for_function_ddl, context_for_snapshot};
pub(crate) use sink::{SinkTarget, wrap_sink};

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
    pub(crate) tumble: Option<TumblePlan>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn plan_statement(
    context: &SessionContext,
    snapshot: &crate::catalog::DefinitionSnapshot,
    sql: &str,
    models: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
    budget: QueryBudget,
    metrics: Arc<QueryMetrics>,
) -> crate::Result<PlannedStatement> {
    let original_sql = sql;
    let sql = normalize::normalize_query(original_sql, snapshot)?;
    let dataframe = context.sql(&sql).await?;
    let (state, plan) = dataframe.into_parts();
    let stream_name = validate_streamability(&plan, snapshot, original_sql)?;
    let (plan, stream_skip, stream_fetch) = if stream_name.is_some() {
        strip_top_level_limit(plan)?
    } else {
        (plan, 0, None)
    };
    let plan = inference::extract_inference(
        plan,
        snapshot,
        models,
        fail_on_error,
        cancellation,
        budget,
        metrics,
    )?;
    let plan = annotate_explain(plan, snapshot)?;
    let dataframe = DataFrame::new(state, plan);
    let tumble = if stream_name.is_some() {
        TumblePlan::try_new(&dataframe)?
    } else {
        None
    };
    Ok(PlannedStatement {
        dataframe,
        stream_name,
        stream_skip,
        stream_fetch,
        tumble,
    })
}

fn annotate_explain(
    plan: LogicalPlan,
    snapshot: &crate::catalog::DefinitionSnapshot,
) -> crate::Result<LogicalPlan> {
    let LogicalPlan::Explain(mut explain) = plan else {
        return Ok(plan);
    };
    let mut scans = Vec::new();
    collect_scans(&explain.plan, &mut scans);
    let streams = scans
        .iter()
        .filter_map(|name| rtsp_table(snapshot, name))
        .collect::<Vec<_>>();
    let stream = streams.first();
    let mode = match stream {
        Some(_) if top_level_fetch(&explain.plan).is_some() => "bounded",
        Some(_) => "continuous",
        None => "bounded",
    };
    let image_payload = if stream.is_some() {
        "frame_buffer"
    } else {
        "locator_or_encoded"
    };
    let has_tumble = contains_tumble(&explain.plan);
    let mut lines = vec![format!("VisionQLPlan mode={mode}")];
    if let Some(stream) = stream {
        lines.push(format!(
            "Source RTSP name={} fps={} event_time={:?} watermark_delay_ms={} transport={:?} projection_pushdown=enabled time_range_pushdown=not_applicable",
            stream.name,
            stream.fps,
            stream.event_time,
            stream.watermark_delay_ms,
            stream.transport,
        ));
        let mut topology = vec!["RTSPSource", "EpochCoordinator"];
        if !inference::explain_annotations(&explain.plan, image_payload).is_empty() {
            topology.push("Inference");
        }
        if has_tumble {
            topology.push("TumblePlan");
        }
        topology.push("Watermark");
        lines.push(format!("Topology {}", topology.join(" -> ")));
    } else {
        lines.push(
            "Source bounded projection_pushdown=enabled time_range_pushdown=provider_specific"
                .to_owned(),
        );
    }
    lines.extend(inference::explain_annotations(&explain.plan, image_payload));
    if stream.is_some()
        && let Some((node, suggestion)) = first_unsupported_stream_node(&explain.plan)
    {
        lines.push(format!("Unsupported node={node}; suggestion={suggestion}"));
    }
    explain.stringified_plans.insert(
        0,
        StringifiedPlan::new(PlanType::FinalLogicalPlan, lines.join("\n")),
    );
    Ok(LogicalPlan::Explain(explain))
}

fn top_level_fetch(plan: &LogicalPlan) -> Option<usize> {
    let LogicalPlan::Limit(limit) = plan else {
        return None;
    };
    literal_limit_value(limit.fetch.as_deref(), "LIMIT")
        .ok()
        .flatten()
}

fn contains_tumble(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Aggregate(aggregate) = plan
        && aggregate.group_expr.iter().any(|expression| {
            expression
                .to_string()
                .to_ascii_lowercase()
                .contains("tumble(")
        })
    {
        return true;
    }
    plan.inputs().into_iter().any(contains_tumble)
}

pub(crate) fn bind_stream_epoch(
    dataframe: DataFrame,
    stream_name: &str,
    batches: Vec<RecordBatch>,
) -> crate::Result<DataFrame> {
    bind_relation(
        dataframe,
        stream_name,
        crate::connectors::rtsp::rtsp_schema(),
        batches,
    )
}

pub(crate) fn bind_tumble_output(
    tumble: &TumblePlan,
    batch: RecordBatch,
) -> crate::Result<DataFrame> {
    bind_relation(
        tumble.output(),
        tumble.output_relation(),
        tumble.aggregate_schema(),
        vec![batch],
    )
}

fn bind_relation(
    dataframe: DataFrame,
    relation: &str,
    schema: arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
) -> crate::Result<DataFrame> {
    let provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
    let source = provider_as_source(provider);
    let relation = relation.to_ascii_lowercase();
    let (state, plan) = dataframe.into_parts();
    let plan = plan
        .transform_up(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            if relation_name(&scan.table_name.to_string()) != relation {
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
    sql: &str,
) -> crate::Result<Option<String>> {
    if matches!(plan, LogicalPlan::Explain(_)) {
        return Ok(None);
    }
    let mut scans = Vec::new();
    collect_scans(plan, &mut scans);
    let stream_scans = scans
        .iter()
        .filter(|name| rtsp_table(snapshot, name).is_some())
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
    validate_stream_node(plan, sql)?;
    Ok(Some(stream_name))
}

fn rtsp_table<'a>(
    snapshot: &'a crate::catalog::DefinitionSnapshot,
    name: &str,
) -> Option<&'a crate::catalog::RtspTableConfig> {
    let table = snapshot.table(name)?;
    match &table.definition.provider {
        crate::catalog::TableProvider::Rtsp(definition) => Some(definition),
        _ => None,
    }
}

fn collect_scans(plan: &LogicalPlan, scans: &mut Vec<String>) {
    if let LogicalPlan::TableScan(scan) = plan {
        scans.push(relation_name(&scan.table_name.to_string()));
    }
    for input in plan.inputs() {
        collect_scans(input, scans);
    }
}

fn validate_stream_node(plan: &LogicalPlan, sql: &str) -> crate::Result<()> {
    if let Some((node, message)) = unsupported_stream_node(plan) {
        let location = unsupported_keyword(node)
            .and_then(|keyword| locate_sql_fragment(sql, keyword))
            .map(|(start, end, fragment)| {
                format!(
                    "; first unsupported node: {node} at SQL bytes {start}..{end}: `{fragment}`"
                )
            })
            .unwrap_or_else(|| format!("; first unsupported node: {node}"));
        return Err(crate::VqlError::new(
            crate::ErrorCode::InvalidSql,
            format!("{message}{location}"),
        ));
    }
    for input in plan.inputs() {
        validate_stream_node(input, sql)?;
    }
    Ok(())
}

fn unsupported_keyword(node: &str) -> Option<&'static str> {
    match node {
        "Sort" => Some("ORDER BY"),
        "Distinct" => Some("DISTINCT"),
        "Join" => Some("JOIN"),
        "Union" => Some("UNION"),
        "Window" => Some("OVER"),
        "Analyze" => Some("ANALYZE"),
        _ => None,
    }
}

fn locate_sql_fragment<'a>(sql: &'a str, keyword: &str) -> Option<(usize, usize, &'a str)> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let tokens = tokens
        .iter()
        .filter(|token| {
            !matches!(
                token.token,
                Token::Whitespace(
                    Whitespace::Space
                        | Whitespace::Newline
                        | Whitespace::Tab
                        | Whitespace::SingleLineComment { .. }
                        | Whitespace::MultiLineComment(_)
                ) | Token::EOF
            )
        })
        .collect::<Vec<_>>();
    let keyword = keyword.split_whitespace().collect::<Vec<_>>();
    let keyword_start = tokens
        .windows(keyword.len())
        .find(|window| {
            window.iter().zip(&keyword).all(|(token, expected)| {
                matches!(&token.token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
            })
        })
        .and_then(|window| location_to_byte(sql, window[0].span.start))?;
    let tail = &sql[keyword_start..];
    let relative_end = tail
        .char_indices()
        .find_map(|(index, character)| {
            (index > keyword.len() && matches!(character, '\n' | ';')).then_some(index)
        })
        .unwrap_or(tail.len());
    let mut end = keyword_start.saturating_add(relative_end);
    if end.saturating_sub(keyword_start) > 160 {
        end = keyword_start + 160;
        while !sql.is_char_boundary(end) {
            end -= 1;
        }
    }
    let fragment = sql[keyword_start..end].trim_end();
    let end = keyword_start + fragment.len();
    Some((keyword_start, end, fragment))
}

fn location_to_byte(sql: &str, location: Location) -> Option<usize> {
    let mut line = 1_u64;
    let mut column = 1_u64;
    for (offset, character) in sql.char_indices() {
        if line == location.line && column == location.column {
            return Some(offset);
        }
        if character == '\n' {
            line = line.saturating_add(1);
            column = 1;
        } else {
            column = column.saturating_add(1);
        }
    }
    (line == location.line && column == location.column).then_some(sql.len())
}

fn first_unsupported_stream_node(plan: &LogicalPlan) -> Option<(&'static str, &'static str)> {
    if let Some((node, message)) = unsupported_stream_node(plan) {
        return Some((node, message));
    }
    plan.inputs()
        .into_iter()
        .find_map(first_unsupported_stream_node)
}

fn unsupported_stream_node(plan: &LogicalPlan) -> Option<(&'static str, &'static str)> {
    match plan {
        LogicalPlan::Sort(_) => Some((
            "Sort",
            "ORDER BY over an unbounded RTSP stream is not supported; move ordering to a bounded result",
        )),
        LogicalPlan::Distinct(_) => Some((
            "Distinct",
            "DISTINCT over an unbounded RTSP stream is not supported; use a bounded window or batch query",
        )),
        LogicalPlan::Join(_) => Some((
            "Join",
            "JOIN is not supported for the single-source RTSP runtime; split sources into independent queries",
        )),
        LogicalPlan::Union(_) => Some((
            "Union",
            "UNION is not supported for the single-source RTSP runtime; split sources into independent queries",
        )),
        LogicalPlan::Window(_) => Some((
            "Window",
            "analytic OVER windows are not supported on an unbounded RTSP stream; use TUMBLE when streaming window state is enabled",
        )),
        LogicalPlan::Analyze(_) => Some((
            "Analyze",
            "EXPLAIN ANALYZE is not supported for an unbounded RTSP query; use EXPLAIN without ANALYZE",
        )),
        _ => None,
    }
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
