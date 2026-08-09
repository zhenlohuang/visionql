mod inference;
mod normalize;
mod session_state;
mod sink;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use tokio_util::sync::CancellationToken;

use crate::models::ModelRuntime;

pub(crate) use session_state::context_for_snapshot;
pub(crate) use sink::wrap_console_sink;

pub(crate) async fn plan_statement(
    context: &SessionContext,
    snapshot: &crate::catalog::DefinitionSnapshot,
    sql: &str,
    models: Arc<ModelRuntime>,
    fail_on_error: Arc<AtomicBool>,
    cancellation: CancellationToken,
) -> crate::Result<DataFrame> {
    let sql = normalize::normalize_query(sql, snapshot)?;
    let dataframe = context.sql(&sql).await?;
    let (state, plan) = dataframe.into_parts();
    let plan = inference::extract_inference(plan, snapshot, models, fail_on_error, cancellation)?;
    Ok(DataFrame::new(state, plan))
}
