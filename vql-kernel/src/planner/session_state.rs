use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;

use crate::PythonUdfHostRef;
use crate::catalog::CatalogStore;
use crate::catalog::{DefinitionSnapshot, TableProviderKind};
use crate::connectors::images::ImagesTableProvider;
use crate::connectors::rtsp::RtspTableProvider;
use crate::connectors::videos::VideosTableProvider;
use crate::functions::{
    VqlFunctionFactory, VqlTypePlanner, box_center_udf, polygon_udf, python_function_udf,
    st_contains_udf, tumble_udf,
};
use crate::media::MediaRuntime;
use crate::models::image_detection;
use crate::planner::inference::VqlQueryPlanner;
use crate::resources::QueryBudget;
use crate::session::QueryMetrics;
use crate::{ErrorCode, Result, VqlError};
use std::sync::atomic::AtomicBool;

pub(crate) fn context_for_snapshot(
    snapshot: &DefinitionSnapshot,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
    budget: &QueryBudget,
    metrics: Arc<QueryMetrics>,
) -> Result<SessionContext> {
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(budget.memory_pool())
            .build()?,
    );
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(VqlQueryPlanner))
        .with_runtime_env(runtime_env as Arc<RuntimeEnv>)
        .build();
    let context = SessionContext::new_with_state(state);
    register_functions(
        &context,
        snapshot,
        Arc::clone(&catalog),
        Arc::clone(&media),
        Arc::clone(&fail_on_error),
        python_udf_host,
        Some(budget.clone()),
    )?;
    register_tables(&context, snapshot, media, metrics)?;
    register_streams(&context, snapshot)?;
    Ok(context)
}

fn register_streams(context: &SessionContext, snapshot: &DefinitionSnapshot) -> Result<()> {
    for (name, _stream) in snapshot.streams() {
        context.register_table(name, Arc::new(RtspTableProvider::new()))?;
    }
    Ok(())
}

pub(crate) fn context_for_function_ddl(
    snapshot: &DefinitionSnapshot,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
    factory: Arc<VqlFunctionFactory>,
) -> Result<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(VqlQueryPlanner))
        .with_type_planner(Arc::new(VqlTypePlanner))
        .with_function_factory(Some(factory))
        .build();
    let context = SessionContext::new_with_state(state);
    register_functions(
        &context,
        snapshot,
        catalog,
        media,
        fail_on_error,
        python_udf_host,
        None,
    )?;
    Ok(context)
}

fn register_functions(
    context: &SessionContext,
    snapshot: &DefinitionSnapshot,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
    budget: Option<QueryBudget>,
) -> Result<()> {
    context.register_udf(box_center_udf());
    context.register_udf(polygon_udf("polygon"));
    context.register_udf(polygon_udf("st_polygon"));
    context.register_udf(st_contains_udf());
    context.register_udf(tumble_udf());
    context.register_udf(image_detection());
    for (_, function) in snapshot.functions() {
        match &function.definition.implementation {
            crate::catalog::FunctionImplementation::Python { .. } => {
                context.register_udf(python_function_udf(
                    function.definition.clone(),
                    python_udf_host.clone(),
                    Arc::clone(&fail_on_error),
                    Arc::clone(&catalog),
                    Arc::clone(&media),
                    budget.clone(),
                )?);
            }
            crate::catalog::FunctionImplementation::SqlMacro { .. } => {}
        }
    }
    Ok(())
}

fn register_tables(
    context: &SessionContext,
    snapshot: &DefinitionSnapshot,
    media: Arc<MediaRuntime>,
    metrics: Arc<QueryMetrics>,
) -> Result<()> {
    for (name, table) in snapshot.tables() {
        match table.definition.provider {
            TableProviderKind::Images => {
                let provider = ImagesTableProvider::try_new(
                    &table.definition.location,
                    table.revision,
                    table.definition.recursive,
                )?
                .with_query_metrics(Arc::clone(&metrics));
                if provider.schema().as_ref() != table.schema.as_ref() {
                    return Err(VqlError::new(
                        ErrorCode::Catalog,
                        format!("catalog schema for table '{name}' does not match its provider"),
                    ));
                }
                context.register_table(name, Arc::new(provider))?;
            }
            TableProviderKind::Videos => {
                let provider = VideosTableProvider::try_new(
                    &table.definition.location,
                    table.revision,
                    table.definition.recursive,
                    table.definition.fps,
                    table.definition.start_time_ms,
                    Arc::clone(&media),
                )?
                .with_query_metrics(Arc::clone(&metrics));
                if provider.schema().as_ref() != table.schema.as_ref() {
                    return Err(VqlError::new(
                        ErrorCode::Catalog,
                        format!("catalog schema for table '{name}' does not match its provider"),
                    ));
                }
                context.register_table(name, Arc::new(provider))?;
            }
        }
    }
    Ok(())
}
