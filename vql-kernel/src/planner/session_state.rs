use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;

use crate::PythonUdfHostRef;
use crate::catalog::CatalogStore;
use crate::catalog::{DefinitionSnapshot, TableProvider as CatalogTableProvider};
use crate::connectors::images::ImagesTableProvider;
use crate::connectors::rtsp::RtspTableProvider;
use crate::connectors::videos::VideosTableProvider;
use crate::functions::{VqlFunctionFactory, VqlTypePlanner, builtin_udfs, python_function_udf};
use crate::media::MediaRuntime;
use crate::models::model_marker;
use crate::planner::inference::VqlQueryPlanner;
use crate::resources::QueryBudget;
use crate::{ErrorCode, Result, VqlError};
use std::sync::atomic::AtomicBool;

pub(crate) fn context_for_snapshot(
    snapshot: &DefinitionSnapshot,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
    budget: &QueryBudget,
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
    register_tables(&context, snapshot, media)?;
    Ok(context)
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
    for function in builtin_udfs()? {
        context.register_udf(function);
    }
    for (_, model) in snapshot.models() {
        context.register_udf(model_marker(&model.definition)?);
    }
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
) -> Result<()> {
    for (name, table) in snapshot.tables() {
        match &table.definition.provider {
            CatalogTableProvider::Images {
                location,
                recursive,
            } => {
                let provider =
                    ImagesTableProvider::try_new(location, table.generation, *recursive)?;
                if provider.schema().as_ref() != table.schema.as_ref() {
                    return Err(VqlError::new(
                        ErrorCode::Catalog,
                        format!("catalog schema for table '{name}' does not match its provider"),
                    ));
                }
                context.register_table(name, Arc::new(provider))?;
            }
            CatalogTableProvider::Videos {
                location,
                recursive,
                fps,
                start_time_ms,
            } => {
                let provider = VideosTableProvider::try_new(
                    location,
                    table.generation,
                    *recursive,
                    *fps,
                    *start_time_ms,
                    Arc::clone(&media),
                )?;
                if provider.schema().as_ref() != table.schema.as_ref() {
                    return Err(VqlError::new(
                        ErrorCode::Catalog,
                        format!("catalog schema for table '{name}' does not match its provider"),
                    ));
                }
                context.register_table(name, Arc::new(provider))?;
            }
            CatalogTableProvider::Rtsp(_) => {
                context.register_table(name, Arc::new(RtspTableProvider::new()))?;
            }
            CatalogTableProvider::Kafka(_) | CatalogTableProvider::External { .. } => {}
        }
    }
    Ok(())
}
