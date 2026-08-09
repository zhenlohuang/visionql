use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;

use crate::PythonUdfHostRef;
use crate::catalog::CatalogStore;
use crate::catalog::{DefinitionSnapshot, TableProviderKind};
use crate::connectors::images::ImagesTableProvider;
use crate::connectors::videos::VideosTableProvider;
use crate::functions::{
    box_center_udf, count_objects_udf, polygon_udf, python_function_udf, st_contains_udf,
    to_jpeg_udf, tumble_udf,
};
use crate::media::MediaRuntime;
use crate::models::model_function_udf;
use crate::planner::inference::VqlQueryPlanner;
use crate::{ErrorCode, Result, VqlError};
use std::sync::atomic::AtomicBool;

pub(crate) fn context_for_snapshot(
    snapshot: &DefinitionSnapshot,
    catalog: Arc<CatalogStore>,
    media: Arc<MediaRuntime>,
    fail_on_error: Arc<AtomicBool>,
    python_udf_host: Option<PythonUdfHostRef>,
) -> Result<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(VqlQueryPlanner))
        .build();
    let context = SessionContext::new_with_state(state);
    context.register_udf(to_jpeg_udf(
        Arc::clone(&catalog),
        Arc::clone(&media),
        Arc::clone(&fail_on_error),
    ));
    context.register_udf(count_objects_udf());
    context.register_udf(box_center_udf());
    context.register_udf(polygon_udf("polygon"));
    context.register_udf(polygon_udf("st_polygon"));
    context.register_udf(st_contains_udf());
    context.register_udf(tumble_udf());
    for (_, function) in snapshot.functions() {
        match &function.definition.implementation {
            crate::catalog::FunctionImplementation::Model { model } => {
                let model = snapshot.model(model).ok_or_else(|| {
                    VqlError::new(
                        ErrorCode::Catalog,
                        format!(
                            "function '{}' references missing model '{model}'",
                            function.definition.name
                        ),
                    )
                })?;
                context.register_udf(model_function_udf(
                    function.definition.clone(),
                    model.definition.clone(),
                ));
            }
            crate::catalog::FunctionImplementation::Python { .. } => {
                context.register_udf(python_function_udf(
                    function.definition.clone(),
                    python_udf_host.clone(),
                    Arc::clone(&fail_on_error),
                    Arc::clone(&catalog),
                    Arc::clone(&media),
                )?);
            }
            crate::catalog::FunctionImplementation::SqlMacro { .. } => {}
        }
    }
    for (name, table) in snapshot.tables() {
        match table.definition.provider {
            TableProviderKind::Images => {
                let provider = ImagesTableProvider::try_new(
                    &table.definition.location,
                    table.revision,
                    table.definition.recursive,
                )?;
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
                )?;
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
    Ok(context)
}
