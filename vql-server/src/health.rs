use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::config::ServiceConfig;
use crate::controller::QueryController;
use crate::flight::VqlFlightSqlService;

#[derive(Debug, Clone)]
pub struct HealthState {
    pub ready: Arc<AtomicBool>,
    pub flight: VqlFlightSqlService,
    pub controller: Arc<QueryController>,
    pub config: Arc<ServiceConfig>,
}

pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, "{\"status\":\"live\"}\n")
}

async fn ready(State(state): State<HealthState>) -> Response {
    if state.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "{\"status\":\"ready\"}\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"status\":\"not_ready\"}\n",
        )
            .into_response()
    }
}

async fn metrics(State(state): State<HealthState>, headers: HeaderMap) -> Response {
    if let Some(expected) = state.config.service_token.as_deref() {
        let authenticated = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            == Some(expected);
        if !authenticated {
            return (StatusCode::UNAUTHORIZED, "authentication is required\n").into_response();
        }
    }
    let body = format!(
        concat!(
            "# TYPE vqld_sessions gauge\n",
            "vqld_sessions {}\n",
            "# TYPE vqld_executions_pending gauge\n",
            "vqld_executions_pending {}\n",
            "# TYPE vqld_executions_attached gauge\n",
            "vqld_executions_attached {}\n",
            "# TYPE vqld_persistent_queries_active gauge\n",
            "vqld_persistent_queries_active {}\n",
            "# TYPE vqld_executions_total counter\n",
            "vqld_executions_total {}\n",
            "# TYPE vqld_executions_failed_total counter\n",
            "vqld_executions_failed_total {}\n",
            "# TYPE vqld_inference_executions_total counter\n",
            "vqld_inference_executions_total {}\n",
            "# TYPE vqld_source_executions_total counter\n",
            "vqld_source_executions_total {}\n",
            "# TYPE vqld_sink_executions_total counter\n",
            "vqld_sink_executions_total {}\n",
            "# TYPE vqld_process_uptime_seconds gauge\n",
            "vqld_process_uptime_seconds {}\n"
        ),
        state.flight.session_count(),
        state.flight.pending_execution_count(),
        state.flight.attached_execution_count(),
        state.controller.active_count(),
        state.flight.total_executions(),
        state.flight.failed_executions(),
        state.flight.inference_executions(),
        state.flight.source_executions(),
        state.flight.sink_executions(),
        state.flight.process_uptime_seconds(),
    );
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #[test]
    fn metrics_do_not_define_per_query_labels() {
        let source = include_str!("health.rs");
        assert!(!source.contains("query_id=\""));
        assert!(!source.contains("query_name=\""));
        for family in [
            "vqld_sessions",
            "vqld_persistent_queries_active",
            "vqld_inference_executions_total",
            "vqld_source_executions_total",
            "vqld_sink_executions_total",
            "vqld_process_uptime_seconds",
        ] {
            assert!(source.contains(family), "missing metric family {family}");
        }
    }
}
