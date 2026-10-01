use std::error::Error as StdError;

use arrow_flight::error::FlightError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProblemSource {
    Vql,
    Connectivity,
    Backend,
    Policy,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiProblem {
    pub source: ProblemSource,
    pub title: Box<str>,
    pub message: Box<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<Box<str>>,
    #[serde(skip)]
    pub status: StatusCode,
}

impl ApiProblem {
    pub fn backend(title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Backend,
            title: title.into().into_boxed_str(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn connectivity(title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Connectivity,
            title: title.into().into_boxed_str(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::BAD_GATEWAY,
        }
    }

    pub fn policy(message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Policy,
            title: "Execution mode required".into(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Policy,
            title: "Workbench request rejected".into(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::FORBIDDEN,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Backend,
            title: "Invalid Workbench request".into(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::BAD_REQUEST,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Policy,
            title: "Execution already active".into(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::CONFLICT,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            source: ProblemSource::Backend,
            title: "Workbench resource not found".into(),
            message: message.into().into_boxed_str(),
            code: None,
            symbol: None,
            target_version: None,
            status: StatusCode::NOT_FOUND,
        }
    }

    pub fn from_flight(error: &FlightError) -> Self {
        if let Some(value) = structured_error(error) {
            return Self {
                source: ProblemSource::Vql,
                title: "VisionQL statement failed".into(),
                message: value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("vqld returned an invalid structured error")
                    .into(),
                code: value
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .map(Into::into),
                symbol: value
                    .get("symbol")
                    .and_then(serde_json::Value::as_str)
                    .map(Into::into),
                target_version: value
                    .get("target_version")
                    .and_then(serde_json::Value::as_str)
                    .map(Into::into),
                status: StatusCode::UNPROCESSABLE_ENTITY,
            };
        }
        Self::connectivity("vqld request failed", error.to_string())
    }

    pub fn cancelled() -> Self {
        Self {
            source: ProblemSource::Vql,
            title: "Execution cancelled".into(),
            message: "The active execution was cancelled.".into(),
            code: Some("VQL-57001".into()),
            symbol: Some("QUERY_CANCELLED".into()),
            target_version: None,
            status: StatusCode::CONFLICT,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.code.as_deref() == Some("VQL-57001")
    }
}

impl IntoResponse for ApiProblem {
    fn into_response(self) -> Response {
        (self.status, Json(self)).into_response()
    }
}

fn structured_error(error: &(dyn StdError + 'static)) -> Option<serde_json::Value> {
    let mut current = Some(error);
    while let Some(error) = current {
        let status = error
            .downcast_ref::<FlightError>()
            .and_then(|error| match error {
                FlightError::Tonic(status) => Some(status.as_ref()),
                _ => None,
            })
            .or_else(|| error.downcast_ref::<tonic::Status>());
        if let Some(status) = status {
            let value = serde_json::from_slice::<serde_json::Value>(status.details()).ok()?;
            if value.get("version").and_then(serde_json::Value::as_u64) == Some(1) {
                return Some(value);
            }
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_vql_details_survive_the_bridge() {
        let details = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "code": "VQL-42001",
            "symbol": "INVALID_SQL",
            "message": "expected a statement",
            "target_version": null,
        }))
        .unwrap();
        let status = tonic::Status::with_details(
            tonic::Code::InvalidArgument,
            "remote failure",
            details.into(),
        );
        let error = FlightError::ExternalError(Box::new(FlightError::from(status)));

        let problem = ApiProblem::from_flight(&error);

        assert_eq!(problem.code.as_deref(), Some("VQL-42001"));
        assert_eq!(problem.symbol.as_deref(), Some("INVALID_SQL"));
        assert_eq!(problem.message.as_ref(), "expected a statement");
    }
}
