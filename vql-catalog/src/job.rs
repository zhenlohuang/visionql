use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The immutable part of a persistent Job submitted through `vqld`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobDefinition {
    pub job_id: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub name: String,
    pub principal: String,
    pub normalized_sql: String,
    pub sql_redacted: String,
    #[serde(default)]
    pub session_settings: BTreeMap<String, String>,
    pub definition_generations: Vec<i64>,
    pub created_at: i64,
}

/// Input used to atomically create a persistent Job and its initial status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateJob {
    pub catalog_name: String,
    pub schema_name: String,
    pub name: String,
    pub principal: String,
    pub normalized_sql: String,
    pub sql_redacted: String,
    pub session_settings: BTreeMap<String, String>,
    pub definition_generations: Vec<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobState {
    Starting,
    Running,
    Stopped,
    Failed,
}

impl JobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

impl TryFrom<&str> for JobState {
    type Error = crate::CatalogError;

    fn try_from(value: &str) -> crate::Result<Self> {
        match value {
            "STARTING" => Ok(Self::Starting),
            "RUNNING" => Ok(Self::Running),
            "STOPPED" => Ok(Self::Stopped),
            "FAILED" => Ok(Self::Failed),
            _ => Err(crate::CatalogError::new(
                crate::CatalogErrorCode::Storage,
                "catalog contains an invalid Job state",
            )),
        }
    }
}

/// Mutable operational state updated with compare-and-swap semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatus {
    pub state: JobState,
    pub status_version: i64,
    pub stop_requested: bool,
    pub source_health: Option<String>,
    pub last_event_time: Option<i64>,
    pub started_at: Option<i64>,
    pub updated_at: i64,
    pub last_restart_at: Option<i64>,
    pub restart_gap_count: i64,
    pub restart_gap_started_at: Option<i64>,
    pub restart_gap_ended_at: Option<i64>,
    pub last_restart_reset_window_state: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl JobStatus {
    pub fn starting(now: i64) -> Self {
        Self {
            state: JobState::Starting,
            status_version: 1,
            stop_requested: false,
            source_health: None,
            last_event_time: None,
            started_at: None,
            updated_at: now,
            last_restart_at: None,
            restart_gap_count: 0,
            restart_gap_started_at: None,
            restart_gap_ended_at: None,
            last_restart_reset_window_state: false,
            error_code: None,
            error_message: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentJob {
    pub definition: JobDefinition,
    pub status: JobStatus,
}
