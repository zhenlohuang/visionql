use std::collections::BTreeMap;

use crate::catalog::{EventTimePolicy, ModelType, RtspTransport, SinkKind, TableProviderKind};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateTable {
    pub(crate) name: String,
    pub(crate) provider: TableProviderKind,
    pub(crate) location: String,
    pub(crate) recursive: bool,
    pub(crate) fps: Option<f64>,
    pub(crate) start_time_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateStream {
    pub(crate) name: String,
    pub(crate) endpoint: String,
    pub(crate) fps: f64,
    pub(crate) event_time: EventTimePolicy,
    pub(crate) watermark_delay_ms: i64,
    pub(crate) transport: RtspTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateModel {
    pub(crate) name: String,
    pub(crate) model_type: ModelType,
    pub(crate) source: String,
    pub(crate) options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShowKind {
    Tables,
    Streams,
    Models,
    Functions,
    Sinks,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VqlStatement {
    CreateTable(CreateTable),
    CreateStream(CreateStream),
    CreateModel(CreateModel),
    CreateFunction { sql: String },
    CreateSink { name: String, kind: SinkKind },
    Drop { kind: ShowKind, name: String },
    Show(ShowKind),
    Describe { name: String },
    Query { sql: String },
    Explain { sql: String },
    Set { sql: String },
}
