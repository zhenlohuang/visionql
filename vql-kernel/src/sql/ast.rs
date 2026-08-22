use std::collections::BTreeMap;

use crate::catalog::{ModelType, TableProvider};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateTable {
    pub(crate) name: String,
    pub(crate) provider: TableProvider,
    pub(crate) columns: Vec<TableColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateModel {
    pub(crate) name: String,
    pub(crate) model_type: ModelType,
    pub(crate) source: String,
    pub(crate) runtime_kind: String,
    pub(crate) options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShowKind {
    Tables,
    Models,
    Functions,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VqlStatement {
    CreateTable(CreateTable),
    CreateModel(CreateModel),
    ResolveModel { name: String },
    CreateFunction { sql: String },
    Drop { kind: ShowKind, name: String },
    Show(ShowKind),
    ShowCreate { kind: ShowKind, name: String },
    Describe { name: String },
    Query { sql: String },
    Explain { sql: String },
    Set { sql: String },
}
