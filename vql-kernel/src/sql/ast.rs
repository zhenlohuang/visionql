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
    pub(crate) if_not_exists: bool,
    pub(crate) name: String,
    pub(crate) interface: ModelInterfaceSpec,
    pub(crate) version: String,
    pub(crate) source: String,
    pub(crate) runtime_kind: Option<String>,
    pub(crate) options: BTreeMap<String, serde_json::Value>,
    pub(crate) comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ModelInterfaceSpec {
    Capability(ModelType),
    Signature {
        parameters: Vec<(String, String)>,
        return_type: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlterModel {
    AddVersion {
        if_not_exists: bool,
        version: String,
        source: String,
        runtime_kind: Option<String>,
        options: BTreeMap<String, serde_json::Value>,
    },
    DropVersion {
        version: String,
    },
    SetDefaultVersion {
        version: String,
    },
    SetComment {
        comment: String,
    },
    RenameTo {
        name: String,
    },
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
    ResolveModel {
        name: String,
        version: Option<String>,
    },
    AlterModel {
        name: String,
        action: AlterModel,
    },
    CreateFunction {
        sql: String,
    },
    Drop {
        kind: ShowKind,
        name: String,
    },
    Show(ShowKind),
    ShowModelVersions {
        name: String,
    },
    ShowCreate {
        kind: ShowKind,
        name: String,
        version: Option<String>,
    },
    Describe {
        kind: ShowKind,
        name: String,
    },
    SubmitQuery {
        name: String,
        sql: String,
    },
    ShowQueries,
    DescribeQuery {
        query_id: String,
    },
    StopQuery {
        query_id: String,
    },
    Query {
        sql: String,
    },
    Explain {
        sql: String,
    },
    Set {
        sql: String,
    },
}
