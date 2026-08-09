use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;

use super::objects::{FunctionDef, ModelDef, SinkDef, TableDef};

#[derive(Debug, Clone)]
pub(crate) struct SnapshotTable {
    pub(crate) revision: i64,
    pub(crate) definition: TableDef,
    pub(crate) schema: SchemaRef,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DefinitionSnapshot {
    tables: BTreeMap<String, SnapshotTable>,
    models: BTreeMap<String, SnapshotObject<ModelDef>>,
    functions: BTreeMap<String, SnapshotObject<FunctionDef>>,
    sinks: BTreeMap<String, SnapshotObject<SinkDef>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotObject<T> {
    pub(crate) revision: i64,
    pub(crate) definition: T,
}

impl DefinitionSnapshot {
    pub(crate) fn new(
        tables: BTreeMap<String, SnapshotTable>,
        models: BTreeMap<String, SnapshotObject<ModelDef>>,
        functions: BTreeMap<String, SnapshotObject<FunctionDef>>,
        sinks: BTreeMap<String, SnapshotObject<SinkDef>>,
    ) -> Self {
        Self {
            tables,
            models,
            functions,
            sinks,
        }
    }

    pub(crate) fn tables(&self) -> impl Iterator<Item = (&str, &SnapshotTable)> {
        self.tables
            .iter()
            .map(|(name, table)| (name.as_str(), table))
    }

    pub(crate) fn table(&self, name: &str) -> Option<&SnapshotTable> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub(crate) fn models(&self) -> impl Iterator<Item = (&str, &SnapshotObject<ModelDef>)> {
        self.models
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub(crate) fn model(&self, name: &str) -> Option<&SnapshotObject<ModelDef>> {
        self.models.get(&name.to_ascii_lowercase())
    }

    pub(crate) fn functions(&self) -> impl Iterator<Item = (&str, &SnapshotObject<FunctionDef>)> {
        self.functions
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub(crate) fn function(&self, name: &str) -> Option<&SnapshotObject<FunctionDef>> {
        self.functions.get(&name.to_ascii_lowercase())
    }

    pub(crate) fn sinks(&self) -> impl Iterator<Item = (&str, &SnapshotObject<SinkDef>)> {
        self.sinks
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub(crate) fn sink(&self, name: &str) -> Option<&SnapshotObject<SinkDef>> {
        self.sinks.get(&name.to_ascii_lowercase())
    }
}
