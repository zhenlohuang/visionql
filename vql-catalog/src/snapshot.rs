use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;

use crate::{FunctionDef, ModelDef, TableDef};

#[derive(Debug, Clone)]
pub struct SnapshotTable {
    pub revision: i64,
    pub definition: TableDef,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone, Default)]
pub struct DefinitionSnapshot {
    tables: BTreeMap<String, SnapshotTable>,
    models: BTreeMap<String, SnapshotObject<ModelDef>>,
    functions: BTreeMap<String, SnapshotObject<FunctionDef>>,
}

#[derive(Debug, Clone)]
pub struct SnapshotObject<T> {
    pub revision: i64,
    pub definition: T,
}

impl DefinitionSnapshot {
    pub(crate) fn new(
        tables: BTreeMap<String, SnapshotTable>,
        models: BTreeMap<String, SnapshotObject<ModelDef>>,
        functions: BTreeMap<String, SnapshotObject<FunctionDef>>,
    ) -> Self {
        Self {
            tables,
            models,
            functions,
        }
    }

    pub fn tables(&self) -> impl Iterator<Item = (&str, &SnapshotTable)> {
        self.tables
            .iter()
            .map(|(name, table)| (name.as_str(), table))
    }

    pub fn table(&self, name: &str) -> Option<&SnapshotTable> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn models(&self) -> impl Iterator<Item = (&str, &SnapshotObject<ModelDef>)> {
        self.models
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn model(&self, name: &str) -> Option<&SnapshotObject<ModelDef>> {
        self.models.get(&name.to_ascii_lowercase())
    }

    pub fn functions(&self) -> impl Iterator<Item = (&str, &SnapshotObject<FunctionDef>)> {
        self.functions
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn function(&self, name: &str) -> Option<&SnapshotObject<FunctionDef>> {
        self.functions.get(&name.to_ascii_lowercase())
    }
}
