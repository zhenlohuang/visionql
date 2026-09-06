use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;

use crate::{FunctionDef, ModelDef, TableDef};

#[derive(Debug, Clone)]
pub struct SnapshotTable {
    /// Stable identity for this Catalog object.
    pub object_id: String,
    /// Opaque storage generation used to locate this exact historical definition.
    pub generation: i64,
    /// Monotonic revision within this Table's lifetime.
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
    /// Stable identity for this Catalog object.
    pub object_id: String,
    /// Opaque storage generation used for compare-and-swap updates.
    pub generation: i64,
    /// Monotonic revision within this object's lifetime.
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

    /// Return the opaque generations that fully identify this snapshot.
    pub fn generations(&self) -> Vec<i64> {
        let mut generations = self
            .tables
            .values()
            .map(|value| value.generation)
            .chain(self.models.values().map(|value| value.generation))
            .chain(self.functions.values().map(|value| value.generation))
            .collect::<Vec<_>>();
        generations.sort_unstable();
        generations
    }
}
