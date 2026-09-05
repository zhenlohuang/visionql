//! Unity Catalog REST API v0.6.0 wire models and service.
//!
//! The API is pinned to the released v0.6.0 OpenAPI contract. VisionQL keeps
//! its own catalog domain and translates at this boundary.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::{
    CatalogError, CatalogErrorCode, CatalogInfo, CatalogStore, EventTimePolicy, KafkaTableConfig,
    Result, RtspTableConfig, RtspTransport, SchemaInfo, SecurableMetadata, SnapshotTable, TableDef,
    TableProvider, provider_schema,
};

pub const OPENAPI_VERSION: &str = "0.6.0";
pub const API_PREFIX: &str = "/api/2.1/unity-catalog";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorResponse {
    pub error_code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateCatalog {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateCatalog {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogInfoResponse {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub properties: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListCatalogsResponse {
    pub catalogs: Vec<CatalogInfoResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateSchema {
    pub name: String,
    pub catalog_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaInfoResponse {
    pub name: String,
    pub catalog_name: String,
    pub full_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub properties: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub schema_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListSchemasResponse {
    pub schemas: Vec<SchemaInfoResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableType {
    Managed,
    External,
    View,
    StreamingTable,
    MaterializedView,
    MetricView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataSourceFormat {
    Delta,
    Csv,
    Json,
    Avro,
    Parquet,
    Orc,
    Text,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_text: String,
    pub type_json: String,
    pub type_name: String,
    pub position: i32,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

const fn default_nullable() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: TableType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_source_format: Option<DataSourceFormat>,
    pub columns: Vec<ColumnInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableInfoResponse {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: TableType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source_format: Option<DataSourceFormat>,
    pub columns: Vec<ColumnInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub properties: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub table_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTablesResponse {
    pub tables: Vec<TableInfoResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UnityCatalogService {
    store: Arc<CatalogStore>,
}

impl UnityCatalogService {
    pub fn new(store: Arc<CatalogStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<CatalogStore> {
        &self.store
    }

    pub fn create_catalog(&self, request: CreateCatalog) -> Result<CatalogInfoResponse> {
        let mut properties = request.properties;
        if let Some(storage_root) = request.storage_root {
            properties.insert("storage_root".to_owned(), storage_root);
        }
        self.store
            .create_catalog(
                &request.name,
                SecurableMetadata {
                    comment: request.comment,
                    properties,
                    owner: None,
                },
            )
            .map(catalog_response)
    }

    pub fn get_catalog(&self, name: &str) -> Result<CatalogInfoResponse> {
        self.store.get_catalog(name).map(catalog_response)
    }

    pub fn list_catalogs(&self) -> Result<Vec<CatalogInfoResponse>> {
        self.store
            .list_catalogs()
            .map(|values| values.into_iter().map(catalog_response).collect())
    }

    pub fn update_catalog(
        &self,
        name: &str,
        request: UpdateCatalog,
    ) -> Result<CatalogInfoResponse> {
        let current = self.store.get_catalog(name)?;
        self.store
            .update_catalog(
                name,
                request.new_name.as_deref(),
                SecurableMetadata {
                    comment: request.comment.or(current.metadata.comment),
                    properties: request.properties.unwrap_or(current.metadata.properties),
                    owner: current.metadata.owner,
                },
            )
            .map(catalog_response)
    }

    pub fn delete_catalog(&self, name: &str, force: bool) -> Result<()> {
        self.store.delete_catalog(name, force)
    }

    pub fn create_schema(&self, request: CreateSchema) -> Result<SchemaInfoResponse> {
        let mut properties = request.properties;
        if let Some(storage_root) = request.storage_root {
            properties.insert("storage_root".to_owned(), storage_root);
        }
        self.store
            .create_schema(
                &request.catalog_name,
                &request.name,
                SecurableMetadata {
                    comment: request.comment,
                    properties,
                    owner: None,
                },
            )
            .map(schema_response)
    }

    pub fn get_schema(&self, full_name: &str) -> Result<SchemaInfoResponse> {
        let (catalog_name, schema_name) = split_schema_name(full_name)?;
        self.store
            .get_schema(catalog_name, schema_name)
            .map(schema_response)
    }

    pub fn list_schemas(&self, catalog_name: &str) -> Result<Vec<SchemaInfoResponse>> {
        self.store
            .list_schemas(catalog_name)
            .map(|values| values.into_iter().map(schema_response).collect())
    }

    pub fn update_schema(
        &self,
        full_name: &str,
        request: UpdateSchema,
    ) -> Result<SchemaInfoResponse> {
        let (catalog_name, schema_name) = split_schema_name(full_name)?;
        let current = self.store.get_schema(catalog_name, schema_name)?;
        self.store
            .update_schema(
                catalog_name,
                schema_name,
                request.new_name.as_deref(),
                SecurableMetadata {
                    comment: request.comment.or(current.metadata.comment),
                    properties: request.properties.unwrap_or(current.metadata.properties),
                    owner: current.metadata.owner,
                },
            )
            .map(schema_response)
    }

    pub fn delete_schema(&self, full_name: &str, force: bool) -> Result<()> {
        let (catalog_name, schema_name) = split_schema_name(full_name)?;
        self.store.delete_schema(catalog_name, schema_name, force)
    }

    pub fn create_table(&self, request: CreateTable) -> Result<TableInfoResponse> {
        if request.table_type != TableType::External {
            return Err(CatalogError::new(
                CatalogErrorCode::InvalidArgument,
                "VisionQL supports UC table_type EXTERNAL",
            ));
        }
        let definition = table_from_request(&request)?;
        let schema = match provider_schema(&definition.provider) {
            Some(schema) => {
                validate_provider_columns(&request.columns, &schema)?;
                schema
            }
            None => schema_from_columns(&request.columns)?,
        };
        self.store.create_table_in(
            &request.catalog_name,
            &request.schema_name,
            &definition,
            &schema,
        )?;
        let snapshot = self
            .store
            .snapshot_in(&request.catalog_name, &request.schema_name)?;
        let table = snapshot.table(&definition.name).ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::Internal,
                format!(
                    "created table '{}' is missing from the catalog",
                    definition.name
                ),
            )
        })?;
        Ok(table_response(
            &request.catalog_name,
            &request.schema_name,
            table,
        ))
    }

    pub fn get_table(&self, full_name: &str) -> Result<TableInfoResponse> {
        let (catalog_name, schema_name, table_name) = split_table_name(full_name)?;
        let snapshot = self.store.snapshot_in(catalog_name, schema_name)?;
        snapshot
            .table(table_name)
            .map(|table| table_response(catalog_name, schema_name, table))
            .ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::NotFound,
                    format!("table '{full_name}' does not exist"),
                )
            })
    }

    pub fn list_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<TableInfoResponse>> {
        self.store
            .snapshot_in(catalog_name, schema_name)
            .map(|snapshot| {
                snapshot
                    .tables()
                    .map(|(_, table)| table_response(catalog_name, schema_name, table))
                    .collect()
            })
    }

    pub fn delete_table(&self, full_name: &str) -> Result<()> {
        let (catalog_name, schema_name, table_name) = split_table_name(full_name)?;
        self.store
            .drop_table_in(catalog_name, schema_name, table_name)?;
        Ok(())
    }
}

fn catalog_response(value: CatalogInfo) -> CatalogInfoResponse {
    let storage_root = value.metadata.properties.get("storage_root").cloned();
    CatalogInfoResponse {
        name: value.name,
        comment: value.metadata.comment,
        properties: value.metadata.properties,
        owner: value.metadata.owner.clone(),
        created_at: value.created_at,
        created_by: value.metadata.owner.clone(),
        updated_at: value.updated_at,
        updated_by: value.metadata.owner,
        id: value.id,
        storage_root,
        storage_location: None,
    }
}

fn schema_response(value: SchemaInfo) -> SchemaInfoResponse {
    let storage_root = value.metadata.properties.get("storage_root").cloned();
    SchemaInfoResponse {
        full_name: format!("{}.{}", value.catalog_name, value.name),
        name: value.name,
        catalog_name: value.catalog_name,
        comment: value.metadata.comment,
        properties: value.metadata.properties,
        owner: value.metadata.owner.clone(),
        created_at: value.created_at,
        created_by: value.metadata.owner.clone(),
        updated_at: value.updated_at,
        updated_by: value.metadata.owner,
        schema_id: value.id,
        storage_root,
        storage_location: None,
    }
}

fn table_from_request(request: &CreateTable) -> Result<TableDef> {
    let provider = request
        .properties
        .get("vql.provider")
        .map(|value| value.to_ascii_lowercase());
    let provider = match provider.as_deref() {
        Some("images") => TableProvider::Images {
            location: required_location(request)?,
            recursive: property_bool(&request.properties, "vql.recursive", false)?,
        },
        Some("videos") => TableProvider::Videos {
            location: required_location(request)?,
            recursive: property_bool(&request.properties, "vql.recursive", false)?,
            fps: property_parse(&request.properties, "vql.fps")?,
            start_time_ms: property_parse(&request.properties, "vql.start_time_ms")?,
        },
        Some("rtsp") => TableProvider::Rtsp(RtspTableConfig {
            name: request.name.clone(),
            endpoint: request
                .properties
                .get("vql.endpoint")
                .cloned()
                .ok_or_else(|| invalid_property("vql.endpoint"))?,
            fps: property_parse(&request.properties, "vql.fps")?.unwrap_or(5.0),
            event_time: match request
                .properties
                .get("vql.event_time")
                .map(String::as_str)
                .unwrap_or("capture_time")
            {
                "capture_time" => EventTimePolicy::CaptureTime,
                "ingest_time" => EventTimePolicy::IngestTime,
                _ => return Err(invalid_property("vql.event_time")),
            },
            watermark_delay_ms: property_parse(&request.properties, "vql.watermark_delay_ms")?
                .unwrap_or(2_000),
            transport: match request
                .properties
                .get("vql.transport")
                .map(String::as_str)
                .unwrap_or("tcp")
            {
                "tcp" => RtspTransport::Tcp,
                "udp" => RtspTransport::Udp,
                _ => return Err(invalid_property("vql.transport")),
            },
        }),
        Some("kafka") => TableProvider::Kafka(KafkaTableConfig {
            bootstrap_servers: required_property(&request.properties, "vql.bootstrap_servers")?,
            topic: required_property(&request.properties, "vql.topic")?,
            credential_ref: request.properties.get("vql.credential_ref").cloned(),
            delivery_timeout_ms: property_parse(&request.properties, "vql.delivery_timeout_ms")?
                .unwrap_or(30_000),
            buffer_capacity: property_parse(&request.properties, "vql.buffer_capacity")?
                .unwrap_or(1_024),
        }),
        Some(_) => return Err(invalid_property("vql.provider")),
        None => TableProvider::External {
            data_source_format: request
                .data_source_format
                .as_ref()
                .map(|format| format!("{format:?}").to_ascii_uppercase()),
            storage_location: request.storage_location.clone(),
        },
    };
    Ok(TableDef {
        name: request.name.clone(),
        provider,
        metadata: SecurableMetadata {
            comment: request.comment.clone(),
            properties: request.properties.clone(),
            owner: None,
        },
    })
}

fn table_response(
    catalog_name: &str,
    schema_name: &str,
    table: &SnapshotTable,
) -> TableInfoResponse {
    let mut properties = table.definition.metadata.properties.clone();
    properties.insert(
        "vql.provider".to_owned(),
        format!("{:?}", table.definition.provider.kind()).to_ascii_lowercase(),
    );
    let capabilities = table.definition.capabilities();
    properties.insert("vql.readable".to_owned(), capabilities.readable.to_string());
    properties.insert("vql.writable".to_owned(), capabilities.writable.to_string());
    properties.insert("vql.bounded".to_owned(), capabilities.bounded.to_string());
    properties.insert("vql.durable".to_owned(), capabilities.durable.to_string());
    append_provider_properties(&table.definition.provider, &mut properties);
    let data_source_format = match &table.definition.provider {
        TableProvider::Kafka(_) => Some(DataSourceFormat::Json),
        TableProvider::External {
            data_source_format, ..
        } => data_source_format
            .as_deref()
            .and_then(parse_data_source_format),
        _ => None,
    };
    TableInfoResponse {
        name: table.definition.name.clone(),
        catalog_name: catalog_name.to_owned(),
        schema_name: schema_name.to_owned(),
        table_type: TableType::External,
        data_source_format,
        columns: columns_from_schema(&table.schema),
        storage_location: match &table.definition.provider {
            TableProvider::Images { location, .. } | TableProvider::Videos { location, .. } => {
                Some(location.clone())
            }
            TableProvider::External {
                storage_location, ..
            } => storage_location.clone(),
            TableProvider::Rtsp(_) | TableProvider::Kafka(_) => None,
        },
        comment: table.definition.metadata.comment.clone(),
        properties,
        owner: table.definition.metadata.owner.clone(),
        table_id: table.object_id.clone(),
    }
}

fn append_provider_properties(provider: &TableProvider, properties: &mut BTreeMap<String, String>) {
    match provider {
        TableProvider::Images { recursive, .. } => {
            properties.insert("vql.recursive".to_owned(), recursive.to_string());
        }
        TableProvider::Videos {
            recursive,
            fps,
            start_time_ms,
            ..
        } => {
            properties.insert("vql.recursive".to_owned(), recursive.to_string());
            if let Some(value) = fps {
                properties.insert("vql.fps".to_owned(), value.to_string());
            }
            if let Some(value) = start_time_ms {
                properties.insert("vql.start_time_ms".to_owned(), value.to_string());
            }
        }
        TableProvider::Rtsp(config) => {
            properties.insert("vql.endpoint".to_owned(), config.endpoint.clone());
            properties.insert("vql.fps".to_owned(), config.fps.to_string());
            properties.insert(
                "vql.event_time".to_owned(),
                match config.event_time {
                    EventTimePolicy::CaptureTime => "capture_time",
                    EventTimePolicy::IngestTime => "ingest_time",
                }
                .to_owned(),
            );
            properties.insert(
                "vql.watermark_delay_ms".to_owned(),
                config.watermark_delay_ms.to_string(),
            );
            properties.insert(
                "vql.transport".to_owned(),
                match config.transport {
                    RtspTransport::Tcp => "tcp",
                    RtspTransport::Udp => "udp",
                }
                .to_owned(),
            );
        }
        TableProvider::Kafka(config) => {
            properties.insert(
                "vql.bootstrap_servers".to_owned(),
                config.bootstrap_servers.clone(),
            );
            properties.insert("vql.topic".to_owned(), config.topic.clone());
            if config.credential_ref.is_some() {
                properties.insert(
                    "vql.credential_ref".to_owned(),
                    "[REDACTED_SECRET_REF]".to_owned(),
                );
            }
            properties.insert(
                "vql.delivery_timeout_ms".to_owned(),
                config.delivery_timeout_ms.to_string(),
            );
            properties.insert(
                "vql.buffer_capacity".to_owned(),
                config.buffer_capacity.to_string(),
            );
        }
        TableProvider::External { .. } => {}
    }
}

fn columns_from_schema(schema: &SchemaRef) -> Vec<ColumnInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let (type_name, type_text) = uc_type(field.data_type());
            ColumnInfo {
                name: field.name().clone(),
                type_json: serde_json::to_string(&spark_field_json(field))
                    .expect("serializing an Arrow field cannot fail"),
                type_text,
                type_name: type_name.to_owned(),
                position: i32::try_from(position).expect("Arrow field count fits i32"),
                nullable: field.is_nullable(),
                comment: None,
            }
        })
        .collect()
}

fn validate_provider_columns(columns: &[ColumnInfo], schema: &SchemaRef) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    let expected = columns_from_schema(schema);
    let matches = columns.len() == expected.len()
        && columns.iter().zip(&expected).all(|(actual, expected)| {
            actual.name == expected.name
                && actual.type_name.eq_ignore_ascii_case(&expected.type_name)
                && actual.type_text.eq_ignore_ascii_case(&expected.type_text)
                && actual.nullable == expected.nullable
                && actual.position == expected.position
        });
    if matches {
        Ok(())
    } else {
        Err(CatalogError::new(
            CatalogErrorCode::InvalidArgument,
            "VisionQL provider columns must be omitted or match the provider-owned schema",
        ))
    }
}

fn spark_field_json(field: &Field) -> serde_json::Value {
    serde_json::json!({
        "name": field.name(),
        "type": spark_type_json(field.data_type()),
        "nullable": field.is_nullable(),
        "metadata": field.metadata(),
    })
}

fn spark_type_json(data_type: &DataType) -> serde_json::Value {
    use serde_json::{Value, json};

    match data_type {
        DataType::Boolean => Value::String("boolean".to_owned()),
        DataType::Int8 => Value::String("byte".to_owned()),
        DataType::Int16 => Value::String("short".to_owned()),
        DataType::Int32 => Value::String("integer".to_owned()),
        DataType::Int64 => Value::String("long".to_owned()),
        DataType::Float32 => Value::String("float".to_owned()),
        DataType::Float64 => Value::String("double".to_owned()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Value::String("string".to_owned())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Value::String("binary".to_owned())
        }
        DataType::Date32 | DataType::Date64 => Value::String("date".to_owned()),
        DataType::Timestamp(_, timezone) => Value::String(
            if timezone.is_some() {
                "timestamp"
            } else {
                "timestamp_ntz"
            }
            .to_owned(),
        ),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Value::String(format!("decimal({precision},{scale})"))
        }
        DataType::List(element) | DataType::LargeList(element) => json!({
            "type": "array",
            "elementType": spark_type_json(element.data_type()),
            "containsNull": element.is_nullable(),
        }),
        DataType::FixedSizeList(element, _) => json!({
            "type": "array",
            "elementType": spark_type_json(element.data_type()),
            "containsNull": element.is_nullable(),
        }),
        DataType::Struct(fields) => json!({
            "type": "struct",
            "fields": fields.iter().map(|field| spark_field_json(field)).collect::<Vec<_>>(),
        }),
        DataType::Map(entries, _) => {
            let (key_type, value_type, value_contains_null) =
                if let DataType::Struct(fields) = entries.data_type() {
                    if fields.len() == 2 {
                        (
                            spark_type_json(fields[0].data_type()),
                            spark_type_json(fields[1].data_type()),
                            fields[1].is_nullable(),
                        )
                    } else {
                        (
                            Value::String("string".to_owned()),
                            Value::String("string".to_owned()),
                            true,
                        )
                    }
                } else {
                    (
                        Value::String("string".to_owned()),
                        Value::String("string".to_owned()),
                        true,
                    )
                };
            json!({
                "type": "map",
                "keyType": key_type,
                "valueType": value_type,
                "valueContainsNull": value_contains_null,
            })
        }
        DataType::Dictionary(_, value) => spark_type_json(value),
        _ => Value::String(data_type.to_string().to_ascii_lowercase()),
    }
}

fn schema_from_columns(columns: &[ColumnInfo]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                &column.name,
                arrow_type(&column.type_name, &column.type_text)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn uc_type(data_type: &DataType) -> (&'static str, String) {
    match data_type {
        DataType::Boolean => ("BOOLEAN", "BOOLEAN".to_owned()),
        DataType::Int8 => ("BYTE", "TINYINT".to_owned()),
        DataType::Int16 => ("SHORT", "SMALLINT".to_owned()),
        DataType::Int32 => ("INT", "INT".to_owned()),
        DataType::Int64 => ("LONG", "BIGINT".to_owned()),
        DataType::Float32 => ("FLOAT", "FLOAT".to_owned()),
        DataType::Float64 => ("DOUBLE", "DOUBLE".to_owned()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ("STRING", "STRING".to_owned())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            ("BINARY", "BINARY".to_owned())
        }
        DataType::Date32 | DataType::Date64 => ("DATE", "DATE".to_owned()),
        DataType::Timestamp(_, _) => ("TIMESTAMP", "TIMESTAMP".to_owned()),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            ("ARRAY", data_type.to_string())
        }
        DataType::Struct(_) => ("STRUCT", data_type.to_string()),
        DataType::Map(_, _) => ("MAP", data_type.to_string()),
        _ => ("USER_DEFINED_TYPE", data_type.to_string()),
    }
}

fn arrow_type(type_name: &str, type_text: &str) -> Result<DataType> {
    let value = match type_name.to_ascii_uppercase().as_str() {
        "BOOLEAN" => DataType::Boolean,
        "BYTE" => DataType::Int8,
        "SHORT" => DataType::Int16,
        "INT" => DataType::Int32,
        "LONG" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" | "CHAR" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        _ => {
            return Err(CatalogError::new(
                CatalogErrorCode::InvalidArgument,
                format!("unsupported UC column type '{type_text}'"),
            ));
        }
    };
    Ok(value)
}

fn parse_data_source_format(value: &str) -> Option<DataSourceFormat> {
    match value {
        "DELTA" => Some(DataSourceFormat::Delta),
        "CSV" => Some(DataSourceFormat::Csv),
        "JSON" => Some(DataSourceFormat::Json),
        "AVRO" => Some(DataSourceFormat::Avro),
        "PARQUET" => Some(DataSourceFormat::Parquet),
        "ORC" => Some(DataSourceFormat::Orc),
        "TEXT" => Some(DataSourceFormat::Text),
        _ => None,
    }
}

fn required_location(request: &CreateTable) -> Result<String> {
    request
        .storage_location
        .clone()
        .ok_or_else(|| invalid_property("storage_location"))
}

fn required_property(properties: &BTreeMap<String, String>, key: &str) -> Result<String> {
    properties
        .get(key)
        .cloned()
        .ok_or_else(|| invalid_property(key))
}

fn property_bool(properties: &BTreeMap<String, String>, key: &str, default: bool) -> Result<bool> {
    match properties.get(key) {
        Some(value) => value.parse().map_err(|_| invalid_property(key)),
        None => Ok(default),
    }
}

fn property_parse<T: std::str::FromStr>(
    properties: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<T>> {
    properties
        .get(key)
        .map(|value| value.parse().map_err(|_| invalid_property(key)))
        .transpose()
}

fn invalid_property(name: &str) -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::InvalidArgument,
        format!("invalid or missing property '{name}'"),
    )
}

fn split_schema_name(full_name: &str) -> Result<(&str, &str)> {
    let parts = full_name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [catalog, schema] => Ok((catalog, schema)),
        _ => Err(CatalogError::new(
            CatalogErrorCode::InvalidArgument,
            "schema full_name must be catalog.schema",
        )),
    }
}

fn split_table_name(full_name: &str) -> Result<(&str, &str, &str)> {
    let parts = full_name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [catalog, schema, table] => Ok((catalog, schema, table)),
        _ => Err(CatalogError::new(
            CatalogErrorCode::InvalidArgument,
            "table full_name must be catalog.schema.table",
        )),
    }
}

#[cfg(feature = "http")]
fn paginate<T>(
    values: Vec<T>,
    max_results: Option<i32>,
    page_token: Option<&str>,
    maximum: usize,
) -> Result<(Vec<T>, Option<String>)> {
    if max_results.is_some_and(|value| value < 0) {
        return Err(CatalogError::new(
            CatalogErrorCode::InvalidArgument,
            "max_results cannot be negative",
        ));
    }
    let offset = page_token
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidArgument, "invalid page_token"))?;
    if offset > values.len() {
        return Err(CatalogError::new(
            CatalogErrorCode::InvalidArgument,
            "invalid page_token",
        ));
    }
    let limit = usize::try_from(max_results.unwrap_or(0))
        .unwrap_or_default()
        .clamp(1, maximum);
    let total = values.len();
    let end = total.min(offset.saturating_add(limit));
    let values = values
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next = (end < total).then(|| end.to_string());
    Ok((values, next))
}

#[cfg(feature = "http")]
pub mod http {
    use axum::extract::rejection::{JsonRejection, QueryRejection};
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use axum::{Json, Router};

    use super::*;

    #[derive(Debug, Clone, Deserialize, Default)]
    struct PageQuery {
        max_results: Option<i32>,
        page_token: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize, Default)]
    struct ForceQuery {
        #[serde(default)]
        force: bool,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct SchemaListQuery {
        catalog_name: String,
        max_results: Option<i32>,
        page_token: Option<String>,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct TableListQuery {
        catalog_name: String,
        schema_name: String,
        max_results: Option<i32>,
        page_token: Option<String>,
    }

    pub fn router(store: Arc<CatalogStore>) -> Router {
        Router::new()
            .route(
                &format!("{API_PREFIX}/catalogs"),
                post(create_catalog).get(list_catalogs),
            )
            .route(
                &format!("{API_PREFIX}/catalogs/{{name}}"),
                get(get_catalog)
                    .patch(update_catalog)
                    .delete(delete_catalog),
            )
            .route(
                &format!("{API_PREFIX}/schemas"),
                post(create_schema).get(list_schemas),
            )
            .route(
                &format!("{API_PREFIX}/schemas/{{full_name}}"),
                get(get_schema).patch(update_schema).delete(delete_schema),
            )
            .route(
                &format!("{API_PREFIX}/tables"),
                post(create_table).get(list_tables),
            )
            .route(
                &format!("{API_PREFIX}/tables/{{full_name}}"),
                get(get_table).delete(delete_table),
            )
            .with_state(UnityCatalogService::new(store))
    }

    async fn create_catalog(
        State(service): State<UnityCatalogService>,
        request: std::result::Result<Json<CreateCatalog>, JsonRejection>,
    ) -> HttpResult<Json<CatalogInfoResponse>> {
        let Json(request) = request?;
        service
            .create_catalog(request)
            .map(Json)
            .map_err(Into::into)
    }

    async fn list_catalogs(
        State(service): State<UnityCatalogService>,
        query: std::result::Result<Query<PageQuery>, QueryRejection>,
    ) -> HttpResult<Json<ListCatalogsResponse>> {
        let Query(query) = query?;
        let (catalogs, next_page_token) = paginate(
            service.list_catalogs()?,
            query.max_results,
            query.page_token.as_deref(),
            1_000,
        )?;
        Ok(Json(ListCatalogsResponse {
            catalogs,
            next_page_token,
        }))
    }

    async fn get_catalog(
        State(service): State<UnityCatalogService>,
        Path(name): Path<String>,
    ) -> HttpResult<Json<CatalogInfoResponse>> {
        service.get_catalog(&name).map(Json).map_err(Into::into)
    }

    async fn update_catalog(
        State(service): State<UnityCatalogService>,
        Path(name): Path<String>,
        request: std::result::Result<Json<UpdateCatalog>, JsonRejection>,
    ) -> HttpResult<Json<CatalogInfoResponse>> {
        let Json(request) = request?;
        service
            .update_catalog(&name, request)
            .map(Json)
            .map_err(Into::into)
    }

    async fn delete_catalog(
        State(service): State<UnityCatalogService>,
        Path(name): Path<String>,
        query: std::result::Result<Query<ForceQuery>, QueryRejection>,
    ) -> HttpResult<Json<serde_json::Value>> {
        let Query(query) = query?;
        service.delete_catalog(&name, query.force)?;
        Ok(Json(serde_json::json!({})))
    }

    async fn create_schema(
        State(service): State<UnityCatalogService>,
        request: std::result::Result<Json<CreateSchema>, JsonRejection>,
    ) -> HttpResult<Json<SchemaInfoResponse>> {
        let Json(request) = request?;
        service.create_schema(request).map(Json).map_err(Into::into)
    }

    async fn list_schemas(
        State(service): State<UnityCatalogService>,
        query: std::result::Result<Query<SchemaListQuery>, QueryRejection>,
    ) -> HttpResult<Json<ListSchemasResponse>> {
        let Query(query) = query?;
        let (schemas, next_page_token) = paginate(
            service.list_schemas(&query.catalog_name)?,
            query.max_results,
            query.page_token.as_deref(),
            1_000,
        )?;
        Ok(Json(ListSchemasResponse {
            schemas,
            next_page_token,
        }))
    }

    async fn get_schema(
        State(service): State<UnityCatalogService>,
        Path(full_name): Path<String>,
    ) -> HttpResult<Json<SchemaInfoResponse>> {
        service.get_schema(&full_name).map(Json).map_err(Into::into)
    }

    async fn update_schema(
        State(service): State<UnityCatalogService>,
        Path(full_name): Path<String>,
        request: std::result::Result<Json<UpdateSchema>, JsonRejection>,
    ) -> HttpResult<Json<SchemaInfoResponse>> {
        let Json(request) = request?;
        service
            .update_schema(&full_name, request)
            .map(Json)
            .map_err(Into::into)
    }

    async fn delete_schema(
        State(service): State<UnityCatalogService>,
        Path(full_name): Path<String>,
        query: std::result::Result<Query<ForceQuery>, QueryRejection>,
    ) -> HttpResult<Json<serde_json::Value>> {
        let Query(query) = query?;
        service.delete_schema(&full_name, query.force)?;
        Ok(Json(serde_json::json!({})))
    }

    async fn create_table(
        State(service): State<UnityCatalogService>,
        request: std::result::Result<Json<CreateTable>, JsonRejection>,
    ) -> HttpResult<Json<TableInfoResponse>> {
        let Json(request) = request?;
        service.create_table(request).map(Json).map_err(Into::into)
    }

    async fn list_tables(
        State(service): State<UnityCatalogService>,
        query: std::result::Result<Query<TableListQuery>, QueryRejection>,
    ) -> HttpResult<Json<ListTablesResponse>> {
        let Query(query) = query?;
        let (tables, next_page_token) = paginate(
            service.list_tables(&query.catalog_name, &query.schema_name)?,
            query.max_results,
            query.page_token.as_deref(),
            50,
        )?;
        Ok(Json(ListTablesResponse {
            tables,
            next_page_token,
        }))
    }

    async fn get_table(
        State(service): State<UnityCatalogService>,
        Path(full_name): Path<String>,
    ) -> HttpResult<Json<TableInfoResponse>> {
        service.get_table(&full_name).map(Json).map_err(Into::into)
    }

    async fn delete_table(
        State(service): State<UnityCatalogService>,
        Path(full_name): Path<String>,
    ) -> HttpResult<Json<serde_json::Value>> {
        service.delete_table(&full_name)?;
        Ok(Json(serde_json::json!({})))
    }

    struct HttpError {
        error: CatalogError,
        status: Option<StatusCode>,
    }

    type HttpResult<T> = std::result::Result<T, HttpError>;

    impl From<CatalogError> for HttpError {
        fn from(value: CatalogError) -> Self {
            Self {
                error: value,
                status: None,
            }
        }
    }

    impl From<JsonRejection> for HttpError {
        fn from(value: JsonRejection) -> Self {
            Self {
                status: Some(value.status()),
                error: CatalogError::new(CatalogErrorCode::InvalidArgument, value.body_text()),
            }
        }
    }

    impl From<QueryRejection> for HttpError {
        fn from(value: QueryRejection) -> Self {
            Self {
                status: Some(value.status()),
                error: CatalogError::new(CatalogErrorCode::InvalidArgument, value.body_text()),
            }
        }
    }

    impl IntoResponse for HttpError {
        fn into_response(self) -> Response {
            let (default_status, error_code) = match self.error.code {
                CatalogErrorCode::AlreadyExists => (StatusCode::CONFLICT, "ALREADY_EXISTS"),
                CatalogErrorCode::NameConflict => (StatusCode::CONFLICT, "NAME_CONFLICT"),
                CatalogErrorCode::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND"),
                CatalogErrorCode::InvalidArgument => (StatusCode::BAD_REQUEST, "INVALID_ARGUMENT"),
                CatalogErrorCode::Conflict => {
                    (StatusCode::PRECONDITION_FAILED, "FAILED_PRECONDITION")
                }
                CatalogErrorCode::Storage | CatalogErrorCode::Internal => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL")
                }
            };
            let status = self.status.unwrap_or(default_status);
            (
                status,
                Json(ErrorResponse {
                    error_code: error_code.to_owned(),
                    message: self.error.message,
                }),
            )
                .into_response()
        }
    }

    impl From<CatalogError> for axum::response::Response {
        fn from(value: CatalogError) -> Self {
            HttpError::from(value).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{DEFAULT_CATALOG, DEFAULT_SCHEMA, images_schema};

    #[test]
    fn vql_tables_are_external_uc_tables_with_capabilities() {
        let temp = tempdir().unwrap();
        let store = Arc::new(CatalogStore::open(&temp.path().join("vql.db")).unwrap());
        let table = TableDef::new(
            "camera",
            TableProvider::Rtsp(RtspTableConfig {
                name: "camera".to_owned(),
                endpoint: "rtsp://camera/live".to_owned(),
                fps: 5.0,
                event_time: EventTimePolicy::CaptureTime,
                watermark_delay_ms: 2_000,
                transport: RtspTransport::Tcp,
            }),
        );
        store
            .create_table(&table, &Arc::new(Schema::empty()))
            .unwrap();
        let info = UnityCatalogService::new(store)
            .get_table("vql.default.camera")
            .unwrap();
        assert_eq!(info.table_type, TableType::External);
        assert_eq!(info.properties["vql.provider"], "rtsp");
        assert_eq!(info.properties["vql.bounded"], "false");
    }

    #[test]
    fn uc_v0_6_provider_tables_use_the_provider_owned_schema() {
        assert_eq!(OPENAPI_VERSION, "0.6.0");
        assert_eq!(
            serde_json::to_string(&TableType::View).unwrap(),
            r#""VIEW""#
        );

        let temp = tempdir().unwrap();
        let images = temp.path().join("images");
        std::fs::create_dir(&images).unwrap();
        let store = Arc::new(CatalogStore::open(&temp.path().join("vql.db")).unwrap());
        let service = UnityCatalogService::new(Arc::clone(&store));
        let info = service
            .create_table(CreateTable {
                name: "photos".to_owned(),
                catalog_name: DEFAULT_CATALOG.to_owned(),
                schema_name: DEFAULT_SCHEMA.to_owned(),
                table_type: TableType::External,
                data_source_format: None,
                columns: Vec::new(),
                storage_location: Some(images.to_string_lossy().into_owned()),
                comment: None,
                properties: BTreeMap::from([("vql.provider".to_owned(), "images".to_owned())]),
            })
            .unwrap();

        assert_eq!(
            info.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["uri", "image", "width", "height", "captured_at"]
        );
        let image_type: serde_json::Value =
            serde_json::from_str(&info.columns[1].type_json).unwrap();
        assert_eq!(image_type["name"], "image");
        assert_eq!(image_type["type"]["type"], "struct");
        assert_eq!(image_type["metadata"]["ARROW:extension:name"], "vql.image");
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.table("photos").unwrap().schema, images_schema());
    }

    #[test]
    fn uc_rejects_invalid_provider_columns_and_video_fps() {
        let temp = tempdir().unwrap();
        let media = temp.path().join("media");
        std::fs::create_dir(&media).unwrap();
        let service = UnityCatalogService::new(Arc::new(
            CatalogStore::open(&temp.path().join("vql.db")).unwrap(),
        ));
        let request = |name: &str, provider: &str, columns: Vec<ColumnInfo>| CreateTable {
            name: name.to_owned(),
            catalog_name: DEFAULT_CATALOG.to_owned(),
            schema_name: DEFAULT_SCHEMA.to_owned(),
            table_type: TableType::External,
            data_source_format: None,
            columns,
            storage_location: Some(media.to_string_lossy().into_owned()),
            comment: None,
            properties: BTreeMap::from([("vql.provider".to_owned(), provider.to_owned())]),
        };

        let error = service
            .create_table(request(
                "photos",
                "images",
                vec![ColumnInfo {
                    name: "wrong".to_owned(),
                    type_text: "STRING".to_owned(),
                    type_json: r#""string""#.to_owned(),
                    type_name: "STRING".to_owned(),
                    position: 0,
                    nullable: true,
                    comment: None,
                }],
            ))
            .unwrap_err();
        assert_eq!(error.code, CatalogErrorCode::InvalidArgument);
        assert!(error.message.contains("provider-owned schema"));

        let mut invalid_video = request("clips", "videos", Vec::new());
        invalid_video
            .properties
            .insert("vql.fps".to_owned(), "-1".to_owned());
        let error = service.create_table(invalid_video).unwrap_err();
        assert_eq!(error.code, CatalogErrorCode::InvalidArgument);
        assert!(error.message.contains("video fps"));
    }

    #[test]
    fn catalog_boundary_rejects_provider_secrets_and_invalid_targets() {
        let temp = tempdir().unwrap();
        let store = CatalogStore::open(&temp.path().join("vql.db")).unwrap();
        let table = TableDef::new(
            "camera",
            TableProvider::Rtsp(RtspTableConfig {
                name: "camera".to_owned(),
                endpoint: "rtsp://user:password@camera/live".to_owned(),
                fps: 5.0,
                event_time: EventTimePolicy::CaptureTime,
                watermark_delay_ms: 2_000,
                transport: RtspTransport::Tcp,
            }),
        );

        let error = store
            .create_table(&table, &Arc::new(Schema::empty()))
            .unwrap_err();

        assert_eq!(error.code, CatalogErrorCode::InvalidArgument);
        assert!(error.message.contains("credentials"));

        let missing = TableDef::new(
            "missing",
            TableProvider::Images {
                location: temp.path().join("missing").to_string_lossy().into_owned(),
                recursive: false,
            },
        );
        let error = store
            .create_table(&missing, &Arc::new(Schema::empty()))
            .unwrap_err();
        assert_eq!(error.code, CatalogErrorCode::InvalidArgument);
        assert!(error.message.contains("readable directory"));
    }

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn http_routes_use_the_uc_2_1_paths_and_error_envelope() {
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let temp = tempdir().unwrap();
        let store = Arc::new(CatalogStore::open(&temp.path().join("vql.db")).unwrap());
        let table = TableDef::new(
            "camera",
            TableProvider::Rtsp(RtspTableConfig {
                name: "camera".to_owned(),
                endpoint: "rtsp://camera/live".to_owned(),
                fps: 5.0,
                event_time: EventTimePolicy::CaptureTime,
                watermark_delay_ms: 2_000,
                transport: RtspTransport::Tcp,
            }),
        );
        store
            .create_table(&table, &Arc::new(Schema::empty()))
            .unwrap();
        let router = http::router(store);

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/2.1/unity-catalog/tables/vql.default.camera")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: TableInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.catalog_name, DEFAULT_CATALOG);
        assert_eq!(body.schema_name, DEFAULT_SCHEMA);
        assert_eq!(body.table_type, TableType::External);

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/2.1/unity-catalog/tables/vql.default.missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.error_code, "NOT_FOUND");

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/2.1/unity-catalog/catalogs")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.error_code, "INVALID_ARGUMENT");

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/2.1/unity-catalog/schemas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.error_code, "INVALID_ARGUMENT");
    }
}
