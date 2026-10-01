use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Float32Array, Float32Builder, ListArray, ListBuilder, StringArray,
    StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_doc::Documentation;
use datafusion_macros::user_doc;

use crate::types::box2d_field;

pub(crate) fn point_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("x", DataType::Float32, false)),
        Arc::new(Field::new("y", DataType::Float32, false)),
    ])
}

pub(crate) fn point_type() -> DataType {
    DataType::Struct(point_fields())
}

#[user_doc(
    doc_section(label = "Spatial functions"),
    description = "Return the center of a normalized BOX2D as POINT2D. NULL input returns NULL.",
    syntax_example = "BOX_CENTER(box BOX2D) -> POINT2D",
    alternative_syntax = "box.center",
    argument(name = "box", description = "A normalized BOX2D value."),
    sql_example = r#"With `sample_images` and the resolved `yolo` Model from the [SQL reference](sql-reference.md#create-model):

```sql
SELECT f.uri, BOX_CENTER(det.box) AS center
FROM sample_images AS f,
     UNNEST(yolo(f.image, classes => ['person'])) AS u(det);
```"#,
    related_udf(name = "st_contains")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct BoxCenter(Signature);

impl ScalarUDFImpl for BoxCenter {
    fn name(&self) -> &str {
        "box_center"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(point_type())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let boxes = arrays[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "BOX_CENTER expects BOX2D".to_owned(),
                )
            })?;
        let coordinates = (0..4)
            .map(|index| {
                boxes
                    .column(index)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("BOX2D coordinate is Float32")
            })
            .collect::<Vec<_>>();
        let mut builder = StructBuilder::new(
            point_fields(),
            vec![
                Box::new(Float32Builder::with_capacity(boxes.len())),
                Box::new(Float32Builder::with_capacity(boxes.len())),
            ],
        );
        for row in 0..boxes.len() {
            if boxes.is_null(row) {
                builder
                    .field_builder::<Float32Builder>(0)
                    .unwrap()
                    .append_null();
                builder
                    .field_builder::<Float32Builder>(1)
                    .unwrap()
                    .append_null();
                builder.append(false);
            } else {
                builder
                    .field_builder::<Float32Builder>(0)
                    .unwrap()
                    .append_value(coordinates[0].value(row) + coordinates[2].value(row) / 2.0);
                builder
                    .field_builder::<Float32Builder>(1)
                    .unwrap()
                    .append_value(coordinates[1].value(row) + coordinates[3].value(row) / 2.0);
                builder.append(true);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

pub(crate) fn box_center_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BoxCenter(Signature::exact(
        vec![box2d_field("box", true).data_type().clone()],
        Volatility::Immutable,
    )))
}

pub(crate) fn polygon_type() -> DataType {
    DataType::List(Arc::new(Field::new("point", point_type(), false)))
}

#[user_doc(
    doc_section(label = "Spatial functions"),
    description = "Parse a closed polygon from normalized x y pairs and return POLYGON. Coordinates must be finite and within [0,1]. A polygon needs at least three edges and repeats its first point at the end. NULL input returns NULL.",
    syntax_example = "POLYGON(text STRING) -> POLYGON",
    alternative_syntax = "ST_POLYGON(text STRING)",
    argument(
        name = "text",
        description = "Comma-separated x y coordinates, optionally wrapped in POLYGON((...))."
    ),
    sql_example = r#"```sql
SELECT POLYGON('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS area;
```"#,
    related_udf(name = "st_contains")
)]
#[derive(Debug)]
struct Polygon {
    name: &'static str,
    signature: Signature,
}
impl PartialEq for Polygon {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Polygon {}
impl Hash for Polygon {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for Polygon {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(polygon_type())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("POLYGON expects STRING".to_owned())
            })?;
        let point_builder = StructBuilder::new(
            point_fields(),
            vec![
                Box::new(Float32Builder::new()),
                Box::new(Float32Builder::new()),
            ],
        );
        let mut builder = ListBuilder::new(point_builder).with_field(Arc::new(Field::new(
            "point",
            point_type(),
            false,
        )));
        for row in 0..text.len() {
            if text.is_null(row) {
                builder.append(false);
                continue;
            }
            let points = parse_polygon(text.value(row))
                .map_err(datafusion::common::DataFusionError::Execution)?;
            for (x, y) in points {
                let values = builder.values();
                values
                    .field_builder::<Float32Builder>(0)
                    .unwrap()
                    .append_value(x);
                values
                    .field_builder::<Float32Builder>(1)
                    .unwrap()
                    .append_value(y);
                values.append(true);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

pub(crate) fn polygon_udf(name: &'static str) -> ScalarUDF {
    ScalarUDF::new_from_impl(Polygon {
        name,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
}

fn parse_polygon(value: &str) -> std::result::Result<Vec<(f32, f32)>, String> {
    let trimmed = value.trim();
    let body = trimmed
        .strip_prefix("POLYGON((")
        .and_then(|value| value.strip_suffix("))"))
        .unwrap_or(trimmed);
    let points = body
        .split(',')
        .map(|point| {
            let coordinates = point.split_whitespace().collect::<Vec<_>>();
            if coordinates.len() != 2 {
                return Err("POLYGON points must be 'x y' pairs".to_owned());
            }
            let x = coordinates[0]
                .parse::<f32>()
                .map_err(|_| "POLYGON coordinate is not numeric".to_owned())?;
            let y = coordinates[1]
                .parse::<f32>()
                .map_err(|_| "POLYGON coordinate is not numeric".to_owned())?;
            if !x.is_finite()
                || !y.is_finite()
                || !(0.0..=1.0).contains(&x)
                || !(0.0..=1.0).contains(&y)
            {
                return Err("POLYGON coordinates must be finite and within [0,1]".to_owned());
            }
            Ok((x, y))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if points.len() < 4 || points.first() != points.last() {
        return Err("POLYGON must be closed and contain at least three edges".to_owned());
    }
    Ok(points)
}

#[user_doc(
    doc_section(label = "Spatial functions"),
    description = "Return BOOLEAN indicating whether a POLYGON contains a POINT2D. Boundary points are included. NULL arguments return NULL.",
    syntax_example = "ST_CONTAINS(polygon POLYGON, point POINT2D) -> BOOLEAN",
    argument(
        name = "polygon",
        description = "A closed polygon in normalized coordinates."
    ),
    argument(
        name = "point",
        description = "A point in the same normalized coordinate space."
    ),
    sql_example = r#"With `sample_images` and the resolved `yolo` Model from the [SQL reference](sql-reference.md#create-model):

```sql
SELECT f.uri, det.label
FROM sample_images AS f,
     UNNEST(yolo(f.image, classes => ['person'])) AS u(det)
WHERE ST_CONTAINS(
  POLYGON('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
  BOX_CENTER(det.box)
);
```"#,
    related_udf(name = "polygon"),
    related_udf(name = "box_center")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct StContains(Signature);
impl ScalarUDFImpl for StContains {
    fn name(&self) -> &str {
        "st_contains"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let polygons = arrays[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "ST_CONTAINS expects POLYGON".to_owned(),
                )
            })?;
        let points = arrays[1]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "ST_CONTAINS expects POINT2D".to_owned(),
                )
            })?;
        let xs = points
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let ys = points
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut output = BooleanBuilder::with_capacity(polygons.len());
        for row in 0..polygons.len() {
            if polygons.is_null(row) || points.is_null(row) {
                output.append_null();
                continue;
            }
            let polygon = polygons.value(row);
            let polygon = polygon.as_any().downcast_ref::<StructArray>().unwrap();
            let px = polygon
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let py = polygon
                .column(1)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            output.append_value(contains(
                px.values(),
                py.values(),
                xs.value(row),
                ys.value(row),
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

pub(crate) fn st_contains_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(StContains(Signature::exact(
        vec![polygon_type(), point_type()],
        Volatility::Immutable,
    )))
}

fn contains(xs: &[f32], ys: &[f32], x: f32, y: f32) -> bool {
    let mut inside = false;
    for index in 0..xs.len().saturating_sub(1) {
        let (ax, ay) = (xs[index], ys[index]);
        let (bx, by) = (xs[index + 1], ys[index + 1]);
        let cross = (x - ax) * (by - ay) - (y - ay) * (bx - ax);
        if cross.abs() <= 1e-6
            && x >= ax.min(bx)
            && x <= ax.max(bx)
            && y >= ay.min(by)
            && y <= ay.max(by)
        {
            return true;
        }
        if (ay > y) != (by > y) && x < (bx - ax) * (y - ay) / (by - ay) + ax {
            inside = !inside;
        }
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn polygon_sql_preserves_declared_list_field_and_nulls() {
        let context = datafusion::execution::context::SessionContext::new();
        for name in ["polygon", "st_polygon"] {
            context.register_udf(polygon_udf(name));
            let batches = context
                .sql(&format!(
                    "SELECT {name}(shape) AS area FROM \
                     (VALUES ('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), (NULL)) AS input(shape)"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let polygons = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(polygons.data_type(), &polygon_type());
            assert_eq!(polygons.value_length(0), 5);
            assert!(polygons.is_null(1));
        }
    }

    #[test]
    fn polygon_validation_and_boundary_rule() {
        let polygon = parse_polygon("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))").unwrap();
        let (xs, ys): (Vec<_>, Vec<_>) = polygon.into_iter().unzip();
        assert!(contains(&xs, &ys, 0.5, 0.5));
        assert!(contains(&xs, &ys, 0.0, 0.5));
        assert!(!contains(&xs, &ys, 1.5, 0.5));
        assert!(parse_polygon("0 0, 1 0, 0 1").is_err());
    }
}
