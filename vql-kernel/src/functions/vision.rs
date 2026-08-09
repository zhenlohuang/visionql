use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Float32Array, Float32Builder, Float64Array, Int64Builder, ListArray,
    ListBuilder, StringArray, StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::models::detections_type;
use crate::types::box2d_field;

#[derive(Debug, PartialEq, Eq, Hash)]
struct CountObjects(Signature);

impl ScalarUDFImpl for CountObjects {
    fn name(&self) -> &str {
        "count_objects"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let detections = arrays[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "COUNT_OBJECTS expects detections".to_owned(),
                )
            })?;
        let labels = arrays[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "COUNT_OBJECTS label must be STRING".to_owned(),
                )
            })?;
        let confidences = arrays[2]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "COUNT_OBJECTS confidence must be DOUBLE".to_owned(),
                )
            })?;
        let mut output = Int64Builder::with_capacity(detections.len());
        for row in 0..detections.len() {
            if detections.is_null(row) || labels.is_null(row) || confidences.is_null(row) {
                output.append_null();
                continue;
            }
            let values = detections.value(row);
            let values = values
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("detection list contains structs");
            let det_labels = values
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("detection label is Utf8");
            let det_confidences = values
                .column(1)
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("detection confidence is Float32");
            let count = (0..values.len())
                .filter(|index| {
                    !values.is_null(*index)
                        && det_labels.value(*index) == labels.value(row)
                        && f64::from(det_confidences.value(*index)) >= confidences.value(row)
                })
                .count();
            output.append_value(count as i64);
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

pub(crate) fn count_objects_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CountObjects(Signature::exact(
        vec![detections_type(), DataType::Utf8, DataType::Float64],
        Volatility::Immutable,
    )))
}

pub(crate) fn point_fields() -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("x", DataType::Float32, false)),
        Arc::new(Field::new("y", DataType::Float32, false)),
    ])
}

pub(crate) fn point_type() -> DataType {
    DataType::Struct(point_fields())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BoxCenter(Signature);

impl ScalarUDFImpl for BoxCenter {
    fn name(&self) -> &str {
        "box_center"
    }
    fn signature(&self) -> &Signature {
        &self.0
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
        let mut builder = ListBuilder::new(point_builder);
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct StContains(Signature);
impl ScalarUDFImpl for StContains {
    fn name(&self) -> &str {
        "st_contains"
    }
    fn signature(&self) -> &Signature {
        &self.0
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
