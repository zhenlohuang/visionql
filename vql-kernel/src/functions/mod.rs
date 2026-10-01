mod ai;
mod factory;
mod image_encoding;
mod python_udf;
mod tumble;
mod vision;

pub(crate) use ai::{BuiltinAiFunction, builtin_ai_udf};
pub(crate) use factory::{VqlFunctionFactory, VqlTypePlanner};
pub(crate) use image_encoding::{
    MaterializedBatch, materialize_batch_images, materialize_encoded_images,
};
pub(crate) use python_udf::python_function_udf;
pub(crate) use tumble::tumble_udf;
pub(crate) use vision::{box_center_udf, polygon_udf, st_contains_udf};

/// Release-owned functions shared by Session registration and documentation.
pub(crate) fn builtin_udfs() -> crate::Result<Vec<datafusion::logical_expr::ScalarUDF>> {
    let mut functions = vec![
        box_center_udf(),
        polygon_udf("polygon"),
        polygon_udf("st_polygon"),
        st_contains_udf(),
        tumble_udf(),
    ];
    for function in BuiltinAiFunction::ALL {
        functions.push(builtin_ai_udf(function)?);
    }
    Ok(functions)
}
