use std::sync::Arc;

use arrow::array::types::IntervalMonthDayNanoType;
use arrow::array::{
    Array, IntervalMonthDayNanoArray, TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
struct Tumble(Signature);

impl ScalarUDFImpl for Tumble {
    fn name(&self) -> &str {
        "tumble"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let timestamps = arrays[0]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "TUMBLE expects TIMESTAMP(ms)".to_owned(),
                )
            })?;
        let intervals = arrays[1]
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("TUMBLE expects INTERVAL".to_owned())
            })?;
        let mut output = TimestampMillisecondBuilder::with_capacity(timestamps.len());
        for row in 0..timestamps.len() {
            if timestamps.is_null(row) || intervals.is_null(row) {
                output.append_null();
                continue;
            }
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(intervals.value(row));
            if months != 0 {
                return Err(datafusion::common::DataFusionError::Execution(
                    "TUMBLE interval cannot contain calendar months".to_owned(),
                ));
            }
            let width = i64::from(days)
                .checked_mul(86_400_000)
                .and_then(|value| value.checked_add(nanos / 1_000_000))
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "TUMBLE interval must be positive".to_owned(),
                    )
                })?;
            output.append_value(timestamps.value(row).div_euclid(width) * width);
        }
        let timezone = match timestamps.data_type() {
            DataType::Timestamp(_, timezone) => timezone.clone(),
            _ => None,
        };
        Ok(ColumnarValue::Array(Arc::new(
            output.finish().with_timezone_opt(timezone),
        )))
    }
}

pub(crate) fn tumble_udf() -> ScalarUDF {
    let intervals = DataType::Interval(IntervalUnit::MonthDayNano);
    ScalarUDF::new_from_impl(Tumble(Signature::one_of(
        vec![
            datafusion::logical_expr::TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                intervals.clone(),
            ]),
            datafusion::logical_expr::TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                intervals,
            ]),
        ],
        Volatility::Immutable,
    )))
}
