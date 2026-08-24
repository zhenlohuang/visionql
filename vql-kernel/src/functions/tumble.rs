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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    #[test]
    fn buckets_timestamps_with_euclidean_rounding_and_preserves_nulls() {
        let timestamps =
            TimestampMillisecondArray::from(vec![Some(7_800), None, Some(-1)]).with_timezone("UTC");
        let five_seconds = IntervalMonthDayNanoType::make_value(0, 0, 5_000_000_000);
        let intervals = IntervalMonthDayNanoArray::from(vec![
            Some(five_seconds),
            Some(five_seconds),
            Some(five_seconds),
        ]);

        let result = invoke_tumble(Arc::new(timestamps), Arc::new(intervals)).unwrap();

        assert_eq!(result.value(0), 5_000);
        assert!(result.is_null(1));
        assert_eq!(result.value(2), -5_000);
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
    }

    #[test]
    fn rejects_calendar_months_and_non_positive_intervals() {
        for (interval, expected) in [
            (
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                "TUMBLE interval cannot contain calendar months",
            ),
            (
                IntervalMonthDayNanoType::make_value(0, 0, 0),
                "TUMBLE interval must be positive",
            ),
            (
                IntervalMonthDayNanoType::make_value(0, 0, -1_000_000),
                "TUMBLE interval must be positive",
            ),
        ] {
            let error = invoke_tumble(
                Arc::new(TimestampMillisecondArray::from(vec![Some(1)])),
                Arc::new(IntervalMonthDayNanoArray::from(vec![Some(interval)])),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
    }

    fn invoke_tumble(
        timestamps: ArrayRef,
        intervals: ArrayRef,
    ) -> datafusion::common::Result<TimestampMillisecondArray> {
        let number_rows = timestamps.len();
        let timestamp_type = timestamps.data_type().clone();
        let result = tumble_udf().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(timestamps),
                ColumnarValue::Array(intervals),
            ],
            arg_fields: vec![
                Field::new("timestamp", timestamp_type.clone(), true).into(),
                Field::new(
                    "interval",
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    true,
                )
                .into(),
            ],
            number_rows,
            return_field: Field::new("tumble", timestamp_type, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let array = result.into_array(number_rows)?;
        Ok(array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("TUMBLE returns timestamp milliseconds")
            .clone())
    }
}
