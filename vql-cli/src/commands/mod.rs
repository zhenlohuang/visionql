pub(crate) mod shell;

use std::path::Path;

use vql_kernel::{
    ErrorCode, FrameDropReason, QueryInterruptAction, QueryResource, Result, Session, Statement,
    VqlError, split_statements,
};

pub(crate) fn install_interrupt_handler(session: Session) -> Result<()> {
    ctrlc::set_handler(move || match session.interrupt_active_query() {
        QueryInterruptAction::NoActiveQuery => {}
        QueryInterruptAction::GracefulStopRequested => {
            eprintln!("graceful stop requested; press Ctrl-C again to cancel immediately");
        }
        QueryInterruptAction::ImmediateCancellationRequested => {
            eprintln!("cancelling active query immediately");
        }
    })
    .map_err(|error| {
        VqlError::new(
            ErrorCode::Execution,
            format!("failed to install Ctrl-C handler: {error}"),
        )
    })
}

pub(crate) fn run_file(session: &Session, path: &Path, show_metrics: bool) -> Result<()> {
    let script = std::fs::read_to_string(path)?;
    install_interrupt_handler(session.clone())?;
    run_statements(session, split_statements(&script)?, show_metrics)
}

fn run_statements(session: &Session, statements: Vec<String>, show_metrics: bool) -> Result<()> {
    let statement_count = statements.len();
    for (index, sql) in statements.into_iter().enumerate() {
        let statement = session.sql(&sql)?;
        if statement.is_unbounded() {
            if index + 1 != statement_count {
                return Err(VqlError::new(
                    ErrorCode::InvalidSql,
                    "an unbounded statement must be last in a vql run script; split the script before this statement",
                ));
            }
            statement.for_each_batch(|batch| {
                super::render::print_batches(std::slice::from_ref(batch))
            })?;
        } else {
            super::render::print_batches(&statement.collect()?)?;
        }
        if show_metrics {
            print_metrics(&statement);
        }
    }
    Ok(())
}

pub(crate) fn print_metrics(statement: &Statement) {
    let Some(metrics) = statement.metrics() else {
        return;
    };
    let resource = |kind| {
        let usage = metrics.resource_usage(kind);
        serde_json::json!({
            "current_bytes": usage.current_bytes,
            "peak_bytes": usage.peak_bytes,
        })
    };
    let dropped_ranges = metrics
        .dropped_frame_ranges()
        .into_iter()
        .map(|range| {
            let reason = match range.reason {
                FrameDropReason::SourceOverrun => "source_overrun",
                FrameDropReason::ResourceBudget => "resource_budget",
                FrameDropReason::DecodeError => "decode_error",
            };
            serde_json::json!({
                "reason": reason,
                "count": range.count,
                "first_event_time_ms": range.first_event_time_ms,
                "last_event_time_ms": range.last_event_time_ms,
            })
        })
        .collect::<Vec<_>>();
    let total = metrics.total_resource_usage();
    eprintln!(
        "{}",
        serde_json::json!({
            "type": "visionql_query_metrics",
            "rows": {
                "input": metrics.input_rows(),
                "output": metrics.output_rows(),
                "errors": metrics.error_rows(),
                "late": metrics.late_rows(),
            },
            "decode_frames": metrics.decode_frames(),
            "inference": {
                "rows": metrics.inference_rows(),
                "batches": metrics.inference_batches(),
                "p50_ms": metrics.inference_p50_ms(),
                "p95_ms": metrics.inference_p95_ms(),
                "batch_histogram": metrics.batch_histogram(),
                "queue_p50_ms": metrics.model_queue_p50_ms(),
                "queue_p95_ms": metrics.model_queue_p95_ms(),
                "service_p50_ms": metrics.model_service_p50_ms(),
                "service_p95_ms": metrics.model_service_p95_ms(),
            },
            "source": {
                "generation": metrics.source_generation(),
                "reconnects": metrics.source_reconnects(),
                "event_time_fallbacks": metrics.event_time_fallbacks(),
                "sampled_frames": metrics.sampled_frames(),
                "sampled_fps": metrics.sampled_fps(),
                "input_bytes": metrics.source_input_bytes(),
                "input_bitrate_bps": metrics.input_bitrate_bps(),
                "gap_duration_ms": metrics.source_gap_duration_ms(),
                "dropped_frames": metrics.source_dropped_frames(),
                "dropped_ranges": dropped_ranges,
                "watermark_ms": metrics.watermark_ms(),
            },
            "window_state_bytes": metrics.window_state_bytes(),
            "sink_retries": metrics.sink_retries(),
            "latency": {
                "epoch_p50_ms": metrics.epoch_p50_ms(),
                "epoch_p95_ms": metrics.epoch_p95_ms(),
                "end_to_end_p50_ms": metrics.end_to_end_p50_ms(),
                "end_to_end_p95_ms": metrics.end_to_end_p95_ms(),
            },
            "resources": {
                "total": {"current_bytes": total.current_bytes, "peak_bytes": total.peak_bytes},
                "arrow": resource(QueryResource::Arrow),
                "media": resource(QueryResource::Media),
                "frame_buffer": resource(QueryResource::FrameBuffer),
                "model_tensor": resource(QueryResource::ModelTensor),
                "model_queue": resource(QueryResource::ModelQueue),
                "triton_payload": resource(QueryResource::TritonPayload),
                "window_state": resource(QueryResource::WindowState),
                "sink_buffer": resource(QueryResource::SinkBuffer),
                "device_memory": {
                    "available": false,
                    "current_bytes": metrics.resource_usage(QueryResource::DeviceMemory).current_bytes,
                    "peak_bytes": metrics.resource_usage(QueryResource::DeviceMemory).peak_bytes,
                },
            },
        })
    );
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use vql_kernel::{Engine, EngineConfig};

    use super::*;

    #[test]
    fn run_rejects_an_unbounded_statement_before_later_sql() {
        let temp = tempdir().unwrap();
        let engine = Engine::new(EngineConfig::new(temp.path().join("catalog.db"))).unwrap();
        let session = engine.session().build().unwrap();
        let statements = split_statements(
            "CREATE STREAM camera FROM 'rtsp://127.0.0.1/live';
             SELECT frame_id FROM camera;
             DROP STREAM camera;",
        )
        .unwrap();

        let error = run_statements(&session, statements, false).unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidSql);
        assert!(error.message.contains("must be last"));
        session.sql("DESCRIBE camera").unwrap();
    }
}
