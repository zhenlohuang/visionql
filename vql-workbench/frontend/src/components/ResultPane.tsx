import {
  Braces,
  CheckCircle2,
  Columns3,
  LoaderCircle,
  Rows3,
  Sparkles,
} from "lucide-react";
import { useEffect, useState } from "react";

import type {
  ExecutionState,
  OverlayConfig,
  QueryResult,
  ResultRow,
} from "../lib/types";
import { InspectorDrawer } from "./InspectorDrawer";
import { OverlayDialog } from "./OverlayDialog";
import { ProblemPanel } from "./ProblemPanel";
import { ResultsTable } from "./ResultsTable";

export function ResultPane({
  execution,
  overlay,
  onOverlayChange,
}: {
  execution: ExecutionState;
  overlay: OverlayConfig;
  onOverlayChange: (overlay: OverlayConfig) => void;
}) {
  const [view, setView] = useState<"table" | "json">("table");
  const [selectedRow, setSelectedRow] = useState<ResultRow | null>(null);
  useEffect(() => setSelectedRow(null), [execution.executionId]);
  const busy = ["preparing", "running", "cancelling"].includes(execution.phase);
  const elapsed =
    execution.elapsedMs ??
    (execution.startedAt ? Date.now() - execution.startedAt : null);
  return (
    <section
      className="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface"
      aria-label="Query results"
    >
      <header className="flex min-h-[54px] shrink-0 flex-wrap items-center justify-between gap-3 border-b border-hairline bg-canvas px-4 py-2 sm:h-[54px] sm:py-0">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <h2 className="text-[15px] font-semibold tracking-[-0.01em] text-ink">
              Query results
            </h2>
            {execution.phase !== "idle" &&
            execution.phase !== "disconnected" ? (
              <span className="inline-flex items-center gap-1.5 rounded-full bg-surface-raised px-2 py-1 font-mono text-[10px] text-body">
                <span
                  className={`size-1.5 rounded-full ${
                    busy
                      ? "animate-pulse bg-info"
                      : execution.problem
                        ? "bg-danger"
                        : "bg-success"
                  }`}
                />
                {busy
                  ? execution.phase
                  : elapsed != null
                    ? formatDuration(elapsed)
                    : execution.phase}
              </span>
            ) : null}
          </div>
          {execution.result ? (
            <div className="flex rounded-md border border-hairline bg-surface-raised/55 p-0.5">
              <ViewButton
                active={view === "table"}
                onClick={() => setView("table")}
              >
                <Rows3 size={13} /> Table
              </ViewButton>
              <ViewButton
                active={view === "json"}
                onClick={() => setView("json")}
              >
                <Braces size={13} /> JSON
              </ViewButton>
            </div>
          ) : null}
        </div>
        {execution.result?.fields.some(
          (field) => field.extensionName === "vql.image",
        ) ? (
          <div className="w-full sm:w-auto">
            <OverlayDialog
              fields={execution.result.fields}
              value={overlay}
              onChange={onOverlayChange}
            />
          </div>
        ) : null}
      </header>
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {execution.phase === "running" && execution.problem ? (
          <div
            role="alert"
            className="border-b border-[#efced6] bg-[#fff7f8] px-4 py-2 text-[11px] text-danger"
          >
            {execution.problem.message} The execution is still active; retry
            Cancel.
          </div>
        ) : null}
        {execution.phase !== "running" && execution.problem ? (
          <ProblemPanel problem={execution.problem} />
        ) : execution.result ? (
          view === "table" ? (
            <ResultsTable
              result={execution.result}
              overlay={overlay}
              selectedRowId={selectedRow?.id ?? null}
              onSelectRow={setSelectedRow}
            />
          ) : (
            <pre className="min-h-0 flex-1 overflow-auto bg-[#fbfaf6] p-5 font-mono text-[11px] leading-5 text-body">
              {jsonResult(execution.result)}
            </pre>
          )
        ) : execution.phase === "preparing" ? (
          <ProgressState
            icon={
              <LoaderCircle size={20} className="animate-spin text-accent" />
            }
            title="Preparing statement"
            detail="vqld is classifying the statement and returning Arrow schema metadata."
          />
        ) : execution.phase === "running" ||
          execution.phase === "cancelling" ? (
          <ProgressState
            icon={<LoaderCircle size={20} className="animate-spin text-info" />}
            title={
              execution.phase === "cancelling"
                ? "Cancelling execution"
                : "Waiting for Arrow batches"
            }
            detail={
              execution.resultMode === "unbounded"
                ? "Attached stream preview keeps the latest 500 rows in browser memory."
                : "Record batches appear here as they arrive."
            }
          />
        ) : execution.affectedRows != null ? (
          <ProgressState
            icon={<CheckCircle2 size={21} className="text-success" />}
            title="Statement completed"
            detail={`${execution.affectedRows} row${execution.affectedRows === 1 ? "" : "s"} affected.`}
          />
        ) : (
          <ProgressState
            icon={<Sparkles size={20} className="text-accent" />}
            title="Run a visual query"
            detail="Arrow results, typed values, thumbnails, and overlays will appear here."
          />
        )}
      </div>
      <InspectorDrawer
        row={selectedRow}
        overlay={overlay}
        onOpenChange={(open) => !open && setSelectedRow(null)}
      />
    </section>
  );
}

function ViewButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex h-7 items-center gap-1.5 rounded px-2.5 text-[11px] font-medium transition-colors ${
        active ? "bg-surface text-ink shadow-sm" : "text-muted hover:text-ink"
      }`}
    >
      {children}
    </button>
  );
}

function ProgressState({
  icon,
  title,
  detail,
}: {
  icon: React.ReactNode;
  title: string;
  detail: string;
}) {
  return (
    <div className="m-auto flex max-w-sm flex-col items-center px-6 py-12 text-center">
      <span className="flex size-11 items-center justify-center rounded-xl border border-hairline bg-canvas-soft shadow-sm">
        {icon}
      </span>
      <h3 className="mt-3 text-[13px] font-semibold text-ink">{title}</h3>
      <p className="mt-1 max-w-xs text-[11px] leading-5 text-muted">{detail}</p>
    </div>
  );
}

function formatDuration(milliseconds: number) {
  return milliseconds < 1000
    ? `${milliseconds}ms`
    : `${(milliseconds / 1000).toFixed(1)}s`;
}

function jsonResult(result: QueryResult): string {
  return JSON.stringify(
    result.rows.map((row) => row.values),
    (_, value) => {
      if (typeof value === "bigint") return value.toString();
      if (value instanceof Uint8Array) return `<${value.byteLength} bytes>`;
      return value;
    },
    2,
  );
}
