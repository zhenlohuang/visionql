import { AlertCircle, Cable, CircleSlash2, ShieldAlert } from "lucide-react";

import type { WorkbenchProblem } from "../lib/types";

export function ProblemPanel({ problem }: { problem: WorkbenchProblem }) {
  const Icon =
    problem.source === "vql"
      ? AlertCircle
      : problem.source === "connectivity"
        ? Cable
        : problem.source === "policy"
          ? ShieldAlert
          : CircleSlash2;
  return (
    <div className="panel-enter m-auto w-[calc(100%-32px)] max-w-[720px] overflow-hidden rounded-xl border border-[#efced6] bg-surface shadow-sm">
      <div className="flex items-start gap-3 border-b border-[#f1dce1] bg-[#fff7f8] px-4 py-3.5">
        <span className="mt-0.5 flex size-8 shrink-0 items-center justify-center rounded-lg bg-[#fce8ed] text-danger">
          <Icon size={16} />
        </span>
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-[14px] font-semibold text-ink">
              {problem.title}
            </h3>
            <span className="rounded bg-surface px-1.5 py-0.5 font-mono text-[9px] uppercase tracking-[0.08em] text-danger ring-1 ring-[#efced6]">
              {problem.source}
            </span>
          </div>
          <p className="mt-1 text-[12px] leading-5 text-body">
            {problem.message}
          </p>
        </div>
      </div>
      {problem.code ? (
        <div className="flex flex-wrap items-center gap-x-5 gap-y-1 px-4 py-3 font-mono text-[10px] text-muted">
          <span>
            Code <strong className="text-ink">{problem.code}</strong>
          </span>
          {problem.symbol ? (
            <span>
              Symbol <strong className="text-ink">{problem.symbol}</strong>
            </span>
          ) : null}
          {problem.targetVersion ? (
            <span>
              Target{" "}
              <strong className="text-ink">{problem.targetVersion}</strong>
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
