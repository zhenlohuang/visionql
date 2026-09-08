import * as DialogPrimitive from "@radix-ui/react-dialog";
import {
  Check,
  Clipboard,
  History,
  PencilLine,
  Play,
  Search,
  Trash2,
  XCircle,
} from "lucide-react";
import { useMemo, useState } from "react";

import { cn } from "../lib/cn";
import type { HistoryRecord } from "../lib/types";
import { Button } from "./ui/button";
import { Dialog, DialogContent } from "./ui/dialog";

export function HistoryDrawer({
  open,
  onOpenChange,
  history,
  onLoad,
  onRerun,
  onClear,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  history: HistoryRecord[];
  onLoad: (record: HistoryRecord) => void;
  onRerun: (record: HistoryRecord) => void;
  onClear: () => void;
}) {
  const [search, setSearch] = useState("");
  const [copied, setCopied] = useState<string | null>(null);
  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return history;
    return history.filter(
      (record) =>
        record.draftName.toLowerCase().includes(needle) ||
        record.sql.toLowerCase().includes(needle) ||
        record.problem?.message.toLowerCase().includes(needle),
    );
  }, [history, search]);
  const copy = async (record: HistoryRecord) => {
    await navigator.clipboard.writeText(record.sql);
    setCopied(record.id);
    window.setTimeout(() => setCopied(null), 1200);
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        title="Execution history"
        description="Local SQL execution records without results, credentials, or thumbnails."
        side
        className="flex flex-col"
      >
        <header className="flex shrink-0 items-center gap-3 border-b border-hairline bg-canvas-soft px-5 py-4 pr-14">
          <span className="flex size-9 items-center justify-center rounded-lg border border-[#f2c7b6] bg-[#fff0e9] text-accent">
            <History size={17} />
          </span>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-[16px] font-semibold text-ink">
                Execution history
              </h2>
              <span className="rounded-full bg-surface-raised px-2 py-0.5 font-mono text-[10px] text-body">
                {history.length}
              </span>
            </div>
            <p className="mt-0.5 text-[10px] uppercase tracking-[0.11em] text-muted">
              Local Workbench profile · up to 500 runs
            </p>
          </div>
        </header>
        <div className="shrink-0 border-b border-hairline bg-surface px-5 py-3">
          <label className="flex h-9 items-center gap-2 rounded-md border border-hairline bg-canvas-soft px-3 focus-within:border-accent">
            <Search size={14} className="text-muted" />
            <span className="sr-only">Search execution history</span>
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search SQL or draft name"
              className="min-w-0 flex-1 bg-transparent text-[12px] text-ink outline-none placeholder:text-muted"
            />
          </label>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto bg-surface">
          {filtered.length ? (
            <div className="divide-y divide-hairline">
              {filtered.map((record) => (
                <article
                  key={record.id}
                  className="group space-y-2.5 border-l-2 border-l-transparent px-5 py-4 transition-colors hover:border-l-accent hover:bg-canvas-soft"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex min-w-0 items-center gap-2">
                      <StateDot state={record.state} />
                      <span className="truncate font-mono text-[12px] font-semibold text-ink">
                        {record.draftName}
                      </span>
                      <span className="rounded bg-surface-raised px-1.5 py-0.5 font-mono text-[9px] uppercase text-muted">
                        {record.resultMode ?? record.state}
                      </span>
                    </div>
                    <time className="shrink-0 font-mono text-[9px] text-muted">
                      {relativeTime(record.startedAt)}
                    </time>
                  </div>
                  <pre className="line-clamp-4 overflow-hidden whitespace-pre-wrap rounded-md border border-hairline bg-surface px-3 py-2 font-mono text-[10px] leading-4 text-body">
                    {record.sql}
                  </pre>
                  {record.problem ? (
                    <p className="flex items-start gap-1.5 text-[10px] leading-4 text-danger">
                      <XCircle size={12} className="mt-0.5 shrink-0" />
                      <span className="line-clamp-2">
                        {record.problem.message}
                      </span>
                    </p>
                  ) : null}
                  <div className="flex items-center justify-between gap-3">
                    <p className="font-mono text-[9px] text-muted">
                      {record.rowCount != null
                        ? `${record.rowCount} rows · `
                        : ""}
                      {record.elapsedMs != null
                        ? formatDuration(record.elapsedMs)
                        : "in progress"}
                    </p>
                    <div className="flex items-center gap-1 opacity-75 transition-opacity group-hover:opacity-100">
                      <Button
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => onLoad(record)}
                      >
                        <PencilLine size={12} /> Load
                      </Button>
                      <Button
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => onRerun(record)}
                      >
                        <Play size={12} /> Re-run
                      </Button>
                      <Button
                        size="icon"
                        variant="ghost"
                        className="size-7"
                        onClick={() => void copy(record)}
                        aria-label="Copy SQL"
                      >
                        {copied === record.id ? (
                          <Check size={13} />
                        ) : (
                          <Clipboard size={13} />
                        )}
                      </Button>
                    </div>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="flex h-full min-h-52 flex-col items-center justify-center px-8 text-center">
              <History size={22} className="text-hairline-strong" />
              <p className="mt-3 text-[13px] font-medium text-ink">
                No matching executions
              </p>
              <p className="mt-1 text-[11px] text-muted">
                Run SQL to build local history.
              </p>
            </div>
          )}
        </div>
        <footer className="flex shrink-0 items-center justify-between border-t border-hairline bg-canvas-soft px-5 py-3">
          <Button
            variant="ghost"
            size="sm"
            className="text-danger hover:text-danger"
            disabled={!history.length}
            onClick={() => {
              if (window.confirm("Clear all local execution history?"))
                onClear();
            }}
          >
            <Trash2 size={13} /> Clear history
          </Button>
          <DialogPrimitive.Close asChild>
            <Button size="sm">Close</Button>
          </DialogPrimitive.Close>
        </footer>
      </DialogContent>
    </Dialog>
  );
}

function StateDot({ state }: { state: HistoryRecord["state"] }) {
  const colors = {
    running: "bg-info animate-pulse",
    completed: "bg-success",
    cancelled: "bg-muted",
    failed: "bg-danger",
  };
  return (
    <span className={cn("size-2 shrink-0 rounded-full", colors[state])}>
      <span className="sr-only">{state}</span>
    </span>
  );
}

function relativeTime(timestamp: number): string {
  const seconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(timestamp).toLocaleDateString();
}

function formatDuration(milliseconds: number): string {
  return milliseconds < 1000
    ? `${milliseconds}ms`
    : `${(milliseconds / 1000).toFixed(2)}s`;
}
