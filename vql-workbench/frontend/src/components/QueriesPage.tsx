import {
  Code2,
  Copy,
  FileClock,
  LoaderCircle,
  RefreshCw,
  Search,
  Square,
  SquareTerminal,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { asProblem } from "../lib/api";
import { cn } from "../lib/cn";
import {
  canStopQuery,
  filterQueries,
  formatQueryTime,
  queryStatement,
  readQueries,
  readQueryDetails,
  type PersistentQuery,
  type QueryDetails,
} from "../lib/queries";
import type { QueryResult, WorkbenchProblem } from "../lib/types";
import { ProblemPanel } from "./ProblemPanel";
import { Button } from "./ui/button";
import { Dialog, DialogContent } from "./ui/dialog";

export function QueriesPage({
  connected,
  active = true,
  connectionProblem,
  request,
  onConnect,
  onLoadSql,
  busy = false,
}: {
  connected: boolean;
  active?: boolean;
  connectionProblem: WorkbenchProblem | null;
  request: (sql: string, signal: AbortSignal) => Promise<QueryResult>;
  onConnect: () => void;
  onLoadSql: (sql: string, name: string) => void;
  busy?: boolean;
}) {
  const [queries, setQueries] = useState<PersistentQuery[] | null>(null);
  const [search, setSearch] = useState("");
  const [problem, setProblem] = useState<WorkbenchProblem | null>(null);
  const [operating, setOperating] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [selected, setSelected] = useState<PersistentQuery | null>(null);
  const [details, setDetails] = useState<QueryDetails | null>(null);
  const [detailProblem, setDetailProblem] = useState<WorkbenchProblem | null>(
    null,
  );
  const [copied, setCopied] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const pending = useRef<Promise<void> | null>(null);
  const selectedRef = useRef(selected);
  selectedRef.current = selected;
  const searchRef = useRef<HTMLInputElement>(null);
  const visibleQueries = useMemo(
    () => filterQueries(queries ?? [], search),
    [queries, search],
  );
  const running =
    queries?.filter((query) => query.state === "RUNNING").length ?? 0;

  const operate = useCallback(
    (task: (signal: AbortSignal) => Promise<void>, inDrawer = false) => {
      if (controller.current) return;
      const operation = new AbortController();
      controller.current = operation;
      setOperating(true);
      if (inDrawer) setDetailProblem(null);
      else setProblem(null);
      const completion = (async () => {
        try {
          await task(operation.signal);
        } catch (error) {
          if (!operation.signal.aborted) {
            (inDrawer ? setDetailProblem : setProblem)(asProblem(error));
          }
        } finally {
          if (controller.current === operation) {
            controller.current = null;
            pending.current = null;
            setOperating(false);
          }
        }
      })();
      pending.current = completion;
      return completion;
    },
    [],
  );

  const refresh = useCallback(
    async (signal: AbortSignal) => {
      const result = await request("SHOW JOBS;", signal);
      signal.throwIfAborted();
      setQueries(readQueries(result));
      setUpdatedAt(Date.now());
    },
    [request],
  );

  useEffect(() => {
    if (!connected) {
      setQueries(null);
      setSelected(null);
      setDetails(null);
      setUpdatedAt(null);
      return;
    }
    if (!active) return;
    let cancelled = false;
    void (async () => {
      await pending.current;
      if (!cancelled) void operate(refresh);
    })();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible" && !selectedRef.current) {
        void operate(refresh);
      }
    }, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
      controller.current?.abort();
    };
  }, [active, connected, operate, refresh]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (
        (event.metaKey || event.ctrlKey) &&
        event.key.toLowerCase() === "k" &&
        !selectedRef.current
      ) {
        event.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const showSql = (query: PersistentQuery) => {
    if (controller.current) return;
    setSelected(query);
    setDetails(null);
    setCopied(false);
    void operate(async (signal) => {
      const result = await request(
        queryStatement("DESCRIBE", query.queryId),
        signal,
      );
      signal.throwIfAborted();
      setDetails(readQueryDetails(result));
    }, true);
  };

  const stopQuery = (query: PersistentQuery) => {
    if (!canStopQuery(query.state)) return;
    void operate(async (signal) => {
      await request(queryStatement("STOP", query.queryId), signal);
      await refresh(signal);
    });
  };

  const copySql = async () => {
    if (!details) return;
    try {
      await navigator.clipboard.writeText(details.sql);
      setCopied(true);
    } catch (error) {
      setDetailProblem(asProblem(error));
    }
  };

  return (
    <main
      className="min-h-0 flex-1 overflow-y-auto bg-canvas"
      aria-label="Persistent jobs"
    >
      <div className="mx-auto flex max-w-7xl flex-col gap-6 p-5 md:p-8">
        <header className="border-b border-hairline pb-5">
          <p className="mb-3 flex items-center gap-2 font-mono text-[11px] text-muted">
            <span>VisionQL</span>
            <span>/</span>
            <span>Workspace</span>
            <span>/</span>
            <span className="font-semibold text-ink">Jobs</span>
          </p>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <h1 className="text-[24px] font-semibold tracking-tight">Jobs</h1>
              <p className="mt-1 text-[13px] leading-6 text-body">
                Persistent streaming writes via{" "}
                <code className="rounded border border-hairline bg-surface-raised px-1.5 py-0.5 font-mono text-[12px] text-ink">
                  SHOW JOBS
                </code>
                .
              </p>
            </div>
            <div className="flex items-center gap-2">
              {queries ? (
                <span className="flex items-center gap-2 rounded-full border border-hairline bg-surface px-3 py-1.5 font-mono text-[11px] text-body">
                  <span
                    className={cn(
                      "size-1.5 rounded-full",
                      running ? "bg-success" : "bg-muted",
                    )}
                  />
                  {running} running · {queries.length} total
                </span>
              ) : null}
              <Button
                size="icon"
                disabled={!connected || operating || busy}
                onClick={() => void operate(refresh)}
                aria-label="Refresh jobs"
              >
                <RefreshCw
                  size={15}
                  className={operating ? "animate-spin" : ""}
                />
              </Button>
            </div>
          </div>
        </header>
        <div className="relative">
          <Search
            size={17}
            className="pointer-events-none absolute left-3.5 top-1/2 -translate-y-1/2 text-muted"
          />
          <input
            ref={searchRef}
            type="search"
            aria-label="Search jobs"
            placeholder="Search jobs by name, ID, or status..."
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            className="h-11 w-full rounded-lg border border-hairline bg-surface pl-10 pr-16 text-[13px] placeholder:text-muted focus:border-hairline-strong"
          />
          <kbd className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 rounded border border-hairline bg-canvas px-1.5 py-0.5 font-mono text-[10px] text-muted">
            ⌘K
          </kbd>
        </div>
        {(connected ? problem : connectionProblem) ? (
          <div role="alert">
            <ProblemPanel
              problem={(connected ? problem : connectionProblem)!}
            />
          </div>
        ) : null}
        {!connected ? (
          <EmptyState
            title="Connect to view persistent jobs"
            description="Choose a vqld endpoint in Settings to inspect and stop its registered jobs."
          >
            <Button onClick={onConnect}>Open Settings</Button>
          </EmptyState>
        ) : queries == null && !problem ? (
          <div
            role="status"
            className="flex items-center justify-center gap-2 py-16 text-[13px] text-muted"
          >
            <LoaderCircle size={17} className="animate-spin" />
            Loading jobs…
          </div>
        ) : queries?.length === 0 ? (
          <EmptyState
            title="No registered jobs"
            description="Submit a persistent streaming write with SUBMIT QUERY in the SQL editor."
          />
        ) : queries && visibleQueries.length === 0 ? (
          <EmptyState
            title="No matching jobs"
            description="Try another name, Job ID, or status."
          >
            <Button size="sm" onClick={() => setSearch("")}>
              Clear search
            </Button>
          </EmptyState>
        ) : queries ? (
          <div
            className="overflow-hidden rounded-lg border border-hairline bg-surface"
            aria-busy={operating}
          >
            {visibleQueries.map((query) => (
              <article
                key={query.queryId}
                aria-label={query.name}
                className="row-enter flex flex-col justify-between gap-3 border-b border-hairline p-4 transition-colors last:border-0 hover:bg-canvas-soft lg:flex-row lg:items-center"
              >
                <div className="flex min-w-0 items-start gap-3.5">
                  <span
                    className={cn(
                      "mt-2 size-2 shrink-0 rounded-full",
                      stateColor(query.state),
                    )}
                    aria-hidden="true"
                  />
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
                      <button
                        className="break-all text-left font-mono text-[14px] font-semibold transition-colors hover:text-accent disabled:pointer-events-none"
                        disabled={operating || busy}
                        onClick={() => showSql(query)}
                        aria-label={`Inspect ${query.name}`}
                      >
                        {query.name}
                      </button>
                      <span className="break-all font-mono text-[11px] text-muted">
                        #{query.queryId}
                      </span>
                      <StateBadge state={query.state} />
                    </div>
                    <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 font-mono text-[11px] leading-5 text-muted">
                      <span title={formatQueryTime(query.startedAt)}>
                        {query.startedAt == null
                          ? "Not started"
                          : `Started ${formatQueryTime(query.startedAt)}`}
                      </span>
                      <span className="text-body">
                        Source: {query.sourceHealth ?? "—"}
                      </span>
                      <span>Restart gaps: {query.restartGapCount ?? "—"}</span>
                      <span>
                        Last event: {formatQueryTime(query.lastEventTime)}
                      </span>
                    </div>
                    {query.errorCode || query.errorMessage ? (
                      <p className="mt-2 break-words font-mono text-[11px] leading-5 text-danger">
                        {[query.errorCode, query.errorMessage]
                          .filter(Boolean)
                          .join(" · ")}
                      </p>
                    ) : null}
                  </div>
                </div>
                <div className="flex shrink-0 items-center justify-end gap-2">
                  <Button
                    size="sm"
                    className="font-mono text-[11px] shadow-none"
                    disabled={operating || busy}
                    onClick={() => showSql(query)}
                  >
                    <Code2 size={14} />
                    Show SQL
                  </Button>
                  <Button
                    size="sm"
                    className="font-mono text-[11px] text-danger shadow-none hover:border-danger/25 hover:bg-danger/5 hover:text-danger"
                    disabled={operating || busy || !canStopQuery(query.state)}
                    onClick={() => stopQuery(query)}
                  >
                    <Square size={12} />
                    Stop
                  </Button>
                </div>
              </article>
            ))}
          </div>
        ) : null}
        <footer className="flex flex-wrap items-center justify-between gap-3 px-1 font-mono text-[11px] text-muted">
          <span role="status">
            {queries
              ? `Showing ${visibleQueries.length} of ${queries.length} registered jobs`
              : "Registered jobs"}
          </span>
          <div className="flex flex-wrap items-center gap-4">
            {updatedAt ? (
              <span title={formatQueryTime(updatedAt)}>
                {problem ? "Last successful refresh" : "Updated"}{" "}
                {new Date(updatedAt).toLocaleTimeString()}
              </span>
            ) : null}
            <span className="flex items-center gap-1.5">
              <span
                className={cn(
                  "size-1.5 rounded-full",
                  connected ? "bg-success" : "bg-muted",
                )}
              />
              {connected ? "Connected: vqld (Flight SQL)" : "Disconnected"}
            </span>
          </div>
        </footer>
      </div>
      <Dialog
        open={selected != null}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent
          side
          title="Job SQL and details"
          description="Inspect the server's redacted SQL definition and persistent job state."
          className="flex max-w-[680px] flex-col"
        >
          <header className="border-b border-hairline bg-canvas-soft p-5 pr-14">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-muted">
              Job definition
            </p>
            <h2 className="mt-1 break-all font-mono text-[15px] font-semibold">
              {selected?.name}
            </h2>
            <p className="mt-1 break-all font-mono text-[11px] text-muted">
              {selected?.queryId}
            </p>
          </header>
          <div className="min-h-0 flex-1 space-y-5 overflow-y-auto p-5">
            {detailProblem ? (
              <div role="alert">
                <ProblemPanel problem={detailProblem} />
              </div>
            ) : null}
            {details ? (
              <>
                <div className="flex items-center justify-between gap-3">
                  <StateBadge state={details.state} />
                  <Button size="sm" onClick={() => void copySql()}>
                    <Copy size={13} />
                    {copied ? "Copied" : "Copy SQL"}
                  </Button>
                </div>
                <section>
                  <h3 className="mb-2 text-[12px] font-semibold">SQL</h3>
                  <pre
                    className="overflow-x-auto whitespace-pre-wrap break-words rounded-lg border border-hairline bg-canvas-soft p-4 font-mono text-[12px] leading-6"
                    aria-label="Job SQL"
                  >
                    {details.sql}
                  </pre>
                  <p className="mt-2 text-[11px] leading-5 text-muted">
                    String literals are redacted by vqld. Replace the redacted
                    values before submitting a new job.
                  </p>
                </section>
                <dl className="divide-y divide-hairline font-mono text-[11px]">
                  {[
                    ["Created", formatQueryTime(details.createdAt)],
                    ["Started", formatQueryTime(details.startedAt)],
                    ["Updated", formatQueryTime(details.updatedAt)],
                    ["Last restart", formatQueryTime(details.lastRestartAt)],
                    [
                      "Restart gap started",
                      formatQueryTime(details.restartGapStartedAt),
                    ],
                    [
                      "Restart gap ended",
                      formatQueryTime(details.restartGapEndedAt),
                    ],
                    [
                      "Window state reset",
                      details.resetWindowState == null
                        ? "—"
                        : details.resetWindowState
                          ? "Yes"
                          : "No",
                    ],
                  ].map(([label, value]) => (
                    <div
                      key={label}
                      className="flex flex-wrap justify-between gap-2 py-2.5"
                    >
                      <dt className="text-muted">{label}</dt>
                      <dd className="text-body">{value}</dd>
                    </div>
                  ))}
                </dl>
                {details.errorCode || details.errorMessage ? (
                  <p className="break-words font-mono text-[11px] leading-5 text-danger">
                    {[details.errorCode, details.errorMessage]
                      .filter(Boolean)
                      .join(" · ")}
                  </p>
                ) : null}
              </>
            ) : operating ? (
              <div
                role="status"
                className="flex items-center justify-center gap-2 py-12 text-[12px] text-muted"
              >
                <LoaderCircle size={16} className="animate-spin" />
                Loading job definition…
              </div>
            ) : null}
          </div>
          <footer className="flex justify-end border-t border-hairline bg-canvas-soft px-5 py-3">
            <Button
              disabled={!details || operating || busy}
              onClick={() => {
                if (details) onLoadSql(details.sql, details.name);
              }}
            >
              <SquareTerminal size={14} />
              Load SQL as new draft
            </Button>
          </footer>
        </DialogContent>
      </Dialog>
    </main>
  );
}

function StateBadge({ state }: { state: string }) {
  return (
    <span
      className={cn(
        "rounded border px-2 py-0.5 font-mono text-[10px] font-semibold tracking-wider",
        state === "RUNNING"
          ? "border-success/20 bg-success/10 text-success"
          : state === "FAILED"
            ? "border-danger/20 bg-danger/10 text-danger"
            : state === "STARTING"
              ? "border-info/20 bg-info/10 text-info"
              : "border-hairline bg-canvas text-body",
      )}
    >
      {state}
    </span>
  );
}

function stateColor(state: string) {
  return state === "RUNNING"
    ? "bg-success"
    : state === "FAILED"
      ? "bg-danger"
      : state === "STARTING"
        ? "bg-info"
        : "bg-muted";
}

function EmptyState({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center gap-3 rounded-lg border border-hairline bg-surface px-5 py-14 text-center">
      <FileClock size={25} className="mb-1 text-muted" />
      <h2 className="text-[15px] font-semibold">{title}</h2>
      <p className="max-w-md text-[12px] leading-5 text-muted">{description}</p>
      {children}
    </div>
  );
}
