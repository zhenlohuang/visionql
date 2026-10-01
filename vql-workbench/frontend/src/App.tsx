import {
  BookOpen,
  Braces,
  Cable,
  CircleStop,
  FileClock,
  History,
  Menu,
  PanelLeftClose,
  Pencil,
  Play,
  Plus,
  RotateCw,
  Settings,
  Sparkles,
  SquareTerminal,
  WandSparkles,
  X,
} from "lucide-react";
import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";

import {
  asProblem,
  cancelExecution,
  closeSession,
  createSession,
  executeBoundedSql,
  getResultResponse,
  getSession,
  startExecution,
  waitForTerminalStatus,
  type SessionInput,
} from "./lib/api";
import {
  createDraft,
  loadDrafts,
  loadHistory,
  saveDrafts,
  saveHistory,
  upsertHistory,
} from "./lib/history";
import { executionReducer, initialExecutionState } from "./lib/reducer";
import { formatSql } from "./lib/sql";
import type {
  Draft,
  HistoryRecord,
  OverlayConfig,
  QueryResult,
  WorkbenchProblem,
} from "./lib/types";
import { HistoryDrawer } from "./components/HistoryDrawer";
import { Logo } from "./components/Logo";
import { JobsPage } from "./components/JobsPage";
import { ResultPane } from "./components/ResultPane";
import { RenameDraftDialog } from "./components/RenameDraftDialog";
import { SettingsDialog } from "./components/SettingsDialog";
import { SqlEditor, type SqlEditorHandle } from "./components/SqlEditor";
import { Button } from "./components/ui/button";
import { Tooltip, TooltipProvider } from "./components/ui/tooltip";
import { cn } from "./lib/cn";
import {
  executeCatalogStatement,
  type CatalogObject,
  type CatalogSection,
} from "./lib/catalog";
import { CatalogWorkspace } from "./components/CatalogWorkspace";
import { CatalogTree } from "./components/CatalogTree";
import { useCatalogNavigation } from "./lib/useCatalogNavigation";

type WorkspacePage = "editor" | "jobs" | CatalogSection;

const EMPTY_OVERLAY: OverlayConfig = {
  imageColumn: null,
  boxColumn: null,
  labelColumn: null,
  confidenceColumn: null,
};

export default function App() {
  const [execution, dispatch] = useReducer(
    executionReducer,
    initialExecutionState,
  );
  const [drafts, setDrafts] = useState<Draft[]>(loadDrafts);
  const [activeDraftId, setActiveDraftId] = useState(() => drafts[0]?.id ?? "");
  const [history, setHistory] = useState<HistoryRecord[]>(loadHistory);
  const [overlay, setOverlay] = useState<OverlayConfig>(EMPTY_OVERLAY);
  const [endpoint, setEndpoint] = useState("http://127.0.0.1:6031");
  const [historyOpen, setHistoryOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [renamingDraftId, setRenamingDraftId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [editorHeight, setEditorHeight] = useState(300);
  const [page, setPage] = useState<WorkspacePage>("editor");
  const [catalogBusy, setCatalogBusy] = useState(false);
  const [catalogNavigationBusy, setCatalogNavigationBusy] = useState(false);
  const [catalogNamespace, setCatalogNamespace] = useState<string | null>(null);
  const [catalogSelection, setCatalogSelection] = useState<{
    id: string;
    request: number;
    version: string | null;
  } | null>(null);
  const catalogSelectionRequest = useRef(0);
  const [managementBusy, setManagementBusy] = useState(false);
  const [sessionVersion, setSessionVersion] = useState(0);
  const editorRef = useRef<SqlEditorHandle>(null);
  const executionInFlight = useRef(false);
  const activeRunId = useRef<string | null>(null);
  const activeDraft =
    drafts.find((draft) => draft.id === activeDraftId) ?? drafts[0];
  const renamingDraft = drafts.find((draft) => draft.id === renamingDraftId);
  const executionBusy = ["preparing", "running", "cancelling"].includes(
    execution.phase,
  );
  const busy =
    catalogBusy || catalogNavigationBusy || managementBusy || executionBusy;
  const connected = execution.phase !== "disconnected";

  const requestManagementSql = useCallback(
    async (sql: string, signal: AbortSignal) => {
      if (!connected || executionInFlight.current) {
        throw new Error(
          "Wait for the active execution before starting another statement.",
        );
      }
      executionInFlight.current = true;
      setManagementBusy(true);
      try {
        return await executeBoundedSql(sql, signal);
      } catch (error) {
        const problem = asProblem(error);
        if (problem.httpStatus === 401)
          dispatch({ type: "connection_failed", problem });
        throw error;
      } finally {
        executionInFlight.current = false;
        setManagementBusy(false);
      }
    },
    [connected],
  );

  useEffect(() => {
    void getSession()
      .then((session) => {
        setEndpoint(session.endpoint);
        dispatch({ type: session.connected ? "connected" : "disconnected" });
      })
      .catch((error) => {
        dispatch({ type: "connection_failed", problem: asProblem(error) });
      });
  }, []);

  useEffect(() => saveDrafts(drafts), [drafts]);
  useEffect(() => saveHistory(history), [history]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (renamingDraftId) return;
      if (event.key === "Escape" && execution.phase === "running") {
        event.preventDefault();
        void cancelActive();
      }
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "t") {
        if (page !== "editor" || busy) return;
        event.preventDefault();
        addDraft();
      }
      if (event.altKey && event.shiftKey && event.key.toLowerCase() === "f") {
        if (page !== "editor") return;
        event.preventDefault();
        formatActiveSql();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  });

  const updateDraft = (sql: string) => {
    setDrafts((current) =>
      current.map((draft) =>
        draft.id === activeDraftId
          ? { ...draft, sql, updatedAt: Date.now() }
          : draft,
      ),
    );
  };

  const addDraft = (sql?: string, suggestedName?: string) => {
    const draft = createDraft(drafts, sql ?? "");
    if (suggestedName) draft.name = uniqueDraftName(suggestedName, drafts);
    setDrafts((current) => [...current, draft]);
    setActiveDraftId(draft.id);
    window.setTimeout(() => editorRef.current?.focus(), 0);
    return draft;
  };

  const closeDraft = (id: string) => {
    if (drafts.length === 1) return;
    const index = drafts.findIndex((draft) => draft.id === id);
    const next = drafts.filter((draft) => draft.id !== id);
    setDrafts(next);
    if (id === activeDraftId) {
      setActiveDraftId(next[Math.max(0, index - 1)]?.id ?? next[0].id);
    }
  };

  const executeSql = async (
    sql: string,
    allowUnbounded: boolean,
    draft = activeDraft,
  ) => {
    if (!draft || !sql.trim() || busy || executionInFlight.current) return;
    if (!connected) {
      setSettingsOpen(true);
      return;
    }
    const startedAt = Date.now();
    const historyId = crypto.randomUUID();
    const runId = historyId;
    executionInFlight.current = true;
    activeRunId.current = runId;
    const baseRecord: HistoryRecord = {
      id: historyId,
      draftName: draft.name,
      sql,
      startedAt,
      state: "running",
      elapsedMs: null,
      rowCount: null,
      resultMode: null,
    };
    setHistory((current) => upsertHistory(current, baseRecord));
    dispatch({ type: "preparing", runId, startedAt });
    setOverlay(EMPTY_OVERLAY);
    try {
      const start = await startExecution(sql, allowUnbounded);
      if (start.kind === "update") {
        catalogNavigation.refresh();
        const elapsedMs = start.elapsedMs ?? Date.now() - startedAt;
        dispatch({
          type: "update_completed",
          runId,
          elapsedMs,
          affectedRows: start.affectedRows ?? 0,
        });
        finishHistory(historyId, {
          state: "completed",
          elapsedMs,
          rowCount: null,
          resultMode: "none",
        });
        return;
      }
      if (!start.executionId || start.resultMode === "none") {
        throw new Error(
          "Workbench backend returned an invalid execution response",
        );
      }
      const executionId = start.executionId;
      dispatch({
        type: "started",
        runId,
        executionId,
        resultMode: start.resultMode,
      });
      finishHistory(historyId, {
        state: "running",
        elapsedMs: null,
        rowCount: null,
        resultMode: start.resultMode,
      });
      const response = await getResultResponse(executionId);
      let latestResult: QueryResult | null = null;
      try {
        const { consumeArrowResponse } = await import("./lib/arrow");
        latestResult = await consumeArrowResponse(
          response,
          start.resultMode === "unbounded",
          (result) => {
            latestResult = result;
            dispatch({
              type: "batch",
              executionId,
              result,
            });
            setOverlay((current) => inferOverlay(current, result));
          },
        );
      } catch (decodeError) {
        const status = await waitForTerminalStatus(executionId).catch(
          () => null,
        );
        if (status?.problem) throw status.problem;
        throw decodeError;
      }
      const status = await waitForTerminalStatus(executionId);
      if (status.status === "failed")
        throw status.problem ?? new Error("Execution failed");
      if (status.status === "cancelled") {
        dispatch({
          type: "completed",
          executionId,
          elapsedMs: status.elapsedMs,
        });
        finishHistory(historyId, {
          state: "cancelled",
          elapsedMs: status.elapsedMs,
          rowCount: latestResult?.rows.length ?? 0,
          resultMode: start.resultMode,
          problem: status.problem
            ? historyProblem(status.problem)
            : {
                source: "vql",
                code: "VQL-57001",
                symbol: "QUERY_CANCELLED",
                message: "Execution cancelled",
              },
        });
        return;
      }
      dispatch({
        type: "completed",
        executionId,
        elapsedMs: status.elapsedMs,
      });
      finishHistory(historyId, {
        state: "completed",
        elapsedMs: status.elapsedMs,
        rowCount: latestResult?.rows.length ?? 0,
        resultMode: start.resultMode,
      });
    } catch (error) {
      const problem = asProblem(error);
      const elapsedMs = Date.now() - startedAt;
      if (activeRunId.current === runId) {
        dispatch(
          problem.httpStatus === 401
            ? { type: "connection_failed", problem }
            : { type: "failed", runId, elapsedMs, problem },
        );
      }
      finishHistory(historyId, {
        state: problem.code === "VQL-57001" ? "cancelled" : "failed",
        elapsedMs,
        rowCount: null,
        resultMode: allowUnbounded ? "unbounded" : null,
        problem: historyProblem(problem),
      });
    } finally {
      if (activeRunId.current === runId) {
        activeRunId.current = null;
        executionInFlight.current = false;
      }
    }
  };

  const finishHistory = (
    id: string,
    values: Partial<
      Omit<HistoryRecord, "id" | "draftName" | "sql" | "startedAt">
    >,
  ) => {
    setHistory((current) =>
      current.map((record) =>
        record.id === id ? { ...record, ...values } : record,
      ),
    );
  };

  const cancelActive = async () => {
    if (!execution.executionId || execution.phase !== "running") return;
    const executionId = execution.executionId;
    dispatch({ type: "cancelling", executionId });
    try {
      await cancelExecution(executionId);
    } catch (error) {
      const problem = asProblem(error);
      dispatch(
        problem.httpStatus === 401
          ? { type: "connection_failed", problem }
          : { type: "cancel_failed", executionId, problem },
      );
    }
  };

  const runCurrent = (allowUnbounded = false) => {
    const slice = editorRef.current?.selectionOrCurrent();
    if (slice?.sql) void executeSql(slice.sql, allowUnbounded);
  };

  const runExplain = () => {
    const slice = editorRef.current?.selectionOrCurrent();
    if (!slice?.sql) return;
    const sql = slice.sql.replace(/;\s*$/, "");
    void executeSql(`EXPLAIN ${sql};`, false);
  };

  const formatActiveSql = async () => {
    if (!activeDraft || busy) return;
    try {
      editorRef.current?.replace(await formatSql(activeDraft.sql));
    } catch (error) {
      setProblem({
        source: "browser",
        title: "SQL formatting failed",
        message: error instanceof Error ? error.message : String(error),
      });
    }
  };

  const setProblem = (problem: WorkbenchProblem) => {
    dispatch(
      execution.phase === "disconnected"
        ? { type: "connection_failed", problem }
        : { type: "failed", elapsedMs: null, problem },
    );
  };

  const connect = async (input: SessionInput) => {
    try {
      const session = await createSession(input);
      setEndpoint(session.endpoint);
      setSessionVersion((current) => current + 1);
      setOverlay(EMPTY_OVERLAY);
      dispatch({ type: "connected" });
    } catch (error) {
      dispatch({ type: "connection_failed", problem: asProblem(error) });
      throw error;
    }
  };

  const disconnect = async () => {
    if (busy) return;
    try {
      await closeSession();
    } finally {
      setOverlay(EMPTY_OVERLAY);
      dispatch({ type: "disconnected" });
      setSessionVersion((current) => current + 1);
      setSettingsOpen(false);
    }
  };

  const loadRecord = (record: HistoryRecord, rerun: boolean) => {
    if (busy) return;
    const draft = addDraft(record.sql, record.draftName);
    setPage("editor");
    setHistoryOpen(false);
    if (rerun)
      window.setTimeout(() => void executeSql(record.sql, false, draft), 0);
  };

  const executeCatalogSql = async (
    sql: string,
    signal?: AbortSignal,
    recordHistory = false,
  ): Promise<QueryResult | null> => {
    if (!connected || executionInFlight.current) {
      throw {
        source: "policy",
        title: "Catalog execution unavailable",
        message: connected
          ? "Wait for the active execution to finish."
          : "Connect to vqld before executing catalog SQL.",
      } satisfies WorkbenchProblem;
    }
    executionInFlight.current = true;
    const startedAt = Date.now();
    const id = crypto.randomUUID();
    const record: HistoryRecord = {
      id,
      draftName: "Catalog",
      sql,
      startedAt,
      state: "running",
      elapsedMs: null,
      rowCount: null,
      resultMode: null,
    };
    if (recordHistory) setHistory((current) => upsertHistory(current, record));
    try {
      const result = await executeCatalogStatement(sql, signal);
      if (recordHistory)
        finishHistory(id, {
          state: "completed",
          elapsedMs: Date.now() - startedAt,
          rowCount: result?.rows.length ?? null,
          resultMode: result ? "bounded" : "none",
        });
      return result;
    } catch (error) {
      const problem = asProblem(error);
      if (recordHistory)
        finishHistory(id, {
          state: "failed",
          elapsedMs: Date.now() - startedAt,
          problem: historyProblem(problem),
        });
      if (problem.httpStatus === 401)
        dispatch({ type: "connection_failed", problem });
      throw error;
    } finally {
      executionInFlight.current = false;
    }
  };

  const catalogNavigation = useCatalogNavigation({
    connected,
    sessionVersion,
    busy,
    execute: executeCatalogSql,
    onBusyChange: setCatalogNavigationBusy,
    canExecute: () => !executionInFlight.current,
  });

  useEffect(() => {
    setCatalogSelection(null);
    setCatalogNamespace(null);
  }, [connected, sessionVersion]);

  const navigateCatalog = (
    section: CatalogSection,
    namespace: string,
    object?: CatalogObject,
    version: string | null = null,
  ) => {
    if (busy || executionInFlight.current) return;
    setCatalogNamespace(namespace);
    setCatalogSelection(
      object
        ? { id: object.id, request: ++catalogSelectionRequest.current, version }
        : null,
    );
    setPage(section);
    setSidebarOpen(false);
  };

  const beginResize = (event: React.PointerEvent) => {
    event.currentTarget.setPointerCapture(event.pointerId);
    const originY = event.clientY;
    const originHeight = editorHeight;
    const move = (moveEvent: PointerEvent) => {
      const max = Math.max(220, window.innerHeight - 310);
      setEditorHeight(
        Math.min(
          max,
          Math.max(180, originHeight + moveEvent.clientY - originY),
        ),
      );
    };
    const stop = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop);
  };

  if (!activeDraft) return null;

  return (
    <TooltipProvider>
      <div className="flex h-screen w-full overflow-hidden bg-canvas text-ink">
        <Sidebar
          connected={connected}
          endpoint={endpoint}
          historyCount={history.length}
          mobileOpen={sidebarOpen}
          page={page}
          busy={busy}
          onNavigate={(next) => {
            if (!busy && !executionInFlight.current) {
              setPage(next);
              setSidebarOpen(false);
            }
          }}
          onMobileClose={() => setSidebarOpen(false)}
          onHistory={() => setHistoryOpen(true)}
          onSettings={() => setSettingsOpen(true)}
          catalogTree={
            <CatalogTree
              objects={catalogNavigation.objects}
              connected={connected}
              loading={catalogNavigation.loading}
              busy={busy}
              problem={catalogNavigation.problem}
              activeSection={page === "editor" || page === "jobs" ? null : page}
              activeNamespace={catalogNamespace}
              selectedId={
                page === "editor" || page === "jobs"
                  ? null
                  : (catalogSelection?.id ?? null)
              }
              onRefresh={catalogNavigation.refresh}
              onNavigate={navigateCatalog}
              onSelect={(section, object) =>
                navigateCatalog(section, object.namespace, object)
              }
            />
          }
        />
        <div className="flex min-w-0 flex-1 flex-col md:pl-[236px]">
          <header className="flex h-12 shrink-0 items-center justify-between border-b border-hairline bg-canvas/95 px-3 backdrop-blur-md md:px-4">
            <div className="flex min-w-0 items-center gap-2">
              <Button
                size="icon"
                variant="ghost"
                className="md:hidden"
                onClick={() => setSidebarOpen(true)}
                aria-label="Open navigation"
              >
                <Menu size={17} />
              </Button>
              <div className="flex items-center gap-2 md:hidden">
                <Logo className="size-5" />
                <span className="text-[14px] font-semibold">VisionQL</span>
              </div>
            </div>
            <a
              href="https://github.com/zhenlohuang/visionql/tree/main/docs"
              target="_blank"
              rel="noreferrer"
              className="flex size-8 items-center justify-center rounded-md border border-hairline bg-surface text-muted shadow-sm transition-colors hover:text-ink"
              title="VisionQL documentation"
            >
              <BookOpen size={15} />
              <span className="sr-only">VisionQL documentation</span>
            </a>
          </header>
          {page === "jobs" ? (
            <JobsPage
              key={sessionVersion}
              connected={connected}
              active={!settingsOpen && !historyOpen && !catalogNavigationBusy}
              busy={catalogNavigationBusy}
              connectionProblem={execution.problem}
              request={requestManagementSql}
              onConnect={() => setSettingsOpen(true)}
              onLoadSql={(sql, name) => {
                addDraft(sql, name);
                setPage("editor");
              }}
            />
          ) : null}
          {page !== "editor" && page !== "jobs" ? (
            <CatalogWorkspace
              key={`${sessionVersion}:${page}`}
              section={page}
              connected={connected}
              busy={busy}
              execute={executeCatalogSql}
              onBusyChange={setCatalogBusy}
              namespace={catalogNamespace}
              object={
                catalogSelection
                  ? (catalogNavigation.objects[page].find(
                      (object) => object.id === catalogSelection.id,
                    ) ?? null)
                  : null
              }
              selectionRequest={catalogSelection?.request ?? 0}
              version={catalogSelection?.version ?? null}
              onSelectVersion={(version) => {
                if (!busy && catalogSelection)
                  setCatalogSelection({
                    ...catalogSelection,
                    version,
                    request: ++catalogSelectionRequest.current,
                  });
              }}
              canExecute={() => !executionInFlight.current}
              onConnect={() => setSettingsOpen(true)}
              onOpenSql={(sql, name) => {
                if (!busy) {
                  setPage("editor");
                  addDraft(sql, name);
                }
              }}
            />
          ) : null}
          <div
            className={cn(
              "min-h-0 flex-1 flex-col",
              page === "editor" ? "flex" : "hidden",
            )}
          >
            <DraftTabs
              drafts={drafts}
              activeDraftId={activeDraftId}
              busy={busy}
              onActivate={setActiveDraftId}
              onClose={closeDraft}
              onRename={setRenamingDraftId}
              onAdd={() => addDraft()}
              onFormat={() => void formatActiveSql()}
            />
            <main className="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface">
              <section
                className="flex min-h-[180px] shrink-0 overflow-hidden bg-surface"
                style={{ height: editorHeight }}
                aria-label="SQL workspace"
              >
                <EditorRail
                  connected={connected}
                  busy={busy}
                  cancelling={execution.phase === "cancelling"}
                  onRun={() => void executeSql(activeDraft.sql, false)}
                  onRunCurrent={() => runCurrent(false)}
                  onStream={() => runCurrent(true)}
                  onCancel={() => void cancelActive()}
                  onExplain={runExplain}
                />
                <SqlEditor
                  ref={editorRef}
                  value={activeDraft.sql}
                  onChange={updateDraft}
                  onRun={() => void executeSql(activeDraft.sql, false)}
                  onRunCurrent={() => runCurrent(false)}
                  disabled={catalogBusy || managementBusy || executionBusy}
                />
              </section>
              <button
                type="button"
                aria-label="Resize SQL editor"
                className="group relative z-10 h-1.5 shrink-0 cursor-row-resize border-y border-hairline bg-canvas transition-colors hover:bg-surface-raised"
                onPointerDown={beginResize}
              >
                <span className="absolute left-1/2 top-1/2 h-0.5 w-8 -translate-x-1/2 -translate-y-1/2 rounded-full bg-hairline-strong transition-colors group-hover:bg-muted" />
              </button>
              <ResultPane
                execution={execution}
                overlay={overlay}
                onOverlayChange={setOverlay}
              />
            </main>
          </div>
        </div>
        <HistoryDrawer
          open={historyOpen}
          onOpenChange={setHistoryOpen}
          history={history}
          onLoad={(record) => loadRecord(record, false)}
          onRerun={(record) => loadRecord(record, true)}
          onClear={() => setHistory([])}
        />
        <SettingsDialog
          open={settingsOpen}
          onOpenChange={setSettingsOpen}
          endpoint={endpoint}
          connected={connected}
          busy={busy}
          onConnect={connect}
          onDisconnect={disconnect}
        />
        {renamingDraft ? (
          <RenameDraftDialog
            key={renamingDraft.id}
            draft={renamingDraft}
            drafts={drafts}
            onClose={() => setRenamingDraftId(null)}
            onRename={(name) => {
              setDrafts((current) =>
                current.map((draft) =>
                  draft.id === renamingDraft.id
                    ? { ...draft, name, updatedAt: Date.now() }
                    : draft,
                ),
              );
              setRenamingDraftId(null);
            }}
          />
        ) : null}
      </div>
    </TooltipProvider>
  );
}

function Sidebar({
  connected,
  endpoint,
  historyCount,
  mobileOpen,
  page,
  busy,
  onNavigate,
  onMobileClose,
  onHistory,
  onSettings,
  catalogTree,
}: {
  connected: boolean;
  endpoint: string;
  historyCount: number;
  mobileOpen: boolean;
  page: WorkspacePage;
  busy: boolean;
  onNavigate: (page: WorkspacePage) => void;
  onMobileClose: () => void;
  onHistory: () => void;
  onSettings: () => void;
  catalogTree: React.ReactNode;
}) {
  return (
    <>
      {mobileOpen ? (
        <button
          className="fixed inset-0 z-30 bg-ink/15 backdrop-blur-[1px] md:hidden"
          onClick={onMobileClose}
          aria-label="Close navigation"
        />
      ) : null}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-40 flex w-[236px] flex-col border-r border-hairline bg-canvas transition-transform duration-200 md:translate-x-0",
          mobileOpen ? "translate-x-0 shadow-2xl" : "-translate-x-full",
        )}
      >
        <div className="border-b border-hairline p-4">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <Logo />
              <span className="text-[16px] font-semibold tracking-[-0.02em]">
                VisionQL
              </span>
            </div>
            <span className="rounded-md border border-hairline bg-surface-raised px-1.5 py-0.5 font-mono text-[10px] text-body">
              v0.3
            </span>
          </div>
          <button
            type="button"
            onClick={onSettings}
            className="mt-3 flex max-w-full items-center gap-2 rounded-md px-1 py-0.5 text-left transition-colors hover:bg-surface-raised"
          >
            <span
              className={cn(
                "size-2 shrink-0 rounded-full",
                connected ? "pulse-ring bg-success" : "bg-muted",
              )}
            />
            <span className="truncate font-mono text-[10px] text-body">
              {connected
                ? `vqld: ${compactEndpoint(endpoint)}`
                : "vqld: disconnected"}
            </span>
          </button>
        </div>
        <nav className="min-h-0 flex-1 overflow-y-auto p-2">
          <NavGroup title="Workspace">
            <NavItem
              icon={<SquareTerminal size={17} />}
              label="SQL editor"
              active={page === "editor"}
              disabled={busy && page !== "editor"}
              onClick={() => onNavigate("editor")}
            />
            <NavItem
              icon={<History size={17} />}
              label="History"
              count={historyCount}
              onClick={onHistory}
            />
            <NavItem
              icon={<FileClock size={17} />}
              label="Jobs"
              active={page === "jobs"}
              disabled={busy && page !== "jobs"}
              onClick={() => onNavigate("jobs")}
            />
          </NavGroup>
          {catalogTree}
        </nav>
        <div className="border-t border-hairline p-2">
          <NavItem
            icon={<Settings size={17} />}
            label="Settings"
            onClick={onSettings}
          />
        </div>
      </aside>
    </>
  );
}

function NavGroup({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-0.5">
      <p className="px-2 py-1.5 text-[9px] font-semibold uppercase tracking-[0.13em] text-muted">
        {title}
      </p>
      {children}
    </div>
  );
}

function NavItem({
  icon,
  label,
  active,
  count,
  onClick,
  disabled,
}: {
  icon: React.ReactNode;
  label: string;
  active?: boolean;
  count?: number;
  onClick?: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex h-9 w-full items-center gap-2 rounded-md px-2 text-[12px] transition-colors",
        active
          ? "border border-hairline bg-surface font-semibold text-ink shadow-sm"
          : "text-body hover:bg-surface-raised hover:text-ink",
        disabled && "opacity-55",
      )}
    >
      {icon}
      <span>{label}</span>
      {count != null ? (
        <span className="ml-auto rounded bg-surface-raised px-1.5 py-0.5 font-mono text-[9px] text-muted">
          {Math.min(999, count)}
        </span>
      ) : null}
    </button>
  );
}

function DraftTabs({
  drafts,
  activeDraftId,
  busy,
  onActivate,
  onClose,
  onRename,
  onAdd,
  onFormat,
}: {
  drafts: Draft[];
  activeDraftId: string;
  busy: boolean;
  onActivate: (id: string) => void;
  onClose: (id: string) => void;
  onRename: (id: string) => void;
  onAdd: () => void;
  onFormat: () => void;
}) {
  return (
    <div className="flex h-[52px] shrink-0 items-center justify-between gap-3 border-b border-hairline bg-[#f7f3ea] px-3 md:px-4">
      <div className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto py-1">
        {drafts.map((draft) => {
          const active = draft.id === activeDraftId;
          return (
            <div
              key={draft.id}
              className={cn(
                "group flex h-9 shrink-0 items-center gap-1 rounded-md border pl-3 pr-2 transition-all",
                active
                  ? "border-hairline border-b-2 border-b-accent bg-surface text-ink shadow-sm"
                  : "border-transparent text-muted hover:border-hairline hover:bg-surface/65 hover:text-ink",
                busy && !active && "opacity-50",
              )}
            >
              <button
                type="button"
                disabled={busy}
                aria-label={draft.name}
                aria-pressed={active}
                title="Double-click to rename"
                onClick={() => onActivate(draft.id)}
                onDoubleClick={() => onRename(draft.id)}
                className="flex h-full min-w-0 items-center gap-2 pr-1"
              >
                <SquareTerminal
                  size={14}
                  className={active ? "text-accent" : "text-muted"}
                />
                <span className="max-w-[180px] truncate font-mono text-[11px] font-medium">
                  {draft.name}
                </span>
                <span
                  className={cn(
                    "size-1.5 rounded-full",
                    active ? "bg-success" : "bg-hairline-strong",
                  )}
                />
              </button>
              <Tooltip label="Rename query file">
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => onRename(draft.id)}
                  className="flex size-5 items-center justify-center rounded text-muted opacity-70 transition-all hover:bg-surface-raised hover:text-ink disabled:opacity-20 group-hover:opacity-100"
                  aria-label={`Rename ${draft.name}`}
                >
                  <Pencil size={12} />
                </button>
              </Tooltip>
              <button
                type="button"
                disabled={drafts.length === 1 || busy}
                onClick={(event) => {
                  event.stopPropagation();
                  onClose(draft.id);
                }}
                className="ml-0.5 flex size-5 items-center justify-center rounded text-muted opacity-70 transition-all hover:bg-surface-raised hover:text-ink disabled:opacity-20 group-hover:opacity-100"
                aria-label={`Close ${draft.name}`}
              >
                <X size={12} />
              </button>
            </div>
          );
        })}
        <Tooltip label="New draft (⌘T)">
          <Button
            size="icon"
            variant="ghost"
            className="size-8 shrink-0"
            disabled={busy}
            aria-label="New draft"
            onClick={onAdd}
          >
            <Plus size={16} />
          </Button>
        </Tooltip>
      </div>
      <Tooltip label="Format SQL (⌥⇧F)">
        <Button size="icon" variant="ghost" disabled={busy} onClick={onFormat}>
          <WandSparkles size={15} />
        </Button>
      </Tooltip>
    </div>
  );
}

function EditorRail({
  connected,
  busy,
  cancelling,
  onRun,
  onRunCurrent,
  onStream,
  onCancel,
  onExplain,
}: {
  connected: boolean;
  busy: boolean;
  cancelling: boolean;
  onRun: () => void;
  onRunCurrent: () => void;
  onStream: () => void;
  onCancel: () => void;
  onExplain: () => void;
}) {
  const canRun = connected && !busy;
  return (
    <div className="flex w-11 shrink-0 flex-col items-center gap-1 border-r border-hairline bg-canvas-soft py-2">
      <RailButton
        label="Run buffer (⌘Enter)"
        disabled={!canRun}
        onClick={onRun}
        accent
      >
        <Play size={17} fill="currentColor" />
      </RailButton>
      <RailButton
        label="Run selection or current statement (⇧⌘Enter)"
        disabled={!canRun}
        onClick={onRunCurrent}
        accent
      >
        <span className="relative">
          <Play size={16} fill="currentColor" />
          <Plus
            size={9}
            strokeWidth={3}
            className="absolute -bottom-1 -right-1 rounded-full bg-canvas-soft"
          />
        </span>
      </RailButton>
      <RailButton
        label="Run as attached stream"
        disabled={!canRun}
        onClick={onStream}
        accent
      >
        <RotateCw size={16} />
      </RailButton>
      <RailButton
        label="Cancel active execution (Esc)"
        disabled={!busy || cancelling}
        onClick={onCancel}
        danger
      >
        <CircleStop size={16} fill="currentColor" />
      </RailButton>
      <span className="my-1 h-px w-5 bg-hairline" />
      <RailButton
        label="Explain selection or current statement"
        disabled={!canRun}
        onClick={onExplain}
        info
      >
        <Braces size={16} />
      </RailButton>
    </div>
  );
}

function RailButton({
  label,
  children,
  accent,
  danger,
  info,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  label: string;
  accent?: boolean;
  danger?: boolean;
  info?: boolean;
}) {
  return (
    <Tooltip label={label}>
      <Button
        size="icon"
        variant="ghost"
        aria-label={label}
        className={cn(
          "size-8",
          accent && "text-accent hover:text-accent-strong",
          danger && "text-muted hover:text-danger",
          info && "text-info hover:text-info",
        )}
        {...props}
      >
        {children}
      </Button>
    </Tooltip>
  );
}

function inferOverlay(
  current: OverlayConfig,
  result: QueryResult,
): OverlayConfig {
  if (current.imageColumn) return current;
  const image =
    result.fields.find((field) => field.extensionName === "vql.image")?.key ??
    null;
  const box =
    result.fields.find((field) => field.extensionName === "vql.box2d")?.key ??
    null;
  const label =
    result.fields.find((field) => /^label$/i.test(field.name))?.key ?? null;
  const confidence =
    result.fields.find((field) => /^(confidence|score)$/i.test(field.name))
      ?.key ?? null;
  return {
    imageColumn: image,
    boxColumn: box,
    labelColumn: label,
    confidenceColumn: confidence,
  };
}

function historyProblem(problem: WorkbenchProblem): HistoryRecord["problem"] {
  return {
    source: problem.source,
    code: problem.code,
    symbol: problem.symbol,
    message: problem.message,
  };
}

function uniqueDraftName(suggested: string, drafts: Draft[]): string {
  const base = suggested.replace(/\.sql$/i, "");
  let candidate = `${base}.sql`;
  let suffix = 2;
  while (drafts.some((draft) => draft.name === candidate)) {
    candidate = `${base}_${suffix}.sql`;
    suffix += 1;
  }
  return candidate;
}

function compactEndpoint(endpoint: string): string {
  try {
    const url = new URL(endpoint);
    return `${url.hostname}${url.port ? `:${url.port}` : ""}`;
  } catch {
    return endpoint;
  }
}
