import { Check, Copy, LoaderCircle, Plus } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { asProblem } from "../lib/api";
import {
  CATALOG_KINDS,
  catalogAddress,
  createTemplate,
  loadCatalogObject,
  type CatalogExecutor,
  type CatalogObject,
  type CatalogSection,
} from "../lib/catalog";
import { cn } from "../lib/cn";
import type { WorkbenchProblem } from "../lib/types";
import { FormattedDdl } from "./FormattedDdl";
import { ProblemPanel } from "./ProblemPanel";
import { Button } from "./ui/button";

export function CatalogWorkspace({
  section,
  namespace,
  object,
  selectionRequest,
  version = null,
  onSelectVersion,
  connected,
  busy,
  execute,
  onBusyChange,
  onOpenSql,
  onConnect,
  canExecute,
}: {
  section: CatalogSection;
  namespace: string | null;
  object: CatalogObject | null;
  selectionRequest: number;
  version?: string | null;
  onSelectVersion: (version: string) => void;
  connected: boolean;
  busy: boolean;
  execute: CatalogExecutor;
  onBusyChange: (busy: boolean) => void;
  onOpenSql: (sql: string, name: string) => void;
  onConnect: () => void;
  canExecute: () => boolean;
}) {
  const [attemptedKey, setAttemptedKey] = useState("");
  const [detail, setDetail] = useState<{
    key: string;
    object: CatalogObject;
  } | null>(null);
  const [problem, setProblem] = useState<{
    key: string;
    problem: WorkbenchProblem;
  } | null>(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const generation = useRef(0);
  const props = useRef({ object, execute, onBusyChange, canExecute });
  props.current = { object, execute, onBusyChange, canExecute };
  const key = object
    ? JSON.stringify([object.id, selectionRequest, version])
    : "";
  const selected = connected && detail?.key === key ? detail.object : null;
  const currentProblem =
    problem?.key === key ? problem.problem : selected?.problem;
  const ddl = selected?.ddl ?? "";
  const displayedVersion = selected?.ddlVersion || version;
  const kind = CATALOG_KINDS[section].toLowerCase();
  const versions = object?.versions ?? [];
  const showVersions =
    connected &&
    object?.kind === "MODEL" &&
    (versions.length > 1 || object.versionsProblem);

  useEffect(() => {
    setAttemptedKey("");
    setCopied(false);
    return () => {
      generation.current += 1;
      controller.current?.abort();
    };
  }, [key, connected]);

  useEffect(() => {
    if (
      !connected ||
      !key ||
      busy ||
      controller.current ||
      attemptedKey === key ||
      !props.current.canExecute()
    )
      return;
    const operation = new AbortController();
    const currentGeneration = generation.current;
    const currentObject = props.current.object!;
    controller.current = operation;
    setAttemptedKey(key);
    setLoading(true);
    props.current.onBusyChange(true);
    void (async () => {
      try {
        // Let StrictMode cleanup cancel its first effect before issuing SQL.
        await Promise.resolve();
        operation.signal.throwIfAborted();
        const next = await loadCatalogObject(
          currentObject,
          props.current.execute,
          operation.signal,
          version,
        );
        if (currentGeneration === generation.current) {
          setDetail({ key, object: next });
          setProblem(null);
        }
      } catch (error) {
        if (
          !operation.signal.aborted &&
          currentGeneration === generation.current
        )
          setProblem({ key, problem: asProblem(error) });
      } finally {
        controller.current = null;
        setLoading(false);
        props.current.onBusyChange(false);
      }
    })();
  }, [key, connected, busy, attemptedKey, loading]);

  async function copyDdl() {
    try {
      await navigator.clipboard.writeText(ddl);
      setCopied(true);
    } catch (error) {
      setProblem({ key, problem: asProblem(error) });
    }
  }

  return (
    <main
      aria-label="Catalog DDL"
      className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-surface"
    >
      <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-hairline px-5 py-4 sm:px-7">
        <div className="min-w-0">
          <p className="mb-1 break-all font-mono text-[10px] text-muted">
            {object?.namespace ?? namespace ?? "Catalog"} ·{" "}
            {object?.kind ?? CATALOG_KINDS[section]}
            {displayedVersion !== null ? ` · Version ${displayedVersion}` : ""}
          </p>
          <h1 className="break-all text-[17px] font-semibold text-ink">
            {object?.name ?? section[0].toUpperCase() + section.slice(1)}
          </h1>
        </div>
        <div className="flex flex-wrap items-center gap-1">
          {object ? (
            <Button
              variant="ghost"
              size="sm"
              disabled={!ddl || busy}
              onClick={() => void copyDdl()}
            >
              {copied ? <Check size={14} /> : <Copy size={14} />}
              {copied ? "Copied" : "Copy DDL"}
            </Button>
          ) : (
            <Button
              size="sm"
              disabled={!connected || busy}
              onClick={() =>
                onOpenSql(
                  createTemplate(section, namespace ?? undefined),
                  `New ${kind}`,
                )
              }
            >
              <Plus size={14} />
              Create {kind}
            </Button>
          )}
        </div>
      </header>
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden md:flex-row">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
          {!connected ? (
            <div className="m-auto px-6 text-center">
              <p className="mb-4 text-sm text-muted">
                Connect to browse the catalog
              </p>
              <Button onClick={onConnect}>Connect</Button>
            </div>
          ) : currentProblem ? (
            <ProblemPanel problem={currentProblem} />
          ) : !object ? (
            <div className="m-auto px-6 text-center">
              <p className="text-sm font-medium text-body">Select an object</p>
              <p className="mt-2 text-xs text-muted">
                Choose a catalog object in the sidebar to view its DDL.
              </p>
            </div>
          ) : !selected ? (
            <div
              role="status"
              className="m-auto flex items-center gap-2 text-sm text-muted"
            >
              <LoaderCircle size={16} className="animate-spin" />
              Loading DDL…
            </div>
          ) : (
            <FormattedDdl sql={ddl} />
          )}
        </div>
        {object && showVersions ? (
          <aside
            aria-label="Model versions"
            className="order-first flex max-h-[180px] shrink-0 flex-col border-b border-hairline bg-canvas/50 md:order-last md:max-h-none md:w-[208px] md:border-b-0 md:border-l"
          >
            <div className="flex shrink-0 items-center justify-between px-4 pb-2 pt-4">
              <h2 className="text-[11px] font-semibold text-body">Versions</h2>
              {versions.length ? (
                <span className="font-mono text-[10px] text-muted">
                  {versions.length}
                </span>
              ) : null}
            </div>
            {object.versionsProblem ? (
              <p
                role="alert"
                className="overflow-auto break-words px-4 pb-4 text-xs leading-5 text-danger"
              >
                {object.versionsProblem.code ? (
                  <span className="block font-mono text-[10px]">
                    {object.versionsProblem.code}{" "}
                    {object.versionsProblem.symbol}
                  </span>
                ) : null}
                {object.versionsProblem.message}
              </p>
            ) : (
              <ul
                aria-label={`Versions of model ${object.namespace}.${catalogAddress(object.name).name}`}
                className="flex min-h-0 gap-1 overflow-x-auto px-3 pb-3 md:flex-1 md:flex-col md:overflow-x-hidden md:overflow-y-auto"
              >
                {versions.map((item) => {
                  const active = displayedVersion === item.name;
                  return (
                    <li key={item.name} className="shrink-0">
                      <button
                        type="button"
                        disabled={busy || !connected}
                        aria-label={`Show DDL for model ${object.namespace}.${catalogAddress(object.name).name} version ${item.name}`}
                        aria-current={active ? "true" : undefined}
                        title={item.name}
                        onClick={() => onSelectVersion(item.name)}
                        className={cn(
                          "flex w-full min-w-0 max-w-[240px] items-center gap-2 rounded-md px-2.5 py-2 text-left text-[11px] text-body transition-colors hover:bg-surface-raised hover:text-ink disabled:opacity-40 md:max-w-none",
                          active && "bg-accent/10 text-accent",
                        )}
                      >
                        <span className="truncate font-mono">{item.name}</span>
                        {item.isDefault ? (
                          <span className="ml-auto shrink-0 text-[9px] text-muted">
                            Default
                          </span>
                        ) : null}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </aside>
        ) : null}
      </div>
    </main>
  );
}
