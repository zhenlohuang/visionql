import {
  Box,
  Check,
  Code2,
  Copy,
  FunctionSquare,
  LoaderCircle,
  Plus,
  RefreshCw,
  Search,
  SquareTerminal,
  Table2,
  Trash2,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { asProblem } from "../lib/api";
import {
  CATALOG_KINDS,
  createTemplate,
  loadCatalog,
  modelAction,
  quoteName,
  textValue,
  type CatalogExecutor,
  type CatalogObject,
  type CatalogSection,
} from "../lib/catalog";
import { cn } from "../lib/cn";
import type { QueryResult, WorkbenchProblem } from "../lib/types";
import { ProblemPanel } from "./ProblemPanel";
import { Button } from "./ui/button";
import { Dialog, DialogContent } from "./ui/dialog";

const SECTIONS = {
  tables: {
    title: "Tables",
    description: "Multimodal sources, video streams, and writable relations.",
    placeholder: "Search tables by name, provider, location, or schema…",
    Icon: Table2,
  },
  models: {
    title: "Models",
    description:
      "Versioned callables, model interfaces, and resolved execution contracts.",
    placeholder: "Search models by name, interface, or version…",
    Icon: Box,
  },
  functions: {
    title: "Functions",
    description:
      "SQL expressions, Python functions, and registered Model callables.",
    placeholder: "Search functions by name, signature, or language…",
    Icon: FunctionSquare,
  },
};

interface SqlAction {
  title: string;
  sql: string;
  destructive: boolean;
  editable: boolean;
}

export function CatalogWorkspace({
  section,
  connected,
  busy,
  execute,
  onBusyChange,
  onOpenSql,
  onConnect,
}: {
  section: CatalogSection;
  connected: boolean;
  busy: boolean;
  execute: CatalogExecutor;
  onBusyChange: (busy: boolean) => void;
  onOpenSql: (sql: string, name: string) => void;
  onConnect: () => void;
}) {
  const [objects, setObjects] = useState<CatalogObject[]>([]);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("All");
  const [selected, setSelected] = useState<CatalogObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [problem, setProblem] = useState<WorkbenchProblem | null>(null);
  const [action, setAction] = useState<SqlAction | null>(null);
  const [actionSql, setActionSql] = useState("");
  const [actionProblem, setActionProblem] = useState<WorkbenchProblem | null>(
    null,
  );
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState("");
  const [copied, setCopied] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const inFlight = useRef(false);
  const generation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const pending = useRef<Promise<void> | null>(null);
  const propsRef = useRef({ execute, onBusyChange });
  propsRef.current = { execute, onBusyChange };
  const { title, description, placeholder, Icon } = SECTIONS[section];
  const blocked = busy || loading || submitting;

  const refresh = (selectedName?: string) => {
    if (!connected || inFlight.current) return;
    const currentGeneration = ++generation.current;
    inFlight.current = true;
    setLoading(true);
    setProblem(null);
    propsRef.current.onBusyChange(true);
    const operation = new AbortController();
    controller.current = operation;
    const completion = (async () => {
      try {
        const next = await loadCatalog(
          section,
          propsRef.current.execute,
          operation.signal,
        );
        if (currentGeneration !== generation.current) return;
        setObjects(next);
        setLoaded(true);
        if (selectedName)
          setSelected(
            next.find((object) => object.name === selectedName) ?? null,
          );
      } catch (error) {
        if (
          !operation.signal.aborted &&
          currentGeneration === generation.current
        ) {
          setObjects([]);
          setLoaded(false);
          setProblem(asProblem(error));
        }
      } finally {
        inFlight.current = false;
        controller.current = null;
        pending.current = null;
        propsRef.current.onBusyChange(false);
        if (currentGeneration === generation.current) setLoading(false);
      }
    })();
    pending.current = completion;
    return completion;
  };

  useEffect(() => {
    setObjects([]);
    setLoaded(false);
    setSelected(null);
    setSearch("");
    setFilter("All");
    setNotice("");
    setProblem(null);
    let disposed = false;
    if (connected)
      void (async () => {
        await pending.current;
        if (!disposed) await refresh();
      })();
    return () => {
      disposed = true;
      generation.current += 1;
      controller.current?.abort();
    };
  }, [section, connected]);

  useEffect(() => {
    const focusSearch = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener("keydown", focusSearch);
    return () => window.removeEventListener("keydown", focusSearch);
  }, []);

  const openAction = (next: SqlAction) => {
    setAction(next);
    setActionSql(next.sql);
    setActionProblem(null);
    setNotice("");
  };

  const submitAction = async () => {
    if (!action || !actionSql.trim() || blocked || inFlight.current) return;
    inFlight.current = true;
    setSubmitting(true);
    setActionProblem(null);
    propsRef.current.onBusyChange(true);
    const selectedName = selected?.name;
    try {
      await propsRef.current.execute(actionSql, undefined, true);
      setNotice(`${action.title} completed.`);
      setAction(null);
      setSelected(null);
      inFlight.current = false;
      await refresh(selectedName);
    } catch (error) {
      setActionProblem(asProblem(error));
    } finally {
      inFlight.current = false;
      setSubmitting(false);
      propsRef.current.onBusyChange(false);
    }
  };

  const copyDdl = async () => {
    if (!selected) return;
    try {
      await navigator.clipboard.writeText(selected.ddl);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch (error) {
      setProblem(asProblem(error));
    }
  };

  const categories = [
    "All",
    ...new Set(objects.map((object) => object.category)),
  ];
  const query = search.trim().toLowerCase();
  const visible = objects.filter(
    (object) =>
      (filter === "All" || filter === object.category) &&
      [
        object.name,
        object.namespace,
        object.category,
        object.summary,
        object.signature,
        object.ddl,
        ...(object.versions?.rows.map((row) => textValue(row, "version")) ??
          []),
      ]
        .join(" ")
        .toLowerCase()
        .includes(query),
  );

  return (
    <main
      className="min-h-0 flex-1 overflow-y-auto bg-canvas"
      aria-label={`${title} catalog`}
    >
      <div className="mx-auto flex max-w-[1280px] flex-col gap-6 p-4 md:p-8">
        <div className="flex flex-wrap items-center gap-2 border-b border-hairline pb-3 font-mono text-[10px] text-muted">
          <span className="font-semibold tracking-wider">CATALOG</span>
          <span>/</span>
          <span className="font-semibold uppercase text-ink">{title}</span>
          <span className="rounded border border-hairline bg-surface-raised px-2 py-0.5">
            vql.default
          </span>
          <span>
            {connected && loaded
              ? `${objects.length} registered`
              : connected
                ? "Loading catalog…"
                : "Disconnected"}
          </span>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
            <p className="mt-1.5 text-xs text-body">{description}</p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              disabled={!connected || blocked}
              onClick={() => void refresh()}
            >
              <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
              Refresh
            </Button>
            <Button
              size="sm"
              variant="primary"
              disabled={!connected || blocked}
              onClick={() =>
                openAction({
                  title: `Create ${CATALOG_KINDS[section].toLowerCase()}`,
                  sql: createTemplate(section),
                  destructive: false,
                  editable: true,
                })
              }
            >
              <Plus size={14} />
              Create {CATALOG_KINDS[section].toLowerCase()}
            </Button>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          {categories.length > 2 ? (
            <div className="flex flex-wrap gap-1" aria-label="Catalog filters">
              {categories.map((category) => (
                <button
                  type="button"
                  key={category}
                  aria-pressed={filter === category}
                  onClick={() => setFilter(category)}
                  className={cn(
                    "flex h-8 items-center gap-2 rounded-md border px-2.5 text-xs transition-colors",
                    filter === category
                      ? "border-hairline-strong bg-surface font-medium text-ink"
                      : "border-transparent text-body hover:bg-surface-raised",
                  )}
                >
                  {category}
                  <span className="font-mono text-[10px] text-muted">
                    {category === "All"
                      ? objects.length
                      : objects.filter((object) => object.category === category)
                          .length}
                  </span>
                </button>
              ))}
            </div>
          ) : null}
          <div className="relative min-w-[220px] flex-1">
            <Search
              size={16}
              className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-muted"
            />
            <input
              ref={searchRef}
              type="search"
              aria-label={`Search ${title.toLowerCase()}`}
              placeholder={placeholder}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              className="h-10 w-full rounded-lg border border-hairline bg-surface pl-9 pr-3 text-xs shadow-sm transition-colors focus:border-accent focus:outline-none"
            />
          </div>
        </div>
        {notice ? (
          <p role="status" className="text-xs text-success">
            {notice}
          </p>
        ) : null}
        {problem ? <ProblemPanel problem={problem} /> : null}
        {!connected ? (
          <EmptyState
            Icon={Icon}
            title="Connect to browse the catalog"
            description="Connect Workbench to a vqld endpoint to inspect and manage its objects."
          >
            <Button size="sm" onClick={onConnect}>
              Connect to vqld
            </Button>
          </EmptyState>
        ) : loading ? (
          <div
            className="flex items-center justify-center gap-2 py-16 text-xs text-muted"
            role="status"
          >
            <LoaderCircle size={16} className="animate-spin" />
            Loading {title.toLowerCase()}…
          </div>
        ) : loaded && !visible.length ? (
          <EmptyState
            Icon={Icon}
            title={
              objects.length
                ? "No matching objects"
                : `No ${title.toLowerCase()} registered`
            }
            description={
              objects.length
                ? "Change your search or filter to show more objects."
                : "Create an object using its SQL declaration."
            }
          />
        ) : (
          <div className="flex flex-col gap-3">
            {visible.map((object) => (
              <CatalogCard
                key={object.id}
                object={object}
                section={section}
                disabled={blocked}
                selected={selected?.id === object.id}
                onSelect={() => {
                  setCopied(false);
                  setSelected(object);
                }}
                onDrop={() =>
                  openAction({
                    title: `Drop ${object.kind.toLowerCase()}`,
                    sql: `DROP ${object.kind} ${quoteName(object.name)};`,
                    destructive: true,
                    editable: false,
                  })
                }
              />
            ))}
          </div>
        )}
      </div>

      <Dialog
        open={!!selected && !action}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent
          title={`${selected?.kind ?? "Object"} DDL and schema`}
          description="Inspect the defining SQL and catalog metadata."
          className="flex max-h-[90vh] max-w-[1060px] flex-col overflow-hidden"
        >
          {selected ? (
            <>
              <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-hairline px-5 py-4 pr-14">
                <div className="flex min-w-0 items-center gap-2">
                  <Code2 size={18} className="shrink-0 text-accent" />
                  <span className="text-xs text-muted">DDL &amp; Schema</span>
                  <strong className="break-all font-mono text-sm">
                    {selected.name}
                  </strong>
                </div>
                <Button
                  size="sm"
                  disabled={!selected.ddl}
                  onClick={() => void copyDdl()}
                >
                  {copied ? <Check size={14} /> : <Copy size={14} />}
                  {copied ? "Copied" : "Copy DDL"}
                </Button>
              </header>
              <div className="min-h-0 overflow-y-auto">
                {selected.problem ? (
                  <div className="py-4">
                    <ProblemPanel problem={selected.problem} />
                  </div>
                ) : null}
                <div className="grid divide-y divide-hairline lg:grid-cols-2 lg:divide-x lg:divide-y-0">
                  <section className="min-w-0 p-5">
                    <h2 className="mb-3 text-[10px] font-semibold uppercase tracking-wider text-muted">
                      Declaration SQL
                    </h2>
                    <pre className="max-h-[360px] overflow-auto rounded-lg bg-ink p-4 font-mono text-xs leading-6 text-white">
                      <code>{selected.ddl || "Definition unavailable"}</code>
                    </pre>
                    {selected.category === "Python UDF" ? (
                      <p className="mt-3 text-xs leading-5 text-body">
                        Python Functions use the batched Arrow ABI in the
                        embedded host. Execution in vqld is unavailable.
                      </p>
                    ) : null}
                    <Button
                      size="sm"
                      className="mt-4"
                      disabled={!selected.ddl || blocked}
                      onClick={() => onOpenSql(selected.ddl, selected.name)}
                    >
                      <SquareTerminal size={14} />
                      Open in SQL editor
                    </Button>
                  </section>
                  <section className="min-w-0 p-5">
                    <h2 className="mb-3 text-[10px] font-semibold uppercase tracking-wider text-muted">
                      {selected.kind === "TABLE"
                        ? "Column schema"
                        : "Callable contract"}
                    </h2>
                    <MetadataTable result={selected.description} />
                    {selected.kind === "MODEL" ? (
                      <p className="mt-3 text-xs leading-5 text-body">
                        RESOLVED means the execution schema is resolved. Runtime
                        health is evaluated when a query executes.
                      </p>
                    ) : null}
                  </section>
                </div>
                {selected.kind === "MODEL" ? (
                  <section className="border-t border-hairline p-5">
                    <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                      <h2 className="text-[10px] font-semibold uppercase tracking-wider text-muted">
                        Model versions
                      </h2>
                      <Button
                        size="sm"
                        disabled={blocked || !!selected.problem}
                        onClick={() =>
                          openAction({
                            title: "Alter model",
                            sql: `ALTER MODEL ${quoteName(selected.name)} ADD VERSION 'v2'\nFROM '/path/to/model.onnx' USING ONNX_RUNTIME;`,
                            destructive: false,
                            editable: true,
                          })
                        }
                      >
                        <Plus size={14} />
                        Alter model
                      </Button>
                    </div>
                    <div className="space-y-2">
                      {selected.versions?.rows.map((row) => {
                        const version = textValue(row, "version");
                        const resolved =
                          textValue(row, "status") === "RESOLVED";
                        const isDefault = row.values.is_default === true;
                        return (
                          <div
                            key={version}
                            role="group"
                            aria-label={`Model version ${version}`}
                            className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-hairline p-3"
                          >
                            <div className="min-w-0 space-y-1">
                              <div className="flex flex-wrap items-center gap-2 font-mono text-xs">
                                <strong>{version}</strong>
                                <span
                                  className={
                                    resolved ? "text-success" : "text-muted"
                                  }
                                >
                                  {resolved ? "Schema resolved" : "Unresolved"}
                                </span>
                                {isDefault ? <Badge>Default</Badge> : null}
                                <Badge>{textValue(row, "volatility")}</Badge>
                              </div>
                              {textValue(row, "fingerprint") ? (
                                <p className="break-all font-mono text-[10px] text-muted">
                                  Fingerprint: {textValue(row, "fingerprint")}
                                </p>
                              ) : null}
                            </div>
                            <div className="flex flex-wrap items-center gap-2">
                              <Button
                                size="sm"
                                disabled={blocked}
                                onClick={() =>
                                  openAction({
                                    title: "Resolve model",
                                    sql: modelAction(
                                      selected,
                                      "resolve",
                                      version,
                                    ),
                                    destructive: false,
                                    editable: false,
                                  })
                                }
                              >
                                Resolve
                              </Button>
                              <Button
                                size="sm"
                                disabled={blocked || isDefault || !resolved}
                                onClick={() =>
                                  openAction({
                                    title: "Set default version",
                                    sql: modelAction(
                                      selected,
                                      "default",
                                      version,
                                    ),
                                    destructive: false,
                                    editable: false,
                                  })
                                }
                              >
                                Set default
                              </Button>
                              <Button
                                size="sm"
                                variant="ghost"
                                disabled={blocked}
                                onClick={() =>
                                  openAction({
                                    title: "Drop model version",
                                    sql: modelAction(
                                      selected,
                                      "dropVersion",
                                      version,
                                    ),
                                    destructive: true,
                                    editable: false,
                                  })
                                }
                              >
                                <Trash2 size={13} />
                                Drop version
                              </Button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </section>
                ) : null}
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog
        open={!!action}
        onOpenChange={(open) => {
          if (!open && !submitting) setAction(null);
        }}
      >
        <DialogContent
          title={action?.title ?? "Execute catalog SQL"}
          description="Review the exact SQL statement before execution."
          className="flex max-h-[90vh] max-w-[760px] flex-col overflow-hidden"
        >
          <header className="border-b border-hairline p-5 pr-14">
            <h2 className="text-base font-semibold">{action?.title}</h2>
            <p className="mt-1 text-xs text-body">
              {action?.destructive
                ? "Confirm the SQL below to remove this catalog object. Dependencies may prevent removal."
                : "Review the SQL statement before executing it."}
            </p>
          </header>
          <div className="min-h-0 space-y-4 overflow-y-auto p-5">
            <label
              className="block text-[10px] font-semibold uppercase tracking-wider text-muted"
              htmlFor="catalog-action-sql"
            >
              SQL statement
            </label>
            <textarea
              id="catalog-action-sql"
              aria-label="Catalog SQL statement"
              value={actionSql}
              onChange={(event) => setActionSql(event.target.value)}
              readOnly={!action?.editable}
              disabled={submitting}
              spellCheck={false}
              className="min-h-[180px] w-full resize-y rounded-lg border border-hairline bg-canvas-soft p-4 font-mono text-xs leading-6 focus:border-accent focus:outline-none"
            />
            {actionProblem ? <ProblemPanel problem={actionProblem} /> : null}
          </div>
          <footer className="flex shrink-0 flex-wrap justify-end gap-2 border-t border-hairline p-4">
            <Button
              size="sm"
              disabled={submitting}
              onClick={() => setAction(null)}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              disabled={blocked}
              onClick={() => {
                setAction(null);
                onOpenSql(actionSql, action?.title ?? "Catalog SQL");
              }}
            >
              <SquareTerminal size={14} />
              Open in SQL editor
            </Button>
            <Button
              size="sm"
              variant={action?.destructive ? "danger" : "primary"}
              disabled={!connected || blocked || !actionSql.trim()}
              onClick={() => void submitAction()}
            >
              {submitting ? (
                <LoaderCircle size={14} className="animate-spin" />
              ) : null}
              {submitting
                ? "Executing…"
                : action?.destructive
                  ? "Confirm drop"
                  : "Execute SQL"}
            </Button>
          </footer>
        </DialogContent>
      </Dialog>
    </main>
  );
}

function CatalogCard({
  object,
  section,
  selected,
  disabled,
  onSelect,
  onDrop,
}: {
  object: CatalogObject;
  section: CatalogSection;
  selected: boolean;
  disabled: boolean;
  onSelect: () => void;
  onDrop: () => void;
}) {
  const Icon =
    object.kind === "TABLE"
      ? Table2
      : object.kind === "MODEL"
        ? Box
        : FunctionSquare;
  const status = object.description?.rows[0]
    ? textValue(object.description.rows[0], "status")
    : "";
  return (
    <article
      aria-label={object.name}
      className={cn(
        "row-enter rounded-lg border bg-surface p-4 shadow-sm transition-colors md:p-5",
        selected
          ? "border-accent ring-1 ring-accent"
          : "border-hairline hover:border-hairline-strong",
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 flex-wrap items-center gap-2.5">
          <Icon size={17} className="shrink-0 text-accent" />
          <button
            type="button"
            disabled={disabled}
            onClick={onSelect}
            className="break-all text-left font-mono text-sm font-semibold hover:text-accent"
          >
            {object.name}
          </button>
          <Badge>{object.category}</Badge>
          {object.namespace !== "vql.default" ? (
            <span className="font-mono text-[10px] text-muted">
              {object.namespace}
            </span>
          ) : null}
          {status ? (
            <span
              className={cn(
                "font-mono text-[10px]",
                status === "RESOLVED" ? "text-success" : "text-muted",
              )}
            >
              {status === "RESOLVED" ? "Schema resolved" : status}
            </span>
          ) : null}
        </div>
        <div className="flex items-center gap-1">
          <Button size="sm" disabled={disabled} onClick={onSelect}>
            <Code2 size={14} />
            Show DDL
          </Button>
          <Button
            size="icon"
            variant="ghost"
            disabled={disabled || !!object.problem}
            onClick={onDrop}
            aria-label={`Drop ${object.name}`}
          >
            <Trash2 size={14} />
          </Button>
        </div>
      </div>
      {object.summary ? (
        <p className="mt-2 break-all font-mono text-[11px] text-muted">
          {object.summary}
        </p>
      ) : null}
      {section === "tables" ? (
        <div className="mt-3 flex flex-wrap items-center gap-1.5">
          <span className="mr-1 text-[9px] font-semibold uppercase tracking-wider text-muted">
            Schema
          </span>
          {object.description?.rows.map((row) => (
            <span
              key={textValue(row, "column_name")}
              title={`${textValue(row, "column_name")}: ${textValue(row, "data_type")}`}
              className="min-w-0 max-w-[260px] truncate rounded border border-hairline bg-canvas-soft px-2 py-1 font-mono text-[11px]"
            >
              {textValue(row, "column_name")}:{" "}
              <span className="text-muted">{textValue(row, "data_type")}</span>
            </span>
          ))}
        </div>
      ) : (
        <code className="mt-3 block overflow-x-auto rounded border border-hairline bg-canvas-soft px-3 py-2 font-mono text-xs leading-5">
          {object.signature}
        </code>
      )}
      {object.versions?.rows.length ? (
        <div className="mt-3 flex flex-wrap gap-2">
          {object.versions.rows.map((row) => (
            <span
              key={textValue(row, "version")}
              className="font-mono text-[10px] text-muted"
            >
              {textValue(row, "version")}
              {row.values.is_default === true ? " · default" : ""} ·{" "}
              {textValue(row, "status").toLowerCase()}
            </span>
          ))}
        </div>
      ) : null}
      {object.problem ? (
        <p className="mt-3 text-xs text-danger">
          {object.problem.symbol ?? object.problem.title}:{" "}
          {object.problem.message}
        </p>
      ) : null}
    </article>
  );
}

function MetadataTable({ result }: { result: QueryResult | null }) {
  if (!result)
    return <p className="text-xs text-muted">Metadata unavailable.</p>;
  const columnSchema = result.fields.some(
    (field) => field.name === "column_name",
  );
  if (!columnSchema)
    return (
      <dl className="divide-y divide-hairline rounded-lg border border-hairline">
        {result.fields
          .filter(
            (field) =>
              !["catalog", "schema", "name", "kind"].includes(field.name),
          )
          .map((field) => (
            <div key={field.name} className="flex flex-col gap-1 px-3 py-2.5">
              <dt className="font-mono text-[10px] text-muted">
                {field.name.replaceAll("_", " ")}
              </dt>
              <dd className="break-all font-mono text-xs leading-5">
                {result.rows[0]
                  ? textValue(result.rows[0], field.name) || "—"
                  : "—"}
              </dd>
            </div>
          ))}
      </dl>
    );
  return (
    <div className="overflow-x-auto rounded-lg border border-hairline">
      <table className="w-full text-left font-mono text-[11px]">
        <thead className="border-b border-hairline bg-canvas">
          <tr>
            <th className="p-2.5">Field</th>
            <th className="p-2.5">Arrow type</th>
            <th className="p-2.5">Nullable</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-hairline">
          {result.rows.map((row) => (
            <tr key={row.id}>
              <td className="p-2.5">{textValue(row, "column_name")}</td>
              <td className="break-all p-2.5 text-body">
                {textValue(row, "data_type")}
              </td>
              <td className="p-2.5 text-muted">{textValue(row, "nullable")}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Badge({ children }: { children: React.ReactNode }) {
  return (
    <span className="rounded border border-hairline bg-canvas px-2 py-0.5 font-mono text-[10px] text-body">
      {children}
    </span>
  );
}

function EmptyState({
  Icon,
  title,
  description,
  children,
}: {
  Icon: typeof Table2;
  title: string;
  description: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center gap-3 py-16 text-center">
      <Icon size={25} className="text-muted" />
      <h2 className="text-sm font-medium">{title}</h2>
      <p className="max-w-sm text-xs leading-5 text-muted">{description}</p>
      {children}
    </div>
  );
}
