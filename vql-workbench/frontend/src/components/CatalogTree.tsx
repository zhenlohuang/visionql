import {
  Box,
  ChevronRight,
  Database,
  FunctionSquare,
  Layers,
  LoaderCircle,
  RefreshCw,
  Table2,
} from "lucide-react";
import { useState } from "react";

import {
  catalogAddress,
  type CatalogObject,
  type CatalogSection,
  type CatalogSnapshot,
} from "../lib/catalog";
import { cn } from "../lib/cn";
import type { WorkbenchProblem } from "../lib/types";

const SECTIONS = {
  tables: { title: "Tables", Icon: Table2 },
  models: { title: "Models", Icon: Box },
  functions: { title: "Functions", Icon: FunctionSquare },
};

export function CatalogTree({
  objects,
  connected,
  loading,
  busy,
  problem,
  activeSection,
  activeNamespace,
  selectedId,
  onRefresh,
  onNavigate,
  onSelect,
}: {
  objects: CatalogSnapshot;
  connected: boolean;
  loading: boolean;
  busy: boolean;
  problem: WorkbenchProblem | null;
  activeSection: CatalogSection | null;
  activeNamespace: string | null;
  selectedId: string | null;
  onRefresh: () => void;
  onNavigate: (section: CatalogSection, namespace: string) => void;
  onSelect: (section: CatalogSection, object: CatalogObject) => void;
}) {
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const toggle = (key: string) =>
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  const namespaces = new Map<string, Set<string>>([
    ["vql", new Set(["default"])],
  ]);
  for (const object of Object.values(objects).flat()) {
    const [catalog, schema] = object.namespace.split(".");
    if (!namespaces.has(catalog)) namespaces.set(catalog, new Set());
    namespaces.get(catalog)!.add(schema);
  }
  const branch = (key: string, label: string, Icon: typeof Database) => (
    <button
      type="button"
      aria-expanded={!collapsed.has(key)}
      onClick={() => toggle(key)}
      className="flex w-full min-w-0 items-center gap-1.5 rounded-md py-1.5 text-left text-xs text-body transition-colors hover:bg-surface-raised"
    >
      <ChevronRight
        size={12}
        className={cn(
          "shrink-0 transition-transform",
          !collapsed.has(key) && "rotate-90",
        )}
      />
      <Icon size={14} className="shrink-0 text-muted" />
      <span className="truncate font-mono" title={label}>
        {label}
      </span>
    </button>
  );
  return (
    <section
      aria-label="Catalog navigation"
      className="mt-2 border-t border-hairline pt-2"
    >
      <div className="flex items-center justify-between px-2">
        <button
          type="button"
          aria-expanded={!collapsed.has("root")}
          onClick={() => toggle("root")}
          className="flex items-center gap-1 py-1.5 text-[9px] font-semibold uppercase tracking-[0.13em] text-muted"
        >
          <ChevronRight
            size={11}
            className={cn(
              "transition-transform",
              !collapsed.has("root") && "rotate-90",
            )}
          />
          Catalog
        </button>
        <button
          type="button"
          aria-label="Refresh catalog navigation"
          disabled={!connected || busy}
          onClick={onRefresh}
          className="rounded p-1 text-muted transition-colors hover:text-ink disabled:opacity-40"
        >
          {loading ? (
            <LoaderCircle size={12} className="animate-spin" />
          ) : (
            <RefreshCw size={12} />
          )}
        </button>
      </div>
      {!collapsed.has("root") ? (
        <>
          {!connected ? (
            <p className="px-3 py-1 text-[10px] text-muted">
              Connect to browse objects
            </p>
          ) : null}
          {problem ? (
            <p
              role="alert"
              className="break-words px-3 py-1 text-[10px] text-danger"
            >
              {problem.code ? (
                <span className="block font-mono">
                  {problem.code} {problem.symbol}
                </span>
              ) : null}
              {problem.message}
            </p>
          ) : null}
          <ul aria-label="Catalogs" className="px-1">
            {[...namespaces]
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([catalog, schemas]) => (
                <li key={catalog}>
                  {branch(`catalog:${catalog}`, catalog, Database)}
                  {!collapsed.has(`catalog:${catalog}`) ? (
                    <ul
                      aria-label={`${catalog} schemas`}
                      className="ml-2.5 border-l border-hairline pl-1.5"
                    >
                      {[...schemas].sort().map((schema) => {
                        const namespace = `${catalog}.${schema}`;
                        return (
                          <li key={namespace}>
                            {branch(`schema:${namespace}`, schema, Layers)}
                            {!collapsed.has(`schema:${namespace}`) ? (
                              <ul
                                aria-label={`${namespace} objects`}
                                className="ml-2.5 border-l border-hairline pl-1.5"
                              >
                                {(Object.keys(SECTIONS) as CatalogSection[])
                                  .filter(
                                    (section) =>
                                      section !== "tables" ||
                                      namespace === "vql.default",
                                  )
                                  .map((section) => {
                                    const { title, Icon } = SECTIONS[section];
                                    const key = `${namespace}:${section}`;
                                    const children = objects[section].filter(
                                      (object) =>
                                        object.namespace === namespace,
                                    );
                                    const active =
                                      activeSection === section &&
                                      activeNamespace === namespace;
                                    return (
                                      <li key={key}>
                                        <div
                                          className={cn(
                                            "flex items-center rounded-md transition-colors hover:bg-surface-raised",
                                            active &&
                                              "bg-surface-raised text-ink",
                                          )}
                                        >
                                          <button
                                            type="button"
                                            aria-label={`${title} in ${namespace}`}
                                            aria-expanded={!collapsed.has(key)}
                                            onClick={() => toggle(key)}
                                            className="shrink-0 p-1"
                                          >
                                            <ChevronRight
                                              size={12}
                                              className={cn(
                                                "text-muted transition-transform",
                                                !collapsed.has(key) &&
                                                  "rotate-90",
                                              )}
                                            />
                                          </button>
                                          <button
                                            type="button"
                                            disabled={busy}
                                            onClick={() =>
                                              onNavigate(section, namespace)
                                            }
                                            aria-current={
                                              active ? "page" : undefined
                                            }
                                            className="flex min-w-0 flex-1 items-center gap-1.5 py-1.5 pr-1 text-left text-[11px] text-body disabled:opacity-40"
                                          >
                                            <Icon
                                              size={13}
                                              className="shrink-0"
                                            />
                                            <span>{title}</span>
                                            <span
                                              aria-hidden="true"
                                              className="ml-auto font-mono text-[9px] text-muted"
                                            >
                                              {children.length}
                                            </span>
                                          </button>
                                        </div>
                                        {!collapsed.has(key) &&
                                        children.length ? (
                                          <ul
                                            aria-label={`${namespace} ${title.toLowerCase()}`}
                                            className="ml-3 border-l border-hairline pl-1"
                                          >
                                            {children.map((object) => (
                                              <CatalogObjectNode
                                                key={object.id}
                                                object={object}
                                                connected={connected}
                                                busy={busy}
                                                selected={
                                                  activeSection === section &&
                                                  selectedId === object.id
                                                }
                                                onSelect={() =>
                                                  onSelect(section, object)
                                                }
                                              />
                                            ))}
                                          </ul>
                                        ) : null}
                                      </li>
                                    );
                                  })}
                              </ul>
                            ) : null}
                          </li>
                        );
                      })}
                    </ul>
                  ) : null}
                </li>
              ))}
          </ul>
        </>
      ) : null}
    </section>
  );
}

function CatalogObjectNode({
  object,
  connected,
  busy,
  selected,
  onSelect,
}: {
  object: CatalogObject;
  connected: boolean;
  busy: boolean;
  selected: boolean;
  onSelect: () => void;
}) {
  const name = catalogAddress(object.name).name;
  const address = `${object.namespace}.${name}`;
  return (
    <li>
      <button
        type="button"
        disabled={busy || !connected}
        aria-label={`Show DDL for ${object.kind.toLowerCase()} ${address}`}
        aria-current={selected ? "true" : undefined}
        title={object.name}
        onClick={onSelect}
        className={cn(
          "flex w-full min-w-0 items-center gap-1.5 rounded-md px-1.5 py-1.5 text-left font-mono text-[10px] text-body transition-colors hover:bg-surface-raised hover:text-ink disabled:opacity-40",
          selected && "bg-accent/10 text-accent",
        )}
      >
        <span className="size-1 shrink-0 rounded-full bg-current opacity-50" />
        <span className="truncate">{name}</span>
      </button>
    </li>
  );
}
