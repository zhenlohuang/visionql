import {
  asProblem,
  cancelExecution,
  getResultResponse,
  startExecution,
  waitForTerminalStatus,
} from "./api";
import type { QueryResult, ResultRow, WorkbenchProblem } from "./types";

export type CatalogSection = "tables" | "models" | "functions";
export type CatalogKind = "TABLE" | "MODEL" | "FUNCTION";
export type CatalogExecutor = (
  sql: string,
  signal?: AbortSignal,
  recordHistory?: boolean,
) => Promise<QueryResult | null>;

export interface CatalogObject {
  id: string;
  name: string;
  kind: CatalogKind;
  namespace: string;
  ddl: string;
  ddlVersion?: string;
  problem: WorkbenchProblem | null;
  lookupNames: string[];
  versionCount?: number;
  versions?: CatalogVersion[];
  versionsProblem?: WorkbenchProblem;
}

export interface CatalogVersion {
  name: string;
  isDefault: boolean;
}

export type CatalogSnapshot = Record<CatalogSection, CatalogObject[]>;

export function emptyCatalog(): CatalogSnapshot {
  return { tables: [], models: [], functions: [] };
}

// Match the public SQL callable display-address convention.
export function catalogAddress(name: string): {
  catalog: string;
  schema: string;
  name: string;
} {
  const parts = name.split(".");
  if (parts.length === 3)
    return { catalog: parts[0], schema: parts[1], name: parts[2] };
  if (parts.length === 2)
    return { catalog: "vql", schema: parts[0], name: parts[1] };
  return { catalog: "vql", schema: "default", name };
}

export const CATALOG_KINDS: Record<CatalogSection, CatalogKind> = {
  tables: "TABLE",
  models: "MODEL",
  functions: "FUNCTION",
};

export function textValue(row: ResultRow, key: string): string {
  const value = row.values[key];
  return value == null ? "" : String(value);
}

export function quoteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

export function quoteName(name: string): string {
  return name.split(".").map(quoteIdentifier).join(".");
}

function quoteString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

// Use the existing execution transport, including terminal status: a successful
// HTTP stream can still end with a structured server-side execution failure.
export async function executeCatalogStatement(
  sql: string,
  signal?: AbortSignal,
): Promise<QueryResult | null> {
  signal?.throwIfAborted();
  const start = await startExecution(sql, false);
  if (start.kind === "update") return null;
  if (!start.executionId) {
    throw new Error("Catalog statements require a bounded result");
  }
  const executionId = start.executionId;
  try {
    signal?.throwIfAborted();
    if (start.resultMode !== "bounded")
      throw new Error("Catalog statements require a bounded result");
    const response = await getResultResponse(executionId, signal);
    const { consumeArrowResponse } = await import("./arrow");
    let result: QueryResult;
    try {
      result = await consumeArrowResponse(response, false, () => undefined);
    } catch (error) {
      const status = signal?.aborted
        ? null
        : await waitForTerminalStatus(executionId).catch(() => null);
      throw status?.problem ?? error;
    }
    const status = await waitForTerminalStatus(executionId);
    if (status.status !== "completed") {
      throw status.problem ?? new Error(`Catalog execution ${status.status}`);
    }
    signal?.throwIfAborted();
    return result;
  } catch (error) {
    await cancelExecution(executionId).catch(() => undefined);
    throw error;
  }
}

export async function listCatalog(
  section: CatalogSection,
  execute: CatalogExecutor,
  signal?: AbortSignal,
): Promise<CatalogObject[]> {
  const request: CatalogExecutor = (sql) => {
    signal?.throwIfAborted();
    return execute(sql, signal);
  };
  const listed = await request(`SHOW ${section.toUpperCase()};`);
  const addressCount = new Map<string, number>();
  for (const row of listed?.rows ?? []) {
    const key = addressKey(row);
    addressCount.set(key, (addressCount.get(key) ?? 0) + 1);
  }
  const objects: CatalogObject[] = [];
  for (const row of listed?.rows ?? []) {
    const kind =
      section === "functions" && textValue(row, "kind") === "MODEL"
        ? "MODEL"
        : CATALOG_KINDS[section];
    const name =
      section === "tables"
        ? textValue(row, "table_name")
        : textValue(row, "name");
    const namespace =
      section === "tables"
        ? "vql.default"
        : `${textValue(row, "catalog")}.${textValue(row, "schema")}`;
    const ambiguous =
      section !== "tables" && (addressCount.get(addressKey(row)) ?? 0) > 1;
    const object: CatalogObject = {
      id: JSON.stringify([
        kind,
        namespace,
        name,
        ...(ambiguous ? [row.id] : []),
      ]),
      name,
      kind,
      namespace,
      ddl: "",
      problem: null,
      lookupNames: section === "tables" ? [name] : objectNames(row),
      ...(section === "models"
        ? { versionCount: Number(textValue(row, "versions")) }
        : {}),
    };
    if (ambiguous) {
      object.problem = {
        source: "policy",
        title: "Ambiguous catalog address",
        message:
          "Multiple stored names have this display address. Use an explicit object name in the SQL editor to inspect or modify it.",
      };
    }
    objects.push(object);
  }
  signal?.throwIfAborted();
  return objects.sort((left, right) => left.name.localeCompare(right.name));
}

export async function loadCatalogObject(
  object: CatalogObject,
  execute: CatalogExecutor,
  signal?: AbortSignal,
  version: string | null = null,
): Promise<CatalogObject> {
  signal?.throwIfAborted();
  if (object.problem) return object;
  const definition = await queryCatalogObject(
    object,
    (name) =>
      `SHOW CREATE ${object.kind} ${quoteName(name)}${object.kind === "MODEL" && version !== null ? ` VERSION ${quoteString(version)}` : ""};`,
    execute,
    signal,
  );
  const definitionRow = definition?.rows[0];
  if (!definitionRow) throw new Error("SHOW CREATE returned no definition");
  signal?.throwIfAborted();
  return {
    ...object,
    name: textValue(definitionRow, "object_name"),
    ddl: textValue(definitionRow, "create_sql"),
    ddlVersion:
      object.kind === "MODEL" ? textValue(definitionRow, "version") : undefined,
  };
}

export async function listCatalogVersions(
  object: CatalogObject,
  execute: CatalogExecutor,
  signal?: AbortSignal,
): Promise<CatalogVersion[]> {
  signal?.throwIfAborted();
  if (object.problem) throw object.problem;
  const result = await queryCatalogObject(
    object,
    (name) => `SHOW MODEL VERSIONS ${quoteName(name)};`,
    execute,
    signal,
  );
  signal?.throwIfAborted();
  return (result?.rows ?? []).map((row) => ({
    name: textValue(row, "version"),
    isDefault: row.values.is_default === true,
  }));
}

async function queryCatalogObject(
  object: CatalogObject,
  statement: (name: string) => string,
  execute: CatalogExecutor,
  signal?: AbortSignal,
): Promise<QueryResult | null> {
  // SHOW MODELS/FUNCTIONS returns display addresses rather than the stored
  // SQL name. SHOW CREATE supplies that name. Try only equivalent address
  // spellings, and only fall back on the public NOT_FOUND symbol.
  const candidates = object.lookupNames;
  for (let index = 0; index < candidates.length; index += 1) {
    try {
      signal?.throwIfAborted();
      return await execute(statement(candidates[index]), signal);
    } catch (error) {
      if (
        asProblem(error).symbol !== "NOT_FOUND" ||
        index === candidates.length - 1
      )
        throw error;
    }
  }
  throw new Error("Catalog object has no lookup name");
}

function addressKey(row: ResultRow): string {
  return JSON.stringify(
    ["catalog", "schema", "name", "kind"].map((key) => textValue(row, key)),
  );
}

function objectNames(row: ResultRow): string[] {
  const catalog = textValue(row, "catalog");
  const schema = textValue(row, "schema");
  const name = textValue(row, "name");
  return [
    ...(catalog === "vql" && schema === "default" ? [name] : []),
    ...(catalog === "vql" ? [`${schema}.${name}`] : []),
    `${catalog}.${schema}.${name}`,
  ];
}

export function createTemplate(
  section: CatalogSection,
  namespace = "vql.default",
): string {
  const prefix =
    namespace === "vql.default"
      ? ""
      : namespace.startsWith("vql.")
        ? `${namespace.slice(4)}.`
        : `${namespace}.`;
  if (section === "tables")
    return "CREATE TABLE photos\nUSING IMAGES\nLOCATION '/path/to/images';";
  const name = (value: string) =>
    prefix
      ? section === "functions"
        ? quoteIdentifier(`${prefix}${value}`)
        : quoteName(`${prefix}${value}`)
      : value;
  if (section === "models")
    return `CREATE MODEL ${name("detector")} TYPE OBJECT_DETECTION\nVERSION 'v1'\nFROM '/path/to/model.onnx'\nUSING ONNX_RUNTIME;`;
  return `CREATE FUNCTION ${name("plus_one")}(BIGINT)\nRETURNS BIGINT\nRETURN $1 + 1;`;
}
