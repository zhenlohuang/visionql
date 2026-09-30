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
  category: string;
  namespace: string;
  summary: string;
  signature: string;
  ddl: string;
  description: QueryResult | null;
  versions: QueryResult | null;
  problem: WorkbenchProblem | null;
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

export function quoteLiteral(value: string): string {
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

export async function loadCatalog(
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
  // A Session supports one active execution. Enrich rows sequentially, never
  // issue concurrent Flight requests for descriptions or model versions.
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
    const object: CatalogObject = {
      id: `${kind}:${namespace}:${name}:${row.id}`,
      name,
      kind,
      namespace,
      category:
        section === "tables"
          ? textValue(row, "provider")
          : kind === "MODEL"
            ? "Model callable"
            : "Function",
      summary:
        section === "tables"
          ? textValue(row, "location")
          : textValue(row, "comment"),
      signature:
        section === "models"
          ? textValue(row, "interface")
          : section === "functions"
            ? `${name}(${textValue(row, "arguments")}) → ${textValue(row, "return_type")}`
            : "",
      ddl: "",
      description: null,
      versions: null,
      problem: null,
    };
    try {
      if (
        section !== "tables" &&
        (addressCount.get(addressKey(row)) ?? 0) > 1
      ) {
        throw {
          source: "policy",
          title: "Ambiguous catalog address",
          message:
            "Multiple stored names have this display address. Use an explicit object name in the SQL editor to inspect or modify it.",
        } satisfies WorkbenchProblem;
      }
      // SHOW MODELS/FUNCTIONS returns display addresses rather than the stored
      // SQL name. SHOW CREATE supplies that name. Try only equivalent address
      // spellings, and only fall back on the public NOT_FOUND symbol.
      const candidates = section === "tables" ? [name] : objectNames(row);
      let definition: QueryResult | null = null;
      for (let index = 0; index < candidates.length; index += 1) {
        try {
          definition = await request(
            `SHOW CREATE ${kind} ${quoteName(candidates[index])};`,
          );
          break;
        } catch (error) {
          if (
            asProblem(error).symbol !== "NOT_FOUND" ||
            index === candidates.length - 1
          )
            throw error;
        }
      }
      const definitionRow = definition?.rows[0];
      if (!definitionRow) throw new Error("SHOW CREATE returned no definition");
      object.name = textValue(definitionRow, "object_name");
      object.ddl = textValue(definitionRow, "create_sql");
      object.description = await request(
        `DESCRIBE ${kind} ${quoteName(object.name)};`,
      );
      if (kind === "MODEL")
        object.versions = await request(
          `SHOW MODEL VERSIONS ${quoteName(object.name)};`,
        );
      if (kind === "FUNCTION") {
        const keywords = object.ddl.replace(
          /'(?:[^']|'')*'|"(?:[^"]|"")*"/g,
          "",
        );
        object.category = /\bLANGUAGE\s+PYTHON\s+AS\b/i.test(keywords)
          ? "Python UDF"
          : "SQL Expression";
      }
    } catch (error) {
      signal?.throwIfAborted();
      object.problem = asProblem(error);
      if (object.problem.httpStatus === 401) throw error;
    }
    objects.push(object);
  }
  return objects.sort((left, right) => left.name.localeCompare(right.name));
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

export function modelAction(
  object: CatalogObject,
  action: "resolve" | "default" | "dropVersion",
  version: string,
): string {
  const name = quoteName(object.name);
  const literal = quoteLiteral(version);
  if (action === "resolve") return `RESOLVE MODEL ${name} VERSION ${literal};`;
  if (action === "default")
    return `ALTER MODEL ${name} SET DEFAULT_VERSION = ${literal};`;
  return `ALTER MODEL ${name} DROP VERSION ${literal};`;
}

export function createTemplate(section: CatalogSection): string {
  if (section === "tables")
    return "CREATE TABLE photos\nUSING IMAGES\nLOCATION '/path/to/images';";
  if (section === "models")
    return "CREATE MODEL detector TYPE OBJECT_DETECTION\nVERSION 'v1'\nFROM '/path/to/model.onnx'\nUSING ONNX_RUNTIME;";
  return "CREATE FUNCTION plus_one(BIGINT)\nRETURNS BIGINT\nRETURN $1 + 1;";
}
