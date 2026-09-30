import type { QueryResult, ResultValue } from "./types";

export interface PersistentQuery {
  queryId: string;
  name: string;
  state: string;
  sourceHealth: string | null;
  lastEventTime: number | null;
  startedAt: number | null;
  updatedAt: number | null;
  restartGapCount: string | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export interface QueryDetails {
  queryId: string;
  name: string;
  state: string;
  sql: string;
  createdAt: number | null;
  startedAt: number | null;
  updatedAt: number | null;
  lastRestartAt: number | null;
  restartGapStartedAt: number | null;
  restartGapEndedAt: number | null;
  resetWindowState: boolean | null;
  errorCode: string | null;
  errorMessage: string | null;
}

export function queryStatement(action: "DESCRIBE" | "STOP", queryId: string) {
  return `${action} QUERY '${queryId.replaceAll("'", "''")}';`;
}

export function readQueries(result: QueryResult): PersistentQuery[] {
  return result.rows.map(({ values }) => ({
    queryId: requiredString(values.query_id, "query_id"),
    name: requiredString(values.name, "name"),
    state: requiredString(values.state, "state"),
    sourceHealth: stringValue(values.source_health),
    lastEventTime: timestampValue(values.last_event_time),
    startedAt: timestampValue(values.started_at),
    updatedAt: timestampValue(values.updated_at),
    restartGapCount: integerValue(values.restart_gap_count),
    errorCode: stringValue(values.error_code),
    errorMessage: stringValue(values.error_message),
  }));
}

export function readQueryDetails(result: QueryResult): QueryDetails {
  if (result.rows.length !== 1) {
    throw new Error("DESCRIBE QUERY must return one Query definition");
  }
  const { values } = result.rows[0];
  return {
    queryId: requiredString(values.query_id, "query_id"),
    name: requiredString(values.name, "name"),
    state: requiredString(values.state, "state"),
    sql: requiredString(values.sql_redacted, "sql_redacted"),
    createdAt: timestampValue(values.created_at),
    startedAt: timestampValue(values.started_at),
    updatedAt: timestampValue(values.updated_at),
    lastRestartAt: timestampValue(values.last_restart_at),
    restartGapStartedAt: timestampValue(values.restart_gap_started_at),
    restartGapEndedAt: timestampValue(values.restart_gap_ended_at),
    resetWindowState:
      typeof values.last_restart_reset_window_state === "boolean"
        ? values.last_restart_reset_window_state
        : null,
    errorCode: stringValue(values.error_code),
    errorMessage: stringValue(values.error_message),
  };
}

export function canStopQuery(state: string) {
  return state === "STARTING" || state === "RUNNING";
}

export function filterQueries(queries: PersistentQuery[], search: string) {
  const term = search.trim().toLowerCase();
  return queries.filter((query) =>
    [query.name, query.queryId, query.state, query.sourceHealth ?? ""].some(
      (value) => value.toLowerCase().includes(term),
    ),
  );
}

export function formatQueryTime(value: number | null) {
  if (value == null) return "—";
  return new Date(value).toLocaleString();
}

function requiredString(value: ResultValue | undefined, field: string) {
  if (typeof value !== "string") {
    throw new Error(`Query response is missing the '${field}' string field`);
  }
  return value;
}

function stringValue(value: ResultValue | undefined) {
  return typeof value === "string" ? value : null;
}

function integerValue(value: ResultValue | undefined) {
  return typeof value === "bigint" ||
    (typeof value === "number" && Number.isInteger(value))
    ? value.toLocaleString("en-US")
    : null;
}

function timestampValue(value: ResultValue | undefined) {
  const milliseconds = typeof value === "bigint" ? Number(value) : value;
  return typeof milliseconds === "number" &&
    Number.isFinite(milliseconds) &&
    Math.abs(milliseconds) <= 8.64e15
    ? milliseconds
    : null;
}
