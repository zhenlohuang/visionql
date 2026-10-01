import type { QueryResult, ResultValue } from "./types";

export interface PersistentJob {
  jobId: string;
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

export interface JobDetails {
  jobId: string;
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

export function jobStatement(action: "DESCRIBE" | "STOP", jobId: string) {
  return `${action} JOB '${jobId.replaceAll("'", "''")}';`;
}

export function readJobs(result: QueryResult): PersistentJob[] {
  return result.rows.map(({ values }) => ({
    jobId: requiredString(values.job_id, "job_id"),
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

export function readJobDetails(result: QueryResult): JobDetails {
  if (result.rows.length !== 1) {
    throw new Error("DESCRIBE JOB must return one Job definition");
  }
  const { values } = result.rows[0];
  return {
    jobId: requiredString(values.job_id, "job_id"),
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

export function canStopJob(state: string) {
  return state === "STARTING" || state === "RUNNING";
}

export function filterJobs(jobs: PersistentJob[], search: string) {
  const term = search.trim().toLowerCase();
  return jobs.filter((job) =>
    [job.name, job.jobId, job.state, job.sourceHealth ?? ""].some((value) =>
      value.toLowerCase().includes(term),
    ),
  );
}

export function formatJobTime(value: number | null) {
  if (value == null) return "—";
  return new Date(value).toLocaleString();
}

function requiredString(value: ResultValue | undefined, field: string) {
  if (typeof value !== "string") {
    throw new Error(`Job response is missing the '${field}' string field`);
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
