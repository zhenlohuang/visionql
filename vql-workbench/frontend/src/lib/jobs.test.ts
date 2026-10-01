import { describe, expect, it } from "vitest";

import {
  canStopJob,
  filterJobs,
  jobStatement,
  readJobs,
  readJobDetails,
} from "./jobs";
import type { QueryResult, ResultValue } from "./types";

function queryResult(...rows: Record<string, ResultValue>[]): QueryResult {
  return {
    fields: [],
    rows: rows.map((values, id) => ({ id, values })),
    receivedBatches: 1,
    rolling: false,
  };
}

describe("persistent Job contracts", () => {
  it("selects named fields, keeps unknown states and health, and preserves large counters", () => {
    const [job] = readJobs(
      queryResult({
        future_field: "ignored",
        state: "RECOVERING",
        name: "people",
        job_id: "job-1",
        source_health: "degraded",
        started_at: 1_700_000_000_000,
        last_event_time: null,
        restart_gap_count: 9007199254740993n,
      }),
    );
    expect(job.state).toBe("RECOVERING");
    expect(job.sourceHealth).toBe("degraded");
    expect(job.startedAt).toBe(1_700_000_000_000);
    expect(job.lastEventTime).toBeNull();
    expect(job.restartGapCount).toBe("9,007,199,254,740,993");
    expect(canStopJob(job.state)).toBe(false);
  });

  it("stops only supported non-terminal states and safely quotes the exact ID", () => {
    expect(canStopJob("STARTING")).toBe(true);
    expect(canStopJob("RUNNING")).toBe(true);
    for (const state of ["STOPPED", "FAILED", "COMPLETED", "future"])
      expect(canStopJob(state)).toBe(false);
    expect(jobStatement("STOP", "job'1; --")).toBe("STOP JOB 'job''1; --';");
  });

  it("filters names, IDs, states, and source health without modifying SQL", () => {
    const jobs = readJobs(
      queryResult({
        job_id: "ABC",
        name: "People",
        state: "RUNNING",
        source_health: "reconnecting",
      }),
    );
    for (const term of ["people", " abc ", "RUNNING", "reconnecting"])
      expect(filterJobs(jobs, term)).toEqual(jobs);
    expect(filterJobs(jobs, "stopped")).toEqual([]);
  });

  it("preserves the redacted definition and missing inspection values", () => {
    const details = readJobDetails(
      queryResult({
        job_id: "id",
        name: "people",
        state: "FAILED",
        sql_redacted: "INSERT INTO sink SELECT '?'",
        created_at: 1700000000000n,
        last_restart_reset_window_state: false,
        error_code: "VQL-57001",
      }),
    );
    expect(details.sql).toBe("INSERT INTO sink SELECT '?'");
    expect(details.createdAt).toBe(1700000000000);
    expect(details.startedAt).toBeNull();
    expect(details.resetWindowState).toBe(false);
    expect(details.errorCode).toBe("VQL-57001");
    expect(() => readJobDetails(queryResult())).toThrow("one Job definition");
    expect(() =>
      readJobs(queryResult({ name: "people", state: "RUNNING" })),
    ).toThrow("job_id");
  });
});
