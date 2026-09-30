import { describe, expect, it } from "vitest";

import {
  canStopQuery,
  filterQueries,
  queryStatement,
  readQueries,
  readQueryDetails,
} from "./queries";
import type { QueryResult, ResultValue } from "./types";

function queryResult(...rows: Record<string, ResultValue>[]): QueryResult {
  return {
    fields: [],
    rows: rows.map((values, id) => ({ id, values })),
    receivedBatches: 1,
    rolling: false,
  };
}

describe("persistent Query contracts", () => {
  it("selects named fields, keeps unknown states and health, and preserves large counters", () => {
    const [query] = readQueries(
      queryResult({
        future_field: "ignored",
        state: "RECOVERING",
        name: "people",
        query_id: "query-1",
        source_health: "degraded",
        started_at: 1_700_000_000_000,
        last_event_time: null,
        restart_gap_count: 9007199254740993n,
      }),
    );
    expect(query.state).toBe("RECOVERING");
    expect(query.sourceHealth).toBe("degraded");
    expect(query.startedAt).toBe(1_700_000_000_000);
    expect(query.lastEventTime).toBeNull();
    expect(query.restartGapCount).toBe("9,007,199,254,740,993");
    expect(canStopQuery(query.state)).toBe(false);
  });

  it("stops only supported non-terminal states and safely quotes the exact ID", () => {
    expect(canStopQuery("STARTING")).toBe(true);
    expect(canStopQuery("RUNNING")).toBe(true);
    for (const state of ["STOPPED", "FAILED", "COMPLETED", "future"])
      expect(canStopQuery(state)).toBe(false);
    expect(queryStatement("STOP", "query'1; --")).toBe(
      "STOP QUERY 'query''1; --';",
    );
  });

  it("filters names, IDs, states, and source health without modifying SQL", () => {
    const queries = readQueries(
      queryResult({
        query_id: "ABC",
        name: "People",
        state: "RUNNING",
        source_health: "reconnecting",
      }),
    );
    for (const term of ["people", " abc ", "RUNNING", "reconnecting"])
      expect(filterQueries(queries, term)).toEqual(queries);
    expect(filterQueries(queries, "stopped")).toEqual([]);
  });

  it("preserves the redacted definition and missing inspection values", () => {
    const details = readQueryDetails(
      queryResult({
        query_id: "id",
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
    expect(() => readQueryDetails(queryResult())).toThrow(
      "one Query definition",
    );
    expect(() =>
      readQueries(queryResult({ name: "people", state: "RUNNING" })),
    ).toThrow("query_id");
  });
});
