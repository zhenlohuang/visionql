import { describe, expect, it } from "vitest";

import { selectedOrCurrentStatement } from "./sql";

describe("selectedOrCurrentStatement", () => {
  it("keeps semicolons inside strings and comments out of statement boundaries", () => {
    const sql = "SELECT ';' AS value; -- not ; a delimiter\nSELECT 2;";
    const cursor = sql.lastIndexOf("2");

    expect(selectedOrCurrentStatement(sql, cursor, cursor).sql).toBe(
      "-- not ; a delimiter\nSELECT 2;",
    );
  });

  it("returns the exact selected editor range", () => {
    const sql = "SELECT 1;\nSELECT 2;";
    expect(selectedOrCurrentStatement(sql, 0, 8)).toEqual({
      sql: "SELECT 1",
      from: 0,
      to: 8,
    });
  });

  it("supports nested block comments", () => {
    const sql = "/* outer /* inner ; */ done */ SELECT 7; SELECT 8;";
    const cursor = sql.lastIndexOf("8");
    expect(selectedOrCurrentStatement(sql, cursor, cursor).sql).toBe(
      "SELECT 8;",
    );
  });
});
