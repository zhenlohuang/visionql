import { describe, expect, it } from "vitest";

import { formatCatalogDdl, selectedOrCurrentStatement } from "./sql";

describe("Catalog DDL formatting", () => {
  it("formats VisionQL Table and Model clauses without changing quoted contents", async () => {
    expect(
      await formatCatalogDdl(
        "CREATE TABLE photos USING IMAGES LOCATION '/USING photos''s' OPTIONS (recursive=true);",
      ),
    ).toBe(
      "CREATE TABLE photos\nUSING IMAGES\nLOCATION '/USING photos''s'\nOPTIONS (recursive = TRUE);",
    );
    expect(
      await formatCatalogDdl(
        "CREATE MODEL detector TYPE OBJECT_DETECTION VERSION 'v1' FROM 'mock://person' USING ONNX_RUNTIME;",
      ),
    ).toBe(
      "CREATE MODEL detector\nTYPE OBJECT_DETECTION\nVERSION 'v1'\nFROM 'mock://person'\nUSING ONNX_RUNTIME;",
    );
  });

  it("preserves Function parameters, quoted identifiers, and Python entry points", async () => {
    expect(
      await formatCatalogDdl(
        'CREATE FUNCTION "quality"."score"(BIGINT) RETURNS BIGINT RETURN $1 + 1;',
      ),
    ).toContain('"quality"."score" (BIGINT)\nRETURNS BIGINT\nRETURN $1 + 1;');
    expect(
      await formatCatalogDdl(
        "CREATE FUNCTION score(value BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'quality:score';",
      ),
    ).toContain("\nLANGUAGE PYTHON AS 'quality:score';");
  });
});

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
