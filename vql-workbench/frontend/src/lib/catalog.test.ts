import { describe, expect, it, vi } from "vitest";

import {
  executeCatalogStatement,
  loadCatalog,
  modelAction,
  quoteName,
  type CatalogObject,
} from "./catalog";
import type { QueryResult, ResultValue } from "./types";
import * as api from "./api";
import * as arrow from "./arrow";

function result(...rows: Record<string, ResultValue>[]): QueryResult {
  return {
    fields: [],
    rows: rows.map((values, index) => ({ id: index, values })),
    receivedBatches: 1,
    rolling: false,
  };
}

describe("catalog public SQL mapping", () => {
  it("uses MODEL metadata for model callables in SHOW FUNCTIONS", async () => {
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW FUNCTIONS;")
        return result({
          catalog: "vql",
          schema: "default",
          name: "detector",
          kind: "MODEL",
          arguments: "frame IMAGE",
          return_type: "ARRAY<STRUCT>",
        });
      if (sql.startsWith("SHOW CREATE MODEL"))
        return result({
          object_name: "detector",
          create_sql:
            "CREATE MODEL detector TYPE OBJECT_DETECTION FROM 'mock://person'",
        });
      if (sql.startsWith("DESCRIBE MODEL"))
        return result({ status: "UNRESOLVED" });
      if (sql.startsWith("SHOW MODEL VERSIONS"))
        return result({
          version: "v1",
          status: "UNRESOLVED",
          is_default: true,
        });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const [object] = await loadCatalog("functions", execute);
    expect(object.kind).toBe("MODEL");
    expect(object.category).toBe("Model callable");
    expect(object.versions?.rows).toHaveLength(1);
    expect(execute.mock.calls.map(([sql]) => sql)).toEqual([
      "SHOW FUNCTIONS;",
      'SHOW CREATE MODEL "detector";',
      'DESCRIBE MODEL "detector";',
      'SHOW MODEL VERSIONS "detector";',
    ]);
  });

  it("recovers stored qualified names only on structured NOT_FOUND", async () => {
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW FUNCTIONS;")
        return result({
          catalog: "vql",
          schema: "quality",
          name: "blur",
          kind: "FUNCTION",
          arguments: "img IMAGE",
          return_type: "FLOAT",
        });
      if (sql === 'SHOW CREATE FUNCTION "quality"."blur";')
        throw {
          source: "vql",
          title: "Not found",
          message: "missing",
          symbol: "NOT_FOUND",
        };
      if (sql === 'SHOW CREATE FUNCTION "vql"."quality"."blur";')
        return result({
          object_name: "vql.quality.blur",
          create_sql:
            "CREATE FUNCTION blur(img IMAGE) RETURNS FLOAT LANGUAGE PYTHON AS 'quality:blur'",
        });
      if (sql === 'DESCRIBE FUNCTION "vql"."quality"."blur";')
        return result({ status: "AVAILABLE" });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const [object] = await loadCatalog("functions", execute);
    expect(object.name).toBe("vql.quality.blur");
    expect(object.category).toBe("Python UDF");
    expect(object.problem).toBeNull();
  });

  it("retains row metadata and exact structured errors when enrichment fails", async () => {
    const problem = {
      source: "vql",
      title: "Denied",
      message: "restricted",
      symbol: "PERMISSION_DENIED",
      code: "VQL-test",
    };
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW MODELS;")
        return result({
          catalog: "vql",
          schema: "default",
          name: "model",
          interface: "IMAGE → FLOAT",
        });
      throw problem;
    });
    const [object] = await loadCatalog("models", execute);
    expect(object.problem).toEqual(problem);
    expect(object.signature).toBe("IMAGE → FLOAT");
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("does not classify SQL expression string contents as a Python declaration", async () => {
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW FUNCTIONS;")
        return result({
          catalog: "vql",
          schema: "default",
          name: "words",
          kind: "FUNCTION",
          arguments: "BIGINT",
          return_type: "TEXT",
        });
      if (sql.startsWith("SHOW CREATE"))
        return result({
          object_name: "words",
          create_sql:
            "CREATE FUNCTION words(BIGINT) RETURNS TEXT RETURN 'LANGUAGE PYTHON AS ''quality:score'''",
        });
      return result({ status: "AVAILABLE" });
    });
    expect((await loadCatalog("functions", execute))[0].category).toBe(
      "SQL Expression",
    );
  });

  it("quotes object names and versions so actions remain one statement", () => {
    expect(quoteName('quality.odd"name')).toBe('"quality"."odd""name"');
    const object = { name: 'odd"name' } as CatalogObject;
    expect(
      modelAction(object, "dropVersion", "v1'; DROP MODEL other; --"),
    ).toBe(
      "ALTER MODEL \"odd\"\"name\" DROP VERSION 'v1''; DROP MODEL other; --';",
    );
  });

  it("stops enrichment after a cancelled request", async () => {
    const controller = new AbortController();
    const execute = vi.fn(async () => {
      controller.abort();
      return result({
        table_name: "photos",
        provider: "IMAGES",
        location: "/images",
      });
    });
    await expect(
      loadCatalog("tables", execute, controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("does not guess a mutation target when display addresses collide", async () => {
    const execute = vi.fn(async () =>
      result(
        {
          catalog: "vql",
          schema: "default",
          name: "same",
          kind: "FUNCTION",
          arguments: "BIGINT",
          return_type: "BIGINT",
        },
        {
          catalog: "vql",
          schema: "default",
          name: "same",
          kind: "FUNCTION",
          arguments: "FLOAT",
          return_type: "FLOAT",
        },
      ),
    );
    const objects = await loadCatalog("functions", execute);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(new Set(objects.map((object) => object.id)).size).toBe(2);
    expect(
      objects.every(
        (object) => object.problem?.source === "policy" && !object.ddl,
      ),
    ).toBe(true);
  });
});

describe("catalog execution transport", () => {
  it("accepts update responses without opening an Arrow stream", async () => {
    const start = vi.spyOn(api, "startExecution").mockResolvedValue({
      kind: "update",
      resultMode: "none",
      executionId: null,
      affectedRows: 0,
      elapsedMs: 1,
    });
    const get = vi.spyOn(api, "getResultResponse");
    try {
      expect(await executeCatalogStatement("DROP TABLE photos;")).toBeNull();
      expect(get).not.toHaveBeenCalled();
    } finally {
      start.mockRestore();
      get.mockRestore();
    }
  });

  it("forwards a late VQL error even when Arrow decoding succeeds", async () => {
    const problem = {
      source: "vql" as const,
      title: "Failed",
      message: "contract changed",
      code: "VQL-10000",
      symbol: "EXECUTION",
    };
    const spies = [
      vi.spyOn(api, "startExecution").mockResolvedValue({
        kind: "query",
        resultMode: "bounded",
        executionId: "execution-1",
        affectedRows: null,
        elapsedMs: null,
      }),
      vi.spyOn(api, "getResultResponse").mockResolvedValue(new Response()),
      vi.spyOn(arrow, "consumeArrowResponse").mockResolvedValue(result()),
      vi.spyOn(api, "waitForTerminalStatus").mockResolvedValue({
        executionId: "execution-1",
        status: "failed",
        resultMode: "bounded",
        elapsedMs: 1,
        problem,
      }),
      vi.spyOn(api, "cancelExecution").mockResolvedValue(),
    ];
    try {
      await expect(executeCatalogStatement("SHOW TABLES;")).rejects.toEqual(
        problem,
      );
      expect(api.cancelExecution).toHaveBeenCalledWith("execution-1");
    } finally {
      spies.forEach((spy) => spy.mockRestore());
    }
  });
});
