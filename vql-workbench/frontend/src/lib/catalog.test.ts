import { describe, expect, it, vi } from "vitest";
import {
  executeCatalogStatement,
  loadCatalogObject,
  listCatalog,
  listCatalogVersions,
  catalogAddress,
  createTemplate,
  quoteName,
} from "./catalog";
import type { QueryResult, ResultValue } from "./types";
import * as api from "./api";
import * as arrow from "./arrow";

function result(...rows: Record<string, ResultValue>[]): QueryResult {
  return {
    fields: [],
    rows: rows.map((values, id) => ({ id, values })),
    receivedBatches: 1,
    rolling: false,
  };
}

describe("catalog public SQL mapping", () => {
  it("lists same-named callables across namespaces without loading definitions", async () => {
    const execute = vi.fn(async () =>
      result(
        { catalog: "vql", schema: "default", name: "score", kind: "FUNCTION" },
        { catalog: "vql", schema: "quality", name: "score", kind: "FUNCTION" },
        { catalog: "team", schema: "media", name: "score", kind: "FUNCTION" },
      ),
    );
    const objects = await listCatalog("functions", execute);
    expect(objects.map((object) => object.namespace).sort()).toEqual([
      "team.media",
      "vql.default",
      "vql.quality",
    ]);
    expect(new Set(objects.map((object) => object.id)).size).toBe(3);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(catalogAddress("team.media.photos")).toEqual({
      catalog: "team",
      schema: "media",
      name: "photos",
    });
    expect(createTemplate("functions", "vql.quality")).toContain(
      'CREATE FUNCTION "quality.plus_one"',
    );
    expect(createTemplate("models", "team.media")).toContain(
      'CREATE MODEL "team"."media"."detector"',
    );
    expect(quoteName('quality.odd"name')).toBe('"quality"."odd""name"');
  });

  it("keeps Tables in the supported default namespace", async () => {
    const execute = vi.fn(async () =>
      result({ table_name: "photos", provider: "IMAGES" }),
    );
    const [object] = await listCatalog("tables", execute);
    expect(object.namespace).toBe("vql.default");
    expect(object.lookupNames).toEqual(["photos"]);
    expect(execute).toHaveBeenCalledWith("SHOW TABLES;", undefined);
  });

  it("preserves selection identity when refreshed rows change order", async () => {
    const first = await listCatalog("tables", async () =>
      result({ table_name: "photos" }),
    );
    const refreshed = await listCatalog("tables", async () =>
      result({ table_name: "camera" }, { table_name: "photos" }),
    );
    expect(refreshed.find((object) => object.name === "photos")?.id).toBe(
      first[0].id,
    );
  });

  it("loads only selected defining SQL and uses MODEL for model callables", async () => {
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW FUNCTIONS;")
        return result({
          catalog: "vql",
          schema: "default",
          name: "detector",
          kind: "MODEL",
        });
      if (sql === 'SHOW CREATE MODEL "detector";')
        return result({
          object_name: "detector",
          version: "v1",
          create_sql:
            "CREATE MODEL detector TYPE OBJECT_DETECTION VERSION 'v1' FROM 'mock://person'",
        });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const [listed] = await listCatalog("functions", execute);
    const object = await loadCatalogObject(listed, execute);
    expect(object.kind).toBe("MODEL");
    expect(object.ddl).toContain("CREATE MODEL");
    expect(object.ddlVersion).toBe("v1");
    expect(listed.ddl).toBe("");
    expect(execute.mock.calls.map(([sql]) => sql)).toEqual([
      "SHOW FUNCTIONS;",
      'SHOW CREATE MODEL "detector";',
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
          create_sql: "CREATE FUNCTION blur(BIGINT) RETURNS BIGINT RETURN $1",
        });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const [listed] = await listCatalog("functions", execute);
    expect((await loadCatalogObject(listed, execute)).name).toBe(
      "vql.quality.blur",
    );
    expect(execute).toHaveBeenCalledTimes(3);
  });

  it("preserves structured errors and does not fall back on permission failure", async () => {
    const problem = {
      source: "vql",
      title: "Denied",
      message: "restricted",
      symbol: "PERMISSION_DENIED",
      code: "VQL-test",
    };
    const [object] = await listCatalog("models", async () =>
      result({ catalog: "vql", schema: "quality", name: "model" }),
    );
    const execute = vi.fn(async () => {
      throw problem;
    });
    await expect(loadCatalogObject(object, execute)).rejects.toEqual(problem);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("stops fallback after cancellation", async () => {
    const controller = new AbortController();
    const [object] = await listCatalog("models", async () =>
      result({ catalog: "vql", schema: "quality", name: "model" }),
    );
    const execute = vi.fn(async () => {
      controller.abort();
      throw {
        source: "vql",
        title: "Not found",
        message: "missing",
        symbol: "NOT_FOUND",
      };
    });
    await expect(
      loadCatalogObject(object, execute, controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("loads versions via public SQL and retries only equivalent NOT_FOUND addresses", async () => {
    const [object] = await listCatalog("models", async () =>
      result({
        catalog: "vql",
        schema: "quality",
        name: "detector",
        versions: 2,
      }),
    );
    const execute = vi.fn(async (sql: string) => {
      if (sql === 'SHOW MODEL VERSIONS "quality"."detector";')
        throw {
          source: "vql",
          title: "Not found",
          message: "missing",
          symbol: "NOT_FOUND",
        };
      if (sql === 'SHOW MODEL VERSIONS "vql"."quality"."detector";')
        return result(
          { version: "v1", is_default: false },
          { version: "v2", is_default: true },
        );
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    expect(object.versionCount).toBe(2);
    expect(await listCatalogVersions(object, execute)).toEqual([
      { name: "v1", isDefault: false },
      { name: "v2", isDefault: true },
    ]);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("fetches the selected version directly, escapes its name, and forwards NOT_FOUND", async () => {
    const [object] = await listCatalog("models", async () =>
      result({ catalog: "vql", schema: "default", name: "detector" }),
    );
    const versionSql = `CREATE MODEL "detector" TYPE OBJECT_DETECTION VERSION 'one''s;release' FROM 'mock://candidate'`;
    const problem = {
      source: "vql",
      title: "Not found",
      message: "model 'detector' has no version 'removed'",
      symbol: "NOT_FOUND",
    };
    const execute = vi.fn(async (sql: string) => {
      if (sql.endsWith(" VERSION 'removed';")) throw problem;
      return result({
        object_name: "detector",
        version: "one's;release",
        create_sql: versionSql,
      });
    });
    const selected = await loadCatalogObject(
      object,
      execute,
      undefined,
      "one's;release",
    );
    expect(selected.ddl).toBe(versionSql);
    expect(selected.ddlVersion).toBe("one's;release");
    expect(execute).toHaveBeenCalledWith(
      "SHOW CREATE MODEL \"detector\" VERSION 'one''s;release';",
      undefined,
    );
    await expect(
      loadCatalogObject(object, execute, undefined, "removed"),
    ).rejects.toEqual(problem);
  });

  it("does not guess an object when display addresses collide", async () => {
    const execute = vi.fn(async () =>
      result(
        { catalog: "vql", schema: "default", name: "same", kind: "FUNCTION" },
        { catalog: "vql", schema: "default", name: "same", kind: "FUNCTION" },
      ),
    );
    const objects = await listCatalog("functions", execute);
    expect(new Set(objects.map((object) => object.id)).size).toBe(2);
    for (const object of objects)
      expect((await loadCatalogObject(object, execute)).problem?.source).toBe(
        "policy",
      );
    expect(execute).toHaveBeenCalledTimes(1);
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
