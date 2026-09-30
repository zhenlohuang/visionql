import { tableFromArrays, tableToIPC } from "apache-arrow";
import { afterEach, describe, expect, it, vi } from "vitest";

import { executeBoundedSql } from "./api";

afterEach(() => vi.unstubAllGlobals());

const start = {
  kind: "query",
  resultMode: "bounded",
  executionId: "execution-1",
  affectedRows: null,
  elapsedMs: null,
};
const status = {
  executionId: "execution-1",
  status: "completed",
  elapsedMs: 1,
  problem: null,
};
function arrowResponse() {
  const ipc = tableToIPC(
    tableFromArrays({ query_id: ["query-1"], state: ["RUNNING"] }),
    "stream",
  );
  return new Response(
    ipc.buffer.slice(
      ipc.byteOffset,
      ipc.byteOffset + ipc.byteLength,
    ) as ArrayBuffer,
  );
}

describe("bounded management execution", () => {
  it("uses the existing execution routes and consumes Arrow before checking terminal status", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json(start))
      .mockResolvedValueOnce(arrowResponse())
      .mockResolvedValueOnce(Response.json(status));
    vi.stubGlobal("fetch", fetch);
    const result = await executeBoundedSql("SHOW QUERIES;");
    expect(result.rows[0].values).toEqual({
      query_id: "query-1",
      state: "RUNNING",
    });
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      "/api/executions",
      "/api/executions/execution-1/results",
      "/api/executions/execution-1",
    ]);
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({
      sql: "SHOW QUERIES;",
      allowUnbounded: false,
    });
  });

  it("preserves a structured terminal failure even when the Arrow body decodes", async () => {
    const problem = {
      source: "vql",
      title: "VisionQL statement failed",
      code: "VQL-42004",
      symbol: "QUERY_NOT_FOUND",
      message: "Query not found",
    };
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json(start))
      .mockResolvedValueOnce(arrowResponse())
      .mockResolvedValueOnce(
        Response.json({ ...status, status: "failed", problem }),
      )
      .mockResolvedValueOnce(Response.json(status));
    vi.stubGlobal("fetch", fetch);
    await expect(
      executeBoundedSql("DESCRIBE QUERY 'missing';"),
    ).rejects.toEqual(problem);
    expect(fetch.mock.calls[3]).toEqual([
      "/api/executions/execution-1",
      { method: "DELETE" },
    ]);
  });

  it("waits for a started execution's ID and cancels that execution when navigation aborts preparation", async () => {
    let finishStart!: (response: Response) => void;
    const prepared = new Promise<Response>((resolve) => {
      finishStart = resolve;
    });
    const fetch = vi
      .fn()
      .mockReturnValueOnce(prepared)
      .mockResolvedValueOnce(Response.json(status));
    vi.stubGlobal("fetch", fetch);
    const controller = new AbortController();
    const request = executeBoundedSql("SHOW QUERIES;", controller.signal);
    controller.abort();
    finishStart(Response.json(start));
    await expect(request).rejects.toMatchObject({ name: "AbortError" });
    expect(fetch.mock.calls[1]).toEqual([
      "/api/executions/execution-1",
      { method: "DELETE" },
    ]);
  });

  it("rejects an expired Session with its HTTP identity intact", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        Response.json(
          {
            source: "backend",
            title: "Workbench is disconnected",
            message: "Reconnect to vqld.",
          },
          { status: 401 },
        ),
      ),
    );
    await expect(executeBoundedSql("SHOW QUERIES;")).rejects.toMatchObject({
      httpStatus: 401,
      title: "Workbench is disconnected",
    });
  });
});
