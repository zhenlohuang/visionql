import { StrictMode, useState } from "react";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useCatalogNavigation } from "./useCatalogNavigation";
import type { CatalogExecutor } from "./catalog";
import type { QueryResult, ResultValue } from "./types";

const result = (name?: string): QueryResult => ({
  fields: [],
  rows: name
    ? [{ id: 0, values: { table_name: name, provider: "IMAGES" } }]
    : [],
  receivedBatches: 1,
  rolling: false,
});

function mount(execute: CatalogExecutor, blocked = false) {
  return renderHook(
    ({ connected, sessionVersion, blocked }) => {
      const [navigationBusy, setNavigationBusy] = useState(false);
      return useCatalogNavigation({
        connected,
        sessionVersion,
        busy: blocked || navigationBusy,
        execute,
        onBusyChange: setNavigationBusy,
        canExecute: () => !blocked,
      });
    },
    {
      initialProps: { connected: true, sessionVersion: 0, blocked },
      wrapper: StrictMode,
    },
  );
}

afterEach(cleanup);

describe("Catalog navigation requests", () => {
  it("waits for the execution slot and serializes listing reads under StrictMode", async () => {
    let active = 0;
    let maxActive = 0;
    const execute = vi.fn(async (sql: string) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await Promise.resolve();
      active -= 1;
      return sql === "SHOW TABLES;" ? result("quality_photos") : result();
    });
    const view = mount(execute, true);
    expect(execute).not.toHaveBeenCalled();
    view.rerender({ connected: true, sessionVersion: 0, blocked: false });
    await waitFor(() =>
      expect(view.result.current.objects.tables).toHaveLength(1),
    );
    expect(execute.mock.calls.map(([sql]) => sql)).toEqual([
      "SHOW TABLES;",
      "SHOW MODELS;",
      "SHOW FUNCTIONS;",
    ]);
    expect(maxActive).toBe(1);
    expect(view.result.current.loading).toBe(false);
  });

  it("loads each listing once when mounted already connected under StrictMode", async () => {
    const execute = vi.fn(async (_sql: string) => result());
    const view = mount(execute);
    await waitFor(() => expect(view.result.current.loading).toBe(false));
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(3));
    expect(execute.mock.calls.map(([sql]) => sql)).toEqual([
      "SHOW TABLES;",
      "SHOW MODELS;",
      "SHOW FUNCTIONS;",
    ]);
  });

  it("cancels old Session reads and never publishes their stale objects", async () => {
    let resolveOld!: (value: QueryResult) => void;
    let oldSignal: AbortSignal | undefined;
    let first = true;
    const execute = vi.fn((sql: string, signal?: AbortSignal) => {
      if (first) {
        first = false;
        oldSignal = signal;
        return new Promise<QueryResult>((resolve) => {
          resolveOld = resolve;
        });
      }
      return Promise.resolve(
        sql === "SHOW TABLES;" ? result("fresh_photos") : result(),
      );
    });
    const view = mount(execute, true);
    view.rerender({ connected: true, sessionVersion: 0, blocked: false });
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
    view.rerender({ connected: true, sessionVersion: 1, blocked: false });
    expect(oldSignal?.aborted).toBe(true);
    await act(async () => resolveOld(result("stale_photos")));
    await waitFor(() =>
      expect(view.result.current.objects.tables[0]?.name).toBe("fresh_photos"),
    );
    view.rerender({ connected: false, sessionVersion: 1, blocked: false });
    expect(view.result.current.objects.tables).toEqual([]);
  });

  it("preserves structured errors and retries only on explicit refresh", async () => {
    const problem = {
      source: "vql",
      title: "Denied",
      message: "restricted",
      symbol: "PERMISSION_DENIED",
      code: "VQL-test",
    };
    let denied = true;
    const execute = vi.fn(async (sql: string) => {
      if (denied && sql === "SHOW MODELS;") throw problem;
      return sql === "SHOW TABLES;" ? result("photos") : result();
    });
    const view = mount(execute, true);
    view.rerender({ connected: true, sessionVersion: 0, blocked: false });
    await waitFor(() => expect(view.result.current.problem).toEqual(problem));
    expect(view.result.current.loading).toBe(false);
    expect(execute).toHaveBeenCalledTimes(2);
    denied = false;
    act(() => view.result.current.refresh());
    await waitFor(() =>
      expect(view.result.current.objects.tables).toHaveLength(1),
    );
    expect(view.result.current.problem).toBeNull();
    expect(execute).toHaveBeenCalledTimes(5);
  });
  it("shares multi-version Model metadata with callable entries and avoids single-version reads", async () => {
    const rows = (...values: Record<string, ResultValue>[]): QueryResult => ({
      fields: [],
      rows: values.map((values, id) => ({ id, values })),
      receivedBatches: 1,
      rolling: false,
    });
    const execute = vi.fn(async (sql: string) => {
      if (sql === "SHOW TABLES;") return result();
      if (sql === "SHOW MODELS;")
        return rows(
          { catalog: "vql", schema: "quality", name: "detector", versions: 2 },
          { catalog: "vql", schema: "default", name: "single", versions: 1 },
        );
      if (sql === "SHOW FUNCTIONS;")
        return rows({
          catalog: "vql",
          schema: "quality",
          name: "detector",
          kind: "MODEL",
        });
      if (sql === 'SHOW MODEL VERSIONS "quality"."detector";')
        return rows(
          { version: "v1", is_default: false },
          { version: "v2", is_default: true },
        );
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const view = mount(execute);
    await waitFor(() =>
      expect(view.result.current.objects.functions[0]?.versions).toHaveLength(
        2,
      ),
    );
    expect(
      view.result.current.objects.models.find(
        (object) => object.name === "detector",
      )?.versions,
    ).toEqual(view.result.current.objects.functions[0].versions);
    expect(execute.mock.calls.map(([sql]) => sql)).toEqual([
      "SHOW TABLES;",
      "SHOW MODELS;",
      "SHOW FUNCTIONS;",
      'SHOW MODEL VERSIONS "quality"."detector";',
    ]);
    expect(
      view.result.current.objects.functions[0].versions?.[1].isDefault,
    ).toBe(true);
  });

  it("keeps objects available when a version metadata read fails", async () => {
    const denied = {
      source: "vql",
      title: "Denied",
      message: "restricted",
      symbol: "PERMISSION_DENIED",
      code: "VQL-test",
    };
    const execute = vi.fn(async (sql: string): Promise<QueryResult> => {
      if (sql === "SHOW MODELS;")
        return {
          ...result(),
          rows: [
            {
              id: 0,
              values: {
                catalog: "vql",
                schema: "default",
                name: "detector",
                versions: 2,
              },
            },
          ],
        };
      if (sql.startsWith("SHOW MODEL VERSIONS")) throw denied;
      return result();
    });
    const view = mount(execute);
    await waitFor(() =>
      expect(view.result.current.objects.models[0]?.versionsProblem).toEqual(
        denied,
      ),
    );
    expect(view.result.current.problem).toBeNull();
    expect(view.result.current.objects.models[0].name).toBe("detector");
  });
});
