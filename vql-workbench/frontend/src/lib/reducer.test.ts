import { describe, expect, it } from "vitest";

import { executionReducer, initialExecutionState } from "./reducer";

describe("executionReducer", () => {
  it("enforces one connected execution lifecycle", () => {
    const idle = executionReducer(initialExecutionState, { type: "connected" });
    const preparing = executionReducer(idle, {
      type: "preparing",
      runId: "run-1",
      startedAt: 10,
    });
    const running = executionReducer(preparing, {
      type: "started",
      runId: "run-1",
      executionId: "execution-1",
      resultMode: "bounded",
    });
    const ignoredSecondStart = executionReducer(running, {
      type: "preparing",
      runId: "run-2",
      startedAt: 20,
    });

    expect(running.phase).toBe("running");
    expect(ignoredSecondStart).toEqual(running);
    expect(
      executionReducer(running, {
        type: "cancelling",
        executionId: "execution-1",
      }).phase,
    ).toBe("cancelling");
  });

  it("clears transient results when a new execution prepares", () => {
    const completed = {
      ...initialExecutionState,
      phase: "completed" as const,
      elapsedMs: 42,
      affectedRows: 1,
    };
    const next = executionReducer(completed, {
      type: "preparing",
      runId: "run-2",
      startedAt: 100,
    });

    expect(next.phase).toBe("preparing");
    expect(next.affectedRows).toBeNull();
    expect(next.result).toBeNull();
  });

  it("keeps a failed connection disconnected", () => {
    const problem = {
      source: "connectivity" as const,
      title: "Connection failed",
      message: "vqld refused the connection",
    };

    const next = executionReducer(initialExecutionState, {
      type: "connection_failed",
      problem,
    });

    expect(next.phase).toBe("disconnected");
    expect(next.problem).toEqual(problem);
  });

  it("ignores asynchronous updates from a different execution", () => {
    const idle = executionReducer(initialExecutionState, { type: "connected" });
    const preparing = executionReducer(idle, {
      type: "preparing",
      runId: "run-1",
      startedAt: 10,
    });
    const running = executionReducer(preparing, {
      type: "started",
      runId: "run-1",
      executionId: "execution-1",
      resultMode: "bounded",
    });
    const result = {
      fields: [],
      rows: [],
      receivedBatches: 0,
      rolling: false,
    };

    expect(
      executionReducer(running, {
        type: "batch",
        executionId: "execution-2",
        result,
      }),
    ).toEqual(running);
    expect(
      executionReducer(running, {
        type: "completed",
        executionId: "execution-2",
        elapsedMs: 20,
      }),
    ).toEqual(running);
    expect(
      executionReducer(running, {
        type: "failed",
        runId: "run-2",
        elapsedMs: 20,
        problem: {
          source: "backend",
          title: "Wrong run",
          message: "This must be ignored",
        },
      }),
    ).toEqual(running);
  });

  it("returns to running after cancellation fails", () => {
    const running = {
      ...initialExecutionState,
      phase: "running" as const,
      runId: "run-1",
      executionId: "execution-1",
    };
    const cancelling = executionReducer(running, {
      type: "cancelling",
      executionId: "execution-1",
    });
    const problem = {
      source: "connectivity" as const,
      title: "Cancel failed",
      message: "vqld did not acknowledge cancellation",
    };

    const restored = executionReducer(cancelling, {
      type: "cancel_failed",
      executionId: "execution-1",
      problem,
    });

    expect(restored.phase).toBe("running");
    expect(restored.problem).toEqual(problem);
  });
});
