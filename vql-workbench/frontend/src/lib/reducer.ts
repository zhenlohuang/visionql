import type { ExecutionState, QueryResult, WorkbenchProblem } from "./types";

export type ExecutionAction =
  | { type: "connected" }
  | { type: "connection_failed"; problem: WorkbenchProblem }
  | { type: "disconnected" }
  | { type: "preparing"; runId: string; startedAt: number }
  | {
      type: "started";
      runId: string;
      executionId: string;
      resultMode: "bounded" | "unbounded";
    }
  | { type: "batch"; executionId: string; result: QueryResult }
  | {
      type: "update_completed";
      runId: string;
      elapsedMs: number;
      affectedRows: number;
    }
  | { type: "cancelling"; executionId: string }
  | {
      type: "cancel_failed";
      executionId: string;
      problem: WorkbenchProblem;
    }
  | { type: "completed"; executionId: string; elapsedMs: number }
  | {
      type: "failed";
      runId?: string;
      executionId?: string;
      elapsedMs: number | null;
      problem: WorkbenchProblem;
    }
  | { type: "reset" };

export const initialExecutionState: ExecutionState = {
  phase: "disconnected",
  executionId: null,
  resultMode: null,
  startedAt: null,
  elapsedMs: null,
  affectedRows: null,
  result: null,
  problem: null,
  runId: null,
};

export function executionReducer(
  state: ExecutionState,
  action: ExecutionAction,
): ExecutionState {
  switch (action.type) {
    case "connected":
      return { ...initialExecutionState, phase: "idle" };
    case "connection_failed":
      return {
        ...initialExecutionState,
        phase: "disconnected",
        problem: action.problem,
      };
    case "disconnected":
      return initialExecutionState;
    case "preparing":
      if (!["idle", "completed", "failed"].includes(state.phase)) return state;
      return {
        ...initialExecutionState,
        phase: "preparing",
        runId: action.runId,
        startedAt: action.startedAt,
      };
    case "started":
      if (state.phase !== "preparing" || state.runId !== action.runId)
        return state;
      return {
        ...state,
        phase: "running",
        executionId: action.executionId,
        resultMode: action.resultMode,
      };
    case "batch":
      if (
        !["running", "cancelling"].includes(state.phase) ||
        state.executionId !== action.executionId
      )
        return state;
      return { ...state, result: action.result };
    case "update_completed":
      if (state.phase !== "preparing" || state.runId !== action.runId)
        return state;
      return {
        ...state,
        phase: "completed",
        elapsedMs: action.elapsedMs,
        affectedRows: action.affectedRows,
        resultMode: null,
      };
    case "cancelling":
      return state.phase === "running" &&
        state.executionId === action.executionId
        ? { ...state, phase: "cancelling", problem: null }
        : state;
    case "cancel_failed":
      return state.phase === "cancelling" &&
        state.executionId === action.executionId
        ? { ...state, phase: "running", problem: action.problem }
        : state;
    case "completed":
      if (
        !["running", "cancelling"].includes(state.phase) ||
        state.executionId !== action.executionId
      )
        return state;
      return {
        ...state,
        phase: "completed",
        elapsedMs: action.elapsedMs,
        problem: null,
      };
    case "failed":
      if (
        (action.runId !== undefined && state.runId !== action.runId) ||
        (action.executionId !== undefined &&
          state.executionId !== action.executionId)
      )
        return state;
      return {
        ...state,
        phase: "failed",
        elapsedMs: action.elapsedMs,
        problem: action.problem,
      };
    case "reset":
      return {
        ...initialExecutionState,
        phase: state.phase === "disconnected" ? "disconnected" : "idle",
      };
  }
}
