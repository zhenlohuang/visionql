import type { QueryResult, WorkbenchProblem } from "./types";

export interface SessionResponse {
  connected: boolean;
  endpoint: string;
  sessionId: string | null;
}

export interface SessionInput {
  endpoint: string;
  credential?: string;
  tlsCaPem?: string;
}

export interface StartExecutionResponse {
  kind: "query" | "update";
  resultMode: "bounded" | "unbounded" | "none";
  executionId: string | null;
  affectedRows: number | null;
  elapsedMs: number | null;
}

export interface ExecutionStatusResponse {
  executionId: string;
  status: "running" | "cancelling" | "completed" | "cancelled" | "failed";
  resultMode: "bounded" | "unbounded" | null;
  elapsedMs: number;
  problem: WorkbenchProblem | null;
}

export async function getSession(): Promise<SessionResponse> {
  return requestJson<SessionResponse>("/api/session");
}

export async function createSession(
  input: SessionInput,
): Promise<SessionResponse> {
  return requestJson<SessionResponse>("/api/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });
}

export async function closeSession(): Promise<void> {
  const response = await fetch("/api/session", { method: "DELETE" });
  if (!response.ok) throw await responseProblem(response);
}

export async function startExecution(
  sql: string,
  allowUnbounded: boolean,
): Promise<StartExecutionResponse> {
  return requestJson<StartExecutionResponse>("/api/executions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql, allowUnbounded }),
  });
}

export async function getResultResponse(
  executionId: string,
  signal?: AbortSignal,
): Promise<Response> {
  const response = await fetch(
    `/api/executions/${encodeURIComponent(executionId)}/results`,
    { signal },
  );
  if (!response.ok) throw await responseProblem(response);
  return response;
}

// Management statements share the Session's single execution slot with the editor.
// Always consume their Arrow result and check terminal status before releasing it.
export async function executeBoundedSql(
  sql: string,
  signal?: AbortSignal,
): Promise<QueryResult> {
  signal?.throwIfAborted();
  const start = await startExecution(sql, false);
  const executionId = start.executionId;
  if (!executionId) throw new Error("Expected a bounded Arrow query result");
  let completed = false;
  try {
    signal?.throwIfAborted();
    if (start.kind !== "query" || start.resultMode !== "bounded") {
      throw new Error("Expected a bounded Arrow query result");
    }
    const response = await getResultResponse(executionId, signal);
    const { consumeArrowResponse } = await import("./arrow");
    let result: QueryResult;
    try {
      result = await consumeArrowResponse(response, false, () => undefined);
    } catch (error) {
      if (!signal?.aborted) {
        const status = await waitForTerminalStatus(executionId).catch(
          () => null,
        );
        if (status?.problem) throw status.problem;
      }
      throw error;
    }
    const status = await waitForTerminalStatus(executionId);
    if (status.status !== "completed") {
      throw status.problem ?? new Error(`Execution ${status.status}`);
    }
    completed = true;
    signal?.throwIfAborted();
    return result;
  } finally {
    if (!completed) await cancelExecution(executionId).catch(() => undefined);
  }
}

export async function getExecutionStatus(
  executionId: string,
): Promise<ExecutionStatusResponse> {
  return requestJson<ExecutionStatusResponse>(
    `/api/executions/${encodeURIComponent(executionId)}`,
  );
}

export async function cancelExecution(executionId: string): Promise<void> {
  await requestJson(`/api/executions/${encodeURIComponent(executionId)}`, {
    method: "DELETE",
  });
}

export async function waitForTerminalStatus(
  executionId: string,
): Promise<ExecutionStatusResponse> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const status = await getExecutionStatus(executionId);
    if (!["running", "cancelling"].includes(status.status)) return status;
    await new Promise((resolve) => window.setTimeout(resolve, 50));
  }
  return getExecutionStatus(executionId);
}

async function requestJson<T = unknown>(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(input, init);
  if (!response.ok) throw await responseProblem(response);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

async function responseProblem(response: Response): Promise<WorkbenchProblem> {
  try {
    return {
      ...((await response.json()) as WorkbenchProblem),
      httpStatus: response.status,
    };
  } catch {
    return {
      source: "browser",
      title: "Invalid Workbench response",
      message: `The Workbench backend returned HTTP ${response.status}.`,
      httpStatus: response.status,
    };
  }
}

export function asProblem(error: unknown): WorkbenchProblem {
  if (isProblem(error)) return error;
  return {
    source: "browser",
    title: "Browser operation failed",
    message: error instanceof Error ? error.message : String(error),
  };
}

function isProblem(error: unknown): error is WorkbenchProblem {
  if (!error || typeof error !== "object") return false;
  const value = error as Partial<WorkbenchProblem>;
  return (
    typeof value.source === "string" &&
    typeof value.title === "string" &&
    typeof value.message === "string"
  );
}
