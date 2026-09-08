import type { WorkbenchProblem } from "./types";

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
): Promise<Response> {
  const response = await fetch(
    `/api/executions/${encodeURIComponent(executionId)}/results`,
  );
  if (!response.ok) throw await responseProblem(response);
  return response;
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
