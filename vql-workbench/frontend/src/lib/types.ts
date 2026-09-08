export type ExecutionPhase =
  | "disconnected"
  | "idle"
  | "preparing"
  | "running"
  | "cancelling"
  | "completed"
  | "failed";

export type ProblemSource =
  "vql" | "connectivity" | "backend" | "policy" | "browser";

export interface WorkbenchProblem {
  source: ProblemSource;
  title: string;
  message: string;
  code?: string;
  symbol?: string;
  targetVersion?: string;
  httpStatus?: number;
}

export interface Draft {
  id: string;
  name: string;
  sql: string;
  updatedAt: number;
}

export interface ResultField {
  name: string;
  type: string;
  extensionName?: string;
  nullable: boolean;
}

export type ResultValue =
  | null
  | boolean
  | number
  | string
  | bigint
  | Uint8Array
  | ResultValue[]
  | { [key: string]: ResultValue };

export interface ResultRow {
  id: number;
  values: Record<string, ResultValue>;
}

export interface QueryResult {
  fields: ResultField[];
  rows: ResultRow[];
  receivedBatches: number;
  rolling: boolean;
}

export interface OverlayConfig {
  imageColumn: string | null;
  boxColumn: string | null;
  labelColumn: string | null;
  confidenceColumn: string | null;
}

export interface ExecutionState {
  phase: ExecutionPhase;
  executionId: string | null;
  resultMode: "bounded" | "unbounded" | null;
  startedAt: number | null;
  elapsedMs: number | null;
  affectedRows: number | null;
  result: QueryResult | null;
  problem: WorkbenchProblem | null;
  runId: string | null;
}

export interface HistoryRecord {
  id: string;
  draftName: string;
  sql: string;
  startedAt: number;
  state: "running" | "completed" | "cancelled" | "failed";
  elapsedMs: number | null;
  rowCount: number | null;
  resultMode: "bounded" | "unbounded" | "none" | null;
  problem?: Pick<WorkbenchProblem, "source" | "code" | "symbol" | "message">;
}

export interface ImageValue {
  uri?: string | null;
  locator?: string | null;
  pts_ms?: number | bigint | null;
  frame_id?: number | bigint | null;
  encoded?: Uint8Array | null;
  encoding?: string | null;
  width?: number | null;
  height?: number | null;
  buffer_id?: number | bigint | null;
  buffer_slot?: number | null;
}

export interface Box2dValue {
  x: number;
  y: number;
  w: number;
  h: number;
}
