import type { Draft, HistoryRecord } from "./types";

const HISTORY_KEY = "visionql.workbench.history.v1";
const DRAFTS_KEY = "visionql.workbench.drafts.v1";
const HISTORY_LIMIT = 500;

export const DEFAULT_SQL = `-- Query visual detections from a bounded video sample
SELECT f.ts,
       f.frame AS thumbnail,
       det.label,
       det.confidence,
       det.box
FROM entrance_videos AS f,
     UNNEST(yolo26n(f.frame, min_confidence => 0.60)) AS det
WHERE det.label = 'person'
ORDER BY f.ts DESC
LIMIT 5;`;

export function createDraft(drafts: Draft[] = [], sql = DEFAULT_SQL): Draft {
  const names = new Set(drafts.map((draft) => draft.name));
  let name = "Untitle.sql";
  let suffix = 1;
  while (names.has(name)) {
    name = `Untitle${suffix}.sql`;
    suffix += 1;
  }
  return {
    id: crypto.randomUUID(),
    name,
    sql,
    updatedAt: Date.now(),
  };
}

export function loadDrafts(): Draft[] {
  const drafts = readJson<Draft[]>(DRAFTS_KEY);
  if (!drafts?.length) return [createDraft()];
  return drafts.filter(isDraft);
}

export function saveDrafts(drafts: Draft[]): void {
  localStorage.setItem(DRAFTS_KEY, JSON.stringify(drafts));
}

export function loadHistory(): HistoryRecord[] {
  return (readJson<HistoryRecord[]>(HISTORY_KEY) ?? [])
    .filter(isHistoryRecord)
    .map((record) =>
      record.state === "running"
        ? {
            ...record,
            state: "cancelled" as const,
            problem: {
              source: "browser" as const,
              message:
                "The browser closed before this attached execution completed.",
            },
          }
        : record,
    )
    .slice(0, HISTORY_LIMIT);
}

export function saveHistory(history: HistoryRecord[]): void {
  localStorage.setItem(
    HISTORY_KEY,
    JSON.stringify(history.slice(0, HISTORY_LIMIT)),
  );
}

export function upsertHistory(
  history: HistoryRecord[],
  record: HistoryRecord,
): HistoryRecord[] {
  return [record, ...history.filter((item) => item.id !== record.id)].slice(
    0,
    HISTORY_LIMIT,
  );
}

function readJson<T>(key: string): T | null {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

function isDraft(value: unknown): value is Draft {
  if (!value || typeof value !== "object") return false;
  const draft = value as Partial<Draft>;
  return (
    typeof draft.id === "string" &&
    typeof draft.name === "string" &&
    typeof draft.sql === "string" &&
    typeof draft.updatedAt === "number"
  );
}

function isHistoryRecord(value: unknown): value is HistoryRecord {
  if (!value || typeof value !== "object") return false;
  const record = value as Partial<HistoryRecord>;
  return (
    typeof record.id === "string" &&
    typeof record.draftName === "string" &&
    typeof record.sql === "string" &&
    typeof record.startedAt === "number"
  );
}
