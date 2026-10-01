import {
  DataType,
  RecordBatchReader,
  Struct,
  type Field,
  type RecordBatch,
  type Vector,
} from "apache-arrow";

import type { QueryResult, ResultField, ResultRow, ResultValue } from "./types";

const ROLLING_ROW_LIMIT = 500;

export async function consumeArrowResponse(
  response: Response,
  rolling: boolean,
  onBatch: (result: QueryResult) => void,
): Promise<QueryResult> {
  if (!response.body) throw new Error("Arrow response did not contain a body");
  const reader = await RecordBatchReader.from(response.body);
  await reader.open();
  const result: QueryResult = {
    fields: schemaFields(reader.schema.fields),
    rows: [],
    receivedBatches: 0,
    rolling,
  };
  let nextId = 1;
  for await (const batch of reader) {
    const rows = rowsFromBatch(batch, result.fields, nextId);
    nextId += rows.length;
    result.rows = rolling
      ? [...result.rows, ...rows].slice(-ROLLING_ROW_LIMIT)
      : [...result.rows, ...rows];
    result.receivedBatches += 1;
    onBatch({ ...result, rows: [...result.rows] });
  }
  if (result.receivedBatches === 0) {
    onBatch({ ...result, rows: [] });
  }
  return result;
}

export function schemaFields(fields: Field[]): ResultField[] {
  const keys = columnKeys(fields);
  return fields.map((field, index) => ({
    key: keys[index],
    name: field.name ?? "",
    type: displayType(field),
    extensionName: field.metadata?.get("ARROW:extension:name"),
    nullable: field.nullable,
  }));
}

function columnKeys(fields: Field[]): string[] {
  const names = fields.map((field) => field.name ?? "");
  const counts = new Map<string, number>();
  for (const name of names) {
    counts.set(name, (counts.get(name) ?? 0) + 1);
  }
  // Reserve original names so a generated duplicate key cannot hide a real
  // column named, for example, "id [column 1]".
  const used = new Set(names);
  return names.map((name, index) => {
    if (name && counts.get(name) === 1) return name;
    let key = `${name} [column ${index + 1}]`;
    while (used.has(key)) key += "#";
    used.add(key);
    return key;
  });
}

function displayType(field: Field): string {
  const extension = field.metadata?.get("ARROW:extension:name");
  if (extension === "vql.image") return "IMAGE";
  if (extension === "vql.box2d") return "BOX2D";
  return field.type
    .toString()
    .replace(/<.*>/, (value: string) => value.toUpperCase());
}

function rowsFromBatch(
  batch: RecordBatch,
  fields: ResultField[],
  startId: number,
): ResultRow[] {
  return Array.from({ length: batch.numRows }, (_, rowIndex) => {
    const values = Object.fromEntries(
      batch.schema.fields.map((field, columnIndex) => {
        const vector = batch.getChildAt(columnIndex);
        return [
          fields[columnIndex].key,
          vector ? readValue(vector, rowIndex, field) : null,
        ];
      }),
    );
    return { id: startId + rowIndex, values };
  });
}

function readValue(vector: Vector, index: number, field: Field): ResultValue {
  if (!vector.isValid(index)) return null;
  if (field.type instanceof Struct || DataType.isStruct(field.type)) {
    const children = field.type.children;
    return Object.fromEntries(
      children.map((child, childIndex) => {
        const childVector = vector.getChildAt(childIndex);
        return [
          child.name,
          childVector ? readValue(childVector, index, child) : null,
        ];
      }),
    );
  }
  return normalize(vector.get(index));
}

function normalize(value: unknown): ResultValue {
  if (value == null) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  if (value instanceof Uint8Array) return value;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(normalize);
  if (typeof value === "object") {
    const maybeJson = value as { toJSON?: () => unknown };
    if (typeof maybeJson.toJSON === "function") {
      const json = maybeJson.toJSON();
      if (json !== value) return normalize(json);
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, normalize(nested)]),
    );
  }
  return String(value);
}
