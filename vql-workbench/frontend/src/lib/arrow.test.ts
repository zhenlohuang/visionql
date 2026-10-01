import {
  Field,
  Int32,
  RecordBatch,
  Schema,
  Struct,
  Table,
  makeData,
  tableFromArrays,
  tableToIPC,
} from "apache-arrow";
import { describe, expect, it, vi } from "vitest";

import { consumeArrowResponse } from "./arrow";

describe("consumeArrowResponse", () => {
  it("opens and incrementally reads an Arrow IPC stream", async () => {
    const ipc = tableToIPC(
      tableFromArrays({ answer: Int32Array.from([42]), engine: ["VisionQL"] }),
      "stream",
    );
    const onBatch = vi.fn();
    const body = ipc.buffer.slice(
      ipc.byteOffset,
      ipc.byteOffset + ipc.byteLength,
    ) as ArrayBuffer;

    const result = await consumeArrowResponse(
      new Response(body),
      false,
      onBatch,
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "answer",
      "engine",
    ]);
    expect(result.rows[0].values).toEqual({ answer: 42, engine: "VisionQL" });
    expect(result.receivedBatches).toBe(1);
    expect(onBatch).toHaveBeenCalledOnce();
  });

  it("publishes typed schema when the stream has no rows", async () => {
    const ipc = tableToIPC(
      tableFromArrays({ answer: Int32Array.from([]) }),
      "stream",
    );
    const onBatch = vi.fn();
    const body = ipc.buffer.slice(
      ipc.byteOffset,
      ipc.byteOffset + ipc.byteLength,
    ) as ArrayBuffer;

    const result = await consumeArrowResponse(
      new Response(body),
      false,
      onBatch,
    );

    expect(result.fields.map((field) => field.name)).toEqual(["answer"]);
    expect(result.rows).toEqual([]);
    expect(onBatch).toHaveBeenCalledWith(
      expect.objectContaining({ fields: result.fields, rows: [] }),
    );
  });

  it("preserves duplicate columns and avoids collisions with original names", async () => {
    const schema = new Schema(
      ["id", "id", "id [column 1]", "row", ""].map(
        (name) => new Field(name, new Int32(), false),
      ),
    );
    const batch = new RecordBatch(
      schema,
      makeData({
        type: new Struct(schema.fields),
        length: 1,
        children: [1, 2, 3, 4, 5].map((value) =>
          makeData({ type: new Int32(), data: Int32Array.from([value]) }),
        ),
      }),
    );
    const ipc = tableToIPC(new Table(batch), "stream");
    const result = await consumeArrowResponse(
      new Response(
        ipc.buffer.slice(
          ipc.byteOffset,
          ipc.byteOffset + ipc.byteLength,
        ) as ArrayBuffer,
      ),
      false,
      () => undefined,
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "id",
      "id",
      "id [column 1]",
      "row",
      "",
    ]);
    expect(result.fields.map((field) => field.key)).toEqual([
      "id [column 1]#",
      "id [column 2]",
      "id [column 1]",
      "row",
      " [column 5]",
    ]);
    expect(
      result.fields.map((field) => result.rows[0].values[field.key]),
    ).toEqual([1, 2, 3, 4, 5]);
    expect(Object.keys(result.rows[0].values)).toHaveLength(5);
  });
});
