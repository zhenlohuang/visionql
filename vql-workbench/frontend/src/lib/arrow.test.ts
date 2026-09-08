import { tableFromArrays, tableToIPC } from "apache-arrow";
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
});
