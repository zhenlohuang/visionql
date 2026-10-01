import { describe, expect, it } from "vitest";

import { overlayRect } from "./overlay";

describe("overlayRect", () => {
  it("accounts for horizontal letterboxing", () => {
    const rect = overlayRect(
      { x: 0.25, y: 0.25, w: 0.5, h: 0.5 },
      { width: 100, height: 100 },
      { width: 200, height: 100 },
    );

    expect(rect).toEqual({ x: 75, y: 25, width: 50, height: 50 });
  });

  it("clips geometry without mutating the source value", () => {
    const box = { x: -0.1, y: 0.8, w: 0.4, h: 0.5 };
    const rect = overlayRect(
      box,
      { width: 100, height: 100 },
      { width: 100, height: 100 },
    );

    expect(rect?.x).toBeCloseTo(0);
    expect(rect?.y).toBeCloseTo(80);
    expect(rect?.width).toBeCloseTo(30);
    expect(rect?.height).toBeCloseTo(20);
    expect(box).toEqual({ x: -0.1, y: 0.8, w: 0.4, h: 0.5 });
  });

  it("rejects invalid or empty boxes", () => {
    expect(
      overlayRect(
        { x: 0, y: 0, w: 0, h: 1 },
        { width: 100, height: 100 },
        { width: 100, height: 100 },
      ),
    ).toBeNull();
  });
});
