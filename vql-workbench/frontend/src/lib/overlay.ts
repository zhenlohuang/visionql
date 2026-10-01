import type { Box2dValue } from "./types";

export interface Size {
  width: number;
  height: number;
}

export interface PixelRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export function overlayRect(
  box: Box2dValue,
  image: Size,
  viewport: Size,
): PixelRect | null {
  if (
    ![
      box.x,
      box.y,
      box.w,
      box.h,
      image.width,
      image.height,
      viewport.width,
      viewport.height,
    ].every(Number.isFinite) ||
    image.width <= 0 ||
    image.height <= 0 ||
    viewport.width <= 0 ||
    viewport.height <= 0 ||
    box.w <= 0 ||
    box.h <= 0
  ) {
    return null;
  }
  const scale = Math.min(
    viewport.width / image.width,
    viewport.height / image.height,
  );
  const contentWidth = image.width * scale;
  const contentHeight = image.height * scale;
  const offsetX = (viewport.width - contentWidth) / 2;
  const offsetY = (viewport.height - contentHeight) / 2;
  const left = clamp(box.x, 0, 1);
  const top = clamp(box.y, 0, 1);
  const right = clamp(box.x + box.w, 0, 1);
  const bottom = clamp(box.y + box.h, 0, 1);
  if (right <= left || bottom <= top) return null;
  return {
    x: offsetX + left * contentWidth,
    y: offsetY + top * contentHeight,
    width: (right - left) * contentWidth,
    height: (bottom - top) * contentHeight,
  };
}

export function isBox2d(value: unknown): value is Box2dValue {
  if (!value || typeof value !== "object") return false;
  const box = value as Partial<Box2dValue>;
  return [box.x, box.y, box.w, box.h].every(
    (coordinate) =>
      typeof coordinate === "number" && Number.isFinite(coordinate),
  );
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
