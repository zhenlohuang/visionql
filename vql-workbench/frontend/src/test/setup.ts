import "@testing-library/jest-dom/vitest";

Object.defineProperty(globalThis, "ResizeObserver", {
  writable: true,
  value: class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  },
});

// JSDOM does not implement text layout; CodeMirror measures ranges when drawing
// selections. Browser tests cover the real editor's geometry and interactions.
Object.defineProperties(Range.prototype, {
  getClientRects: { value: () => [] },
  getBoundingClientRect: { value: () => new DOMRect() },
});
