import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ImagePreview, imageMime } from "./ImagePreview";

describe("ImagePreview", () => {
  const createObjectURL = vi.fn(() => "blob:thumbnail");
  const revokeObjectURL = vi.fn();

  beforeEach(() => {
    createObjectURL.mockReset();
    createObjectURL.mockReturnValue("blob:thumbnail");
    Object.defineProperty(URL, "createObjectURL", {
      value: createObjectURL,
      configurable: true,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      value: revokeObjectURL,
      configurable: true,
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("creates and revokes a Blob URL for encoded thumbnails", () => {
    const { unmount } = render(
      <ImagePreview
        image={{ encoded: new Uint8Array([1, 2, 3]), encoding: "jpeg" }}
      />,
    );

    expect(screen.getByAltText("Returned VisionQL thumbnail")).toHaveAttribute(
      "src",
      "blob:thumbnail",
    );
    unmount();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:thumbnail");
  });

  it("does not turn a locator into an image URL", () => {
    render(<ImagePreview image={{ locator: "vql://media/v1/1/secret.jpg" }} />);
    expect(screen.getByText("No thumbnail")).toBeInTheDocument();
    expect(createObjectURL).not.toHaveBeenCalled();
  });

  it("retries decoding when a new thumbnail replaces a failed one", () => {
    createObjectURL
      .mockReturnValueOnce("blob:first")
      .mockReturnValueOnce("blob:second");
    const { rerender } = render(
      <ImagePreview
        image={{ encoded: new Uint8Array([1]), encoding: "jpeg" }}
      />,
    );
    fireEvent.error(screen.getByAltText("Returned VisionQL thumbnail"));
    expect(screen.getByText("Decode failed")).toBeInTheDocument();

    rerender(
      <ImagePreview
        image={{ encoded: new Uint8Array([2]), encoding: "jpeg" }}
      />,
    );

    expect(screen.getByAltText("Returned VisionQL thumbnail")).toHaveAttribute(
      "src",
      "blob:second",
    );
  });

  it("renders a normalized overlay from returned image dimensions", () => {
    const clientWidth = vi
      .spyOn(HTMLElement.prototype, "clientWidth", "get")
      .mockReturnValue(200);
    const clientHeight = vi
      .spyOn(HTMLElement.prototype, "clientHeight", "get")
      .mockReturnValue(100);

    const { container } = render(
      <ImagePreview
        image={{
          encoded: new Uint8Array([1, 2, 3]),
          encoding: "jpeg",
          width: 100,
          height: 100,
        }}
        box={{ x: 0.25, y: 0.25, w: 0.5, h: 0.5 }}
        label="person"
        confidence={0.9}
      />,
    );

    const rect = container.querySelector("rect");
    expect(rect).toHaveAttribute("x", "75");
    expect(rect).toHaveAttribute("y", "25");
    expect(rect).toHaveAttribute("width", "50");
    expect(rect).toHaveAttribute("height", "50");
    expect(screen.getByText(/person/)).toHaveTextContent("person 0.90");

    clientWidth.mockRestore();
    clientHeight.mockRestore();
  });
});

describe("imageMime", () => {
  it("allows browser image encodings and rejects unknown encodings", () => {
    expect(imageMime("jpeg")).toBe("image/jpeg");
    expect(imageMime("image/webp")).toBe("image/webp");
    expect(imageMime("raw-rgb")).toBeNull();
  });
});
