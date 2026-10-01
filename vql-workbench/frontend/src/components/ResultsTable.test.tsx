import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { OverlayConfig, QueryResult } from "../lib/types";
import { ResultsTable } from "./ResultsTable";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const result: QueryResult = {
  fields: [
    {
      key: "image",
      name: "image",
      type: "IMAGE",
      extensionName: "vql.image",
      nullable: false,
    },
  ],
  rows: [],
  receivedBatches: 1,
  rolling: false,
};

const overlay: OverlayConfig = {
  imageColumn: "image",
  boxColumn: null,
  labelColumn: null,
  confidenceColumn: null,
};

describe("ResultsTable", () => {
  it("does not claim a BOX2D overlay until a box column is mapped", () => {
    render(
      <ResultsTable
        result={result}
        overlay={overlay}
        selectedRowId={null}
        onSelectRow={vi.fn()}
      />,
    );

    expect(
      screen.getByRole("columnheader", { name: "image [IMAGE]" }),
    ).toBeInTheDocument();
    expect(screen.queryByText(/BOX2D/)).not.toBeInTheDocument();
  });

  it("renders duplicate column names and a column named row independently", () => {
    render(
      <ResultsTable
        result={{
          fields: [
            {
              key: "id [column 1]",
              name: "id",
              type: "Int32",
              nullable: false,
            },
            {
              key: "id [column 2]",
              name: "id",
              type: "Int32",
              nullable: false,
            },
            { key: "row", name: "row", type: "Int32", nullable: false },
          ],
          rows: [
            {
              id: 1,
              values: { "id [column 1]": 11, "id [column 2]": 22, row: 33 },
            },
          ],
          receivedBatches: 1,
          rolling: false,
        }}
        overlay={overlay}
        selectedRowId={null}
        onSelectRow={vi.fn()}
      />,
    );

    expect(
      screen.getAllByRole("columnheader", { name: "id [Int32]" }),
    ).toHaveLength(2);
    expect(screen.getAllByRole("cell").map((cell) => cell.textContent)).toEqual(
      ["1", "11", "22", "33"],
    );
  });

  it.each([
    {
      firstName: "original",
      secondName: "crop",
      firstKey: "original",
      secondKey: "crop",
    },
    {
      firstName: "image",
      secondName: "image",
      firstKey: "image [column 1]",
      secondKey: "image [column 2]",
    },
  ])(
    "moves the overlay between $firstKey and $secondKey",
    ({ firstName, secondName, firstKey, secondKey }) => {
      Object.defineProperty(URL, "createObjectURL", {
        value: vi.fn(() => "blob:thumbnail"),
        configurable: true,
      });
      Object.defineProperty(URL, "revokeObjectURL", {
        value: vi.fn(),
        configurable: true,
      });
      vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(
        200,
      );
      vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(
        100,
      );
      const image = {
        encoded: new Uint8Array([1, 2, 3]),
        encoding: "jpeg",
        width: 100,
        height: 100,
      };
      const images: QueryResult = {
        fields: [
          {
            key: firstKey,
            name: firstName,
            type: "IMAGE",
            extensionName: "vql.image",
            nullable: false,
          },
          {
            key: secondKey,
            name: secondName,
            type: "IMAGE",
            extensionName: "vql.image",
            nullable: false,
          },
        ],
        rows: [
          {
            id: 1,
            values: {
              [firstKey]: image,
              [secondKey]: image,
              box: { x: 0.25, y: 0.25, w: 0.5, h: 0.5 },
              label: "person",
              confidence: 0.9,
            },
          },
        ],
        receivedBatches: 1,
        rolling: false,
      };
      const mapping = {
        imageColumn: firstKey,
        boxColumn: "box",
        labelColumn: "label",
        confidenceColumn: "confidence",
      };
      const props = {
        result: images,
        overlay: mapping,
        selectedRowId: null,
        onSelectRow: vi.fn(),
      };
      const { container, rerender } = render(<ResultsTable {...props} />);
      let first = screen.getAllByAltText("Returned VisionQL thumbnail")[0]
        .parentElement!;
      let second = screen.getAllByAltText("Returned VisionQL thumbnail")[1]
        .parentElement!;

      expect(first.querySelector("rect")).toHaveAttribute("x", "75");
      expect(first).toHaveTextContent("person 0.90");
      expect(second.querySelector("rect")).toBeNull();
      expect(second).not.toHaveTextContent("person");

      rerender(
        <ResultsTable
          {...props}
          overlay={{ ...mapping, imageColumn: secondKey }}
        />,
      );
      first = screen.getAllByAltText("Returned VisionQL thumbnail")[0]
        .parentElement!;
      second = screen.getAllByAltText("Returned VisionQL thumbnail")[1]
        .parentElement!;
      expect(first.querySelector("rect")).toBeNull();
      expect(first).not.toHaveTextContent("person");
      expect(second.querySelector("rect")).toHaveAttribute("width", "50");
      expect(container.querySelectorAll("rect")).toHaveLength(1);

      rerender(
        <ResultsTable {...props} overlay={{ ...mapping, imageColumn: null }} />,
      );
      expect(container.querySelectorAll("rect")).toHaveLength(0);
      expect(screen.queryByText("person 0.90")).not.toBeInTheDocument();
    },
  );
});
