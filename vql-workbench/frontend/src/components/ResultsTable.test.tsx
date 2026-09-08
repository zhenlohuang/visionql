import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { OverlayConfig, QueryResult } from "../lib/types";
import { ResultsTable } from "./ResultsTable";

const result: QueryResult = {
  fields: [
    {
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
});
