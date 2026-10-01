import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { EditorView } from "@codemirror/view";
import { StrictMode } from "react";
import { FormattedDdl } from "./FormattedDdl";

afterEach(cleanup);

describe("formatted DDL syntax highlighting", () => {
  it("highlights SQL and VisionQL keywords without highlighting quoted names or string contents", async () => {
    render(
      <FormattedDdl
        sql={
          "CREATE MODEL \"CREATE MODEL\" TYPE OBJECT_DETECTION VERSION 'v1' FROM 'mock://CREATE MODEL' USING ONNX_RUNTIME;"
        }
      />,
    );
    const ddl = screen.getByRole("region", { name: "Formatted DDL" });
    await waitFor(() =>
      expect(
        EditorView.findFromDOM(
          ddl.querySelector(".cm-content")!,
        )?.state.doc.toString(),
      ).toContain("\nVERSION 'v1'"),
    );
    const keywords = [...ddl.querySelectorAll(".tok-keyword")].map(
      (span) => span.textContent,
    );
    expect(keywords).toEqual(
      expect.arrayContaining([
        "CREATE",
        "MODEL",
        "TYPE",
        "VERSION",
        "FROM",
        "USING",
      ]),
    );
    expect(keywords).not.toContain('"CREATE MODEL"');
    expect(keywords).not.toContain("'mock://CREATE MODEL'");
    expect(
      EditorView.findFromDOM(
        ddl.querySelector(".cm-content")!,
      )?.state.doc.toString(),
    ).toContain('"CREATE MODEL"');
    expect(
      EditorView.findFromDOM(
        ddl.querySelector(".cm-content")!,
      )?.state.doc.toString(),
    ).toContain("'mock://CREATE MODEL'");
  });

  it("formats table clauses and highlights LOCATION", async () => {
    render(
      <FormattedDdl sql="CREATE TABLE photos USING IMAGES LOCATION '/photos';" />,
    );
    const ddl = screen.getByRole("region", { name: "Formatted DDL" });
    await waitFor(() =>
      expect(
        EditorView.findFromDOM(
          ddl.querySelector(".cm-content")!,
        )?.state.doc.toString(),
      ).toContain("\nLOCATION '/photos'"),
    );
    expect(ddl.querySelectorAll(".tok-keyword")).not.toHaveLength(0);
    expect(
      [...ddl.querySelectorAll(".tok-keyword")].some(
        (span) => span.textContent === "LOCATION",
      ),
    ).toBe(true);
  });
  it("keeps the shared editor read-only with line numbers and updates its document", async () => {
    const view = render(
      <StrictMode>
        <FormattedDdl sql="CREATE TABLE photos USING IMAGES LOCATION '/photos';" />
      </StrictMode>,
    );
    const editor = screen.getByRole("textbox", { name: "DDL editor" });
    const code = EditorView.findFromDOM(editor)!;
    await waitFor(() => expect(code.state.doc.lines).toBe(3));
    expect(code.state.readOnly).toBe(true);
    expect(editor).toHaveAttribute("aria-readonly", "true");
    expect(view.container.querySelector(".cm-lineNumbers")).not.toBeNull();
    expect(view.container.querySelectorAll(".cm-editor")).toHaveLength(1);
    view.rerender(
      <StrictMode>
        <FormattedDdl sql="CREATE FUNCTION score(BIGINT) RETURNS BIGINT RETURN $1 + 1;" />
      </StrictMode>,
    );
    await waitFor(() =>
      expect(code.state.doc.toString()).toContain("\nRETURN $1 + 1;"),
    );
    expect(code.state.doc.toString()).not.toContain("photos");
    expect(code.state.readOnly).toBe(true);
  });
});
