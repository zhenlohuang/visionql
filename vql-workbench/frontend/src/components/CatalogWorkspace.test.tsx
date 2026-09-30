import { StrictMode } from "react";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CatalogWorkspace } from "./CatalogWorkspace";
import { EditorView } from "@codemirror/view";
import type { CatalogObject } from "../lib/catalog";
import type { QueryResult } from "../lib/types";

const photos: CatalogObject = {
  id: "photos",
  name: "photos",
  namespace: "vql.default",
  kind: "TABLE",
  lookupNames: ["photos"],
  ddl: "",
  problem: null,
};
const rawDdl = "CREATE TABLE photos USING IMAGES LOCATION '/photos';";
function definition(name = "photos", ddl = rawDdl): QueryResult {
  return {
    fields: [],
    rows: [{ id: 0, values: { object_name: name, create_sql: ddl } }],
    receivedBatches: 1,
    rolling: false,
  };
}
function props() {
  return {
    section: "tables" as const,
    namespace: "vql.default",
    object: photos,
    selectionRequest: 1,
    connected: true,
    busy: false,
    execute: vi.fn(async (_sql: string) => definition()),
    onBusyChange: vi.fn(),
    onOpenSql: vi.fn(),
    onSelectVersion: vi.fn(),
    onConnect: vi.fn(),
    canExecute: () => true,
  };
}

afterEach(cleanup);

describe("Catalog DDL workspace", () => {
  it("loads only the selected object's DDL once under StrictMode without a list or dialog", async () => {
    const options = props();
    render(
      <StrictMode>
        <CatalogWorkspace {...options} />
      </StrictMode>,
    );
    const ddl = await screen.findByRole("region", { name: "Formatted DDL" });
    await waitFor(() =>
      expect(
        EditorView.findFromDOM(
          ddl.querySelector(".cm-content")!,
        )?.state.doc.toString(),
      ).toContain("\nLOCATION '/photos'"),
    );
    expect(options.execute).toHaveBeenCalledTimes(1);
    expect(options.execute).toHaveBeenCalledWith(
      'SHOW CREATE TABLE "photos";',
      expect.any(AbortSignal),
    );
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByRole("article")).toBeNull();
    expect(screen.queryByRole("searchbox")).toBeNull();
    expect(
      screen.queryByRole("complementary", { name: "Model versions" }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Open in SQL editor" }),
    ).toBeNull();
    expect(screen.queryByRole("button", { name: "Refresh DDL" })).toBeNull();
  });

  it("copies the original defining SQL", async () => {
    const user = userEvent.setup();
    const options = props();
    const copy = vi.spyOn(navigator.clipboard, "writeText");
    render(<CatalogWorkspace {...options} />);
    await screen.findByRole("region", { name: "Formatted DDL" });
    await user.click(screen.getByRole("button", { name: "Copy DDL" }));
    expect(copy).toHaveBeenCalledWith(rawDdl);
    expect(options.onOpenSql).not.toHaveBeenCalled();
    copy.mockRestore();
  });

  it("switches version DDL and copies only the current version statement", async () => {
    const user = userEvent.setup();
    const copy = vi.spyOn(navigator.clipboard, "writeText");
    const options = props();
    const first = `CREATE MODEL "detector" TYPE OBJECT_DETECTION VERSION 'v1' FROM 'mock://person';`;
    const second = `CREATE MODEL "detector" TYPE OBJECT_DETECTION VERSION 'v2' FROM 'mock://candidate'`;
    const model: CatalogObject = {
      ...photos,
      id: "detector",
      name: "detector",
      kind: "MODEL",
      lookupNames: ["detector"],
      versions: [
        { name: "v1", isDefault: true },
        { name: "v2", isDefault: false },
      ],
    };
    options.execute.mockImplementation(async (sql) => ({
      ...definition(),
      rows: [
        {
          id: 0,
          values: {
            object_name: "detector",
            create_sql: sql.includes("VERSION 'v1'") ? first : second,
            version: sql.includes("VERSION 'v1'") ? "v1" : "v2",
          },
        },
      ],
    }));
    const view = render(
      <CatalogWorkspace {...options} section="models" object={model} />,
    );
    const editor = await screen.findByRole("textbox", { name: "DDL editor" });
    await waitFor(() =>
      expect(EditorView.findFromDOM(editor)?.state.doc.toString()).toContain(
        "VERSION 'v2'",
      ),
    );
    expect(EditorView.findFromDOM(editor)?.state.doc.toString()).not.toContain(
      "mock://person",
    );
    const panel = screen.getByRole("complementary", { name: "Model versions" });
    const firstVersion = within(panel).getByRole("button", {
      name: "Show DDL for model vql.default.detector version v1",
    });
    expect(within(firstVersion).getByText("Default")).toBeVisible();
    expect(
      within(panel).getByRole("button", {
        name: "Show DDL for model vql.default.detector version v2",
      }),
    ).toHaveAttribute("aria-current", "true");
    await user.click(screen.getByRole("button", { name: "Copy DDL" }));
    expect(copy).toHaveBeenLastCalledWith(second);
    await user.click(firstVersion);
    expect(options.onSelectVersion).toHaveBeenCalledWith("v1");
    view.rerender(
      <CatalogWorkspace
        {...options}
        section="models"
        object={model}
        version="v1"
      />,
    );
    await waitFor(() =>
      expect(
        EditorView.findFromDOM(
          screen.getByRole("textbox", { name: "DDL editor" }),
        )?.state.doc.toString(),
      ).toContain("mock://person"),
    );
    expect(
      screen.getByRole("textbox", { name: "DDL editor" }),
    ).not.toHaveTextContent("mock://candidate");
    expect(firstVersion).toHaveAttribute("aria-current", "true");
    await user.click(screen.getByRole("button", { name: "Copy DDL" }));
    expect(copy).toHaveBeenLastCalledWith(first);
    copy.mockRestore();
  });

  it("shows version metadata failures beside an available Model definition", async () => {
    const options = props();
    render(
      <CatalogWorkspace
        {...options}
        section="models"
        object={{
          ...photos,
          kind: "MODEL",
          versionsProblem: {
            source: "vql",
            title: "Denied",
            message: "versions restricted",
            symbol: "PERMISSION_DENIED",
            code: "VQL-test",
          },
        }}
      />,
    );
    await screen.findByRole("textbox", { name: "DDL editor" });
    const panel = screen.getByRole("complementary", { name: "Model versions" });
    expect(within(panel).getByRole("alert")).toHaveTextContent(
      "versions restricted",
    );
    expect(within(panel).getByText(/PERMISSION_DENIED/)).toBeVisible();
  });

  it("preserves structured definition errors in the main area", async () => {
    const options = props();
    options.execute.mockRejectedValue({
      source: "vql",
      title: "Denied",
      message: "restricted",
      code: "VQL-test",
      symbol: "PERMISSION_DENIED",
    });
    render(<CatalogWorkspace {...options} />);
    await screen.findByText("PERMISSION_DENIED");
    expect(screen.getByText("restricted")).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy DDL" })).toBeDisabled();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("aborts the previous selection and never displays its late definition", async () => {
    const options = props();
    let finish: (result: QueryResult) => void = () => undefined;
    let firstSignal: AbortSignal | undefined;
    const execute = vi.fn(async (_sql: string, signal?: AbortSignal) => {
      if (execute.mock.calls.length === 1) {
        firstSignal = signal;
        return new Promise<QueryResult>((resolve) => {
          finish = resolve;
        });
      }
      return definition(
        "camera",
        "CREATE TABLE camera USING IMAGES LOCATION '/camera';",
      );
    });
    const view = render(<CatalogWorkspace {...options} execute={execute} />);
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
    view.rerender(
      <CatalogWorkspace
        {...options}
        execute={execute}
        object={{
          ...photos,
          id: "camera",
          name: "camera",
          lookupNames: ["camera"],
        }}
        selectionRequest={2}
      />,
    );
    expect(firstSignal?.aborted).toBe(true);
    finish(definition());
    await waitFor(() => expect(execute).toHaveBeenCalledTimes(2));
    await screen.findByRole("region", { name: "Formatted DDL" });
    await waitFor(() =>
      expect(
        EditorView.findFromDOM(
          screen.getByRole("textbox", { name: "DDL editor" }),
        )?.state.doc.toString(),
      ).toContain("'/camera'"),
    );
    expect(
      EditorView.findFromDOM(
        screen.getByRole("textbox", { name: "DDL editor" }),
      )?.state.doc.toString(),
    ).not.toContain("'/photos'");
  });

  it("opens a namespace-specific creation draft from an empty category without querying", async () => {
    const options = props();
    render(
      <CatalogWorkspace
        {...options}
        section="functions"
        namespace="team.media"
        object={null}
      />,
    );
    expect(screen.getByText("Select an object")).toBeVisible();
    await userEvent.click(
      screen.getByRole("button", { name: "Create function" }),
    );
    expect(options.onOpenSql).toHaveBeenCalledWith(
      expect.stringContaining('CREATE FUNCTION "team.media.plus_one"'),
      "New function",
    );
    expect(options.execute).not.toHaveBeenCalled();
  });

  it("does not query while disconnected or while the Session is busy", () => {
    const options = props();
    const view = render(<CatalogWorkspace {...options} connected={false} />);
    expect(screen.getByText("Connect to browse the catalog")).toBeVisible();
    expect(options.execute).not.toHaveBeenCalled();
    view.rerender(<CatalogWorkspace {...options} busy />);
    expect(options.execute).not.toHaveBeenCalled();
  });
});
