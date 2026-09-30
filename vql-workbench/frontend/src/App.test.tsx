import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { getSession } from "./lib/api";
import { executeCatalogStatement } from "./lib/catalog";
import { createDraft, loadDrafts, saveDrafts } from "./lib/history";
import type { Draft, QueryResult, ResultValue } from "./lib/types";

vi.mock("./lib/catalog", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./lib/catalog")>()),
  executeCatalogStatement: vi.fn(),
}));

vi.mock("./lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./lib/api")>()),
  getSession: vi.fn(),
}));

vi.mock("./components/SqlEditor", () => ({
  SqlEditor: ({
    value,
    onChange,
    ariaLabel = "SQL editor",
    readOnly = false,
  }: {
    value: string;
    onChange?: (value: string) => void;
    ariaLabel?: string;
    readOnly?: boolean;
  }) => (
    <textarea
      aria-label={ariaLabel}
      readOnly={readOnly}
      value={value}
      onChange={(event) => onChange?.(event.target.value)}
    />
  ),
}));

beforeEach(() => {
  localStorage.clear();
  vi.mocked(getSession).mockResolvedValue({
    connected: false,
    endpoint: "http://127.0.0.1:6031",
    sessionId: null,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

async function mount() {
  const view = render(<App />);
  await waitFor(() => expect(getSession).toHaveBeenCalled());
  return view;
}

function tab(name: string) {
  return screen.getByRole("button", { name });
}

describe("Catalog navigation", () => {
  it("opens formatted DDL for same-named objects in distinct namespaces and keeps the original draft SQL", async () => {
    const result = (...rows: Record<string, ResultValue>[]): QueryResult => ({
      fields: [],
      rows: rows.map((values, id) => ({ id, values })),
      receivedBatches: 1,
      rolling: false,
    });
    vi.mocked(getSession).mockResolvedValue({
      connected: true,
      endpoint: "http://127.0.0.1:6031",
      sessionId: "session",
    });
    vi.mocked(executeCatalogStatement).mockImplementation(async (sql) => {
      if (sql === "SHOW FUNCTIONS;")
        return result(
          {
            catalog: "vql",
            schema: "default",
            name: "score",
            kind: "FUNCTION",
            arguments: "BIGINT",
            return_type: "TEXT",
          },
          {
            catalog: "vql",
            schema: "quality",
            name: "score",
            kind: "FUNCTION",
            arguments: "BIGINT",
            return_type: "TEXT",
          },
        );
      if (sql === "SHOW MODELS;" || sql === "SHOW TABLES;") return result();
      if (sql.startsWith("SHOW CREATE FUNCTION")) {
        const qualified = sql.includes('"quality"');
        return result({
          object_name: qualified ? "quality.score" : "score",
          create_sql: `CREATE FUNCTION "${qualified ? "quality.score" : "score"}"(BIGINT) RETURNS TEXT RETURN '${qualified ? "/quality" : "/default"}';`,
        });
      }
      if (sql.startsWith("DESCRIBE FUNCTION"))
        return result({ status: "AVAILABLE" });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    await mount();
    const originalSql = (
      screen.getByRole("textbox", { name: "SQL editor" }) as HTMLTextAreaElement
    ).value;
    const navigation = screen.getByRole("region", {
      name: "Catalog navigation",
    });
    const quality = await within(navigation).findByRole("button", {
      name: "Show DDL for function vql.quality.score",
    });
    await waitFor(() => expect(quality).toBeEnabled());
    await userEvent.click(quality);
    const workspace = await screen.findByRole("main", { name: "Catalog DDL" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(within(workspace).queryByRole("article")).toBeNull();
    expect(within(workspace).queryByRole("searchbox")).toBeNull();
    await waitFor(() =>
      expect(
        (
          within(workspace).getByRole("textbox", {
            name: "DDL editor",
          }) as HTMLTextAreaElement
        ).value,
      ).toContain("\nRETURN '/quality'"),
    );
    await userEvent.click(
      within(navigation).getByRole("button", {
        name: "Show DDL for function vql.default.score",
      }),
    );
    await waitFor(() =>
      expect(
        (
          within(workspace).getByRole("textbox", {
            name: "DDL editor",
          }) as HTMLTextAreaElement
        ).value,
      ).toContain("\nRETURN '/default'"),
    );
    await userEvent.click(screen.getByRole("button", { name: "SQL editor" }));
    expect(screen.getByRole("textbox", { name: "SQL editor" })).toHaveValue(
      originalSql,
    );
  });
  it("switches a Model's version DDL in the right panel and shows the latest version on object selection", async () => {
    const result = (...rows: Record<string, ResultValue>[]): QueryResult => ({
      fields: [],
      rows: rows.map((values, id) => ({ id, values })),
      receivedBatches: 1,
      rolling: false,
    });
    const first = `CREATE MODEL "quality.detector" TYPE OBJECT_DETECTION VERSION 'release-1' FROM 'mock://person';`;
    const second = `CREATE MODEL "quality.detector" TYPE OBJECT_DETECTION VERSION 'v2' FROM 'mock://candidate'`;
    vi.mocked(getSession).mockResolvedValue({
      connected: true,
      endpoint: "http://127.0.0.1:6031",
      sessionId: "session",
    });
    vi.mocked(executeCatalogStatement).mockImplementation(async (sql) => {
      if (sql === "SHOW TABLES;") return result();
      if (sql === "SHOW MODELS;")
        return result({
          catalog: "vql",
          schema: "quality",
          name: "detector",
          versions: 2,
        });
      if (sql === "SHOW FUNCTIONS;")
        return result({
          catalog: "vql",
          schema: "quality",
          name: "detector",
          kind: "MODEL",
        });
      if (sql === 'SHOW MODEL VERSIONS "quality"."detector";')
        return result(
          { version: "release-1", is_default: false },
          { version: "v2", is_default: true },
        );
      if (sql.startsWith('SHOW CREATE MODEL "quality"."detector"'))
        return result({
          object_name: "quality.detector",
          version: sql.includes("VERSION 'release-1'") ? "release-1" : "v2",
          create_sql: sql.includes("VERSION 'release-1'") ? first : second,
        });
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    await mount();
    const models = await screen.findByRole("list", {
      name: "vql.quality models",
    });
    const model = await within(models).findByRole("button", {
      name: "Show DDL for model vql.quality.detector",
    });
    await waitFor(() => expect(model).toBeEnabled());
    await userEvent.click(model);
    const versions = await screen.findByRole("complementary", {
      name: "Model versions",
    });
    expect(
      within(models).queryByRole("list", { name: /Versions of model/ }),
    ).toBeNull();
    const secondVersion = within(versions).getByRole("button", {
      name: "Show DDL for model vql.quality.detector version v2",
    });
    await waitFor(() => expect(secondVersion).toBeEnabled());
    await userEvent.click(secondVersion);
    const ddl = () =>
      screen.getByRole("textbox", {
        name: "DDL editor",
      }) as HTMLTextAreaElement;
    await waitFor(() => expect(ddl().value).toContain("mock://candidate"));
    expect(ddl().value).not.toContain("mock://person");
    expect(secondVersion).toHaveAttribute("aria-current", "true");
    expect(model).toHaveAttribute("aria-current", "true");
    await userEvent.click(
      within(versions).getByRole("button", {
        name: "Show DDL for model vql.quality.detector version release-1",
      }),
    );
    await waitFor(() => expect(ddl().value).toContain("mock://person"));
    expect(ddl().value).not.toContain("mock://candidate");
    await userEvent.click(
      within(models).getByRole("button", {
        name: "Show DDL for model vql.quality.detector",
      }),
    );
    await waitFor(() => expect(ddl().value).toContain("mock://candidate"));
    expect(ddl().value).not.toContain("mock://person");
    expect(screen.getByText("vql.quality · MODEL · Version v2")).toBeVisible();
    expect(
      vi.mocked(executeCatalogStatement).mock.calls.map(([sql]) => sql),
    ).toEqual([
      "SHOW TABLES;",
      "SHOW MODELS;",
      "SHOW FUNCTIONS;",
      'SHOW MODEL VERSIONS "quality"."detector";',
      'SHOW CREATE MODEL "quality"."detector";',
      'SHOW CREATE MODEL "quality"."detector" VERSION \'v2\';',
      'SHOW CREATE MODEL "quality"."detector" VERSION \'release-1\';',
      'SHOW CREATE MODEL "quality"."detector";',
    ]);
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

describe("query files", () => {
  it("uses the first available Untitle name after closing a draft", async () => {
    await mount();
    expect(tab("Untitle.sql")).toHaveAttribute("aria-pressed", "true");
    await userEvent.click(tab("New draft"));
    await userEvent.click(tab("New draft"));
    expect(loadDrafts().map((draft) => draft.name)).toEqual([
      "Untitle.sql",
      "Untitle1.sql",
      "Untitle2.sql",
    ]);
    await userEvent.click(tab("Close Untitle1.sql"));
    await userEvent.click(tab("New draft"));
    expect(tab("Untitle1.sql")).toHaveAttribute("aria-pressed", "true");
    expect(loadDrafts().map((draft) => draft.name)).toEqual([
      "Untitle.sql",
      "Untitle2.sql",
      "Untitle1.sql",
    ]);
  });

  it("renames an inactive file, preserves SQL and selection, and restores the name on reload", async () => {
    const first = createDraft([], "SELECT 42;");
    const second = createDraft([first], "SELECT 7;");
    saveDrafts([first, second]);
    localStorage.setItem("visionql.workbench.history.v1", "[]");
    const view = await mount();
    await userEvent.click(tab(second.name));
    await userEvent.click(tab(`Rename ${first.name}`));
    const dialog = screen.getByRole("dialog", { name: "Rename query file" });
    const input = within(dialog).getByRole("textbox", { name: "File name" });
    expect(input).toHaveFocus();
    await userEvent.clear(input);
    await userEvent.type(input, "  people  ");
    await userEvent.click(
      within(dialog).getByRole("button", { name: "Rename" }),
    );
    expect(tab("people.sql")).toHaveAttribute("aria-pressed", "false");
    expect(tab(second.name)).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("textbox", { name: "SQL editor" })).toHaveValue(
      "SELECT 7;",
    );
    expect(loadDrafts()).toEqual([
      expect.objectContaining({
        id: first.id,
        name: "people.sql",
        sql: first.sql,
      }),
      second,
    ]);
    expect(localStorage.getItem("visionql.workbench.history.v1")).toBe("[]");
    view.unmount();
    await mount();
    expect(tab("people.sql")).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("textbox", { name: "SQL editor" })).toHaveValue(
      "SELECT 42;",
    );
  });

  it("validates names, cancels with Escape, and saves a double-click rename with Enter", async () => {
    await mount();
    await userEvent.click(tab("New draft"));
    const saved = loadDrafts();
    await userEvent.dblClick(tab("Untitle1.sql"));
    const input = screen.getByRole("textbox", { name: "File name" });
    const rename = screen.getByRole("button", { name: "Rename" });
    await userEvent.clear(input);
    await userEvent.type(input, "   ");
    expect(screen.getByText("Enter a file name.")).toBeVisible();
    expect(rename).toBeDisabled();
    await userEvent.clear(input);
    await userEvent.type(input, "Untitle");
    expect(
      screen.getByText("A query file with this name already exists."),
    ).toBeVisible();
    expect(rename).toBeDisabled();
    await userEvent.clear(input);
    await userEvent.type(input, "reports/people");
    expect(
      screen.getByText("Enter a file name without slashes."),
    ).toBeVisible();
    expect(rename).toBeDisabled();
    await userEvent.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(loadDrafts()).toEqual(saved);
    await userEvent.dblClick(tab("Untitle1.sql"));
    const nextInput = screen.getByRole("textbox", { name: "File name" });
    await userEvent.clear(nextInput);
    await userEvent.type(nextInput, "reports.sql{Enter}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(tab("reports.sql")).toHaveAttribute("aria-pressed", "true");
    expect(loadDrafts()[1]).toMatchObject({
      id: saved[1].id,
      name: "reports.sql",
    });
  });

  it("keeps legacy names and avoids names reserved by a rename", async () => {
    const legacy: Draft = { ...createDraft(), name: "query_1.sql" };
    saveDrafts([legacy]);
    await mount();
    expect(tab("query_1.sql")).toBeVisible();
    await userEvent.click(tab("Rename query_1.sql"));
    const input = screen.getByRole("textbox", { name: "File name" });
    await userEvent.clear(input);
    await userEvent.type(input, "Untitle1.sql{Enter}");
    await userEvent.click(tab("New draft"));
    await userEvent.click(tab("New draft"));
    expect(loadDrafts().map((draft) => draft.name)).toEqual([
      "Untitle1.sql",
      "Untitle.sql",
      "Untitle2.sql",
    ]);
  });
});
