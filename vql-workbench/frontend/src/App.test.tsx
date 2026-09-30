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
import { createDraft, loadDrafts, saveDrafts } from "./lib/history";
import type { Draft } from "./lib/types";

vi.mock("./lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./lib/api")>()),
  getSession: vi.fn(),
}));

vi.mock("./components/SqlEditor", () => ({
  SqlEditor: ({
    value,
    onChange,
  }: {
    value: string;
    onChange: (value: string) => void;
  }) => (
    <textarea
      aria-label="SQL editor"
      value={value}
      onChange={(event) => onChange(event.target.value)}
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
