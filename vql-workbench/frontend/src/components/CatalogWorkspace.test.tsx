import { StrictMode } from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CatalogWorkspace } from "./CatalogWorkspace";
import type { QueryResult, ResultValue } from "../lib/types";

function result(...rows: Record<string, ResultValue>[]): QueryResult {
  return {
    fields: [],
    rows: rows.map((values, index) => ({ id: index, values })),
    receivedBatches: 1,
    rolling: false,
  };
}

function setup(
  options: { connected?: boolean; strict?: boolean; rejectDrop?: boolean } = {},
) {
  const execute = vi.fn(async (sql: string) => {
    if (sql === "SHOW TABLES;")
      return result(
        { table_name: "photos", provider: "IMAGES", location: "/photos" },
        { table_name: "camera", provider: "RTSP", location: "rtsp://camera" },
      );
    if (sql.startsWith("SHOW CREATE TABLE")) {
      const name = sql.includes('"photos"') ? "photos" : "camera";
      return result({
        object_name: name,
        create_sql: `CREATE TABLE ${name} USING IMAGES LOCATION '/photos';`,
      });
    }
    if (sql.startsWith("DESCRIBE TABLE"))
      return result({
        column_name: "image",
        data_type: "Struct",
        nullable: "true",
      });
    if (sql.startsWith("DROP TABLE")) {
      if (options.rejectDrop)
        throw {
          source: "vql",
          title: "VisionQL statement failed",
          message: "Object is referenced",
          code: "VQL-10001",
          symbol: "DEPENDENCY",
        };
      return null;
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const onBusyChange = vi.fn();
  const onOpenSql = vi.fn();
  const content = (
    <CatalogWorkspace
      section="tables"
      connected={options.connected ?? true}
      busy={false}
      execute={execute}
      onBusyChange={onBusyChange}
      onOpenSql={onOpenSql}
      onConnect={vi.fn()}
    />
  );
  render(options.strict ? <StrictMode>{content}</StrictMode> : content);
  return { execute, onBusyChange, onOpenSql };
}

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Catalog workspace", () => {
  it("loads once under StrictMode and combines category and text filters", async () => {
    const { execute } = setup({ strict: true });
    await screen.findByRole("button", { name: "photos" });
    expect(
      execute.mock.calls.filter(([sql]) => sql === "SHOW TABLES;"),
    ).toHaveLength(1);
    await userEvent.click(screen.getByRole("button", { name: /IMAGES\s*1/ }));
    fireEvent.change(screen.getByRole("searchbox", { name: "Search tables" }), {
      target: { value: "camera" },
    });
    expect(screen.getByText("No matching objects")).toBeVisible();
    await userEvent.click(screen.getByRole("button", { name: /All\s*2/ }));
    expect(screen.getByRole("button", { name: "camera" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "photos" })).toBeNull();
  });

  it("requires explicit drop confirmation and preserves failure details", async () => {
    const { execute } = setup({ rejectDrop: true });
    await screen.findByRole("button", { name: "photos" });
    await userEvent.click(screen.getByRole("button", { name: "Drop photos" }));
    expect(execute.mock.calls.some(([sql]) => sql.startsWith("DROP"))).toBe(
      false,
    );
    const dialog = screen.getByRole("dialog");
    expect(
      within(dialog).getByRole("textbox", { name: "Catalog SQL statement" }),
    ).toHaveValue('DROP TABLE "photos";');
    await userEvent.click(
      within(dialog).getByRole("button", { name: "Confirm drop" }),
    );
    await within(dialog).findByText("DEPENDENCY");
    expect(within(dialog).getByText("Object is referenced")).toBeVisible();
    expect(
      within(dialog).getByRole("textbox", { name: "Catalog SQL statement" }),
    ).toHaveValue('DROP TABLE "photos";');
    await waitFor(() =>
      expect(
        within(dialog).getByRole("button", { name: "Confirm drop" }),
      ).toBeEnabled(),
    );
  });

  it("opens returned defining SQL as a new editor draft", async () => {
    const { onOpenSql } = setup();
    await userEvent.click(
      await screen.findByRole("button", { name: "photos" }),
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Open in SQL editor" }),
    );
    expect(onOpenSql).toHaveBeenCalledWith(
      "CREATE TABLE photos USING IMAGES LOCATION '/photos';",
      "photos",
    );
  });

  it("does not query or enable mutations while disconnected", () => {
    const { execute } = setup({ connected: false });
    expect(screen.getByText("Connect to browse the catalog")).toBeVisible();
    expect(screen.getByRole("button", { name: "Create table" })).toBeDisabled();
    expect(execute).not.toHaveBeenCalled();
  });
});
