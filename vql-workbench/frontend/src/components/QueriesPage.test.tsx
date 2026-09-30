import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { QueryResult, ResultValue } from "../lib/types";
import { QueriesPage } from "./QueriesPage";

const row = {
  query_id: "query'1",
  name: "entrance_people_stream",
  state: "RUNNING",
  source_health: "reconnecting",
  restart_gap_count: 2n,
  started_at: 1700000000000,
  last_event_time: null,
};
const result = (...rows: Record<string, ResultValue>[]): QueryResult => ({
  fields: [],
  rows: rows.map((values, id) => ({ id, values })),
  receivedBatches: 1,
  rolling: false,
});
const list = result(
  row,
  { ...row, query_id: "query-2", name: "perimeter_scan", state: "STOPPED" },
  {
    ...row,
    query_id: "query-3",
    name: "failed_write",
    state: "FAILED",
    error_code: "VQL-57001",
    error_message: "Source disconnected",
  },
);
const detail = result({
  ...row,
  sql_redacted:
    "INSERT INTO sink SELECT frame_id FROM camera WHERE label = '?'",
  created_at: 1700000000000,
  last_restart_reset_window_state: true,
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function mount(request = vi.fn().mockResolvedValue(list), connected = true) {
  const onLoadSql = vi.fn();
  const onConnect = vi.fn();
  const props = {
    connected,
    connectionProblem: null,
    request,
    onLoadSql,
    onConnect,
  };
  const view = render(
    <StrictMode>
      <QueriesPage {...props} />
    </StrictMode>,
  );
  return { ...view, request, onLoadSql, onConnect, props };
}

describe("QueriesPage", () => {
  it("loads in StrictMode, searches, shows real fields, and disables terminal Stop actions", async () => {
    const { request } = mount();
    const article = await screen.findByRole("article", { name: row.name });
    expect(request).toHaveBeenCalledTimes(1);
    expect(within(article).getByText("Source: reconnecting")).toBeVisible();
    expect(within(article).getByText("Restart gaps: 2")).toBeVisible();
    expect(within(article).getByText("Last event: —")).toBeVisible();
    expect(
      screen.queryByText(/Watermark|checkpoint|Seq:/),
    ).not.toBeInTheDocument();
    expect(within(article).getByRole("button", { name: "Stop" })).toBeEnabled();
    expect(
      within(screen.getByRole("article", { name: "perimeter_scan" })).getByRole(
        "button",
        { name: "Stop" },
      ),
    ).toBeDisabled();
    expect(
      within(screen.getByRole("article", { name: "failed_write" })).getByRole(
        "button",
        { name: "Stop" },
      ),
    ).toBeDisabled();
    await userEvent.type(
      screen.getByRole("searchbox", { name: "Search queries" }),
      "STOPPED",
    );
    expect(screen.getAllByRole("article")).toHaveLength(1);
    fireEvent.keyDown(window, { key: "k", ctrlKey: true });
    expect(screen.getByRole("searchbox")).toHaveFocus();
  });

  it("loads redacted SQL into a new draft without executing it", async () => {
    const request = vi
      .fn()
      .mockResolvedValueOnce(list)
      .mockResolvedValueOnce(detail);
    const { onLoadSql } = mount(request);
    const article = await screen.findByRole("article", { name: row.name });
    await userEvent.click(
      within(article).getByRole("button", { name: "Show SQL" }),
    );
    expect(await screen.findByLabelText("Query SQL")).toHaveTextContent(
      "label = '?'",
    );
    expect(request.mock.calls[1][0]).toBe("DESCRIBE QUERY 'query''1';");
    await userEvent.click(
      screen.getByRole("button", { name: "Load SQL as new draft" }),
    );
    expect(onLoadSql).toHaveBeenCalledWith(
      detail.rows[0].values.sql_redacted,
      row.name,
    );
    expect(request).toHaveBeenCalledTimes(2);
  });

  it("serializes Stop and refresh, targets the ID, and updates the terminal state", async () => {
    let resolveStop!: (value: QueryResult) => void;
    const stopped = new Promise<QueryResult>((resolve) => {
      resolveStop = resolve;
    });
    const request = vi
      .fn()
      .mockResolvedValueOnce(list)
      .mockReturnValueOnce(stopped)
      .mockResolvedValueOnce(result({ ...row, state: "STOPPED" }));
    mount(request);
    const article = await screen.findByRole("article", { name: row.name });
    const stop = within(article).getByRole("button", {
      name: "Stop",
    });
    await userEvent.click(stop);
    expect(stop).toBeDisabled();
    expect(
      screen.getByRole("button", { name: "Refresh queries" }),
    ).toBeDisabled();
    expect(request.mock.calls[1][0]).toBe("STOP QUERY 'query''1';");
    expect(request).toHaveBeenCalledTimes(2);
    await act(async () => resolveStop(result({ ...row, state: "STOPPED" })));
    await waitFor(() =>
      expect(within(article).getByText("STOPPED")).toBeVisible(),
    );
    expect(stop).toBeDisabled();
    expect(request.mock.calls[2][0]).toBe("SHOW QUERIES;");
  });

  it("retains the last list and structured failure when Stop fails", async () => {
    const request = vi.fn().mockResolvedValueOnce(list).mockRejectedValueOnce({
      source: "vql",
      title: "VisionQL statement failed",
      message: "Query not found",
      code: "VQL-42004",
      symbol: "QUERY_NOT_FOUND",
    });
    mount(request);
    const article = await screen.findByRole("article", { name: row.name });
    await userEvent.click(
      within(article).getByRole("button", { name: "Stop" }),
    );
    const alert = await screen.findByRole("alert");
    expect(within(alert).getByText("VQL-42004")).toBeVisible();
    expect(within(article).getByText("RUNNING")).toBeVisible();
    expect(request).toHaveBeenCalledTimes(2);
  });

  it("shows disconnected, empty, and filtered-empty states", async () => {
    const { request, onConnect, rerender, props } = mount(
      vi.fn().mockResolvedValue(result()),
      false,
    );
    expect(
      screen.getByText("Connect to view persistent queries"),
    ).toBeVisible();
    expect(request).not.toHaveBeenCalled();
    await userEvent.click(
      screen.getByRole("button", { name: "Open Settings" }),
    );
    expect(onConnect).toHaveBeenCalledOnce();
    rerender(<QueriesPage {...props} connected />);
    expect(await screen.findByText("No registered queries")).toBeVisible();
    request.mockResolvedValueOnce(list);
    await userEvent.click(
      screen.getByRole("button", { name: "Refresh queries" }),
    );
    await screen.findByRole("article", { name: row.name });
    await userEvent.type(screen.getByRole("searchbox"), "missing");
    expect(screen.getByText("No matching queries")).toBeVisible();
  });

  it("polls only while visible, pauses for settings, and aborts pending work on unmount", async () => {
    const visibility = vi
      .spyOn(document, "visibilityState", "get")
      .mockReturnValue("visible");
    vi.useFakeTimers();
    const { request, rerender, props, unmount } = mount();
    await act(async () => undefined);
    expect(screen.getByRole("article", { name: row.name })).toBeVisible();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(request).toHaveBeenCalledTimes(2);
    visibility.mockReturnValue("hidden");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(request).toHaveBeenCalledTimes(2);
    visibility.mockReturnValue("visible");
    rerender(<QueriesPage {...props} active={false} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(request).toHaveBeenCalledTimes(2);
    request.mockImplementationOnce(
      () => new Promise<QueryResult>(() => undefined),
    );
    rerender(<QueriesPage {...props} />);
    await act(async () => undefined);
    const signal = request.mock.calls[2][1] as AbortSignal;
    unmount();
    expect(signal.aborted).toBe(true);
  });
});
