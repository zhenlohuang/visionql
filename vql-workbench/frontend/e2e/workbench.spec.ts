import { createServer, type Socket } from "node:net";

import { expect, test, type Page } from "@playwright/test";

const baseURL = requiredEnvironment("VQL_WORKBENCH_E2E_BASE_URL");
const vqldEndpoint = requiredEnvironment("VQL_WORKBENCH_E2E_VQLD_ENDPOINT");
const imageDirectory = requiredEnvironment("VQL_WORKBENCH_E2E_IMAGE_DIR");

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("textbox", { name: "SQL editor", exact: true }),
  ).toBeVisible();
});

test("rejects foreign hosts and origins on the built backend", async ({
  page,
}) => {
  const foreignHost = await page.request.get(`${baseURL}/api/session`, {
    headers: { Host: "attacker.example:6040" },
  });
  expect(foreignHost.status()).toBe(403);

  const missingOrigin = await page.request.delete(`${baseURL}/api/session`);
  expect(missingOrigin.status()).toBe(403);
});

test("runs keyboard queries once and preserves schema-only results", async ({
  page,
}) => {
  await connect(page);
  await replaceSql(page, "SELECT 42 AS answer;");
  let starts = 0;
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      new URL(request.url()).pathname === "/api/executions"
    ) {
      starts += 1;
    }
  });
  await page
    .getByRole("button", { name: /Run buffer/ })
    .evaluate((button: HTMLButtonElement) => {
      button.click();
      button.click();
    });
  await expect(
    resultPane(page).getByRole("cell", { name: "42" }),
  ).toBeVisible();
  expect(starts).toBe(1);

  await replaceSql(page, "SELECT 1 AS empty_value WHERE FALSE;");
  await page
    .getByRole("textbox", { name: "SQL editor", exact: true })
    .press(process.platform === "darwin" ? "Meta+Enter" : "Control+Enter");
  await expect(
    resultPane(page).getByRole("columnheader", { name: /empty_value/ }),
  ).toBeVisible();
  await expect(resultPane(page).getByText("Showing 0–0 of 0")).toBeVisible();

  await replaceSql(page, "SELEC invalid;");
  await page.getByRole("button", { name: /Run buffer/ }).click();
  await expect(
    resultPane(page).getByText("VisionQL statement failed"),
  ).toBeVisible();
  await expect(resultPane(page).getByText("INVALID_SQL")).toBeVisible();
});

test("renders a vql.image thumbnail from shipped public protocols", async ({
  page,
}) => {
  await connect(page);
  const escapedDirectory = imageDirectory.replaceAll("'", "''");
  await runSql(
    page,
    `CREATE TABLE workbench_images USING IMAGES LOCATION '${escapedDirectory}';`,
  );
  await expect(resultPane(page).getByText("Statement completed")).toBeVisible();

  await runSql(page, "SELECT image FROM workbench_images LIMIT 1;");
  const image = resultPane(page).getByAltText("Returned VisionQL thumbnail");
  await expect(image).toBeVisible();
  await expect(image).toHaveAttribute("src", /^blob:/);
});

test("cancels explicit and dropped attached result streams", async ({
  page,
}) => {
  const sockets = new Set<Socket>();
  const source = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("data", () => undefined);
  });
  await new Promise<void>((resolve, reject) => {
    source.once("error", reject);
    source.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = source.address();
    if (!address || typeof address === "string") {
      throw new Error("RTSP fixture did not expose a TCP port");
    }
    await connect(page);
    await runSql(
      page,
      `CREATE TABLE workbench_camera USING RTSP OPTIONS (
        url = 'rtsp://127.0.0.1:${address.port}/main',
        fps = 1,
        event_time = 'capture_time',
        watermark = '2 seconds',
        transport = 'tcp'
      );`,
    );
    await expect(
      resultPane(page).getByText("Statement completed"),
    ).toBeVisible();

    await replaceSql(page, "SELECT frame_id FROM workbench_camera;");
    await page.getByRole("button", { name: "Run as attached stream" }).click();
    const cancel = page.getByRole("button", {
      name: /Cancel active execution/,
    });
    await expect(cancel).toBeEnabled();
    await cancel.click();
    await expect(cancel).toBeDisabled();
    await page.getByRole("button", { name: /History/ }).click();
    const cancelledRecord = page
      .getByRole("article")
      .filter({ hasText: "SELECT frame_id FROM workbench_camera" });
    await expect(
      cancelledRecord.getByText("cancelled", { exact: true }),
    ).toBeVisible();
    await page
      .getByRole("contentinfo")
      .getByRole("button", { name: "Close", exact: true })
      .click();

    await replaceSql(page, "SELECT frame_id FROM workbench_camera;");
    const started = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname === "/api/executions",
    );
    await page.getByRole("button", { name: "Run as attached stream" }).click();
    const startPayload = (await (await started).json()) as {
      executionId: string;
    };
    await expect(cancel).toBeEnabled();
    await page.goto("about:blank");

    await expect
      .poll(async () => {
        const response = await page.request.get(
          `${baseURL}/api/executions/${startPayload.executionId}`,
        );
        return response.ok()
          ? ((await response.json()) as { status: string }).status
          : `http-${response.status()}`;
      })
      .toBe("cancelled");
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => source.close(() => resolve()));
  }
});

test("expires an idle browser Session and asks for reconnection", async ({
  page,
}) => {
  test.setTimeout(40_000);
  await connect(page);
  await page.waitForTimeout(11_000);
  await runSql(page, "SELECT 1;");

  await expect(
    resultPane(page).getByText("Workbench is disconnected"),
  ).toBeVisible();
  await expect(page.getByText("vqld: disconnected")).toBeVisible();
});

test("manages Jobs through public SQL and preserves editor drafts", async ({
  page,
}, testInfo) => {
  const sockets = new Set<Socket>();
  const source = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("data", () => undefined);
  });
  await new Promise<void>((resolve, reject) => {
    source.once("error", reject);
    source.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = source.address();
    if (!address || typeof address === "string")
      throw new Error("Missing RTSP fixture port");
    await connect(page);
    await runSql(
      page,
      `CREATE TABLE jobs_camera USING RTSP OPTIONS (
      url = 'rtsp://127.0.0.1:${address.port}/main', fps = 1,
      event_time = 'capture_time', watermark = '2 seconds', transport = 'tcp'
    );`,
    );
    await expect(
      resultPane(page).getByText("Statement completed"),
    ).toBeVisible();
    // The RTSP fixture emits no frames, so the sink never needs a broker connection.
    await runSql(
      page,
      "CREATE TABLE jobs_sink (frame_id BIGINT) USING KAFKA OPTIONS (bootstrap_servers = '127.0.0.1:1', topic = 'jobs-e2e');",
    );
    await expect(
      resultPane(page).getByText("Statement completed"),
    ).toBeVisible();
    await runSql(
      page,
      "SUBMIT QUERY entrance_people_stream AS INSERT INTO jobs_sink SELECT frame_id FROM jobs_camera WHERE frame_id > 0;",
    );
    await expect(
      resultPane(page).getByRole("cell", { name: "entrance_people_stream" }),
    ).toBeVisible();
    const editor = page.getByRole("textbox", {
      name: "SQL editor",
      exact: true,
    });
    await replaceSql(page, "SELECT 42 AS original_draft;");
    const requests: string[] = [];
    page.on("request", (request) => {
      if (
        request.method() === "POST" &&
        new URL(request.url()).pathname === "/api/executions"
      ) {
        requests.push((request.postDataJSON() as { sql: string }).sql);
      }
    });
    await page.getByRole("button", { name: "Jobs", exact: true }).click();
    const queries = page.getByRole("main", { name: "Persistent jobs" });
    const query = queries.getByRole("article", {
      name: "entrance_people_stream",
    });
    await expect(query.getByText("RUNNING", { exact: true })).toBeVisible();
    await expect(queries.getByText("1 running · 1 total")).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath("jobs-desktop.png"),
      animations: "disabled",
    });

    await queries.getByRole("searchbox").fill("missing");
    await expect(queries.getByText("No matching jobs")).toBeVisible();
    await queries.getByRole("button", { name: "Clear search" }).click();
    await query.getByRole("button", { name: "Show SQL" }).click();
    const dialog = page.getByRole("dialog", { name: "Job SQL and details" });
    await expect(dialog.getByLabel("Job SQL")).toContainText("INSERT INTO");
    await expect(
      dialog.getByText(/String literals are redacted/),
    ).toBeVisible();
    await dialog.getByRole("button", { name: "Load SQL as new draft" }).click();
    await expect(editor).toBeVisible();
    await expect(editor).toContainText("INSERT INTO");
    await page
      .getByRole("button", { name: "Untitle.sql", exact: true })
      .click();
    await expect(editor).toHaveText("SELECT 42 AS original_draft;");
    await page.getByRole("button", { name: "Jobs", exact: true }).click();
    await expect(
      query.getByRole("button", { name: "Stop", exact: true }),
    ).toBeEnabled();
    await query.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(query.getByText("STOPPED", { exact: true })).toBeVisible();
    await expect(
      query.getByRole("button", { name: "Stop", exact: true }),
    ).toBeDisabled();
    expect(requests.some((sql) => /^STOP QUERY '/.test(sql))).toBe(true);
    expect(
      requests.every((sql) =>
        /^(SHOW JOBS|DESCRIBE QUERY|STOP QUERY)/.test(sql),
      ),
    ).toBe(true);

    await page.setViewportSize({ width: 390, height: 844 });
    await expect(query).toBeVisible();
    expect(
      await queries.evaluate(
        (element) => element.scrollWidth <= element.clientWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath("jobs-mobile.png"),
      animations: "disabled",
    });
    await page.reload();
    await expect(editor).toBeVisible();
    await page.getByRole("button", { name: "Open navigation" }).click();
    await page.getByRole("button", { name: "Jobs", exact: true }).click();
    await expect(query.getByText("STOPPED", { exact: true })).toBeVisible();
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => source.close(() => resolve()));
  }
});

test("reconnects after an expired Query inspection Session", async ({
  page,
}) => {
  test.setTimeout(40_000);
  await connect(page);
  await page.waitForTimeout(11_000);
  await page.getByRole("button", { name: "Jobs", exact: true }).click();
  const queries = page.getByRole("main", { name: "Persistent jobs" });
  await expect(queries.getByText("Workbench is disconnected")).toBeVisible();
  await expect(
    queries.getByText("Connect to view persistent jobs"),
  ).toBeVisible();
  await connect(page);
  await expect(
    queries.getByRole("button", { name: "Refresh jobs" }),
  ).toBeEnabled();
  await expect(queries.getByText("Connected: vqld (Flight SQL)")).toBeVisible();
  await expect(
    queries.getByText("Workbench is disconnected"),
  ).not.toBeVisible();
});

async function connect(page: Page) {
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await page.getByLabel("vqld endpoint").fill(vqldEndpoint);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByText(/vqld: 127\.0\.0\.1:/)).toBeVisible();
}

async function runSql(page: Page, sql: string) {
  await replaceSql(page, sql);
  await page.getByRole("button", { name: /Run buffer/ }).click();
}

async function replaceSql(page: Page, sql: string) {
  await page
    .getByRole("textbox", { name: "SQL editor", exact: true })
    .fill(sql);
}

function resultPane(page: Page) {
  return page.getByRole("region", { name: "Query results" });
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
