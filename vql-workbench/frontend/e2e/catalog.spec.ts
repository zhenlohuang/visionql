import { expect, test, type Page } from "@playwright/test";

const vqldEndpoint = process.env.VQL_WORKBENCH_E2E_VQLD_ENDPOINT!;
const imageDirectory = process.env.VQL_WORKBENCH_E2E_IMAGE_DIR!;

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await page.getByLabel("vqld endpoint").fill(vqldEndpoint);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByText(/vqld: 127\.0\.0\.1:/)).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh catalog navigation" }),
  ).toBeEnabled();
});

test("creates a table through an editor draft and displays highlighted DDL in the main workspace", async ({
  page,
}) => {
  await page
    .getByRole("list", { name: "vql.default objects" })
    .getByRole("button", { name: "Tables", exact: true })
    .click();
  await expect(page.getByText("Select an object")).toBeVisible();
  await page.getByRole("button", { name: "Create table", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "SQL editor", exact: true }),
  ).toContainText("CREATE TABLE photos");
  await executeSql(
    page,
    `CREATE TABLE catalog_photos USING IMAGES LOCATION '${imageDirectory.replaceAll("'", "''")}';`,
  );
  await selectObject(page, "table", "vql.default.catalog_photos");
  const workspace = page.getByRole("main", { name: "Catalog DDL" });
  const ddl = workspace.getByRole("region", { name: "Formatted DDL" });
  await expect
    .poll(() => ddl.locator(".cm-content").innerText())
    .toMatch(/\nUSING IMAGES\n/);
  await expect(
    ddl.locator(".cm-line").filter({ hasText: /^LOCATION / }),
  ).toBeVisible();
  await expect(
    ddl.locator(".tok-keyword", { hasText: /^LOCATION$/ }),
  ).toBeVisible();
  await expect(workspace.getByRole("article")).toHaveCount(0);
  await expect(workspace.getByRole("searchbox")).toHaveCount(0);
  await expect(
    workspace.getByRole("button", { name: "Open in SQL editor" }),
  ).toHaveCount(0);
  await expect(
    workspace.getByRole("button", { name: "Refresh DDL" }),
  ).toHaveCount(0);
  await expect(page.getByRole("dialog")).toHaveCount(0);
  const editor = workspace.getByRole("textbox", {
    name: "DDL editor",
    exact: true,
  });
  await expect(editor).toHaveAttribute("aria-readonly", "true");
  await expect(
    workspace
      .locator(".cm-lineNumbers .cm-gutterElement")
      .filter({ hasText: /^1$/ }),
  ).toBeVisible();
  const before = await editor.innerText();
  const executedStatements: string[] = [];
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      new URL(request.url()).pathname === "/api/executions"
    )
      executedStatements.push(request.postData() ?? "");
  });
  await editor.click();
  await editor.press(process.platform === "darwin" ? "Meta+A" : "Control+A");
  await editor.press("Backspace");
  await editor.pressSequentially("DROP TABLE catalog_photos;");
  await editor.press(
    process.platform === "darwin" ? "Meta+Enter" : "Control+Enter",
  );
  await expect.poll(() => editor.innerText()).toBe(before);
  expect(executedStatements).toEqual([]);
  await editor.press(process.platform === "darwin" ? "Meta+f" : "Control+f");
  const search = workspace.getByRole("textbox", {
    name: "Find",
    exact: true,
  });
  await expect(search).toBeVisible();
  await search.fill("");
  await search.pressSequentially("catalog_photos");
  await search.press("Enter");
  await expect(workspace.locator(".cm-searchMatch-selected")).toBeVisible();
  await search.press("Escape");
  await expect(search).toHaveCount(0);
  await expect.poll(() => editor.innerText()).toBe(before);
  await page.getByRole("button", { name: "SQL editor", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "SQL editor", exact: true }),
  ).toContainText("catalog_photos");
  await executeSql(page, "DROP TABLE catalog_photos;");
  await expect(
    page.getByRole("button", {
      name: "Show DDL for table vql.default.catalog_photos",
    }),
  ).toHaveCount(0);
});

test("shows Model versions in the right panel and switches DDL from both object entries", async ({
  page,
}, testInfo) => {
  await executeSql(
    page,
    "CREATE MODEL catalog_detector TYPE OBJECT_DETECTION VERSION 'release-1' FROM 'mock://person' USING ONNX_RUNTIME;",
  );
  await executeSql(
    page,
    "ALTER MODEL catalog_detector ADD VERSION 'v2' FROM 'mock://candidate' USING ONNX_RUNTIME;",
  );
  await executeSql(page, "RESOLVE MODEL catalog_detector VERSION 'v2';");
  await executeSql(page, "RESOLVE MODEL catalog_detector VERSION 'release-1';");
  const tree = page.getByRole("region", { name: "Catalog navigation" });
  const models = tree.getByRole("list", { name: "vql.default models" });
  await selectObject(page, "model", "vql.default.catalog_detector", "Models");
  const workspace = page.getByRole("main", { name: "Catalog DDL" });
  const panel = workspace.getByRole("complementary", {
    name: "Model versions",
  });
  const versions = panel.getByRole("list", {
    name: "Versions of model vql.default.catalog_detector",
  });
  const initial = versions.getByRole("button", {
    name: "Show DDL for model vql.default.catalog_detector version release-1",
    exact: true,
  });
  const second = versions.getByRole("button", {
    name: "Show DDL for model vql.default.catalog_detector version v2",
    exact: true,
  });
  await expect(initial.getByText("Default", { exact: true })).toBeVisible();
  await expect(
    tree.getByRole("list", { name: /Versions of model/ }),
  ).toHaveCount(0);
  await expect(second).toHaveAttribute("aria-current", "true");
  const ddlBounds = await workspace
    .getByRole("region", { name: "Formatted DDL" })
    .boundingBox();
  const versionBounds = await panel.boundingBox();
  expect(versionBounds!.x).toBeGreaterThanOrEqual(
    ddlBounds!.x + ddlBounds!.width - 1,
  );
  await initial.click();
  const ddl = workspace.getByRole("textbox", { name: "DDL editor" });
  await expect(ddl).toContainText("CREATE MODEL");
  await expect(ddl).toContainText("mock://person");
  await expect(ddl).not.toContainText("mock://candidate");
  await expect(
    models.getByRole("button", {
      name: "Show DDL for model vql.default.catalog_detector",
      exact: true,
    }),
  ).toHaveAttribute("aria-current", "true");
  await expect(
    workspace.locator(".tok-keyword", { hasText: /^VERSION$/ }),
  ).toBeVisible();
  await expect(
    workspace.getByRole("button", { name: "Copy DDL" }),
  ).toBeEnabled();
  await second.click();
  await expect(ddl).toContainText("CREATE MODEL");
  await expect(ddl).toContainText("mock://candidate");
  await expect(ddl).not.toContainText("mock://person");
  await expect(second).toHaveAttribute("aria-current", "true");
  await expect(
    workspace.getByRole("button", { name: "Copy DDL" }),
  ).toBeEnabled();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-model-version-ddl.png"),
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await initial.click();
  await expect(ddl).toContainText("mock://person");
  await second.click();
  await expect(ddl).toContainText("mock://candidate");
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-model-versions-mobile.png"),
  });
  await page.setViewportSize({ width: 1280, height: 720 });
  await selectObject(
    page,
    "model",
    "vql.default.catalog_detector",
    "Functions",
  );
  await initial.click();
  await expect(ddl).toContainText("mock://person");
  await second.click();
  await expect(ddl).toContainText("mock://candidate");
  await expect(ddl).not.toContainText("mock://person");
  await selectObject(page, "model", "vql.default.catalog_detector", "Models");
  await expect(ddl).not.toContainText("mock://person");
  await expect(ddl).toContainText("mock://candidate");
  await expect(
    workspace.getByText("vql.default · MODEL · Version v2"),
  ).toBeVisible();
  await selectObject(page, "model", "vql.default.catalog_detector", "Models");
  await expect(ddl).not.toContainText("ALTER MODEL");
  await expect(ddl).not.toContainText("mock://person");
  await expect(
    workspace.getByRole("button", { name: "Copy DDL" }),
  ).toBeEnabled();
  await executeSql(
    page,
    "ALTER MODEL catalog_detector SET DEFAULT_VERSION = 'v2';",
  );
  await executeSql(
    page,
    "ALTER MODEL catalog_detector DROP VERSION 'release-1';",
  );
  await selectObject(page, "model", "vql.default.catalog_detector", "Models");
  await expect(panel).toHaveCount(0);
  await expect(ddl).toContainText("mock://candidate");
  await executeSql(page, "DROP MODEL catalog_detector;");
});

test("switches same-named objects across namespaces without a modal and fits mobile screens", async ({
  page,
}, testInfo) => {
  for (const name of ["quality.tree_score", "team.media.tree_score"]) {
    await executeSql(
      page,
      `CREATE FUNCTION "${name}"(BIGINT) RETURNS BIGINT RETURN $1 + 1;`,
    );
  }
  const tree = page.getByRole("region", { name: "Catalog navigation" });
  await expect(
    tree.getByRole("button", { name: "team", exact: true }),
  ).toBeVisible();
  const teamObject = tree.getByRole("button", {
    name: "Show DDL for function team.media.tree_score",
    exact: true,
  });
  await tree
    .getByRole("button", { name: "Functions in team.media", exact: true })
    .click();
  await expect(teamObject).toHaveCount(0);
  await tree
    .getByRole("button", { name: "Functions in team.media", exact: true })
    .click();
  await teamObject.click();
  const workspace = page.getByRole("main", { name: "Catalog DDL" });
  const ddl = workspace.getByRole("region", { name: "Formatted DDL" });
  await expect
    .poll(() => ddl.locator(".cm-content").innerText())
    .toMatch(/\nRETURNS BIGINT\nRETURN/);
  await expect(ddl).toContainText("team.media.tree_score");
  await expect(
    ddl.locator(".tok-keyword", { hasText: /^RETURN$/ }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-ddl-workspace.png"),
  });
  await selectObject(page, "function", "vql.quality.tree_score");
  await expect(ddl).toContainText('"quality.tree_score"');
  await expect(ddl).not.toContainText("team.media.tree_score");
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(ddl).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-ddl-mobile.png"),
  });
  await page.getByRole("button", { name: "Open navigation" }).click();
  await expect(tree).toBeVisible();
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-tree-mobile.png"),
  });
  await page
    .getByRole("button", { name: "Close navigation" })
    .click({ position: { x: 380, y: 50 } });
  await page.setViewportSize({ width: 1280, height: 720 });
  for (const name of ["quality.tree_score", "team.media.tree_score"])
    await executeSql(page, `DROP FUNCTION "${name}";`);
});

async function selectObject(
  page: Page,
  kind: string,
  address: string,
  section?: string,
) {
  const tree = page.getByRole("region", { name: "Catalog navigation" });
  const group = section
    ? tree.getByRole("list", { name: `vql.default ${section.toLowerCase()}` })
    : tree;
  await group
    .getByRole("button", {
      name: `Show DDL for ${kind} ${address}`,
      exact: true,
    })
    .click();
  await expect(page.getByRole("button", { name: "Copy DDL" })).toBeEnabled();
}

async function executeSql(page: Page, sql: string) {
  const editor = page.getByRole("textbox", { name: "SQL editor", exact: true });
  if (!(await editor.isVisible()))
    await page.getByRole("button", { name: "SQL editor", exact: true }).click();
  await expect(page.getByRole("button", { name: /Run buffer/ })).toBeEnabled();
  await editor.fill(sql);
  await page.getByRole("button", { name: /Run buffer/ }).click();
  await expect(
    page
      .getByRole("region", { name: "Query results" })
      .getByText("Statement completed"),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh catalog navigation" }),
  ).toBeEnabled();
}
