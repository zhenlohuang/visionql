import { expect, test, type Page } from "@playwright/test";

const vqldEndpoint = process.env.VQL_WORKBENCH_E2E_VQLD_ENDPOINT!;
const imageDirectory = process.env.VQL_WORKBENCH_E2E_IMAGE_DIR!;

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Settings" }).click();
  await page.getByLabel("vqld endpoint").fill(vqldEndpoint);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByText(/vqld: 127\.0\.0\.1:/)).toBeVisible();
});

test("creates, inspects, filters, and drops Tables and Functions via catalog SQL", async ({
  page,
}, testInfo) => {
  await navigate(page, "Tables");
  await create(
    page,
    "table",
    `CREATE TABLE catalog_photos USING IMAGES LOCATION '${imageDirectory.replaceAll("'", "''")}';`,
  );
  const tables = page.getByRole("main", { name: "Tables catalog" });
  const photos = tables.getByRole("article", { name: "catalog_photos" });
  await expect(photos.getByText("IMAGES", { exact: true })).toBeVisible();
  await photos.getByRole("button", { name: "Show DDL" }).click();
  const details = page.getByRole("dialog", { name: "TABLE DDL and schema" });
  await expect(
    details.getByRole("cell", { name: "image", exact: true }),
  ).toBeVisible();
  await expect(details.locator("pre")).toContainText("CREATE TABLE");
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-table-details.png"),
  });
  await details.getByRole("button", { name: "Open in SQL editor" }).click();
  await expect(
    page.getByRole("textbox", { name: "SQL editor", exact: true }),
  ).toContainText("catalog_photos");

  await navigate(page, "Functions");
  await create(
    page,
    "function",
    "CREATE FUNCTION catalog_plus_one(BIGINT) RETURNS BIGINT RETURN $1 + 1;",
  );
  await create(
    page,
    "function",
    "CREATE FUNCTION catalog_python(value BIGINT) RETURNS BIGINT LANGUAGE PYTHON AS 'quality:score';",
  );
  const functions = page.getByRole("main", { name: "Functions catalog" });
  await functions.getByRole("button", { name: /Python UDF/ }).click();
  await expect(
    functions.getByRole("button", { name: "catalog_python", exact: true }),
  ).toBeVisible();
  await functions.getByRole("searchbox").fill("catalog_plus_one");
  await expect(functions.getByText("No matching objects")).toBeVisible();
  await functions.getByRole("button", { name: /^All/ }).click();
  await expect(
    functions.getByRole("button", { name: "catalog_plus_one", exact: true }),
  ).toBeVisible();
  await functions.getByRole("searchbox").fill("");
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-functions.png"),
  });
  await drop(page, "catalog_python");
  await drop(page, "catalog_plus_one");

  await navigate(page, "Tables");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-tables-mobile.png"),
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await drop(page, "catalog_photos");
});

test("resolves Model versions, changes the default, and confirms version removal", async ({
  page,
}, testInfo) => {
  await navigate(page, "Models");
  await create(
    page,
    "model",
    "CREATE MODEL catalog_detector TYPE OBJECT_DETECTION VERSION 'v1' FROM 'mock://person' USING ONNX_RUNTIME;",
  );
  const models = page.getByRole("main", { name: "Models catalog" });
  await models
    .getByRole("button", { name: "catalog_detector", exact: true })
    .click();
  let details = page.getByRole("dialog", { name: "MODEL DDL and schema" });
  await expect(details.getByText("Unresolved", { exact: true })).toBeVisible();
  await details.getByRole("button", { name: "Resolve", exact: true }).click();
  await executeAction(page);
  await expect(
    details.getByText("Schema resolved", { exact: true }),
  ).toBeVisible();
  await details.getByRole("button", { name: "Alter model" }).click();
  await page
    .getByRole("textbox", { name: "Catalog SQL statement" })
    .fill(
      "ALTER MODEL catalog_detector ADD VERSION 'v2' FROM 'mock://candidate' USING ONNX_RUNTIME;",
    );
  await executeAction(page);
  const v2 = details.getByRole("group", { name: "Model version v2" });
  await v2.getByRole("button", { name: "Resolve", exact: true }).click();
  await executeAction(page);
  await v2.getByRole("button", { name: "Set default", exact: true }).click();
  await executeAction(page);
  await expect(v2.getByText("Default", { exact: true })).toBeVisible();
  const v1 = details.getByRole("group", { name: "Model version v1" });
  await v1.getByRole("button", { name: "Drop version" }).click();
  const confirmation = page.getByRole("dialog", { name: "Drop model version" });
  await expect(confirmation.getByRole("textbox")).toHaveValue(
    "ALTER MODEL \"catalog_detector\" DROP VERSION 'v1';",
  );
  await confirmation.getByRole("button", { name: "Confirm drop" }).click();
  await expect(details).toBeVisible();
  await expect(v2.getByText("Default", { exact: true })).toBeVisible();
  await expect(v1).toHaveCount(0);
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath("catalog-model-versions.png"),
  });
  await details.getByRole("button", { name: "Close", exact: true }).click();

  await navigate(page, "Functions");
  const functions = page.getByRole("main", { name: "Functions catalog" });
  await functions
    .getByRole("button", { name: "catalog_detector", exact: true })
    .click();
  details = page.getByRole("dialog", { name: "MODEL DDL and schema" });
  await expect(details.locator("pre")).toContainText("CREATE MODEL");
  await details.getByRole("button", { name: "Close", exact: true }).click();
  await drop(page, "catalog_detector");
});

async function navigate(page: Page, label: string) {
  await page.getByRole("button", { name: label, exact: true }).click();
  await expect(
    page
      .getByRole("main", { name: `${label} catalog` })
      .getByRole("button", { name: "Refresh", exact: true }),
  ).toBeEnabled();
}

async function create(page: Page, kind: string, sql: string) {
  await page
    .getByRole("button", { name: `Create ${kind}`, exact: true })
    .click();
  await page.getByRole("textbox", { name: "Catalog SQL statement" }).fill(sql);
  await executeAction(page);
  await expect(
    page.getByRole("button", { name: `Create ${kind}`, exact: true }),
  ).toBeEnabled();
}

async function executeAction(page: Page) {
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Execute SQL", exact: true })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Catalog SQL statement" }),
  ).toHaveCount(0);
}

async function drop(page: Page, name: string) {
  await page.getByRole("button", { name: `Drop ${name}`, exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Confirm drop", exact: true })
    .click();
  await expect(page.getByRole("button", { name, exact: true })).toHaveCount(0);
}
