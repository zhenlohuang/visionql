import { readFileSync } from "node:fs";

import { expect, test, type Locator } from "@playwright/test";

interface DetectionRow {
  uri: string;
  label: string;
  confidence: number;
  box: { x: number; y: number; w: number; h: number };
  image: { width: number; height: number };
}

const referencePath = process.env.VQL_WORKBENCH_E2E_PARITY_REFERENCE;

// Real artifacts are opt-in; the strict runner generates the oracle or fails.
if (referencePath) {
  const reference = JSON.parse(readFileSync(referencePath, "utf8")) as {
    setupSql: string;
    querySql: string;
    fields: { name: string; extensionName: string }[];
    rows: DetectionRow[];
  };

  test("matches real Python/notebook detections and renders normalized boxes", async ({
    page,
  }, testInfo) => {
    test.setTimeout(120_000);
    await page.goto("/");
    await page.getByRole("button", { name: "Settings", exact: true }).click();
    await page
      .getByLabel("vqld endpoint")
      .fill(process.env.VQL_WORKBENCH_E2E_VQLD_ENDPOINT!);
    await page.getByRole("button", { name: "Connect", exact: true }).click();
    await expect(page.getByText(/vqld: 127\.0\.0\.1:/)).toBeVisible();
    const editor = page.getByRole("textbox", {
      name: "SQL editor",
      exact: true,
    });
    const run = page.getByRole("button", { name: /Run buffer/ });
    const results = page.getByRole("region", { name: "Query results" });
    for (const sql of reference.setupSql
      .split(";")
      .filter((sql) => sql.trim())) {
      await editor.fill(`${sql.trim()};`);
      await run.click();
      await expect(results.getByText("Statement completed")).toBeVisible();
      await expect(run).toBeEnabled();
    }

    await editor.fill(reference.querySql);
    const submitted = page.waitForRequest(
      (request) =>
        request.method() === "POST" &&
        new URL(request.url()).pathname === "/api/executions",
    );
    const arrow = page.waitForResponse((response) =>
      /\/api\/executions\/[^/]+\/results$/.test(
        new URL(response.url()).pathname,
      ),
    );
    await run.click();
    expect((await submitted).postDataJSON().sql).toBe(reference.querySql);
    expect((await arrow).headers()["content-type"]).toContain(
      "application/vnd.apache.arrow.stream",
    );
    await expect(run).toBeEnabled();
    for (const field of reference.fields) {
      const type =
        field.extensionName === "vql.image"
          ? "IMAGE"
          : field.extensionName === "vql.box2d"
            ? "BOX2D"
            : "";
      await expect(
        results.getByRole("columnheader", { name: new RegExp(field.name) }),
      ).toContainText(type || field.name);
    }

    await results.getByRole("button", { name: "JSON", exact: true }).click();
    const rows = JSON.parse(
      (await results.locator("pre").textContent())!,
    ) as (DetectionRow & {
      image: {
        encoded: string;
        encoding: string;
        uri: null;
        locator: null;
        buffer_id: null;
        buffer_slot: null;
      };
    })[];
    expect(rows).toHaveLength(reference.rows.length);
    for (const [index, row] of rows.entries()) {
      const expected = reference.rows[index];
      expect(row.uri).toBe(expected.uri);
      expect(row.label).toBe(expected.label);
      expect(row.confidence).toBeCloseTo(expected.confidence, 5);
      for (const key of ["x", "y", "w", "h"] as const) {
        expect(row.box[key]).toBeCloseTo(expected.box[key], 5);
      }
      expect(row.image.encoding).toBe("jpeg");
      expect(row.image.encoded).toMatch(/^<[1-9]\d* bytes>$/);
      expect(row.image.uri).toBeNull();
      expect(row.image.locator).toBeNull();
      expect(row.image.buffer_id).toBeNull();
      expect(row.image.buffer_slot).toBeNull();
      expect(row.image.width).toBeGreaterThan(0);
      expect(row.image.height).toBeGreaterThan(0);
      expect(Math.max(row.image.width, row.image.height)).toBeLessThanOrEqual(
        512,
      );
      // Thumbnail resizing preserves aspect ratio within pixel rounding.
      expect(
        Math.abs(
          row.image.height -
            (row.image.width * expected.image.height) / expected.image.width,
        ),
      ).toBeLessThanOrEqual(1);
    }

    await results.getByRole("button", { name: "Table", exact: true }).click();
    const thumbnails = results.getByAltText("Returned VisionQL thumbnail");
    await expect(thumbnails).toHaveCount(rows.length);
    await expect(thumbnails.first()).toHaveAttribute("src", /^blob:/);
    const preview = thumbnails.first().locator("..");
    await checkOverlay(preview, rows[0]);
    await thumbnails.first().click();
    const inspector = page.getByRole("dialog", { name: "Row 1 inspection" });
    const inspected = JSON.parse(
      (await inspector.locator("pre").textContent())!,
    );
    expect(inspected.box).toEqual(rows[0].box);
    expect(inspected.confidence).toBe(rows[0].confidence);
    await checkOverlay(
      inspector.getByAltText("Returned VisionQL thumbnail").locator(".."),
      rows[0],
    );
    await page.screenshot({
      path: testInfo.outputPath("python-workbench-visual-parity.png"),
      animations: "disabled",
    });
    await testInfo.attach("python-reference", {
      path: referencePath,
      contentType: "application/json",
    });
  });
}

async function checkOverlay(preview: Locator, row: DetectionRow) {
  const rect = preview.locator("svg rect");
  await expect(rect).toBeVisible();
  const geometry = await preview.evaluate((container) => {
    const image = container.querySelector("img")!;
    return {
      width: container.clientWidth,
      height: container.clientHeight,
      naturalWidth: image.naturalWidth,
      naturalHeight: image.naturalHeight,
    };
  });
  expect(geometry.naturalWidth).toBe(row.image.width);
  expect(geometry.naturalHeight).toBe(row.image.height);
  const scale = Math.min(
    geometry.width / geometry.naturalWidth,
    geometry.height / geometry.naturalHeight,
  );
  const width = geometry.naturalWidth * scale;
  const height = geometry.naturalHeight * scale;
  const expected = {
    x: (geometry.width - width) / 2 + row.box.x * width,
    y: (geometry.height - height) / 2 + row.box.y * height,
    width: row.box.w * width,
    height: row.box.h * height,
  };
  for (const [key, value] of Object.entries(expected)) {
    expect(Number(await rect.getAttribute(key))).toBeCloseTo(value, 3);
  }
  await expect(preview).toContainText(row.label);
  await expect(preview).toContainText(row.confidence.toFixed(2));
}
