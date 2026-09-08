import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.VQL_WORKBENCH_E2E_BASE_URL;

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  reporter: "list",
  outputDir: "test-results",
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        channel:
          process.env.VQL_WORKBENCH_E2E_BROWSER === "chrome"
            ? "chrome"
            : undefined,
      },
    },
  ],
});
