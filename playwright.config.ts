import { defineConfig, devices } from "@playwright/test";

import { loadEnv } from "./e2e/helpers/env.js";

const appEnv = loadEnv();

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: !!process.env["CI"],
  retries: process.env["CI"] ? 1 : 0,
  workers: process.env["CI"] ? 2 : undefined,
  timeout: 90_000,
  expect: {
    timeout: 15_000,
  },
  outputDir: "test-results",
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report", open: "never" }],
    ["junit", { outputFile: "artifacts/results.xml" }],
    ["json", { outputFile: "artifacts/results.json" }]
  ],
  use: {
    baseURL: appEnv.baseUrl,
    ignoreHTTPSErrors: true,
    trace: process.env["CI"] ? "on-first-retry" : "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    httpCredentials: appEnv.authEnabled
      ? {
          username: appEnv.authUser,
          password: appEnv.authPass
        }
      : undefined,
    extraHTTPHeaders: {
      Accept: "text/html,application/json;q=0.9,*/*;q=0.8"
    }
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"]
      }
    }
  ]
});
