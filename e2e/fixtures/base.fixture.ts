import fs from "node:fs/promises";
import path from "node:path";

import { test as base, expect, type APIRequestContext } from "@playwright/test";

import { makeArtifactPath, sanitizeSlug, ensureDir } from "../helpers/artifacts.js";
import { buildApiContext, buildBrowserContextOptions } from "../helpers/auth.js";
import type { AppEnv } from "../helpers/env.js";
import { loadEnv } from "../helpers/env.js";
import { redactText } from "../helpers/redact.js";

interface Fixtures {
  appEnv: AppEnv;
  api: APIRequestContext;
  artifactDir: string;
}

export const test = base.extend<Fixtures>({
  appEnv: async ({}, use) => {
    await use(loadEnv());
  },

  artifactDir: async ({ appEnv }, use, testInfo) => {
    const dir = makeArtifactPath(
      appEnv.artifactsDir,
      "screenshots",
      sanitizeSlug(`${path.basename(testInfo.file)}-${testInfo.title}`)
    );
    await ensureDir(dir);
    await use(dir);
  },

  context: async ({ browser, appEnv }, use, testInfo) => {
    const harPath = makeArtifactPath(
      appEnv.artifactsDir,
      "har",
      `${sanitizeSlug(`${path.basename(testInfo.file)}-${testInfo.title}`)}.har`
    );
    await ensureDir(path.dirname(harPath));
    const context = await browser.newContext(buildBrowserContextOptions(appEnv, harPath));
    await use(context);
    await context.close();
  },

  page: async ({ context, artifactDir }, use, testInfo) => {
    const page = await context.newPage();
    const logs: string[] = [];
    page.on("console", (msg) => {
      logs.push(`[${msg.type().toUpperCase()}] ${msg.text()}`);
    });
    page.on("pageerror", (error) => {
      logs.push(`[PAGEERROR] ${error.message}`);
    });
    await use(page);
    const consolePath = path.join(artifactDir, "browser-console.log");
    await fs.writeFile(consolePath, redactText(logs.join("\n")), "utf8");
    if (testInfo.status !== testInfo.expectedStatus) {
      await page.screenshot({ path: path.join(artifactDir, "failure.png"), fullPage: true });
    }
  },

  api: async ({ playwright, appEnv }, use) => {
    const api = await buildApiContext(playwright, appEnv);
    await use(api);
    await api.dispose();
  }
});

export { expect };
