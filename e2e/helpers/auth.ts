import type { APIRequestContext, BrowserContextOptions, Page } from "@playwright/test";

import type { AppEnv } from "./env.js";

export function buildBasicAuthHeader(env: AppEnv): string {
  const token = Buffer.from(`${env.authUser}:${env.authPass}`, "utf8").toString("base64");
  return `Basic ${token}`;
}

export function buildBrowserContextOptions(env: AppEnv, harPath?: string): BrowserContextOptions {
  return {
    baseURL: env.baseUrl,
    ignoreHTTPSErrors: true,
    httpCredentials: env.authEnabled
      ? {
          username: env.authUser,
          password: env.authPass
        }
      : undefined,
    recordHar: harPath
      ? {
          path: harPath,
          content: "attach",
          mode: "minimal"
        }
      : undefined
  };
}

interface RequestContextFactory {
  request: {
    newContext(options: {
      baseURL: string;
      ignoreHTTPSErrors: boolean;
      extraHTTPHeaders?: Record<string, string>;
    }): Promise<APIRequestContext>;
  };
}

export async function buildApiContext(
  playwright: RequestContextFactory,
  env: AppEnv,
): Promise<APIRequestContext> {
  return playwright.request.newContext({
    baseURL: env.baseUrl,
    ignoreHTTPSErrors: true,
    extraHTTPHeaders: env.authEnabled
      ? {
          Authorization: buildBasicAuthHeader(env)
        }
      : undefined
  });
}

export async function ensureAuthenticatedPage(page: Page): Promise<void> {
  const currentUrl = page.url();
  if (currentUrl.includes("/status")) {
    return;
  }
  const title = await page.title().catch(() => "");
  if (/Unauthorized|401/i.test(title)) {
    throw new Error("La página quedó en estado 401 aun con credenciales Basic.");
  }
}
