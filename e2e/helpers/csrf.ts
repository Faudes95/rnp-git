import type { APIRequestContext, Page } from "@playwright/test";

const TOKEN_REGEXES = [
  /name=["']csrf_token["']\s+value=["']([^"']+)["']/i,
  /const\s+CSRF_TOKEN\s*=\s*["']([^"']+)["']/i
];

export interface FormCsrfState {
  html: string;
  csrfToken: string;
}

export function extractCsrfTokenFromHtml(html: string): string {
  for (const regex of TOKEN_REGEXES) {
    const match = html.match(regex);
    if (match?.[1]) {
      return match[1];
    }
  }
  throw new Error("No se encontró csrf_token en el HTML del formulario.");
}

export async function fetchFormCsrf(api: APIRequestContext, routePath: string): Promise<FormCsrfState> {
  const response = await api.get(routePath);
  if (!response.ok()) {
    throw new Error(`No se pudo abrir formulario ${routePath}: ${response.status()}`);
  }
  const html = await response.text();
  return {
    html,
    csrfToken: extractCsrfTokenFromHtml(html)
  };
}

export async function readCsrfFromPage(page: Page): Promise<string> {
  const token = await page
    .locator('input[name="csrf_token"]')
    .first()
    .inputValue()
    .catch(() => "");
  if (token) {
    return token;
  }
  const html = await page.content();
  return extractCsrfTokenFromHtml(html);
}
