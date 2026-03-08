import { expect, type Locator, type Page } from "@playwright/test";

export interface SelectOptionRow {
  value: string;
  label: string;
}

export async function listSelectOptions(page: Page, selector: string): Promise<SelectOptionRow[]> {
  const options = page.locator(`${selector} option`);
  const count = await options.count();
  const rows: SelectOptionRow[] = [];
  for (let index = 0; index < count; index += 1) {
    const option = options.nth(index);
    rows.push({
      value: (await option.getAttribute("value")) ?? "",
      label: (await option.textContent())?.trim() ?? "",
    });
  }
  return rows;
}

export async function selectFirstMeaningfulOption(
  page: Page,
  selector: string,
  options: {
    forbiddenSubstrings?: string[];
  } = {},
): Promise<SelectOptionRow> {
  const { forbiddenSubstrings = [] } = options;
  const selectOptions = await listSelectOptions(page, selector);
  const found = selectOptions.find((row) => {
    if (!row.value || !row.label) {
      return false;
    }
    const upper = row.label.toUpperCase();
    return !forbiddenSubstrings.some((item) => upper.includes(item.toUpperCase()));
  });
  if (!found) {
    throw new Error(`No se encontró una opción utilizable en ${selector}`);
  }
  await page.locator(selector).selectOption(found.value);
  return found;
}

export async function selectOptionContaining(
  page: Page,
  selector: string,
  needle: string,
): Promise<SelectOptionRow> {
  const options = await listSelectOptions(page, selector);
  const found = options.find((row) => row.label.toUpperCase().includes(needle.toUpperCase()) && row.value);
  if (!found) {
    throw new Error(`No se encontró opción que contenga "${needle}" en ${selector}`);
  }
  await page.locator(selector).selectOption(found.value);
  return found;
}

export async function fillIfEmpty(page: Page, selector: string, value: string): Promise<void> {
  const locator = page.locator(selector);
  const current = await locator.inputValue().catch(() => "");
  if (!current.trim()) {
    await locator.fill(value);
  }
}

export async function getHrefByContains(page: Page, fragment: string): Promise<string> {
  const locator = page.locator(`a[href*="${fragment}"]`).first();
  await expect(locator).toBeVisible();
  const href = await locator.getAttribute("href");
  if (!href) {
    throw new Error(`No se encontró href para fragmento ${fragment}`);
  }
  return href;
}

export function extractQueryInt(href: string, key: string): number {
  const parsed = new URL(href, "http://127.0.0.1");
  const raw = parsed.searchParams.get(key);
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`No se pudo extraer entero válido para ${key} desde ${href}`);
  }
  return value;
}

export async function expectVisibleText(page: Page, text: string): Promise<void> {
  await expect(page.getByText(text, { exact: false })).toBeVisible();
}

export async function openDetails(locator: Locator): Promise<void> {
  const open = await locator.getAttribute("open");
  if (open == null) {
    await locator.locator("summary").click();
  }
}
