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
    timeoutMs?: number;
  } = {},
): Promise<SelectOptionRow> {
  const found = await trySelectFirstMeaningfulOption(page, selector, options);
  if (!found) {
    throw new Error(`No se encontró una opción utilizable en ${selector}`);
  }
  return found;
}

export async function trySelectFirstMeaningfulOption(
  page: Page,
  selector: string,
  options: {
    forbiddenSubstrings?: string[];
    timeoutMs?: number;
  } = {},
): Promise<SelectOptionRow | undefined> {
  const { forbiddenSubstrings = [], timeoutMs = 1_500 } = options;
  await page.waitForSelector(selector, { state: "attached", timeout: timeoutMs }).catch(() => undefined);
  await page
    .waitForFunction(
      ({ targetSelector, rejected }) => {
        const node = document.querySelector(targetSelector);
        if (!(node instanceof HTMLSelectElement)) {
          return false;
        }
        return Array.from(node.options).some((option) => {
          const label = (option.textContent ?? "").trim().toUpperCase();
          return Boolean(option.value) && Boolean(label) && !rejected.some((item) => label.includes(item));
        });
      },
      {
        targetSelector: selector,
        rejected: forbiddenSubstrings.map((item) => item.toUpperCase()),
      },
      { timeout: timeoutMs },
    )
    .catch(() => undefined);

  const selectOptions = await listSelectOptions(page, selector);
  const found = selectOptions.find((row) => {
    if (!row.value || !row.label) {
      return false;
    }
    const upper = row.label.toUpperCase();
    return !forbiddenSubstrings.some((item) => upper.includes(item.toUpperCase()));
  });
  if (found) {
    await page.locator(selector).selectOption(found.value);
    return found;
  }
  return undefined;
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

export async function selectOptionContainingOrValue(
  page: Page,
  selector: string,
  needle: string,
  fallbackValues: string[],
): Promise<SelectOptionRow> {
  const options = await listSelectOptions(page, selector);
  const normalizedNeedle = needle.toUpperCase();
  const found =
    options.find((row) => row.label.toUpperCase().includes(normalizedNeedle) && row.value) ??
    options.find((row) => row.value && fallbackValues.includes(row.value));
  if (!found) {
    throw new Error(
      `No se encontró opción que contenga "${needle}" ni valor alterno (${fallbackValues.join(", ")}) en ${selector}`,
    );
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
