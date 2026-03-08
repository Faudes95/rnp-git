import { expect, type Page } from "@playwright/test";

export async function openExpediente(page: Page, consultaId: number): Promise<void> {
  await page.goto(`/expediente?consulta_id=${consultaId}`);
  await expect(page).toHaveURL(new RegExp(`/expediente\\?consulta_id=${consultaId}`));
  await expect(page.locator(".search-box")).toBeVisible();
  await expect(page.locator("body")).toContainText("Buscador Inteligente");
}

export async function saveExpedienteFase1(page: Page, consultaId: number): Promise<void> {
  await page.goto(`/expediente/fase1?consulta_id=${consultaId}`);
  await page.locator('select[name="consentimiento_investigacion"]').selectOption({ index: 1 });
  await page.locator('select[name="consentimiento_uso_datos"]').selectOption({ index: 1 });
  await page.locator('input[name="consentimiento_fecha"]').fill(new Date().toISOString().slice(0, 10));
  await page.locator('input[name="consentimiento_responsable"]').fill("E2E UROMED");
  await page.getByRole("button", { name: /Guardar Fase/i }).click();
  await expect(page.getByText("Módulo Fase 1 guardado", { exact: false })).toBeVisible();
}

export async function saveInpatientDailyNote(page: Page, consultaId: number, hospitalizacionId: number): Promise<void> {
  await page.goto(`/expediente/inpatient-captura?consulta_id=${consultaId}&hospitalizacion_id=${hospitalizacionId}&tab=nota`);
  await page.getByRole("button", { name: /Nota diaria/i }).click().catch(() => undefined);
  await expect(page.locator('#nota-diaria.tab-pane.active')).toBeVisible();
  await page.locator('#dailyNoteCaptureForm input[name="note_date"]').fill(new Date().toISOString().slice(0, 10));
  await page.locator('#dailyNoteCaptureForm input[name="author_user_id"]').fill("e2e_uromed");
  await page.locator('#dailyNoteCaptureForm textarea[name="free_text"]').fill("Nota diaria estructurada desde suite E2E.");
  await page.locator('#dailyNoteCaptureForm input[name="hr"]').fill("72");
  await page.locator('#dailyNoteCaptureForm input[name="sbp"]').fill("120");
  await page.locator('#dailyNoteCaptureForm input[name="dbp"]').fill("80");
  await page.locator('#dailyNoteCaptureForm input[name="temp"]').fill("36.5");
  await page.locator('#dailyNoteCaptureForm select[name="upsert"]').selectOption("1");
  await page.locator('#dailyNoteCaptureForm button[type="submit"][name="mode"][value="save"]').click();
  await expect(page).toHaveURL(/updated=daily_note_ok/);
}
