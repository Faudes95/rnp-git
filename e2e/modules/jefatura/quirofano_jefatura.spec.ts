import path from "node:path";

import { test, expect } from "../../fixtures/base.fixture.js";
import { selectFirstMeaningfulOption } from "../../helpers/ui.js";

function fixturePdfPath(projectRoot: string, filename: string): string {
  return path.join(projectRoot, "e2e", "fixtures", "pdfs", filename);
}

function uniqueSuffix(): string {
  return `${Date.now()}`.slice(-6);
}

function lateUniqueTime(seed: string, offset = 0): string {
  const numeric = Number(seed.slice(-4) || "0") + offset * 37;
  const hour = 21 + (numeric % 3);
  const minute = (Math.floor(numeric / 3) % 12) * 5;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function uniqueOperationalDate(seed: string, offset = 0): string {
  const numeric = Number(seed.slice(-4) || "0") + offset * 17;
  const day = 10 + (numeric % 18);
  return `2026-04-${String(day).padStart(2, "0")}`;
}

test.describe("jefatura de quirófano profunda", () => {
  test("plantillas permite guardar catálogo y versionar capacidad", async ({ page }) => {
    await page.goto("/quirofano/jefatura/plantillas");
    await expect(page.getByRole("heading", { name: /Plantilla semanal de salas/i })).toBeVisible();

    const catalogCode = `E2E_QX_${uniqueSuffix()}`;
    const catalogForm = page.locator('form[action="/quirofano/jefatura/plantillas/catalogo"]');
    await catalogForm.locator('input[name="new_line_code"]').fill(catalogCode);
    await catalogForm.locator('input[name="new_line_name"]').fill(`Servicio ${catalogCode}`);
    await catalogForm.evaluate((form) => {
      (form as HTMLFormElement).requestSubmit();
    });
    await expect(page).toHaveURL(/catalog_saved=1/);
    await expect(page.locator(`input[value="Servicio ${catalogCode}"]`)).toBeVisible();

    const versionLabel = `Versión E2E ${uniqueSuffix()}`;
    const versionForm = page.locator('form[action="/quirofano/jefatura/plantillas"]');
    await versionForm.locator('input[name="version_label"]').fill(versionLabel);
    await versionForm.evaluate((form) => {
      (form as HTMLFormElement).requestSubmit();
    });
    await expect(page).toHaveURL(/saved=1/);
    await expect(page.locator("body")).toContainText(versionLabel);
  });

  test("importaciones procesa PDF con revisión y confirmación usando muestras versionadas", async ({ page, appEnv }) => {
    await page.goto("/quirofano/jefatura/importaciones");
    await expect(page.getByRole("heading", { name: /Importaciones PDF/i })).toBeVisible();

    await page.getByRole("button", { name: "Choose File" }).setInputFiles(fixturePdfPath(appEnv.projectRoot, "08-03-26.pdf"));
    await page.getByRole("button", { name: /Procesar PDF/i }).click();
    await expect(page).toHaveURL(/\/quirofano\/jefatura\/importaciones\/\d+/);
    await expect(page.getByText(/Archivo:\s*08-03-26\.pdf/i)).toBeVisible();
    await expect(page.getByRole("button", { name: /Guardar conciliación/i })).toBeVisible();
    await page.getByRole("button", { name: /Guardar conciliación/i }).click();
    await expect(page).toHaveURL(/review_saved=1/);

    await page.goto("/quirofano/jefatura/importaciones");
    await page.getByRole("button", { name: "Choose File" }).setInputFiles(fixturePdfPath(appEnv.projectRoot, "09-03-26.pdf"));
    await page.getByRole("button", { name: /Procesar PDF/i }).click();
    await expect(page).toHaveURL(/\/quirofano\/jefatura\/importaciones\/\d+/);
    await expect(page.getByText(/Archivo:\s*09-03-26\.pdf/i)).toBeVisible();
    await page.getByRole("button", { name: /Confirmar importación/i }).click();
    await expect(page).toHaveURL(/confirmed=1/);
  });

  test("programación diaria permite corregir distribución y capturar caso manual", async ({ page }) => {
    const suffix = uniqueSuffix();
    const targetDate = uniqueOperationalDate(suffix);
    const uniquePatient = `PACIENTE E2E QX ${suffix}`;
    const uniqueDiagnosis = `DIAGNOSTICO E2E ${suffix}`;

    await page.goto(`/quirofano/jefatura/programacion/${targetDate}`);
    await expect(page.getByRole("heading", { name: /Programación del/i })).toBeVisible();

    const firstNotesInput = page.locator('input[name^="notes__"]').first();
    await firstNotesInput.fill(`Nota operativa ${uniquePatient}`);
    await page.getByRole("button", { name: /Guardar distribución diaria/i }).click();
    await expect(page).toHaveURL(/blocks_saved=1/);
    await expect(firstNotesInput).toHaveValue(`Nota operativa ${uniquePatient}`);

    const caseForm = page.locator(`form[action="/quirofano/jefatura/programacion/${targetDate}/casos"]`);
    await selectFirstMeaningfulOption(page, `form[action="/quirofano/jefatura/programacion/${targetDate}/casos"] select[name="daily_block_id"]`, {
      forbiddenSubstrings: ["SELECCIONAR SALA"],
    });
    await caseForm.locator('select[name="status"]').selectOption("PROGRAMADA");
    await caseForm.locator('input[name="scheduled_time"]').fill(lateUniqueTime(suffix));
    await caseForm.locator('input[name="duracion_estimada_min"]').fill("20");
    await caseForm.locator('input[name="cama"]').fill("E2E-CAMA-QX");
    await caseForm.locator('input[name="patient_name"]').fill(uniquePatient);
    await caseForm.locator('input[name="nss"]').fill(`99${suffix}01`);
    await caseForm.locator('input[name="agregado_medico"]').fill("E2E-QX");
    await caseForm.locator('input[name="edad"]').fill("48");
    await caseForm.locator('input[name="tipo_anestesia"]').fill("Regional");
    await caseForm.locator('textarea[name="diagnostico_preoperatorio"]').fill(uniqueDiagnosis);
    await caseForm.locator('textarea[name="operacion_proyectada"]').fill("Procedimiento E2E de validación");
    await caseForm.locator('input[name="cirujano"]').fill("DR E2E QUIROFANO");
    await caseForm.locator('input[name="anestesiologo"]').fill("DRA E2E ANESTESIA");
    await caseForm.locator('input[name="enfermera_especialista"]').fill("ENF E2E UROMED");
    await caseForm.locator('textarea[name="notes"]').fill("Caso manual agregado por Playwright E2E.");
    await caseForm.getByRole("button", { name: /Guardar caso/i }).click();
    await expect(page).toHaveURL(/case_saved=1/);
    const caseRow = page.locator("tr", { hasText: uniquePatient }).first();
    await expect(caseRow).toBeVisible();
    await expect(caseRow).toContainText(uniqueDiagnosis);
  });

  test("detalle de caso permite agregar staff, evento e incidencia", async ({ page }) => {
    const suffix = uniqueSuffix();
    const targetDate = uniqueOperationalDate(suffix, 1);
    const uniquePatient = `PACIENTE DETALLE ${suffix}`;
    const staffName = `DR STAFF ${suffix}`;
    const eventNote = `Evento E2E ${suffix}`;
    const incidenceNote = `Incidencia E2E ${suffix}`;

    await page.goto(`/quirofano/jefatura/programacion/${targetDate}`);
    const caseForm = page.locator(`form[action="/quirofano/jefatura/programacion/${targetDate}/casos"]`);
    await selectFirstMeaningfulOption(page, `form[action="/quirofano/jefatura/programacion/${targetDate}/casos"] select[name="daily_block_id"]`, {
      forbiddenSubstrings: ["SELECCIONAR SALA"],
    });
    await caseForm.locator('select[name="status"]').selectOption("PROGRAMADA");
    await caseForm.locator('input[name="scheduled_time"]').fill(lateUniqueTime(suffix, 1));
    await caseForm.locator('input[name="duracion_estimada_min"]').fill("20");
    await caseForm.locator('input[name="cama"]').fill("E2E-CAMA-DET");
    await caseForm.locator('input[name="patient_name"]').fill(uniquePatient);
    await caseForm.locator('input[name="nss"]').fill(`88${suffix}02`);
    await caseForm.locator('input[name="agregado_medico"]').fill("E2E-QX-DET");
    await caseForm.locator('input[name="edad"]').fill("52");
    await caseForm.locator('input[name="tipo_anestesia"]').fill("General");
    await caseForm.locator('textarea[name="diagnostico_preoperatorio"]').fill("Diagnóstico detalle E2E");
    await caseForm.locator('textarea[name="operacion_proyectada"]').fill("Procedimiento detalle E2E");
    await caseForm.getByRole("button", { name: /Guardar caso/i }).click();
    await expect(page).toHaveURL(/case_saved=1/);

    const caseRow = page.locator("tr", { hasText: uniquePatient }).first();
    await expect(caseRow).toBeVisible();
    await caseRow.getByRole("link", { name: /Detalle/i }).click();
    await expect(page.getByRole("heading", { name: /Caso #/i })).toBeVisible();

    const staffForm = page.locator('form[action$="/staff"]');
    await staffForm.locator('input[name="staff_name"]').fill(staffName);
    await staffForm.locator('select[name="staff_role"]').selectOption("APOYO");
    await staffForm.locator('textarea[name="notes"]').fill("Staff adicional E2E");
    await staffForm.getByRole("button", { name: /Agregar personal/i }).click();
    await expect(page).toHaveURL(/staff_saved=1/);
    await expect(page.getByText(staffName, { exact: false })).toBeVisible();

    const nowLocal = new Date().toISOString().slice(0, 16);
    const eventsForm = page.locator('form[action$="/eventos"]');
    await eventsForm.locator('select[name="event_type"]').selectOption("INICIO_CIRUGIA");
    await eventsForm.locator('input[name="event_at"]').fill(nowLocal);
    await eventsForm.locator('textarea[name="notes"]').fill(eventNote);
    await eventsForm.getByRole("button", { name: /Registrar evento/i }).click();
    await expect(page).toHaveURL(/event_saved=1/);
    await expect(page.getByText(eventNote, { exact: false })).toBeVisible();

    const incidencesForm = page.locator('form[action$="/incidencias"]');
    await incidencesForm.locator('select[name="incidence_type"]').selectOption("RETRASO");
    await incidencesForm.locator('input[name="event_at"]').fill(nowLocal);
    await incidencesForm.locator('textarea[name="description"]').fill(incidenceNote);
    await incidencesForm.getByRole("button", { name: /Registrar incidencia/i }).click();
    await expect(page).toHaveURL(/incidence_saved=1/);
    await expect(page.getByText(incidenceNote, { exact: false })).toBeVisible();
  });
});
