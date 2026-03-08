import { expect, type Page } from "@playwright/test";

import { uniqueSuffix } from "./ids.js";
import { selectOptionContaining } from "./ui.js";

export async function openResidentProfile(page: Page, residentCode: string): Promise<void> {
  await page.goto(`/jefatura-urologia/programa-academico/residentes/${residentCode}`);
  await expect(page.getByText("Evaluaciones semestrales", { exact: false })).toBeVisible();
}

export async function assertResidentLongitudinalMetrics(page: Page): Promise<void> {
  await expect(page.getByText("Ponderación por rol", { exact: false })).toBeVisible();
  await expect(page.getByText("Ponderación por participación", { exact: false })).toBeVisible();
  await expect(page.getByText("Actividad 30 días", { exact: false })).toBeVisible();
  await expect(page.getByText("Actividad 90 días", { exact: false })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Última cirugía indexada" })).toBeVisible();
}

export async function createCentralExamAndAssign(page: Page, residentNeedle = "AVILA"): Promise<string> {
  const suffix = uniqueSuffix("exam");
  await page.goto("/jefatura-urologia/central/examenes/nuevo");
  const createForm = page.locator('form[action="/jefatura-urologia/central/examenes"]');
  await createForm.locator('input[name="title"]').fill(`Evaluación E2E ${suffix}`);
  await createForm.locator('input[name="period_label"]').fill(`E2E-${suffix.slice(-4)}`);
  await createForm.locator('textarea[name="description"]').fill("Examen E2E longitudinal para validación automática.");
  await createForm.locator('textarea[name="question_prompt[]"]').first().fill("¿Cuál opción fue marcada como correcta en el examen E2E?");
  await createForm.locator('input[name="question_option_a[]"]').first().fill("Opción A correcta");
  await createForm.locator('input[name="question_option_b[]"]').first().fill("Opción B");
  await createForm.locator('input[name="question_option_c[]"]').first().fill("Opción C");
  await createForm.locator('input[name="question_option_d[]"]').first().fill("Opción D");
  await createForm.getByRole("button", { name: /Guardar examen/i }).click();
  await expect(page).toHaveURL(/\/jefatura-urologia\/central\/examenes\/\d+\/asignar/);
  const assignForm = page.locator(`form[action$="/asignar"]`);
  await selectOptionContaining(page, 'form[action$="/asignar"] select[name="resident_code"]', residentNeedle);
  await assignForm.locator('input[name="disponible_desde"]').fill(new Date().toISOString().slice(0, 10));
  await assignForm.locator('input[name="cierra_en"]').fill(new Date(Date.now() + 7 * 86_400_000).toISOString().slice(0, 10));
  await assignForm.getByRole("button", { name: /Guardar asignación/i }).click();
  await expect(page).toHaveURL(/saved=1/);
  return page.url();
}

export async function answerResidentExamFromProfile(page: Page, residentCode: string): Promise<void> {
  await openResidentProfile(page, residentCode);
  await page.locator("a", { hasText: "Realizar examen" }).first().click();
  await expect(page.getByText("Realizar examen", { exact: false })).toBeVisible();
  await page.locator('input[type="radio"]').first().check();
  await page.getByRole("button", { name: /Revisar y enviar examen/i }).click();
  await page.getByRole("button", { name: "Enviar examen", exact: true }).click();
  await expect(page.getByText("%", { exact: false }).first()).toBeVisible();
}

export async function createCentralCase(page: Page, residentNeedle: string, patientSnapshot: string): Promise<void> {
  await page.goto("/jefatura-urologia/central/casos");
  const createForm = page.locator('form[action="/jefatura-urologia/central/casos"]');
  await selectOptionContaining(page, 'form[action="/jefatura-urologia/central/casos"] select[name="resident_code"]', residentNeedle);
  await createForm.locator('input[name="fecha_limite"]').fill(new Date(Date.now() + 3 * 86_400_000).toISOString().slice(0, 10));
  await createForm.locator('input[name="patient_snapshot"]').fill(patientSnapshot);
  await createForm.locator('textarea[name="objetivo"]').fill("Valorar evolución clínica y plan de seguimiento E2E.");
  await createForm.locator('textarea[name="notas"]').fill("Caso generado por suite E2E.");
  await createForm.getByRole("button", { name: /Guardar caso asociado/i }).click();
  await expect(page).toHaveURL(/saved=1/);
}

export async function createCentralIncidence(page: Page, residentNeedle: string, description: string): Promise<void> {
  await page.goto("/jefatura-urologia/central/incidencias");
  const createForm = page.locator('form[action="/jefatura-urologia/central/incidencias"]');
  await selectOptionContaining(page, 'form[action="/jefatura-urologia/central/incidencias"] select[name="resident_code"]', residentNeedle);
  await createForm.locator('input[name="fecha_evento"]').fill(new Date().toISOString().slice(0, 10));
  await createForm.locator('input[name="tipo"]').fill("Evento operativo E2E");
  await createForm.locator('textarea[name="descripcion"]').fill(description);
  await createForm.locator('textarea[name="resolucion"]').fill("Pendiente de revisión.");
  await createForm.getByRole("button", { name: /Guardar incidencia/i }).click();
  await expect(page).toHaveURL(/saved=1/);
}
