import fs from "node:fs/promises";
import path from "node:path";

import { expect, type APIRequestContext, type Download, type Page } from "@playwright/test";

import { writeJsonFile } from "./artifacts.js";
import type { AppEnv } from "./env.js";
import { normalizeCensoRows, parseWorksheetRows } from "./xlsx.js";

export interface HospitalizacionSaveResult {
  hospitalizacionId: number;
  idempotent: boolean;
}

export async function verifyHospitalizacionPrefill(page: Page, expected: { nombre: string; nss: string; diagnostico?: string }): Promise<void> {
  await expect(page.locator('input[name="nombre_completo"]')).toHaveValue(expected.nombre);
  await expect(page.locator('input[name="nss"]')).toHaveValue(expected.nss);
  if (expected.diagnostico) {
    await expect(page.locator('input[name="diagnostico"]')).not.toHaveValue("");
  }
}

export async function saveHospitalizacion(
  page: Page,
  options: {
    mode: "urgencias" | "programado";
  },
): Promise<HospitalizacionSaveResult> {
  const { mode } = options;
  await page.locator('input[name="fecha_ingreso"]').fill(new Date().toISOString().slice(0, 10));
  await page.locator('input[name="cama"]').fill(mode === "urgencias" ? "C-URG-E2E" : "C-PROG-E2E");
  await page.locator('input[name="agregado_medico"]').fill("E2E UROMED");
  await page.locator('input[name="medico_a_cargo"]').fill("DR E2E UROMED");
  await page.locator('input[name="edad"]').fill("35");
  await page.locator('select[name="estatus_detalle"]').selectOption({ index: 1 });
  await page.locator('input[name="dias_hospitalizacion"]').fill("1");
  await page.locator('input[name="dias_postquirurgicos"]').fill("0");
  await page.locator('select[name="incapacidad"]').selectOption("NO").catch(async () => {});
  if (await page.locator('select[name="incapacidad_emitida"]').count()) {
    await page.locator('select[name="incapacidad_emitida"]').selectOption("NO").catch(async () => {});
  }
  if (await page.locator('select[name="programado"]').count()) {
    await page.locator('select[name="programado"]').selectOption(mode === "programado" ? "SI" : "NO").catch(async () => {});
  }
  if (await page.locator('select[name="urgencia"]').count()) {
    await page.locator('select[name="urgencia"]').selectOption(mode === "urgencias" ? "SI" : "NO").catch(async () => {});
  }
  if (await page.locator('select[name="uci"]').count()) {
    await page.locator('select[name="uci"]').selectOption("NO").catch(async () => {});
  }
  if (await page.locator('select[name="estatus"]').count()) {
    await page.locator('select[name="estatus"]').selectOption("ACTIVO").catch(async () => {});
  }
  await page.getByRole("button", { name: /Guardar ingreso|Guardar/i }).first().click();
  const heading = page.getByRole("heading").first();
  await expect(heading).toContainText(/Ingreso hospitalario/i);
  const altaHref = await page.locator('a[href*="/hospitalizacion/alta?hospitalizacion_id="]').first().getAttribute("href");
  if (!altaHref) {
    throw new Error("No se encontró enlace a alta hospitalaria tras guardar ingreso.");
  }
  const hospitalizacionId = Number(new URL(altaHref, "http://127.0.0.1").searchParams.get("hospitalizacion_id"));
  return {
    hospitalizacionId,
    idempotent: (await heading.textContent())?.toLowerCase().includes("idempotencia") ?? false,
  };
}

export async function precheckIngreso(api: APIRequestContext, consultaId: number): Promise<unknown> {
  const response = await api.get(`/api/hospitalizacion/precheck-ingreso?consulta_id=${consultaId}`);
  expect(response.ok()).toBeTruthy();
  return response.json();
}

export async function captureCensoUiRows(page: Page): Promise<string[]> {
  const rows = page.locator("table tbody tr");
  const count = await rows.count();
  const values: string[] = [];
  for (let index = 0; index < count; index += 1) {
    values.push(((await rows.nth(index).textContent()) ?? "").replace(/\s+/g, " ").trim());
  }
  return values.filter(Boolean);
}

export async function saveGuardia(page: Page): Promise<void> {
  await page.locator('input[name="guardia_r5"]').fill("AVILA CONTRERAS O.");
  await page.locator('input[name="guardia_r4"]').fill("ALVARADO BAÑOS F.");
  await page.locator('input[name="guardia_r3"]').fill("JAUREGUI DIAZ J.");
  await page.locator('input[name="guardia_r2"]').fill("BENITEZ ALDAY P.");
  await page.getByRole("button", { name: /\+ Guardar guardia/i }).click();
  await expect(page).toHaveURL(/saved=1|ok=1|fecha=/);
}

export async function saveCenso(page: Page): Promise<void> {
  await page.getByRole("button", { name: /Guardar cambios de censo/i }).click();
  await expect(page).toHaveURL(/saved=1|fecha=/);
}

async function readDownloadBuffer(download: Download): Promise<Buffer> {
  const filePath = await download.path();
  if (!filePath) {
    throw new Error("Playwright no entregó ruta de descarga para el censo.");
  }
  return fs.readFile(filePath);
}

export async function exportCensoRows(page: Page): Promise<Array<Record<string, string>>> {
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.locator('a[href*="/hospitalizacion/censo/imprimir"]').first().click(),
  ]);
  const buffer = await readDownloadBuffer(download);
  return normalizeCensoRows(parseWorksheetRows(buffer));
}

export async function writeCensoDiff(
  appEnv: AppEnv,
  payload: {
    uiRows: string[];
    exportedRows: Array<Record<string, string>>;
    expectedPatientNames: string[];
  },
): Promise<void> {
  const exportedText = payload.exportedRows.map((row) => Object.values(row).join(" ").toUpperCase());
  const missingFromExport = payload.expectedPatientNames.filter(
    (name) => !exportedText.some((row) => row.includes(name.toUpperCase())),
  );
  const missingFromUi = payload.expectedPatientNames.filter(
    (name) => !payload.uiRows.some((row) => row.toUpperCase().includes(name.toUpperCase())),
  );
  await writeJsonFile(path.join(appEnv.artifactsDir, "censo_print_diff.json"), {
    generated_at: new Date().toISOString(),
    ui_row_count: payload.uiRows.length,
    exported_row_count: payload.exportedRows.length,
    expected_patient_names: payload.expectedPatientNames,
    missing_from_ui: missingFromUi,
    missing_from_export: missingFromExport,
  });
}
