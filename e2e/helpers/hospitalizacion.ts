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

async function collectInvalidFields(page: Page): Promise<Array<{ id: string; name: string; message: string }>> {
  return await page.locator("input:invalid, select:invalid, textarea:invalid").evaluateAll((elements) =>
    elements.map((element) => {
      const field = element as HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;
      return {
        id: field.id || "",
        name: field.getAttribute("name") || "",
        message: field.validationMessage || "",
      };
    }),
  );
}

async function setSelectValue(page: Page, selector: string, value: string): Promise<void> {
  await page.waitForSelector(selector, { state: "attached" });
  await page.locator(selector).evaluate((node, nextValue) => {
    if (!(node instanceof HTMLSelectElement)) {
      throw new Error(`El selector ${node?.nodeName ?? "desconocido"} no apunta a un <select>.`);
    }
    node.value = String(nextValue);
    node.dispatchEvent(new Event("input", { bubbles: true }));
    node.dispatchEvent(new Event("change", { bubbles: true }));
  }, value);
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
  if (await page.locator('input[name="diagnostico"]').count()) {
    const diagnostico = (await page.locator('input[name="diagnostico"]').inputValue()).trim();
    if (!diagnostico) {
      await page.locator('input[name="diagnostico"]').fill("DIAGNOSTICO E2E UROMED");
    }
  }
  if (await page.locator('input[name="hgz_envio"]').count()) {
    const hgzEnvio = (await page.locator('input[name="hgz_envio"]').inputValue()).trim();
    if (!hgzEnvio) {
      await page.locator('input[name="hgz_envio"]').fill("HGZ E2E");
    }
  }
  await page.locator('input[name="agregado_medico"]').fill("E2E UROMED");
  await page.locator('input[name="medico_a_cargo"]').fill("DR E2E UROMED");
  await page.locator('input[name="edad"]').fill("35");
  if (await page.locator('select[name="sexo"]').count()) {
    const sexo = await page.locator('select[name="sexo"]').inputValue();
    if (!sexo) {
      await setSelectValue(page, 'select[name="sexo"]', "MASCULINO");
    }
  }
  await setSelectValue(page, 'select[name="estatus_detalle"]', "DELICADO");
  await page.locator('input[name="dias_hospitalizacion"]').fill("1");
  await page.locator('input[name="dias_postquirurgicos"]').fill("0");
  await setSelectValue(page, 'select[name="incapacidad"]', "NO");
  if (await page.locator('select[name="incapacidad_emitida"]').count()) {
    await setSelectValue(page, 'select[name="incapacidad_emitida"]', "NO");
  }
  await setSelectValue(page, 'select[name="programado"]', mode === "programado" ? "SI" : "NO");
  await setSelectValue(page, 'select[name="urgencia"]', mode === "urgencias" ? "SI" : "NO");
  if (await page.locator('select[name="uci"]').count()) {
    await setSelectValue(page, 'select[name="uci"]', "NO");
  }
  if (await page.locator('select[name="estatus"]').count()) {
    await setSelectValue(page, 'select[name="estatus"]', "ACTIVO");
  }
  await expect(page.locator('select[name="programado"]')).toHaveValue(mode === "programado" ? "SI" : "NO");
  await expect(page.locator('select[name="urgencia"]')).toHaveValue(mode === "urgencias" ? "SI" : "NO");
  if (mode === "programado") {
    await page.locator('input[name="medico_programado"]').fill("DR E2E PROGRAMADO");
  }
  if (mode === "urgencias") {
    await page.waitForSelector('select[name="urgencia_tipo"]', { state: "attached" });
    await page.locator('select[name="urgencia_tipo"]').selectOption({ index: 1 });
  }
  const submitButton = page.getByRole("button", { name: /Guardar ingreso hospitalario/i });
  await submitButton.scrollIntoViewIfNeeded();
  await submitButton.click();
  await page.waitForLoadState("domcontentloaded");
  const heading = page.getByRole("heading").first();
  const altaLink = page.locator('a[href*="/hospitalizacion/alta?hospitalizacion_id="]').first();
  let altaHref: string | null = null;
  try {
    await expect(altaLink).toBeVisible({ timeout: 8_000 });
    altaHref = await altaLink.getAttribute("href");
  } catch {
    const invalidFields = await collectInvalidFields(page);
    if (invalidFields.length > 0) {
      const details = invalidFields
        .map((field) => `${field.name || field.id || "<sin-id>"}: ${field.message || "campo inválido"}`)
        .join("; ");
      throw new Error(`El ingreso hospitalario no avanzó por validación HTML: ${details}. URL actual: ${page.url()}`);
    }
    const explicitError = (await page.locator(".alert.error, .error").first().textContent().catch(() => null))?.trim();
    if (explicitError) {
      throw new Error(`El backend rechazó el ingreso hospitalario: ${explicitError}. URL actual: ${page.url()}`);
    }
    const heading = (await page.getByRole("heading").first().textContent().catch(() => null))?.trim();
    throw new Error(
      `El ingreso hospitalario no mostró el enlace de alta esperado. Encabezado actual: ${heading || "N/D"}. URL actual: ${page.url()}`,
    );
  }
  if (!altaHref) {
    throw new Error("No se encontró enlace a alta hospitalaria tras guardar ingreso.");
  }
  const hospitalizacionId = Number(new URL(altaHref, "http://127.0.0.1").searchParams.get("hospitalizacion_id"));
  const headingText = (await heading.textContent().catch(() => null))?.toLowerCase() ?? "";
  return {
    hospitalizacionId,
    idempotent: headingText.includes("idempotencia"),
  };
}

export async function precheckIngreso(api: APIRequestContext, consultaId: number): Promise<unknown> {
  const response = await api.get(`/api/hospitalizacion/precheck-ingreso?consulta_id=${consultaId}`);
  expect(response.ok()).toBeTruthy();
  return response.json();
}

export async function captureCensoUiRows(page: Page): Promise<string[]> {
  const rows = page.locator('form[action="/hospitalizacion/censo/guardar"] table tr:has(input[name^="cama_"])');
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
  await expect(page.getByText(/Guardia registrada correctamente/i)).toBeVisible();
}

export async function saveCenso(page: Page): Promise<void> {
  await page.getByRole("button", { name: /Guardar cambios de censo/i }).click();
  await expect(page.getByText(/Cambios del censo guardados correctamente/i)).toBeVisible();
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
