import { expect, type Page } from "@playwright/test";

import type { TestPatient } from "./patients.js";
import {
  fillIfEmpty,
  listSelectOptions,
  selectFirstMeaningfulOption,
  trySelectFirstMeaningfulOption,
} from "./ui.js";

export interface SurgicalSuccessLinks {
  expedienteHref?: string;
  postqxHref?: string;
  hospitalizacionHref?: string;
  altaHref?: string;
}

export interface UrgenciaResult extends SurgicalSuccessLinks {
  consultaId?: number;
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

async function selectStableProcedure(page: Page, selector: string): Promise<string> {
  const options = await listSelectOptions(page, selector);
  const candidateOptions = options.filter(
    (option) =>
      option.value &&
      !option.label.toUpperCase().includes("SELECCIONAR") &&
      !option.label.toUpperCase().includes("SUCCION"),
  );
  for (const option of candidateOptions) {
    await page.locator(selector).selectOption(option.value);
    await page.waitForTimeout(100);
    const abordajeVisible = await page.locator("#abordaje_wrap").isVisible().catch(() => false);
    const succionVisible = await page.locator("#sistema_succion_wrap").isVisible().catch(() => false);
    if (!abordajeVisible && !succionVisible) {
      return option.label;
    }
  }
  await selectFirstMeaningfulOption(page, selector, { forbiddenSubstrings: ["SUCCION"] });
  return page.locator(selector).inputValue();
}

async function assertSubmitReachedSuccess(page: Page, successSelector: string, contextLabel: string): Promise<void> {
  const success = page.locator(successSelector).first();
  try {
    await expect(success).toBeVisible({ timeout: 8_000 });
    return;
  } catch {
    const invalidFields = await collectInvalidFields(page);
    if (invalidFields.length > 0) {
      const details = invalidFields
        .map((field) => `${field.name || field.id || "<sin-id>"}: ${field.message || "campo inválido"}`)
        .join("; ");
      throw new Error(
        `${contextLabel} no avanzó porque el navegador mantiene campos inválidos: ${details}. URL actual: ${page.url()}`,
      );
    }
    const heading = (await page.locator("h1").first().textContent().catch(() => null))?.trim();
    if (heading) {
      throw new Error(`${contextLabel} devolvió una respuesta no exitosa: ${heading}. URL actual: ${page.url()}`);
    }
    throw new Error(
      `${contextLabel} no mostró el marcador de éxito esperado (${successSelector}). URL actual: ${page.url()}`,
    );
  }
}

async function assertPostQxSaved(page: Page): Promise<void> {
  const successSignals = page.locator(".msg.ok").first();
  try {
    await expect(successSignals).toBeVisible({ timeout: 15_000 });
    return;
  } catch {
    const invalidFields = await collectInvalidFields(page);
    if (invalidFields.length > 0) {
      const details = invalidFields
        .map((field) => `${field.name || field.id || "<sin-id>"}: ${field.message || "campo inválido"}`)
        .join("; ");
      throw new Error(`La nota postquirúrgica no avanzó por validación HTML: ${details}. URL actual: ${page.url()}`);
    }
    const explicitError = (await page.locator(".msg.err").first().textContent().catch(() => null))?.trim();
    if (explicitError) {
      throw new Error(`El backend rechazó la nota postquirúrgica: ${explicitError}. URL actual: ${page.url()}`);
    }
    throw new Error(`La nota postquirúrgica no mostró confirmación ni enlaces de continuación. URL actual: ${page.url()}`);
  }
}

async function findHrefMaybe(page: Page, selector: string): Promise<string | undefined> {
  const locator = page.locator(selector).first();
  const count = await locator.count();
  if (!count) {
    return undefined;
  }
  return (await locator.getAttribute("href")) ?? undefined;
}

export async function createUrgenciaSolicitud(page: Page, patient: TestPatient): Promise<UrgenciaResult> {
  await page.goto("/quirofano/urgencias/solicitud");
  await page.locator("#nss").fill(patient.nss);
  await page.locator("#agregado_medico").fill(patient.agregadoMedico);
  await page.locator("#nombre_completo").fill(patient.nombre);
  await page.locator("#edad").fill(String(patient.edad));
  await page.locator("#fecha_urgencia").fill(new Date().toISOString().slice(0, 10));
  await page.locator("#hgz").fill(patient.hgz);
  await page.locator("#sexo").selectOption({ label: patient.sexo });
  await selectFirstMeaningfulOption(page, "#patologia", {
    forbiddenSubstrings: ["CANCER", "CALCULO", "TUMOR", "LITIASIS"],
  });
  await selectStableProcedure(page, "#procedimiento_programado");
  if (await page.locator('select[name="sistema_succion"]').count()) {
    await trySelectFirstMeaningfulOption(page, 'select[name="sistema_succion"]', { timeoutMs: 1_500 });
  }
  if (await page.locator('#abordaje').count()) {
    await trySelectFirstMeaningfulOption(page, "#abordaje", { timeoutMs: 5_000 });
  }
  if (await page.locator('.insumo-check').count()) {
    await page.locator('.insumo-check').first().check();
  }
  if (await page.locator('#solicita_hemoderivados').count()) {
    await page.locator("#solicita_hemoderivados").selectOption("NO");
  }
  const submitButton = page.getByRole("button", {
    name: /Ingresar a lista de pacientes programados por cirugía de urgencia|Ingresar a lista de pacientes programados/i,
  });
  await submitButton.first().click();
  await assertSubmitReachedSuccess(
    page,
    'a[href*="/quirofano/urgencias/"][href*="/postquirurgica"]',
    "La solicitud de cirugía de urgencia",
  );
  const expedienteHref = await page.locator('a[href*="/expediente"]').first().getAttribute("href");
  const postqxHref = await page.locator('a[href*="/quirofano/urgencias/"][href*="/postquirurgica"]').first().getAttribute("href");
  const consultaHref = expedienteHref ?? "";
  const consultaId = consultaHref.includes("consulta_id=")
    ? Number(new URL(consultaHref, "http://127.0.0.1").searchParams.get("consulta_id"))
    : undefined;
  return {
    expedienteHref: expedienteHref ?? undefined,
    postqxHref: postqxHref ?? undefined,
    consultaId: Number.isInteger(consultaId) && Number(consultaId) > 0 ? Number(consultaId) : undefined,
  };
}

export async function createCirugiaProgramada(
  page: Page,
  patient: TestPatient,
  consultaId: number,
): Promise<SurgicalSuccessLinks> {
  await page.goto("/quirofano/nuevo");
  await page.locator("#consulta_id").fill(String(consultaId));
  await page.locator("#nss").fill(patient.nss);
  await page.locator("#agregado_medico").fill(patient.agregadoMedico);
  await page.locator("#nombre_completo").fill(patient.nombre);
  await page.locator("#edad").fill(String(patient.edad));
  await page.locator("#fecha_programada").fill(new Date().toISOString().slice(0, 10));
  await page.locator("#hgz").fill(patient.hgz);
  await page.locator("#sexo").selectOption({ label: patient.sexo });
  await selectFirstMeaningfulOption(page, "#patologia", {
    forbiddenSubstrings: ["CANCER", "CALCULO", "TUMOR", "LITIASIS"],
  });
  await selectStableProcedure(page, "#procedimiento_programado");
  if (await page.locator('select[name="sistema_succion"]').count()) {
    await trySelectFirstMeaningfulOption(page, 'select[name="sistema_succion"]', { timeoutMs: 1_500 });
  }
  if (await page.locator('#abordaje').count()) {
    await trySelectFirstMeaningfulOption(page, "#abordaje", { timeoutMs: 5_000 });
  }
  if (await page.locator('.insumo-check').count()) {
    await page.locator('.insumo-check').first().check();
  }
  if (await page.locator('#solicita_hemoderivados').count()) {
    await page.locator("#solicita_hemoderivados").selectOption("NO");
  }
  const submitButton = page.getByRole("button", { name: /Ingresar a lista de pacientes programados/i });
  await submitButton.first().click();
  await assertSubmitReachedSuccess(
    page,
    'a[href*="/quirofano/programada/"][href*="/postquirurgica"]',
    "La cirugía programada",
  );
  return {
    expedienteHref: (await page.locator('a[href*="/expediente?consulta_id="]').first().getAttribute("href")) ?? undefined,
    postqxHref: (await page.locator('a[href*="/quirofano/programada/"][href*="/postquirurgica"]').first().getAttribute("href")) ?? undefined,
  };
}

export async function addPacienteToWaitlist(page: Page, patient: TestPatient, consultaId: number): Promise<void> {
  const response = await page.goto(`/quirofano/lista-espera/ingresar?consulta_id=${consultaId}`);
  if (response?.status() === 404) {
    throw new Error("La ruta /quirofano/lista-espera/ingresar devuelve 404 en el perfil full; el flujo de lista de espera no está montado.");
  }
  await fillIfEmpty(page, 'input[name="nss"]', patient.nss);
  await fillIfEmpty(page, 'input[name="nombre"]', patient.nombre);
  await fillIfEmpty(page, 'input[name="edad"]', String(patient.edad));
  await page.locator('select[name="sexo"]').selectOption({ label: patient.sexo });
  await page.locator('select[name="patologia"]').selectOption("CRECIMIENTO PROSTATICO OBSTRUCTIVO");
  await page.locator('select[name="procedimiento_programado"]').selectOption("RESECCION TRANSURETRAL DE VEJIGA");
  await fillIfEmpty(page, 'input[name="hgz"]', patient.hgz);
  await fillIfEmpty(page, 'input[name="agregado_medico"]', patient.agregadoMedico);
  await page.getByRole("button", { name: /Ingresar al paciente a la lista de espera/i }).click();
  await expect(page).toHaveURL(/saved=/);
}

export interface PostQxResult extends SurgicalSuccessLinks {
  residentDisplay?: string;
}

async function selectResidentIfAvailable(page: Page, residentNeedle: string): Promise<string | undefined> {
  const residentSelects = page.locator('select[name*="[residente]"]');
  const count = await residentSelects.count();
  for (let index = 0; index < count; index += 1) {
    const select = residentSelects.nth(index);
    const name = await select.getAttribute("name");
    const options = await listSelectOptions(page, `select[name="${name}"]`);
    const found = options.find((row) => row.label.toUpperCase().includes(residentNeedle.toUpperCase()) && row.value);
    if (!found || !name) {
      continue;
    }
    await select.selectOption(found.value);
    const roleName = name.replace("[residente]", "[rol]");
    const participationName = name.replace("[residente]", "[participacion]");
    await page.locator(`select[name="${roleName}"]`).selectOption("1ER_AYUDANTE");
    await page.locator(`select[name="${participationName}"]`).selectOption("MAYORIA");
    return found.label;
  }
  return undefined;
}

export async function submitPostQx(
  page: Page,
  routePath: string,
  residentNeedle = "AVILA",
): Promise<PostQxResult> {
  await page.goto(routePath);
  if (await page.locator('select[name="surgical_programacion_id"]').count()) {
    const current = await page.locator('select[name="surgical_programacion_id"]').inputValue();
    if (!current) {
      await selectFirstMeaningfulOption(page, 'select[name="surgical_programacion_id"]');
    }
  }
  await page.locator('input[name="fecha_realizacion"]').fill(new Date().toISOString().slice(0, 10));
  await fillIfEmpty(page, 'input[name="cirujano"]', "E2E UROMED");
  await page.locator('select[name="tipo_abordaje"]').selectOption("ENDOSCOPICO").catch(async () => {
    await selectFirstMeaningfulOption(page, 'select[name="tipo_abordaje"]');
  });
  await page.locator('input[name="sangrado_ml"]').fill("120");
  await fillIfEmpty(page, 'input[name="diagnostico_postop"]', "DIAGNOSTICO E2E");
  await fillIfEmpty(page, 'input[name="procedimiento_realizado"]', "PROCEDIMIENTO E2E");
  await page.locator('textarea[name="nota_postquirurgica"]').fill("Nota E2E postquirúrgica con validación longitudinal.");
  await page.locator('input[name="tiempo_quirurgico_min"]').fill("95");
  if ((await page.locator('select[name="stone_free"]').count()) && (await page.locator('select[name="stone_free"]').isVisible().catch(() => false))) {
    await page.locator('select[name="stone_free"]').selectOption("SI");
  }
  const residentDisplay = await selectResidentIfAvailable(page, residentNeedle);
  await page.getByRole("button", { name: /Guardar nota postquirúrgica/i }).click();
  await assertPostQxSaved(page);
  return {
    residentDisplay,
    expedienteHref:
      (await findHrefMaybe(page, '.actions a.btn-gold[href*="/expediente?consulta_id="]')) ??
      (await findHrefMaybe(page, '.actions a.btn-gold[href*="/expediente?nss="]')),
    hospitalizacionHref: await findHrefMaybe(page, '.actions a.btn-gold[href*="/hospitalizacion/nuevo?"]'),
    altaHref: await findHrefMaybe(page, '.actions a.btn-gold[href*="/hospitalizacion/alta?"]'),
  };
}
