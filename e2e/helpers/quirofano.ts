import { expect, type Page } from "@playwright/test";

import type { TestPatient } from "./patients.js";
import { fillIfEmpty, getHrefByContains, listSelectOptions, selectFirstMeaningfulOption, selectOptionContaining } from "./ui.js";

export interface SurgicalSuccessLinks {
  expedienteHref?: string;
  postqxHref?: string;
  hospitalizacionHref?: string;
  altaHref?: string;
}

export interface UrgenciaResult extends SurgicalSuccessLinks {
  consultaId?: number;
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
  await selectFirstMeaningfulOption(page, "#procedimiento_programado", {
    forbiddenSubstrings: ["SUCCION"],
  });
  if (await page.locator('select[name="sistema_succion"]').count()) {
    await selectFirstMeaningfulOption(page, 'select[name="sistema_succion"]');
  }
  if (await page.locator('#abordaje').count()) {
    await selectFirstMeaningfulOption(page, "#abordaje");
  }
  if (await page.locator('#solicita_hemoderivados').count()) {
    await page.locator("#solicita_hemoderivados").selectOption("NO");
  }
  await page.getByRole("button", { name: /Guardar|Solicitar|Registrar/i }).first().click();
  await expect(page.locator('a[href*="/quirofano/urgencias/"][href*="/postquirurgica"]')).toBeVisible();
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
  await selectFirstMeaningfulOption(page, "#procedimiento_programado", {
    forbiddenSubstrings: ["SUCCION"],
  });
  if (await page.locator('select[name="sistema_succion"]').count()) {
    await selectFirstMeaningfulOption(page, 'select[name="sistema_succion"]');
  }
  if (await page.locator('#abordaje').count()) {
    await selectFirstMeaningfulOption(page, "#abordaje");
  }
  if (await page.locator('#solicita_hemoderivados').count()) {
    await page.locator("#solicita_hemoderivados").selectOption("NO");
  }
  await page.getByRole("button", { name: /Guardar|Programar|Registrar/i }).first().click();
  await expect(page.locator('a[href*="/quirofano/programada/"][href*="/postquirurgica"]')).toBeVisible();
  return {
    expedienteHref: (await page.locator('a[href*="/expediente?consulta_id="]').first().getAttribute("href")) ?? undefined,
    postqxHref: (await page.locator('a[href*="/quirofano/programada/"][href*="/postquirurgica"]').first().getAttribute("href")) ?? undefined,
  };
}

export async function addPacienteToWaitlist(page: Page, patient: TestPatient, consultaId: number): Promise<void> {
  await page.goto(`/quirofano/lista-espera/ingresar?consulta_id=${consultaId}`);
  await fillIfEmpty(page, 'input[name="nss"]', patient.nss);
  await fillIfEmpty(page, 'input[name="nombre"]', patient.nombre);
  await fillIfEmpty(page, 'input[name="edad"]', String(patient.edad));
  await page.locator('select[name="sexo"]').selectOption({ label: patient.sexo });
  await selectFirstMeaningfulOption(page, '#patologia, select[name="patologia"]', {
    forbiddenSubstrings: ["CANCER", "CALCULO", "TUMOR", "LITIASIS"],
  }).catch(async () => {
    await selectFirstMeaningfulOption(page, 'select[name="patologia"]', {
      forbiddenSubstrings: ["CANCER", "CALCULO", "TUMOR", "LITIASIS"],
    });
  });
  await selectFirstMeaningfulOption(page, 'select[name="procedimiento_programado"]', {
    forbiddenSubstrings: ["SUCCION"],
  });
  await fillIfEmpty(page, 'input[name="hgz"]', patient.hgz);
  await fillIfEmpty(page, 'input[name="agregado_medico"]', patient.agregadoMedico);
  await page.getByRole("button", { name: /Guardar|Agregar|Registrar/i }).first().click();
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
  if (await page.locator('select[name="stone_free"]').count()) {
    await page.locator('select[name="stone_free"]').selectOption("SI");
  }
  const residentDisplay = await selectResidentIfAvailable(page, residentNeedle);
  await page.getByRole("button", { name: /Guardar|Registrar/i }).first().click();
  await expect(page.getByText("Nota postquirúrgica guardada", { exact: false })).toBeVisible();
  return {
    residentDisplay,
    expedienteHref: (await page.locator('a[href*="/expediente"]').first().getAttribute("href")) ?? undefined,
    hospitalizacionHref: (await page.locator('a[href*="/hospitalizacion/nuevo"]').first().getAttribute("href").catch(() => null)) ?? undefined,
    altaHref: (await page.locator('a[href*="/hospitalizacion/alta"]').first().getAttribute("href").catch(() => null)) ?? undefined,
  };
}
