import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createUrgenciaSolicitud, submitPostQx } from "../../helpers/quirofano.js";

test("quirofano urgencias -> postquirúrgica -> hospitalización prefill", async ({ api, page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Urgencias solo aplica al perfil full.");
  const patient = buildTestPatient("URG");
  await createConsultaViaMetadata(api, patient);
  const urgencia = await createUrgenciaSolicitud(page, patient);
  expect(urgencia.postqxHref).toBeTruthy();
  const postqx = await submitPostQx(page, urgencia.postqxHref!);
  expect(postqx.hospitalizacionHref).toBeTruthy();
  await page.goto(postqx.hospitalizacionHref!);
  await expect(page.locator('input[name="nombre_completo"]')).toHaveValue(patient.nombre);
  await expect(page.locator('input[name="nss"]')).toHaveValue(patient.nss);
});
