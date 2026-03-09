import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";

test("cirugía programada -> postqx -> enlaces clínicos longitudinales", async ({ api, page, appEnv }) => {
  test.skip(!["full", "quirofano", "pilot_urologia"].includes(appEnv.bootProfile), "Quirófano programado solo aplica a full, quirofano y pilot_urologia.");
  const patient = buildTestPatient("PROG");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  expect(programada.postqxHref).toBeTruthy();
  const postqx = await submitPostQx(page, programada.postqxHref!, "AVILA");
  expect(postqx.expedienteHref).toContain(`/expediente?consulta_id=${consulta.consultaId}`);
});
