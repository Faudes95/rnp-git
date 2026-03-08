import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";

test("consulta metadata crea consulta reutilizable para flujos cruzados", async ({ api, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Consulta metadata solo aplica al perfil full.");
  const patient = buildTestPatient("CONSULTA");
  const seed = await createConsultaViaMetadata(api, patient);
  expect(seed.consultaId).toBeGreaterThan(0);
  const expediente = await api.get(`/expediente?consulta_id=${seed.consultaId}`);
  expect(expediente.ok()).toBeTruthy();
  const html = await expediente.text();
  expect(html).toContain(patient.nss);
});
