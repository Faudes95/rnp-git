import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";
import { saveHospitalizacion, verifyHospitalizacionPrefill } from "../../helpers/hospitalizacion.js";

test("hospitalización programada reutiliza contexto de cirugía programada", async ({ api, page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Hospitalización programada solo aplica al perfil full.");
  const patient = buildTestPatient("HPROG");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  const postqx = await submitPostQx(page, programada.postqxHref!, "AVILA");
  await page.goto(postqx.hospitalizacionHref!);
  await verifyHospitalizacionPrefill(page, { nombre: patient.nombre, nss: patient.nss, diagnostico: "SI" });
  const saved = await saveHospitalizacion(page, { mode: "programado" });
  expect(saved.hospitalizacionId).toBeGreaterThan(0);
});
