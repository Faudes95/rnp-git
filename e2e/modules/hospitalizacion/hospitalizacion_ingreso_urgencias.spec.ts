import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createUrgenciaSolicitud, submitPostQx } from "../../helpers/quirofano.js";
import { precheckIngreso, saveHospitalizacion, verifyHospitalizacionPrefill } from "../../helpers/hospitalizacion.js";

test("hospitalización desde contexto de urgencias guarda y detecta episodio activo", async ({ api, page, appEnv }) => {
  test.skip(!["full", "pilot_urologia"].includes(appEnv.bootProfile), "Hospitalización profunda solo aplica a full y pilot_urologia.");
  const patient = buildTestPatient("HURG");
  const consulta = await createConsultaViaMetadata(api, patient);
  const urgencia = await createUrgenciaSolicitud(page, patient);
  const postqx = await submitPostQx(page, urgencia.postqxHref!);
  await page.goto(postqx.hospitalizacionHref!);
  await verifyHospitalizacionPrefill(page, { nombre: patient.nombre, nss: patient.nss, diagnostico: "SI" });
  const saved = await saveHospitalizacion(page, { mode: "urgencias" });
  expect(saved.hospitalizacionId).toBeGreaterThan(0);
  const linkedConsultaId = urgencia.consultaId ?? consulta.consultaId;
  const precheck = (await precheckIngreso(api, linkedConsultaId)) as Record<string, unknown>;
  expect(precheck).toMatchObject({
    ok: true,
    has_active_episode: true,
  });
  expect(Number(precheck["active_count"] ?? 0)).toBeGreaterThan(0);
});
