import { test } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";
import { saveHospitalizacion, verifyHospitalizacionPrefill } from "../../helpers/hospitalizacion.js";
import { openExpediente, saveExpedienteFase1, saveInpatientDailyNote } from "../../helpers/expediente.js";

test("expediente clínico único conserva continuidad con hospitalización y nota diaria", async ({ api, page, appEnv }) => {
  test.skip(!["full", "pilot_urologia"].includes(appEnv.bootProfile), "Expediente clínico único solo aplica a full y pilot_urologia.");
  const patient = buildTestPatient("EXP");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  const postqx = await submitPostQx(page, programada.postqxHref!, "AVILA");
  await page.goto(postqx.hospitalizacionHref!);
  await verifyHospitalizacionPrefill(page, { nombre: patient.nombre, nss: patient.nss, diagnostico: "SI" });
  const hosp = await saveHospitalizacion(page, { mode: "programado" });
  await openExpediente(page, consulta.consultaId);
  await saveExpedienteFase1(page, consulta.consultaId);
  await saveInpatientDailyNote(page, consulta.consultaId, hosp.hospitalizacionId);
});
