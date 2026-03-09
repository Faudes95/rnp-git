import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";
import { assertResidentLongitudinalMetrics, openResidentProfile } from "../../helpers/urologia.js";

test("nota postquirúrgica indexa actividad en perfil de residente", async ({ api, page, appEnv }) => {
  test.skip(!["full", "jefatura_urologia", "residentes_urologia", "pilot_urologia"].includes(appEnv.bootProfile), "Perfiles de residentes solo aplican a full, jefatura_urologia, residentes_urologia y pilot_urologia.");
  const patient = buildTestPatient("RESI");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  await submitPostQx(page, programada.postqxHref!, "AVILA");
  await openResidentProfile(page, "R5U_AVILA_CONTRERAS_O");
  await assertResidentLongitudinalMetrics(page);
  await expect(page.getByText("Analítica de sangrado", { exact: false })).toBeVisible();
});
