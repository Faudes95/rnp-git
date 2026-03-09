import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";
import { captureCensoUiRows, exportCensoRows, saveCenso, saveGuardia, saveHospitalizacion, verifyHospitalizacionPrefill, writeCensoDiff } from "../../helpers/hospitalizacion.js";

test("guardia y censo exportado permanecen alineados con la UI del censo", async ({ api, page, appEnv }) => {
  test.skip(!["full", "pilot_urologia"].includes(appEnv.bootProfile), "Censo solo aplica a full y pilot_urologia.");
  const patient = buildTestPatient("CENSO");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  const postqx = await submitPostQx(page, programada.postqxHref!, "AVILA");
  await page.goto(postqx.hospitalizacionHref!);
  await verifyHospitalizacionPrefill(page, { nombre: patient.nombre, nss: patient.nss, diagnostico: "SI" });
  await saveHospitalizacion(page, { mode: "programado" });

  const targetDate = new Date().toISOString().slice(0, 10);
  await page.goto(`/hospitalizacion/censo?fecha=${targetDate}`);
  await saveGuardia(page);
  await saveCenso(page);
  const uiRows = await captureCensoUiRows(page);
  const exportedRows = await exportCensoRows(page);
  await writeCensoDiff(appEnv, {
    uiRows,
    exportedRows,
    expectedPatientNames: [patient.nombre],
  });
  expect(uiRows.some((row) => row.toUpperCase().includes(patient.nombre.toUpperCase()))).toBeTruthy();
  expect(exportedRows.some((row) => Object.values(row).join(" ").toUpperCase().includes(patient.nombre.toUpperCase()))).toBeTruthy();
});
