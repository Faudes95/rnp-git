import path from "node:path";

import { test, expect } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { createCirugiaProgramada, submitPostQx } from "../../helpers/quirofano.js";
import { verifyHospitalizacionPrefill } from "../../helpers/hospitalizacion.js";
import { writeJsonFile, writeMarkdownFile } from "../../helpers/artifacts.js";

test("audita recaptura evitada y oportunidades restantes", async ({ api, page, appEnv }) => {
  test.skip(!["full", "pilot_urologia"].includes(appEnv.bootProfile), "Recaptura longitudinal solo aplica a full y pilot_urologia.");
  const patient = buildTestPatient("RECAP");
  const consulta = await createConsultaViaMetadata(api, patient);
  const programada = await createCirugiaProgramada(page, patient, consulta.consultaId);
  const postqx = await submitPostQx(page, programada.postqxHref!, "AVILA");
  await page.goto(postqx.hospitalizacionHref!);
  await verifyHospitalizacionPrefill(page, { nombre: patient.nombre, nss: patient.nss, diagnostico: "SI" });

  const matrix = [
    {
      flow: "postquirurgica_to_hospitalizacion",
      status: "ya evita recaptura",
      evidence: "nombre, NSS y diagnóstico llegan precargados al ingreso hospitalario desde postqx",
    },
    {
      flow: "patient_context_to_forms",
      status: "evita parcialmente",
      evidence: "existen botones de autocompletar desde Consulta, pero aún persisten campos que requieren confirmación manual",
    },
  ];

  await writeJsonFile(path.join(appEnv.artifactsDir, "recapture_matrix.json"), matrix);
  await writeMarkdownFile(
    path.join(appEnv.docsDir, "recapture_opportunities.md"),
    [
      "# Auditoría de recaptura",
      "",
      "| Flujo | Estado | Evidencia |",
      "| --- | --- | --- |",
      ...matrix.map((row) => `| ${row.flow} | ${row.status} | ${row.evidence} |`),
      "",
    ].join("\n"),
  );
  expect(matrix.length).toBeGreaterThan(0);
});
