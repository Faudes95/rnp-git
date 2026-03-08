import { test, expect } from "../../fixtures/base.fixture.js";
import { assertResidentLongitudinalMetrics, createCentralCase, createCentralExamAndAssign, createCentralIncidence, openResidentProfile, answerResidentExamFromProfile } from "../../helpers/urologia.js";

test("central crea examen, lo asigna y refleja calificación en perfil", async ({ page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Central solo aplica al perfil full.");
  await createCentralExamAndAssign(page, "AVILA");
  await answerResidentExamFromProfile(page, "R5U_AVILA_CONTRERAS_O");
  await openResidentProfile(page, "R5U_AVILA_CONTRERAS_O");
  await assertResidentLongitudinalMetrics(page);
  await expect(page.getByText("% · 1/1 aciertos", { exact: false }).first()).toBeVisible();
});

test("central refleja casos asociados e incidencias en perfil del residente", async ({ page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Central solo aplica al perfil full.");
  const patientSnapshot = "PACIENTE E2E CENTRAL";
  const incidenceDescription = "Incidencia E2E de conectividad docente";
  await createCentralCase(page, "AVILA", patientSnapshot);
  await createCentralIncidence(page, "AVILA", incidenceDescription);
  await openResidentProfile(page, "R5U_AVILA_CONTRERAS_O");
  await expect(page.getByText(patientSnapshot, { exact: false }).first()).toBeVisible();
  await expect(page.getByText(incidenceDescription, { exact: false }).first()).toBeVisible();
});
