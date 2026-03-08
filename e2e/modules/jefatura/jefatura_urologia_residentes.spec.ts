import { test, expect } from "../../fixtures/base.fixture.js";
import { openResidentProfile, assertResidentLongitudinalMetrics } from "../../helpers/urologia.js";

test("perfil de residente muestra secciones longitudinales y académicas", async ({ page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Perfiles de residentes solo aplican al perfil full.");
  await openResidentProfile(page, "R5U_AVILA_CONTRERAS_O");
  await assertResidentLongitudinalMetrics(page);
  await expect(page.getByText("Casos asociados", { exact: false })).toBeVisible();
  await expect(page.getByText("Incidencias", { exact: false })).toBeVisible();
  await expect(page.getByText("Evaluaciones semestrales", { exact: false })).toBeVisible();
});
