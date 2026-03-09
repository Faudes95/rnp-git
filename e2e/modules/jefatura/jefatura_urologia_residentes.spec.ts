import { test, expect } from "../../fixtures/base.fixture.js";
import { openResidentProfile, assertResidentLongitudinalMetrics } from "../../helpers/urologia.js";

test("perfil de residente muestra secciones longitudinales y académicas", async ({ page, appEnv }) => {
  test.skip(!["full", "jefatura_urologia", "residentes_urologia", "pilot_urologia"].includes(appEnv.bootProfile), "Perfiles de residentes solo aplican a full, jefatura_urologia, residentes_urologia y pilot_urologia.");
  await openResidentProfile(page, "R5U_AVILA_CONTRERAS_O");
  await assertResidentLongitudinalMetrics(page);
  await expect(page.getByRole("heading", { name: "Casos asociados" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Incidencias" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Evaluaciones semestrales" })).toBeVisible();
});
