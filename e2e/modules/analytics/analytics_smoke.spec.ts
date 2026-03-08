import { test, expect } from "../../fixtures/base.fixture.js";

test("analytics y stats clave responden con payload válido", async ({ api, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Analytics amplio solo aplica al perfil full.");
  const stats = await api.get("/api/stats/expediente/completitud");
  expect(stats.ok()).toBeTruthy();
  const payload = (await stats.json()) as Record<string, unknown>;
  expect(Object.keys(payload).length).toBeGreaterThan(0);
});
