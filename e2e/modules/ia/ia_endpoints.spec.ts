import { test, expect } from "../../fixtures/base.fixture.js";

test("IA expone al menos un status operativo", async ({ api, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "IA solo aplica al perfil full.");
  const candidates = [
    "/api/ai/fau-bot/status",
    "/api/ai/fau-bot-core/status",
    "/api/ai/quirofano/alertas",
  ];
  let ok = false;
  for (const candidate of candidates) {
    const response = await api.get(candidate);
    if (response.ok()) {
      ok = true;
      const payload = await response.json().catch(() => ({}));
      expect(typeof payload).toBe("object");
      break;
    }
  }
  expect(ok).toBeTruthy();
});
