import { test, expect } from "../../fixtures/base.fixture.js";

test("FHIR metadata o expediente FHIR responden", async ({ api, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "FHIR solo aplica al perfil full.");
  const candidates = ["/fhir/metadata", "/fhir/expediente?consulta_id=1"];
  let ok = false;
  for (const candidate of candidates) {
    const response = await api.get(candidate);
    if (response.ok()) {
      ok = true;
      break;
    }
  }
  expect(ok).toBeTruthy();
});
