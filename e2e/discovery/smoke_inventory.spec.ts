import { test, expect } from "../fixtures/base.fixture.js";
import { discoverEndpointMatrix } from "../helpers/discovery.js";
import { loadHttpContracts } from "../helpers/snapshots.js";

test("smoke total de inventario GET estático", async ({ api, appEnv }) => {
  const matrix = await discoverEndpointMatrix(api, appEnv);
  const contracts = new Map((await loadHttpContracts(appEnv.projectRoot)).map((row) => [row.path, row]));
  const candidates = matrix.filter(
    (row) =>
      row.method === "GET" &&
      !row.path.includes("{") &&
      (row.profile === "both" || row.profile === appEnv.bootProfile || appEnv.bootProfile === "full"),
  );

  for (const endpoint of candidates) {
    await test.step(`${endpoint.method} ${endpoint.path}`, async () => {
      const response = await api.get(endpoint.path);
      expect(response.status(), endpoint.path).toBeLessThan(500);
      const contract = contracts.get(endpoint.path);
      if (!contract) {
        return;
      }
      expect(response.status(), `Contrato HTTP ${endpoint.path}`).toBe(contract.status);
      if (contract.kind === "json") {
        const payload = (await response.json()) as Record<string, unknown>;
        for (const key of contract.keys ?? []) {
          expect(payload).toHaveProperty(key);
        }
      } else {
        const html = await response.text();
        for (const marker of contract.contains ?? []) {
          expect(html).toContain(marker);
        }
      }
    });
  }
});
