import path from "node:path";

import { test, expect } from "../fixtures/base.fixture.js";
import { writeJsonFile, writeMarkdownFile } from "../helpers/artifacts.js";

function percentile(values: number[], fraction: number): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * fraction));
  return sorted[index] ?? 0;
}

test("mide p50/p95 de endpoints críticos", async ({ api, appEnv }) => {
  const endpoints =
    appEnv.bootProfile === "minimal_jefatura"
      ? ["/status", "/quirofano/jefatura", "/quirofano/jefatura/plantillas", "/api/quirofano/jefatura/dashboard"]
      : [
          "/status",
          "/",
          "/admin/database/status",
          "/api/forms/consulta_externa/schema",
          "/api/forms/hospitalizacion_ingreso/schema",
          "/api/quirofano/jefatura/dashboard",
          "/jefatura-urologia/central",
        ];

  const samples: Array<{ endpoint: string; ms: number }> = [];
  for (const endpoint of endpoints) {
    await test.step(`perf ${endpoint}`, async () => {
      const started = Date.now();
      const response = await api.get(endpoint);
      expect(response.status()).toBeLessThan(500);
      samples.push({ endpoint, ms: Date.now() - started });
    });
  }

  const concurrentEndpoint = endpoints[0] ?? "/status";
  const concurrentSamples = await Promise.all(
    Array.from({ length: appEnv.perfConcurrency }, async () => {
      const started = Date.now();
      const response = await api.get(concurrentEndpoint);
      expect(response.status()).toBeLessThan(500);
      return Date.now() - started;
    }),
  );

  const combined = [...samples.map((item) => item.ms), ...concurrentSamples];
  const summary = {
    profile: appEnv.bootProfile,
    samples,
    concurrent_endpoint: concurrentEndpoint,
    concurrent_count: appEnv.perfConcurrency,
    p50_ms: percentile(combined, 0.5),
    p95_ms: percentile(combined, 0.95),
    threshold_p50_ms: appEnv.perfP50Ms,
    threshold_p95_ms: appEnv.perfP95Ms,
  };

  await writeJsonFile(path.join(appEnv.artifactsDir, "perf_summary.json"), summary);
  await writeMarkdownFile(
    path.join(appEnv.docsDir, "perf_summary.md"),
    [
      "# Resumen de performance",
      "",
      `- Perfil: \`${summary.profile}\``,
      `- p50: ${summary.p50_ms} ms`,
      `- p95: ${summary.p95_ms} ms`,
      `- Threshold p50: ${summary.threshold_p50_ms} ms`,
      `- Threshold p95: ${summary.threshold_p95_ms} ms`,
      "",
    ].join("\n"),
  );

  expect(summary.p50_ms).toBeLessThanOrEqual(summary.threshold_p50_ms);
  expect(summary.p95_ms).toBeLessThanOrEqual(summary.threshold_p95_ms);
});
