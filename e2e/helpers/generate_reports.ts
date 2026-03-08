import fs from "node:fs/promises";
import path from "node:path";

import { loadEnv } from "./env.js";
import { writeMarkdownFile } from "./artifacts.js";

interface PlaywrightJsonReport {
  stats?: {
    expected?: number;
    unexpected?: number;
    skipped?: number;
  };
}

async function main(): Promise<void> {
  const env = loadEnv();
  const resultsPath = path.join(env.artifactsDir, "results.json");
  let results: PlaywrightJsonReport = {};
  try {
    results = JSON.parse(await fs.readFile(resultsPath, "utf8")) as PlaywrightJsonReport;
  } catch {
    // Reporte aún no generado; se escriben plantillas mínimas.
  }

  const expected = results.stats?.expected ?? 0;
  const unexpected = results.stats?.unexpected ?? 0;
  const skipped = results.stats?.skipped ?? 0;
  const passed = Math.max(expected - unexpected - skipped, 0);

  await writeMarkdownFile(
    path.join(env.docsDir, "executive_summary.md"),
    [
      "# Resumen Ejecutivo E2E",
      "",
      `- Perfil evaluado: \`${env.bootProfile}\``,
      `- Total esperado: ${expected}`,
      `- Passed: ${passed}`,
      `- Failed: ${unexpected}`,
      `- Skipped: ${skipped}`,
      "",
      "## Hallazgos críticos",
      "",
      "- Completar después de la primera corrida de la suite. Los tests de seguridad, censo y recaptura anexarán hallazgos operativos aquí.",
      ""
    ].join("\n")
  );

  await writeMarkdownFile(
    path.join(env.docsDir, "mitigations_checklist.md"),
    [
      "# Checklist de mitigaciones",
      "",
      "- [ ] Revisar credenciales por defecto fuera de entornos locales.",
      "- [ ] Revisar endpoints admin sin auth.",
      "- [ ] Revisar discrepancias UI/export del censo.",
      "- [ ] Revisar hallazgos de recaptura evitables.",
      "- [ ] Revisar p95 de endpoints críticos contra thresholds.",
      ""
    ].join("\n")
  );

  await writeMarkdownFile(
    path.join(env.docsDir, "recapture_opportunities.md"),
    [
      "# Auditoría de recaptura",
      "",
      "La suite E2E actualizará este archivo con hallazgos donde exista contexto clínico reutilizable no aprovechado por la UI o por los endpoints de transición.",
      ""
    ].join("\n")
  );

  await writeMarkdownFile(
    path.join(env.docsDir, "integration_findings.md"),
    [
      "# Hallazgos de integración",
      "",
      "La suite E2E documentará aquí diferencias entre UI, exportes e índices longitudinales (residentes, censo, expediente, quirófano).",
      ""
    ].join("\n")
  );
}

void main();
