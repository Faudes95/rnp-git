import fs from "node:fs/promises";
import path from "node:path";
import { execSync } from "node:child_process";

import { ensureDir, writeMarkdownFile } from "./artifacts.js";
import { loadEnv } from "./env.js";

type Phase = "phase1" | "phase2" | "phase3" | "all";

function resolvePhase(arg: string | undefined): Phase {
  if (arg === "phase1" || arg === "phase2" || arg === "phase3" || arg === "all") {
    return arg;
  }
  return "all";
}

function currentCommit(projectRoot: string): string {
  try {
    return execSync("git rev-parse --short HEAD", {
      cwd: projectRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return "unknown";
  }
}

async function readDocOrPlaceholder(filePath: string, fallbackTitle: string): Promise<string> {
  try {
    return await fs.readFile(filePath, "utf8");
  } catch {
    return `# ${fallbackTitle}\n`;
  }
}

async function main(): Promise<void> {
  const env = loadEnv();
  const phase = resolvePhase(process.argv[2]);
  const now = new Date().toISOString();
  const commit = currentCommit(env.projectRoot);
  const outDir = path.join(env.artifactsDir, "pilot_urologia");
  await ensureDir(outDir);

  const runbookPath = path.join(env.docsDir, "operations", "pilot_urologia_supervised_runbook.md");
  const checklistPath = path.join(env.docsDir, "operations", "pilot_urologia_acceptance_checklist.md");
  const scopePath = path.join(env.docsDir, "operations", "pilot_urologia_scope_matrix.md");

  const runbook = await readDocOrPlaceholder(runbookPath, "Runbook de Piloto");
  const checklist = await readDocOrPlaceholder(checklistPath, "Checklist de Aceptación");
  const scope = await readDocOrPlaceholder(scopePath, "Matriz de Alcance");

  await writeMarkdownFile(
    path.join(outDir, "incident_log_template.md"),
    [
      "# Bitácora de incidentes del piloto `pilot_urologia`",
      "",
      `- Generado: ${now}`,
      `- Commit base: \`${commit}\``,
      "",
      "| Fecha/hora | Ruta | Rol/usuario | Resumen | Severidad | Evidencia | Estado |",
      "|---|---|---|---|---|---|---|",
      "| | | | | | | |",
      "",
      "## Notas",
      "",
      "- Registrar solo incidentes reproducibles o con impacto operativo.",
      "- Si hay 500, adjuntar screenshot y pasos mínimos.",
      "",
    ].join("\n"),
  );

  await writeMarkdownFile(
    path.join(outDir, "daily_summary_template.md"),
    [
      "# Resumen diario del piloto `pilot_urologia`",
      "",
      `- Generado: ${now}`,
      `- Commit base: \`${commit}\``,
      `- Perfil: \`${env.bootProfile}\``,
      "",
      "## Uso del día",
      "",
      "- Usuarios activos:",
      "- Casos ejecutados:",
      "- Flujos probados:",
      "",
      "## Hallazgos",
      "",
      "- Críticos:",
      "- Importantes:",
      "- Menores:",
      "",
      "## Decisión",
      "",
      "- Continuar / corregir / suspender",
      "",
    ].join("\n"),
  );

  await writeMarkdownFile(
    path.join(outDir, "go_no_go_template.md"),
    [
      "# Go / No-Go del piloto `pilot_urologia`",
      "",
      `- Generado: ${now}`,
      `- Commit evaluado: \`${commit}\``,
      "",
      "## Criterios",
      "",
      "- [ ] Sin errores 500 repetibles en corredor crítico",
      "- [ ] Auth y CSRF sanos",
      "- [ ] Censo UI/export alineado",
      "- [ ] Postqx -> residente íntegro",
      "- [ ] Hospitalización -> expediente íntegro",
      "- [ ] Incidencias críticas documentadas y aceptadas",
      "",
      "## Decisión final",
      "",
      "- [ ] Go",
      "- [ ] Go condicionado",
      "- [ ] No-Go",
      "",
      "## Responsable",
      "",
      "- Nombre / fecha / firma operativa",
      "",
    ].join("\n"),
  );

  const packets: Array<{ key: Exclude<Phase, "all">; file: string; title: string; bullets: string[] }> = [
    {
      key: "phase1",
      file: "phase1_dry_run_packet.md",
      title: "Paquete de Fase 1: Dry Run Interno",
      bullets: [
        "- Ejecutar `npm run pilot:phase1:urologia` antes de abrir la ventana.",
        "- Confirmar navegación, auth, CSRF y exportes básicos.",
        "- Correr 5 casos controlados del corredor completo.",
      ],
    },
    {
      key: "phase2",
      file: "phase2_supervised_packet.md",
      title: "Paquete de Fase 2: Piloto Clínico Supervisado",
      bullets: [
        "- Usar solo el alcance definido en la matriz del piloto.",
        "- Registrar incidentes operativos y fricciones de captura.",
        "- Consolidar un resumen diario por jornada.",
      ],
    },
    {
      key: "phase3",
      file: "phase3_closeout_packet.md",
      title: "Paquete de Fase 3: Cierre y Decisión",
      bullets: [
        "- Consolidar incidentes, recaptura y discrepancias de censo.",
        "- Completar plantilla Go / No-Go.",
        "- Decidir apertura de la siguiente ola o correcciones previas.",
      ],
    },
  ];

  for (const packet of packets) {
    if (phase !== "all" && phase !== packet.key) {
      continue;
    }
    await writeMarkdownFile(
      path.join(outDir, packet.file),
      [
        `# ${packet.title}`,
        "",
        `- Generado: ${now}`,
        `- Commit base: \`${commit}\``,
        `- Perfil: \`${env.bootProfile}\``,
        "",
        "## Objetivo",
        "",
        ...packet.bullets,
        "",
        "## Referencias base",
        "",
        "- Runbook:",
        "",
        runbook.trim(),
        "",
        "- Checklist:",
        "",
        checklist.trim(),
        "",
        "- Alcance:",
        "",
        scope.trim(),
        "",
      ].join("\n"),
    );
  }
}

void main();
