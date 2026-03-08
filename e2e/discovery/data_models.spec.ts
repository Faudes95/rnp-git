import fs from "node:fs/promises";
import path from "node:path";

import { test, expect } from "../fixtures/base.fixture.js";
import { tryLoadOpenApi } from "../helpers/openapi.js";

test("valida modelos críticos desde OpenAPI o forms schema", async ({ api, appEnv }) => {
  const openapi = await tryLoadOpenApi(api);
  if (openapi?.components?.schemas && Object.keys(openapi.components.schemas).length > 0) {
    const schemaNames = Object.keys(openapi.components.schemas);
    const criticalNeedles = ["pac", "consulta", "hospital", "quir", "exped", "fhir", "ai"];
    const found = criticalNeedles.filter((needle) =>
      schemaNames.some((name) => name.toLowerCase().includes(needle)),
    );
    expect(found.length).toBeGreaterThan(2);
  } else {
    const consultaSchema = await api.get("/api/forms/consulta_externa/schema");
    const hospSchema = await api.get("/api/forms/hospitalizacion_ingreso/schema");
    expect(consultaSchema.ok()).toBeTruthy();
    expect(hospSchema.ok()).toBeTruthy();
    const consultaPayload = (await consultaSchema.json()) as Record<string, unknown>;
    const hospPayload = (await hospSchema.json()) as Record<string, unknown>;
    expect(consultaPayload).toHaveProperty("sections");
    expect(hospPayload).toHaveProperty("sections");
  }

  if (appEnv.bootProfile === "full") {
    const dbStatus = await api.get("/admin/database/status");
    expect(dbStatus.ok()).toBeTruthy();
    const payload = (await dbStatus.json()) as Record<string, unknown>;
    expect(payload).toHaveProperty("clinical");
    expect(payload).toHaveProperty("surgical");
  }

  const mvSql = await fs.readFile(path.join(appEnv.projectRoot, "materialized_views.sql"), "utf8");
  const partitionSql = await fs.readFile(path.join(appEnv.projectRoot, "partition_vitals.sql"), "utf8");
  const vectorSql = await fs.readFile(path.join(appEnv.projectRoot, "migration_vector.sql"), "utf8");
  expect(mvSql.toLowerCase()).toContain("materialized");
  expect(partitionSql.toLowerCase()).toContain("partition");
  expect(vectorSql.toLowerCase()).toContain("vector");
});
