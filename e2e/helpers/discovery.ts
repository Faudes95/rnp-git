import type { APIRequestContext } from "@playwright/test";

import type { AppEnv, ProfileAvailability } from "./env.js";
import { inferAuthRequired, inferKindFromPath, inferModuleFromPath, inferProfileAvailability, type ModuleName } from "./module_catalog.js";
import { tryLoadOpenApi, type OpenApiDocument, type OpenApiOperation } from "./openapi.js";
import { loadHttpContracts, loadRouteSnapshot } from "./snapshots.js";

export interface EndpointRecord {
  module: ModuleName;
  method: string;
  path: string;
  authRequired: boolean;
  kind: "html" | "json";
  purpose: string;
  profile: ProfileAvailability;
  source: "openapi" | "snapshot" | "contracts" | "code";
  tags: string[];
  status?: number;
}

function purposeFromOperation(routePath: string, operation?: OpenApiOperation): string {
  if (operation?.summary) return operation.summary;
  if (operation?.description) return operation.description.split("\n")[0] ?? routePath;
  return `Ruta ${routePath}`;
}

function kindFromOperation(routePath: string, operation?: OpenApiOperation): "html" | "json" {
  const contentTypes = Object.keys(operation?.responses?.["200"]?.content ?? {});
  if (contentTypes.some((item) => item.includes("json"))) {
    return "json";
  }
  return inferKindFromPath(routePath);
}

function fromOpenApi(doc: OpenApiDocument): EndpointRecord[] {
  const rows: EndpointRecord[] = [];
  for (const [routePath, methods] of Object.entries(doc.paths ?? {})) {
    for (const [method, operation] of Object.entries(methods)) {
      const tags = operation.tags ?? [];
      rows.push({
        module: inferModuleFromPath(routePath, tags),
        method: method.toUpperCase(),
        path: routePath,
        authRequired: inferAuthRequired(routePath),
        kind: kindFromOperation(routePath, operation),
        purpose: purposeFromOperation(routePath, operation),
        profile: inferProfileAvailability(routePath),
        source: "openapi",
        tags
      });
    }
  }
  return rows;
}

export async function discoverEndpointMatrix(api: APIRequestContext, env: AppEnv): Promise<EndpointRecord[]> {
  const fromRuntime = await tryLoadOpenApi(api);
  const routeSnapshot = await loadRouteSnapshot(env.projectRoot);
  const httpContracts = await loadHttpContracts(env.projectRoot);

  const seen = new Map<string, EndpointRecord>();

  const register = (record: EndpointRecord): void => {
    const key = `${record.method}:${record.path}`;
    const current = seen.get(key);
    if (!current) {
      seen.set(key, record);
      return;
    }
    if (current.source !== "openapi" && record.source === "openapi") {
      seen.set(key, record);
      return;
    }
    if (current.source === "snapshot" && record.source === "contracts") {
      seen.set(key, { ...current, kind: record.kind, status: record.status, source: "contracts" });
    }
  };

  for (const record of fromOpenApi(fromRuntime ?? {})) {
    register(record);
  }

  for (const entry of routeSnapshot) {
    for (const method of entry.methods) {
      register({
        module: inferModuleFromPath(entry.path),
        method: method.toUpperCase(),
        path: entry.path,
        authRequired: inferAuthRequired(entry.path),
        kind: inferKindFromPath(entry.path),
        purpose: entry.name ? `Ruta ${entry.name}` : `Ruta ${entry.path}`,
        profile: inferProfileAvailability(entry.path),
        source: "snapshot",
        tags: []
      });
    }
  }

  for (const entry of httpContracts) {
    register({
      module: inferModuleFromPath(entry.path),
      method: "GET",
      path: entry.path,
      authRequired: inferAuthRequired(entry.path),
      kind: entry.kind,
      purpose: `Contrato HTTP esperado para ${entry.path}`,
      profile: inferProfileAvailability(entry.path),
      source: "contracts",
      tags: [],
      status: entry.status
    });
  }

  return [...seen.values()].sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
}

export function endpointMatrixToMarkdown(rows: EndpointRecord[]): string {
  const header = [
    "# Matriz de Endpoints UROMED",
    "",
    "| Módulo | Método | Path | Auth | Tipo | Perfil | Fuente | Propósito |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |"
  ];
  const body = rows.map(
    (row) =>
      `| ${row.module} | ${row.method} | \`${row.path}\` | ${row.authRequired ? "sí" : "no"} | ${row.kind} | ${row.profile} | ${row.source} | ${row.purpose.replace(/\|/g, "/")} |`
  );
  return [...header, ...body, ""].join("\n");
}
