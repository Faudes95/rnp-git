import { test, expect } from "../fixtures/base.fixture.js";
import { buildBasicAuthHeader } from "../helpers/auth.js";
import { discoverEndpointMatrix } from "../helpers/discovery.js";
import { isLocalBaseUrl } from "../helpers/env.js";
import { PUBLIC_ROUTE_ALLOWLIST } from "../helpers/module_catalog.js";

async function getWithoutAuth(baseUrl: string, path: string): Promise<Response> {
  return await fetch(new URL(path, baseUrl), {
    method: "GET",
    redirect: "manual",
    headers: {
      Accept: "text/html,application/json;q=0.9,*/*;q=0.8",
    },
  });
}

test("credenciales por defecto solo se aceptan en entorno local/ci", async ({ playwright, appEnv }) => {
  const ctx = await playwright.request.newContext({
    baseURL: appEnv.baseUrl,
    extraHTTPHeaders: {
      Authorization: buildBasicAuthHeader({
        ...appEnv,
        authUser: "Faudes",
        authPass: "1995",
      }),
    },
  });
  const response = await ctx.get("/status");
  const accepted = response.ok();
  await ctx.dispose();
  const allowed = isLocalBaseUrl(appEnv.baseUrl) || !!process.env["CI"];
  if (accepted && !allowed) {
    throw new Error("Las credenciales Faudes/1995 siguen funcionando fuera de un entorno local permitido.");
  }
});

test("endpoints admin no deben quedar abiertos sin auth", async ({ appEnv }) => {
  const candidates = ["/admin/database/status", "/admin/events/summary", "/admin/outbox/summary"];
  for (const candidate of candidates) {
    const response = await getWithoutAuth(appEnv.baseUrl, candidate);
    expect([401, 403, 404, 307, 303, 302], `${candidate} devolvió ${response.status} sin auth`).toContain(
      response.status,
    );
  }
});

test("rutas públicas inesperadas se reportan contra allowlist", async ({ api, appEnv }) => {
  const matrix = await discoverEndpointMatrix(api, appEnv);
  const publicUnexpected: string[] = [];
  const routeErrors: string[] = [];
  for (const endpoint of matrix.filter((row) => row.method === "GET" && !row.path.includes("{"))) {
    try {
      const response = await getWithoutAuth(appEnv.baseUrl, endpoint.path);
      if (response.ok && !PUBLIC_ROUTE_ALLOWLIST.includes(endpoint.path)) {
        publicUnexpected.push(endpoint.path);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      routeErrors.push(`${endpoint.path} :: ${message}`);
    }
  }
  expect(publicUnexpected).toEqual([]);
  expect(routeErrors).toEqual([]);
});
