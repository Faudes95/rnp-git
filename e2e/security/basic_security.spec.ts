import { test, expect } from "../fixtures/base.fixture.js";
import { buildBasicAuthHeader } from "../helpers/auth.js";
import { discoverEndpointMatrix } from "../helpers/discovery.js";
import { isLocalBaseUrl } from "../helpers/env.js";
import { PUBLIC_ROUTE_ALLOWLIST } from "../helpers/module_catalog.js";

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

test("endpoints admin no deben quedar abiertos sin auth", async ({ playwright, appEnv }) => {
  const ctx = await playwright.request.newContext({ baseURL: appEnv.baseUrl });
  const candidates = ["/admin/database/status", "/admin/events/summary", "/admin/outbox/summary"];
  for (const candidate of candidates) {
    const response = await ctx.get(candidate);
    expect([401, 403, 404, 307, 303]).toContain(response.status());
  }
  await ctx.dispose();
});

test("rutas públicas inesperadas se reportan contra allowlist", async ({ playwright, api, appEnv }) => {
  const unauthenticated = await playwright.request.newContext({ baseURL: appEnv.baseUrl });
  const matrix = await discoverEndpointMatrix(api, appEnv);
  const publicUnexpected: string[] = [];
  for (const endpoint of matrix.filter((row) => row.method === "GET" && !row.path.includes("{"))) {
    const response = await unauthenticated.get(endpoint.path);
    if (response.ok() && !PUBLIC_ROUTE_ALLOWLIST.includes(endpoint.path)) {
      publicUnexpected.push(endpoint.path);
    }
  }
  await unauthenticated.dispose();
  expect(publicUnexpected).toEqual([]);
});
