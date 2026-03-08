import type { ProfileAvailability } from "./env.js";

export type ModuleName =
  | "consulta"
  | "expediente"
  | "hospitalizacion"
  | "quirofano"
  | "urgencias"
  | "jefatura"
  | "analytics"
  | "fhir"
  | "ia"
  | "admin"
  | "general";

export const PUBLIC_ROUTE_ALLOWLIST = ["/", "/status"];

export function inferModuleFromPath(pathname: string, tags: string[] = []): ModuleName {
  const normalized = pathname.toLowerCase();
  const tagsLower = tags.map((tag) => tag.toLowerCase());

  if (tagsLower.some((tag) => tag.includes("fhir")) || normalized.startsWith("/fhir")) return "fhir";
  if (normalized.startsWith("/api/ai") || normalized.startsWith("/ai")) return "ia";
  if (normalized.startsWith("/admin")) return "admin";
  if (normalized.startsWith("/api/stats") || normalized.startsWith("/analytics") || normalized.startsWith("/reporte")) return "analytics";
  if (normalized.startsWith("/jefatura-urologia") || normalized.startsWith("/quirofano/jefatura")) return "jefatura";
  if (normalized.startsWith("/hospitalizacion")) return "hospitalizacion";
  if (normalized.startsWith("/quirofano/urgencias")) return "urgencias";
  if (normalized.startsWith("/quirofano")) return "quirofano";
  if (normalized.startsWith("/expediente")) return "expediente";
  if (normalized.startsWith("/consulta")) return "consulta";
  return "general";
}

export function inferKindFromPath(pathname: string): "html" | "json" {
  return pathname.startsWith("/api/") || pathname.startsWith("/fhir/") ? "json" : "html";
}

export function inferProfileAvailability(pathname: string): ProfileAvailability {
  if (pathname === "/status") {
    return "both";
  }
  if (pathname.startsWith("/quirofano/jefatura") || pathname.startsWith("/api/quirofano/jefatura")) {
    return "both";
  }
  return "full";
}

export function inferAuthRequired(pathname: string): boolean {
  return !PUBLIC_ROUTE_ALLOWLIST.includes(pathname);
}
