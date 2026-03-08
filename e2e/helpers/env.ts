import path from "node:path";

export type BootProfile = "full" | "minimal_jefatura";
export type ProfileAvailability = BootProfile | "both";

export interface AppEnv {
  baseUrl: string;
  authEnabled: boolean;
  authUser: string;
  authPass: string;
  bootProfile: BootProfile;
  startupInterconexionMode: string;
  aiWarmupMode: string;
  databaseUrl?: string;
  surgicalDatabaseUrl?: string;
  ngrokUrl?: string;
  enablePiiEncryption: boolean;
  dataEncryptionKey?: string;
  perfP50Ms: number;
  perfP95Ms: number;
  perfConcurrency: number;
  docsDir: string;
  artifactsDir: string;
  projectRoot: string;
}

function parseBool(raw: string | undefined, defaultValue: boolean): boolean {
  if (raw == null || raw === "") {
    return defaultValue;
  }
  return ["1", "true", "yes", "on", "si"].includes(raw.trim().toLowerCase());
}

function parseNumber(raw: string | undefined, defaultValue: number): number {
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

export function isLocalBaseUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return ["127.0.0.1", "localhost"].includes(parsed.hostname);
  } catch {
    return false;
  }
}

export function loadEnv(): AppEnv {
  const projectRoot = process.cwd();
  const baseUrl = process.env["BASE_URL"] ?? "http://127.0.0.1:8000";
  const bootProfile = (process.env["APP_BOOT_PROFILE"] ?? "full") as BootProfile;
  const authEnabled = parseBool(process.env["AUTH_ENABLED"], true);
  const authUser = process.env["AUTH_USER"] ?? process.env["IMSS_USER"] ?? "Faudes";
  const authPass = process.env["AUTH_PASS"] ?? process.env["IMSS_PASS"] ?? "1995";

  return {
    baseUrl,
    authEnabled,
    authUser,
    authPass,
    bootProfile,
    startupInterconexionMode: process.env["STARTUP_INTERCONEXION_MODE"] ?? "off",
    aiWarmupMode: process.env["AI_WARMUP_MODE"] ?? "off",
    databaseUrl: process.env["DATABASE_URL"],
    surgicalDatabaseUrl: process.env["SURGICAL_DATABASE_URL"],
    ngrokUrl: process.env["NGROK_URL"],
    enablePiiEncryption: parseBool(process.env["ENABLE_PII_ENCRYPTION"], false),
    dataEncryptionKey: process.env["DATA_ENCRYPTION_KEY"],
    perfP50Ms: parseNumber(process.env["PERF_P50_MS"], 1500),
    perfP95Ms: parseNumber(process.env["PERF_P95_MS"], 4000),
    perfConcurrency: parseNumber(process.env["PERF_CONCURRENCY"], 10),
    docsDir: path.join(projectRoot, "docs"),
    artifactsDir: path.join(projectRoot, "artifacts"),
    projectRoot
  };
}
