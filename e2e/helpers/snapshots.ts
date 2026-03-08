import fs from "node:fs/promises";
import path from "node:path";

export interface RouteSnapshotEntry {
  path: string;
  methods: string[];
  name?: string;
}

export interface ContractSnapshotEntry {
  path: string;
  kind: "html" | "json";
  status: number;
  contains?: string[];
  keys?: string[];
}

export async function loadRouteSnapshot(projectRoot: string): Promise<RouteSnapshotEntry[]> {
  const filePath = path.join(projectRoot, "snapshots", "routes_snapshot.json");
  const payload = JSON.parse(await fs.readFile(filePath, "utf8")) as { routes?: RouteSnapshotEntry[] };
  return payload.routes ?? [];
}

export async function loadHttpContracts(projectRoot: string): Promise<ContractSnapshotEntry[]> {
  const filePath = path.join(projectRoot, "snapshots", "http_contracts.json");
  const payload = JSON.parse(await fs.readFile(filePath, "utf8")) as { routes?: ContractSnapshotEntry[] };
  return payload.routes ?? [];
}
