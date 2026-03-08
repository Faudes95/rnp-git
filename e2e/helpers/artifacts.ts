import fs from "node:fs/promises";
import path from "node:path";

import { redactText, redactUnknown } from "./redact.js";

export function sanitizeSlug(raw: string): string {
  return raw
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120)
    .toLowerCase();
}

export async function ensureDir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true });
}

export async function writeJsonFile(filePath: string, payload: unknown): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, JSON.stringify(redactUnknown(payload), null, 2) + "\n", "utf8");
}

export async function writeMarkdownFile(filePath: string, content: string): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, redactText(content), "utf8");
}

export async function appendMarkdownSection(filePath: string, title: string, body: string): Promise<void> {
  await ensureDir(path.dirname(filePath));
  const section = `## ${title}\n\n${redactText(body).trim()}\n\n`;
  await fs.appendFile(filePath, section, "utf8");
}

export function makeArtifactPath(root: string, ...parts: string[]): string {
  return path.join(root, ...parts);
}
