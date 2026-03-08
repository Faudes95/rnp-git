import type { APIRequestContext } from "@playwright/test";

export interface OpenApiOperation {
  summary?: string;
  description?: string;
  tags?: string[];
  responses?: Record<string, { content?: Record<string, unknown> }>;
}

export interface OpenApiDocument {
  openapi?: string;
  paths?: Record<string, Record<string, OpenApiOperation>>;
  components?: {
    schemas?: Record<string, unknown>;
  };
}

export async function tryLoadOpenApi(api: APIRequestContext): Promise<OpenApiDocument | null> {
  const response = await api.get("/openapi.json");
  if (!response.ok()) {
    return null;
  }
  return (await response.json()) as OpenApiDocument;
}
