import path from "node:path";

import { test, expect } from "../fixtures/base.fixture.js";
import { writeJsonFile, writeMarkdownFile } from "../helpers/artifacts.js";
import { discoverEndpointMatrix, endpointMatrixToMarkdown } from "../helpers/discovery.js";

test("genera la matriz de endpoints desde runtime + snapshots", async ({ api, appEnv }) => {
  const matrix = await discoverEndpointMatrix(api, appEnv);
  expect(matrix.length).toBeGreaterThan(0);
  await writeJsonFile(path.join(appEnv.artifactsDir, "endpoints_matrix.json"), matrix);
  await writeMarkdownFile(path.join(appEnv.docsDir, "endpoints_matrix.md"), endpointMatrixToMarkdown(matrix));
});
