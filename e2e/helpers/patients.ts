import type { APIRequestContext } from "@playwright/test";

import { fetchFormCsrf } from "./csrf.js";
import { uniqueSuffix } from "./ids.js";

export interface TestPatient {
  prefix: string;
  nss: string;
  nombre: string;
  curp: string;
  sexo: "MASCULINO" | "FEMENINO";
  curpSexo: "H" | "M";
  edad: number;
  fechaNacimiento: string;
  agregadoMedico: string;
  hgz: string;
}

export interface ConsultaSeed {
  consultaId: number;
  draftId: string;
  patient: TestPatient;
}

const CURP_CHARSET = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ";
const CURP_MAP = Object.fromEntries([...CURP_CHARSET].map((char, index) => [char, index])) as Record<string, number>;

function curpCheckDigit(curp17: string): string {
  let sum = 0;
  [...curp17].forEach((char, index) => {
    sum += (CURP_MAP[char] ?? 0) * (18 - index);
  });
  return String((10 - (sum % 10)) % 10);
}

function generateCurp(sex: "H" | "M", suffix: string): string {
  const homoclave = suffix.replace(/[^A-Z0-9]/g, "").slice(-1) || "0";
  const first17 = `GOCA900101${sex}DFLRN${homoclave}`;
  return `${first17}${curpCheckDigit(first17)}`;
}

function uniqueNss(): string {
  const millis = `${Date.now()}`;
  return millis.slice(-10);
}

export function buildTestPatient(prefix = "UROMED"): TestPatient {
  const suffix = uniqueSuffix(prefix).replace(/[^A-Z0-9]/gi, "").toUpperCase();
  const sexo: TestPatient["sexo"] = "MASCULINO";
  const curpSexo: TestPatient["curpSexo"] = "H";
  return {
    prefix,
    nss: uniqueNss(),
    nombre: `${prefix} ${suffix} PACIENTE`.trim().toUpperCase(),
    curp: generateCurp(curpSexo, suffix),
    sexo,
    curpSexo,
    edad: 35,
    fechaNacimiento: "1990-01-01",
    agregadoMedico: "E2E UROMED",
    hgz: "HGZ 1",
  };
}

export async function createConsultaViaMetadata(
  api: APIRequestContext,
  patient: TestPatient,
): Promise<ConsultaSeed> {
  const { csrfToken } = await fetchFormCsrf(api, "/consulta/metadata");
  const seccionResponse = await api.post("/api/consulta/seccion/guardar", {
    data: {
      csrf_token: csrfToken,
      seccion_codigo: "1",
      seccion_nombre: "Ficha de identificación",
      payload: {
        curp: patient.curp,
        nss: patient.nss,
        nombre: patient.nombre,
        sexo: patient.sexo,
        edad: patient.edad,
        agregado_medico: patient.agregadoMedico,
      },
    },
  });
  expectJsonOk(seccionResponse.status(), "/api/consulta/seccion/guardar");
  const seccionPayload = (await seccionResponse.json()) as {
    ok: boolean;
    saved?: { draft_id?: string };
  };
  if (!seccionPayload.ok || !seccionPayload.saved?.draft_id) {
    throw new Error("No se obtuvo draft_id válido al guardar sección metadata.");
  }
  const draftId = seccionPayload.saved.draft_id;
  const saveResponse = await api.post("/api/consulta/metadata/guardar-expediente", {
    data: {
      csrf_token: csrfToken,
      draft_id: draftId,
    },
  });
  expectJsonOk(saveResponse.status(), "/api/consulta/metadata/guardar-expediente");
  const savePayload = (await saveResponse.json()) as {
    ok: boolean;
    consulta_id?: number;
  };
  if (!savePayload.ok || !savePayload.consulta_id) {
    throw new Error("No se obtuvo consulta_id válido al consolidar expediente metadata.");
  }
  return {
    consultaId: savePayload.consulta_id,
    draftId,
    patient,
  };
}

function expectJsonOk(status: number, routePath: string): void {
  if (status < 200 || status >= 300) {
    throw new Error(`Respuesta inesperada ${status} en ${routePath}`);
  }
}
