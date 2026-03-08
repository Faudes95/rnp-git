import { test } from "../../fixtures/base.fixture.js";
import { buildTestPatient, createConsultaViaMetadata } from "../../helpers/patients.js";
import { addPacienteToWaitlist } from "../../helpers/quirofano.js";

test("lista de espera de programación acepta consulta existente sin recaptura externa", async ({ api, page, appEnv }) => {
  test.skip(appEnv.bootProfile !== "full", "Lista de espera solo aplica al perfil full.");
  const patient = buildTestPatient("WAIT");
  const consulta = await createConsultaViaMetadata(api, patient);
  await addPacienteToWaitlist(page, patient, consulta.consultaId);
});
