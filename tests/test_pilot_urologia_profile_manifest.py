from __future__ import annotations

import unittest

from app.core.profile_manifest import get_profile_manifest


class PilotUrologiaProfileManifestTest(unittest.TestCase):
    def test_pilot_urologia_profile_includes_expected_modules(self):
        manifest = get_profile_manifest("pilot_urologia")
        expected = {
            "compat",
            "legacy_core",
            "shell",
            "quirofano",
            "quirofano_web",
            "urgencias",
            "jefatura_quirofano",
            "jefaturas",
            "hospitalizacion",
            "consulta",
            "expediente_web",
            "expediente_plus",
            "perfil_clinico",
            "ehr_integrado",
            "forms_metadata",
            "master_identity",
            "patient_autofill",
            "api_v1",
            "urology_devices_events",
            "auth_login",
            "governance",
            "clinical_validation",
        }
        self.assertTrue(expected.issubset(manifest.active_modules))

    def test_pilot_urologia_profile_excludes_non_target_domains(self):
        manifest = get_profile_manifest("pilot_urologia")
        forbidden = {
            "consulta_externa",
            "interconsultas",
            "fhir",
            "ai_fau_bot",
            "ai_fau_bot_core",
            "dashboard",
            "reporte",
            "reporte_stats",
        }
        self.assertTrue(forbidden.isdisjoint(manifest.active_modules))


if __name__ == "__main__":
    unittest.main()
