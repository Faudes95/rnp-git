from __future__ import annotations

import unittest

from app.core.profile_manifest import get_profile_manifest


class JefaturaUrologiaProfileManifestTest(unittest.TestCase):
    def test_jefatura_urologia_profile_includes_expected_modules(self):
        manifest = get_profile_manifest("jefatura_urologia")
        expected = {
            "jefaturas",
            "shell",
            "auth_login",
            "api_v1",
        }
        self.assertTrue(expected.issubset(manifest.active_modules))

    def test_jefatura_urologia_profile_excludes_non_target_domains(self):
        manifest = get_profile_manifest("jefatura_urologia")
        forbidden = {
            "hospitalizacion",
            "inpatient_notes",
            "quirofano",
            "urgencias",
            "jefatura_quirofano",
            "consulta",
            "consulta_externa",
            "expediente_plus",
        }
        self.assertTrue(forbidden.isdisjoint(manifest.active_modules))


if __name__ == "__main__":
    unittest.main()
