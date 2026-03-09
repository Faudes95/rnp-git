from __future__ import annotations

import unittest

from app.core.profile_manifest import get_profile_manifest


class QuirofanoProfileManifestTest(unittest.TestCase):
    def test_quirofano_profile_includes_expected_modules(self):
        manifest = get_profile_manifest("quirofano")
        expected = {
            "quirofano",
            "urgencias",
            "jefatura_quirofano",
            "shell",
            "auth_login",
            "api_v1",
        }
        self.assertTrue(expected.issubset(manifest.active_modules))

    def test_quirofano_profile_excludes_non_target_domains(self):
        manifest = get_profile_manifest("quirofano")
        forbidden = {
            "hospitalizacion",
            "inpatient_notes",
            "consulta",
            "consulta_externa",
            "expediente_plus",
            "jefaturas",
        }
        self.assertTrue(forbidden.isdisjoint(manifest.active_modules))


if __name__ == "__main__":
    unittest.main()
