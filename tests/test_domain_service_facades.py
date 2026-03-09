from __future__ import annotations

import importlib
import unittest


class DomainServiceFacadesTest(unittest.TestCase):
    def test_import_jefaturas_facades(self):
        central = importlib.import_module("app.services.jefaturas.central")
        urologia = importlib.import_module("app.services.jefaturas.urologia")
        self.assertTrue(hasattr(central, "render_jefatura_central_home_flow"))
        self.assertTrue(hasattr(central, "render_resident_exam_flow"))
        self.assertTrue(hasattr(urologia, "render_jefatura_urologia_residente_profile_flow"))
        self.assertTrue(hasattr(urologia, "update_resident_profile_from_request"))

    def test_import_quirofano_facades(self):
        clinical = importlib.import_module("app.services.quirofano.clinical")
        jefatura = importlib.import_module("app.services.quirofano.jefatura")
        self.assertTrue(hasattr(clinical, "guardar_quirofano_flow"))
        self.assertTrue(hasattr(clinical, "render_waitlist_lista_flow"))
        self.assertTrue(hasattr(jefatura, "render_jefatura_quirofano_day_flow"))
        self.assertTrue(hasattr(jefatura, "save_template_version_from_request"))


if __name__ == "__main__":
    unittest.main()
