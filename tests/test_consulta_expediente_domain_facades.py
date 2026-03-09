from __future__ import annotations

import importlib
import unittest


class ConsultaExpedienteDomainFacadesTest(unittest.TestCase):
    def test_import_consulta_facades(self):
        consulta = importlib.import_module("app.services.consulta_domain")
        self.assertTrue(hasattr(consulta, "consulta_externa_home_flow"))
        self.assertTrue(hasattr(consulta, "consulta_externa_leoch_guardar_flow"))
        self.assertTrue(hasattr(consulta, "mensaje_estatus_consulta"))

    def test_import_expediente_facades(self):
        expediente = importlib.import_module("app.services.expediente")
        self.assertTrue(hasattr(expediente, "ensure_expediente_plus_schema"))
        self.assertTrue(hasattr(expediente, "ver_expediente_flow"))
        self.assertTrue(hasattr(expediente, "ensure_master_identity_schema"))
        self.assertTrue(hasattr(expediente, "reindex_patient"))


if __name__ == "__main__":
    unittest.main()
