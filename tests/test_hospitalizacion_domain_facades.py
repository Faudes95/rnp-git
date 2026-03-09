from __future__ import annotations

import importlib
import unittest


class HospitalizacionDomainFacadesTest(unittest.TestCase):
    def test_import_hospitalizacion_facades(self):
        clinical = importlib.import_module("app.services.hospitalizacion.clinical")
        guardia = importlib.import_module("app.services.hospitalizacion.guardia")
        egreso = importlib.import_module("app.services.hospitalizacion.egreso")
        notes = importlib.import_module("app.services.hospitalizacion.notes")
        ward = importlib.import_module("app.services.hospitalizacion.ward")

        self.assertTrue(hasattr(clinical, "guardar_hospitalizacion_flow"))
        self.assertTrue(hasattr(clinical, "imprimir_censo_excel_flow"))
        self.assertTrue(hasattr(guardia, "hospitalizacion_guardia_reporte_flow"))
        self.assertTrue(hasattr(guardia, "DATASET_SPECS"))
        self.assertTrue(hasattr(egreso, "hospitalizacion_alta_guardar_flow"))
        self.assertTrue(hasattr(notes, "create_or_get_active_episode"))
        self.assertTrue(hasattr(ward, "ward_round_dashboard_flow"))
        self.assertTrue(hasattr(ward, "command_center_flow"))


if __name__ == "__main__":
    unittest.main()
