import importlib
import unittest


class ImportsContextGuardrailsTest(unittest.TestCase):
    def test_import_ui_context_flow(self):
        mod = importlib.import_module("app.services.ui_context_flow")
        self.assertTrue(hasattr(mod, "save_active_context"))
        self.assertTrue(hasattr(mod, "get_active_context"))

    def test_import_hospitalizacion_flow_new_helpers(self):
        mod = importlib.import_module("app.services.hospitalizacion_flow")
        self.assertTrue(hasattr(mod, "precheck_hospitalizacion_ingreso_flow"))
        self.assertTrue(hasattr(mod, "cerrar_hospitalizacion_activa_flow"))


if __name__ == "__main__":
    unittest.main()
