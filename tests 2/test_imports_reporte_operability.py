import importlib
import unittest


class ImportsReporteOperabilityTest(unittest.TestCase):
    def test_import_reporte_flow(self):
        mod = importlib.import_module("app.services.reporte_flow")
        self.assertTrue(hasattr(mod, "render_reporte_html"))

    def test_import_ui_nav_router(self):
        mod = importlib.import_module("app.api.ui_nav")
        self.assertTrue(hasattr(mod, "router"))


if __name__ == "__main__":
    unittest.main()
