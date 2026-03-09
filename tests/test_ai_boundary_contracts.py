from __future__ import annotations

import unittest

from app.integrations.fau_bot_core.boundary import build_fau_bot_core_boundary_status


class AiBoundaryContractsTest(unittest.TestCase):
    def test_fau_bot_core_boundary_status_exposes_readonly_contract(self):
        payload = build_fau_bot_core_boundary_status({"service": "fau_bot_core", "ok": True})
        self.assertEqual(payload["service"], "fau_bot_core")
        contract = payload["boundary_contract"]
        self.assertTrue(contract["read_only_expected"])
        self.assertTrue(contract["output_isolation"])
        self.assertTrue(contract["readonly_role_scripts"]["clinical"]["exists"])
        self.assertTrue(contract["readonly_role_scripts"]["surgical"]["exists"])


if __name__ == "__main__":
    unittest.main()
