from __future__ import annotations

import unittest

from app.integrations.fhir.capability import build_capability_statement, build_fhir_boundary_status


class FhirBoundaryContractsTest(unittest.TestCase):
    def test_capability_statement_declares_contract(self):
        payload = build_capability_statement(["Patient", "Observation"])
        self.assertEqual(payload["resourceType"], "CapabilityStatement")
        self.assertEqual(payload["fhirVersion"], "4.0.1")
        self.assertEqual(payload["software"]["name"], "UROMED FHIR Bridge")

    def test_boundary_status_reports_expected_contract(self):
        class Route:
            def __init__(self, path: str):
                self.path = path

        status = build_fhir_boundary_status([Route("/fhir/Patient/{curp}"), Route("/fhir/Observation")])
        self.assertTrue(status["capability_statement_ready"])
        self.assertIn("Patient", status["resource_types"])
        self.assertEqual(status["contract"]["metadata_endpoint"], "/api/fhir/metadata")


if __name__ == "__main__":
    unittest.main()
