"""Servicios auxiliares FHIR para extracción progresiva."""

from typing import Dict, List


def build_capability_statement(resources: List[str]) -> Dict[str, object]:
    return {
        "resourceType": "CapabilityStatement",
        "status": "active",
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [
            {
                "mode": "server",
                "resource": [
                    {"type": resource_name}
                    for resource_name in sorted(set(resources))
                ],
            }
        ],
    }


def extract_fhir_resource_name(path: str) -> str:
    clean = path.split("?")[0].strip("/")
    if not clean:
        return ""
    parts = clean.split("/")
    if len(parts) >= 2 and parts[0] == "fhir":
        return parts[1]
    return ""

