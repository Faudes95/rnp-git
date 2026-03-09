from __future__ import annotations

from typing import Any, Dict, Iterable, List


def extract_fhir_resource_name(path: str) -> str:
    clean = str(path or "").split("?")[0].strip("/")
    if not clean:
        return ""
    parts = clean.split("/")
    if len(parts) >= 2 and parts[0] == "fhir":
        return parts[1]
    return ""


def collect_fhir_resources_from_routes(routes: Iterable[Any]) -> List[str]:
    resources: List[str] = []
    for route in routes:
        path = getattr(route, "path", "")
        resource_name = extract_fhir_resource_name(path)
        if resource_name:
            resources.append(resource_name)
    return sorted(set(resources))


def collect_fhir_legacy_paths(routes: Iterable[Any]) -> List[str]:
    paths = []
    for route in routes:
        path = getattr(route, "path", "")
        if str(path).startswith("/fhir/"):
            paths.append(path)
    return sorted(set(paths))


def build_capability_statement(resources: List[str]) -> Dict[str, object]:
    deduped = sorted(set(resources))
    return {
        "resourceType": "CapabilityStatement",
        "status": "active",
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "software": {
            "name": "UROMED FHIR Bridge",
            "version": "phase-1",
        },
        "implementation": {
            "description": "Bridge FHIR incremental de UROMED",
        },
        "rest": [
            {
                "mode": "server",
                "resource": [{"type": resource_name} for resource_name in deduped],
            }
        ],
    }


def build_fhir_boundary_status(routes: Iterable[Any]) -> Dict[str, object]:
    resources = collect_fhir_resources_from_routes(routes)
    legacy_paths = collect_fhir_legacy_paths(routes)
    return {
        "module": "fhir",
        "capability_statement_ready": bool(resources),
        "legacy_route_count": len(legacy_paths),
        "resource_types": resources,
        "contract": {
            "health_endpoint": "/api/fhir/health",
            "metadata_endpoint": "/api/fhir/metadata",
            "legacy_index_endpoint": "/api/fhir/legacy-endpoints",
            "capability_statement": "FHIR R4 CapabilityStatement",
        },
    }
