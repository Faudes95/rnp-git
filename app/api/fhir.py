from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.fhir import build_capability_statement, extract_fhir_resource_name

router = APIRouter(prefix="/api/fhir", tags=["fhir"])


@router.get("/health", response_class=JSONResponse)
def fhir_health():
    return JSONResponse(content={"status": "ok", "module": "fhir"})


@router.get("/metadata", response_class=JSONResponse)
def fhir_metadata():
    from app.core.app_context import main_proxy as main_module

    resources = []
    for route in main_module.app.routes:
        path = getattr(route, "path", "")
        resource_name = extract_fhir_resource_name(path)
        if resource_name:
            resources.append(resource_name)
    return JSONResponse(content=build_capability_statement(resources))


@router.get("/legacy-endpoints", response_class=JSONResponse)
def fhir_legacy_endpoints():
    from app.core.app_context import main_proxy as main_module

    paths = []
    for route in main_module.app.routes:
        path = getattr(route, "path", "")
        if path.startswith("/fhir/"):
            paths.append(path)
    return JSONResponse(content={"total": len(paths), "paths": sorted(set(paths))})
