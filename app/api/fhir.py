from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.fhir import (
    build_capability_statement,
    build_fhir_boundary_status,
    collect_fhir_legacy_paths,
    collect_fhir_resources_from_routes,
)

router = APIRouter(prefix="/api/fhir", tags=["fhir"])


@router.get("/health", response_class=JSONResponse)
def fhir_health():
    from app.core.app_context import main_proxy as main_module

    return JSONResponse(content={"status": "ok", **build_fhir_boundary_status(main_module.app.routes)})


@router.get("/metadata", response_class=JSONResponse)
def fhir_metadata():
    from app.core.app_context import main_proxy as main_module

    resources = collect_fhir_resources_from_routes(main_module.app.routes)
    payload = build_capability_statement(resources)
    payload["contract"] = build_fhir_boundary_status(main_module.app.routes)["contract"]
    return JSONResponse(content=payload)


@router.get("/legacy-endpoints", response_class=JSONResponse)
def fhir_legacy_endpoints():
    from app.core.app_context import main_proxy as main_module

    paths = collect_fhir_legacy_paths(main_module.app.routes)
    return JSONResponse(content={"total": len(paths), "paths": paths})
