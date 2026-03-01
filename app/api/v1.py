from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.schemas.api_v1 import (
    ApiV1EventSummary,
    ApiV1FormsList,
    ApiV1Health,
    ApiV1JobSummary,
    ApiV1ValidationResult,
)
from app.services.db_platform_flow import get_database_platform_status
from app.services import form_metadata_flow as svc_form_metadata_flow
from app.services.consulta_externa_flow import get_consulta_externa_stats
from app.services.event_log_flow import list_events, summary as event_summary
from app.services.job_registry_flow import list_jobs, summary as jobs_summary
from app.services.master_identity_flow import get_master_identity_snapshot
from app.services.patient_context_flow import build_patient_context


router = APIRouter(prefix="/api/v1", tags=["api-v1"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_surgical_db()


@router.get("/health", response_model=ApiV1Health, response_class=JSONResponse)
def api_v1_health():
    return JSONResponse(content=ApiV1Health(timestamp=utcnow().isoformat() + "Z").model_dump())


@router.get("/meta", response_class=JSONResponse)
def api_v1_meta():
    db_status = get_database_platform_status()
    return JSONResponse(
        content={
            "api_version": "v1",
            "status": "stable",
            "contract_frozen": True,
            "breaking_changes_policy": "No se permiten cambios breaking en /api/v1 sin nueva versión.",
            "deprecation_policy": {
                "legacy_routes": "compat",
                "sunset_requires": "nueva versión + ventana de migración documentada",
            },
            "database_target": db_status.get("target_mode"),
            "database_ready_for_target": bool(db_status.get("ready_for_target")),
            "timestamp": utcnow().isoformat() + "Z",
        }
    )


@router.get("/database/status", response_class=JSONResponse)
def api_v1_database_status():
    return JSONResponse(content=get_database_platform_status())


@router.get("/forms", response_model=ApiV1FormsList, response_class=JSONResponse)
def api_v1_forms(db: Session = Depends(_get_db)):
    forms = svc_form_metadata_flow.list_forms(db)
    return JSONResponse(content=ApiV1FormsList(total=len(forms), forms=forms).model_dump())


@router.get("/forms/{form_code}/schema", response_class=JSONResponse)
def api_v1_form_schema(form_code: str, db: Session = Depends(_get_db)):
    try:
        schema = svc_form_metadata_flow.get_form_schema(db, form_code=form_code)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not schema.get("form_code"):
        raise HTTPException(status_code=404, detail="form_code no encontrado")
    return JSONResponse(content=schema)


@router.post("/forms/{form_code}/validate", response_model=ApiV1ValidationResult, response_class=JSONResponse)
async def api_v1_form_validate(form_code: str, request: Request, db: Session = Depends(_get_db)):
    payload: Dict[str, Any] = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    data = payload.get("payload") or payload
    if not isinstance(data, dict):
        data = {}

    result = svc_form_metadata_flow.validate_form_payload(
        db,
        form_code=form_code,
        payload=data,
        strict=bool(payload.get("strict", False)),
    )
    return JSONResponse(
        content=ApiV1ValidationResult(
            ok=bool(result.get("ok")),
            form_code=form_code,
            validation=result,
        ).model_dump()
    )


@router.get("/master-identity/{nss}", response_class=JSONResponse)
def api_v1_master_identity(nss: str, db: Session = Depends(_get_db)):
    snap = get_master_identity_snapshot(db, nss=nss, include_links=True, links_limit=120)
    return JSONResponse(content=snap)


@router.get("/patient/context", response_class=JSONResponse)
def api_v1_patient_context(
    consulta_id: int | None = None,
    nss: str | None = None,
    curp: str | None = None,
    hospitalizacion_id: int | None = None,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    try:
        payload = build_patient_context(
            db,
            m,
            consulta_id=consulta_id,
            nss=nss,
            curp=curp,
            hospitalizacion_id=hospitalizacion_id,
        )
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "detail": str(exc)})
    return JSONResponse(content=payload)


@router.get("/stats/consulta-externa/servicios", response_class=JSONResponse)
def api_v1_stats_consulta_externa(db: Session = Depends(_get_db)):
    return JSONResponse(content=get_consulta_externa_stats(db))


@router.get("/events/summary", response_model=ApiV1EventSummary, response_class=JSONResponse)
def api_v1_events_summary(limit: int = 120, sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(content=ApiV1EventSummary(**event_summary(sdb, limit=limit)).model_dump())


@router.get("/events/recent", response_class=JSONResponse)
def api_v1_events_recent(
    limit: int = 200,
    module: str = "",
    event_type: str = "",
    consulta_id: int | None = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return JSONResponse(
        content={
            "events": list_events(
                sdb,
                limit=limit,
                module=module,
                event_type=event_type,
                consulta_id=consulta_id,
            )
        }
    )


@router.get("/jobs/summary", response_model=ApiV1JobSummary, response_class=JSONResponse)
def api_v1_jobs_summary(limit: int = 120, sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(content=ApiV1JobSummary(**jobs_summary(sdb, limit=limit)).model_dump())


@router.get("/jobs/recent", response_class=JSONResponse)
def api_v1_jobs_recent(
    limit: int = 200,
    status: str = "",
    job_name: str = "",
    sdb: Session = Depends(_get_surgical_db),
):
    return JSONResponse(content={"jobs": list_jobs(sdb, limit=limit, status=status, job_name=job_name)})
