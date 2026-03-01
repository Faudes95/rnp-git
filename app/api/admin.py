from typing import List, Optional
import importlib.util
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.ai_agents.model_registry import clear_model_cache, model_cache_status, warmup_models
from app.core.dependencies import detect_dependencies
from app.core.observability import automatic_alerts, metrics_snapshot, observability_health
from app.core.time_utils import utcnow
from app.services.db_platform_flow import get_database_platform_status
from app.services.event_log_flow import list_events, summary as clinical_event_summary
from app.services.job_registry_flow import list_jobs, summary as jobs_summary
from app.services.outbox_flow import mark_outbox_processed, outbox_summary

router = APIRouter(tags=["admin"])


@router.get("/admin/dependencies", response_class=JSONResponse)
def admin_dependencies():
    deps = detect_dependencies(force_refresh=True)
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            "total": len(deps),
            "available": sum(1 for v in deps.values() if v.get("available")),
            "unavailable": sum(1 for v in deps.values() if not v.get("available")),
            "dependencies": deps,
        }
    )


@router.get("/admin/database/status", response_class=JSONResponse)
def admin_database_status():
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            **get_database_platform_status(),
        }
    )


@router.get("/admin/models/cache", response_class=JSONResponse)
def admin_models_cache_status():
    status = model_cache_status()
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            "total_models": len(status),
            "loaded_models": sum(1 for x in status.values() if x.get("loaded")),
            "cache": status,
        }
    )


@router.post("/admin/models/cache/warmup", response_class=JSONResponse)
async def admin_models_cache_warmup(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    paths: Optional[List[str]] = payload.get("paths") if isinstance(payload, dict) else None
    if not paths:
        paths = [
            "modelo_riesgo_quirurgico.pkl",
            "modelo_riesgo_quirurgico_v2.pkl",
            "modelos/pipeline_duracion_qx.pkl",
            "modelos/pipeline_complicaciones_qx.pkl",
        ]
    loaded = warmup_models([str(p) for p in paths if str(p or "").strip()])
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            "requested": len(paths),
            "loaded": loaded,
        }
    )


@router.post("/admin/models/cache/clear", response_class=JSONResponse)
async def admin_models_cache_clear(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    path = None
    if isinstance(payload, dict):
        value = payload.get("path")
        if value is not None:
            path = str(value)
    result = clear_model_cache(path=path)
    result["timestamp"] = utcnow().isoformat() + "Z"
    return JSONResponse(content=result)


@router.get("/admin/observability/metrics", response_class=JSONResponse)
def admin_observability_metrics(window_minutes: int = 60):
    metrics = metrics_snapshot(window_minutes=window_minutes)
    return JSONResponse(content=metrics)


@router.get("/admin/observability/alerts", response_class=JSONResponse)
def admin_observability_alerts(window_minutes: int = 60):
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            "window_minutes": max(1, int(window_minutes or 60)),
            "alerts": automatic_alerts(window_minutes=window_minutes),
        }
    )


@router.get("/admin/observability/health", response_class=JSONResponse)
def admin_observability_health(window_minutes: int = 60):
    return JSONResponse(content=observability_health(window_minutes=window_minutes))


@router.get("/admin/outbox/summary", response_class=JSONResponse)
def admin_outbox_summary(limit: int = 100):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        return JSONResponse(content=outbox_summary(sdb, limit=limit))
    finally:
        sdb.close()


@router.post("/admin/outbox/ack/{event_id}", response_class=JSONResponse)
def admin_outbox_ack(event_id: int):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        ok = mark_outbox_processed(sdb, event_id=event_id, ok=True)
        return JSONResponse(content={"status": "ok", "acknowledged": bool(ok), "event_id": int(event_id)})
    finally:
        sdb.close()


@router.get("/admin/events/summary", response_class=JSONResponse)
def admin_events_summary(limit: int = 120):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        return JSONResponse(content=clinical_event_summary(sdb, limit=limit))
    finally:
        sdb.close()


@router.get("/admin/events/recent", response_class=JSONResponse)
def admin_events_recent(limit: int = 200, module: str = "", event_type: str = "", consulta_id: Optional[int] = None):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        events = list_events(
            sdb,
            limit=limit,
            module=module,
            event_type=event_type,
            consulta_id=consulta_id,
        )
        return JSONResponse(
            content={
                "total": len(events),
                "events": events,
            }
        )
    finally:
        sdb.close()


@router.get("/admin/jobs/summary", response_class=JSONResponse)
def admin_jobs_summary(limit: int = 120):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        return JSONResponse(content=jobs_summary(sdb, limit=limit))
    finally:
        sdb.close()


@router.get("/admin/jobs/recent", response_class=JSONResponse)
def admin_jobs_recent(limit: int = 200, status: str = "", job_name: str = ""):
    from app.core.app_context import main_proxy as m

    sdb = m.SurgicalSessionLocal()
    try:
        return JSONResponse(
            content={
                "jobs": list_jobs(sdb, limit=limit, status=status, job_name=job_name),
            }
        )
    finally:
        sdb.close()


@router.get("/admin/testing/status", response_class=JSONResponse)
def admin_testing_status():
    root = Path(__file__).resolve().parents[2]
    tests_dir = root / "tests"
    py_tests = list(tests_dir.rglob("test_*.py")) if tests_dir.exists() else []
    return JSONResponse(
        content={
            "timestamp": utcnow().isoformat() + "Z",
            "tests_dir_exists": tests_dir.exists(),
            "test_files_detected": len(py_tests),
            "deps": {
                "pytest_installed": importlib.util.find_spec("pytest") is not None,
                "httpx_installed": importlib.util.find_spec("httpx") is not None,
                "unittest_available": True,
            },
            "hint": "Si pytest/httpx no están instalados, ejecutar: pip install pytest httpx",
        }
    )
