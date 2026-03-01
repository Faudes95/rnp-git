from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.services.event_log_flow import CLINICAL_EVENT_LOG, emit_event, ensure_event_log_schema
from app.services.ui_context_flow import get_active_context, save_active_context
from app.services.ui_error_observability_flow import record_ui_error, ui_error_summary

router = APIRouter(tags=["ui-nav"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


@router.post("/api/v1/ui/nav-event", response_class=JSONResponse)
async def api_ui_nav_event(request: Request, db: Session = Depends(_get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    path = _safe_text(payload.get("path")) or _safe_text(getattr(request.url, "path", ""))
    event_type = _safe_text(payload.get("event_type")).upper() or "PAGE_VIEW"
    if not path:
        raise HTTPException(status_code=422, detail="path requerido")

    emit_event(
        db,
        module="ui_nav",
        event_type=event_type,
        entity="page",
        entity_id=path,
        actor=_safe_text(payload.get("actor")) or "ui_shell",
        source_route=path,
        payload={
            "path": path,
            "referrer": _safe_text(payload.get("referrer")),
            "stage": _safe_text(payload.get("stage")) or "page_load",
            "context": payload.get("context") if isinstance(payload.get("context"), dict) else {},
        },
        commit=True,
    )
    return JSONResponse(content={"status": "ok"})


@router.get("/api/v1/ui/nav-summary", response_class=JSONResponse)
def api_ui_nav_summary(days: int = 7, limit: int = 20, db: Session = Depends(_get_db)):
    ensure_event_log_schema(db)
    since = utcnow() - timedelta(days=max(1, min(int(days or 7), 120)))
    rows = db.execute(
        select(CLINICAL_EVENT_LOG.c.entity_id, func.count())
        .where(
            CLINICAL_EVENT_LOG.c.module == "ui_nav",
            CLINICAL_EVENT_LOG.c.created_at >= since,
        )
        .group_by(CLINICAL_EVENT_LOG.c.entity_id)
        .order_by(func.count().desc())
        .limit(max(1, min(int(limit or 20), 200)))
    ).all()
    return JSONResponse(
        content={
            "days": int(days or 7),
            "top_paths": [{"path": _safe_text(p), "total": int(c or 0)} for p, c in rows],
        }
    )


@router.post("/api/v1/ui/error-event", response_class=JSONResponse)
async def api_ui_error_event(request: Request, db: Session = Depends(_get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    path = _safe_text(payload.get("path")) or _safe_text(getattr(request.url, "path", ""))
    if not path:
        raise HTTPException(status_code=422, detail="path requerido")

    event_id = record_ui_error(
        db,
        path=path,
        event_type=_safe_text(payload.get("event_type")) or "JS_ERROR",
        message=_safe_text(payload.get("message")) or "Error funcional de UI",
        actor=_safe_text(payload.get("actor")) or "ui_shell",
        source=_safe_text(payload.get("source")),
        stack=str(payload.get("stack") or ""),
        severity=_safe_text(payload.get("severity")) or "ERROR",
        context=payload.get("context") if isinstance(payload.get("context"), dict) else {},
    )
    return JSONResponse(content={"status": "ok", "event_id": int(event_id or 0)})


@router.get("/api/v1/ui/error-summary", response_class=JSONResponse)
def api_ui_error_summary(days: int = 7, limit: int = 20, db: Session = Depends(_get_db)):
    return JSONResponse(content=ui_error_summary(db, days=days, limit=limit))


@router.get("/api/v1/contexto-activo", response_class=JSONResponse)
def api_contexto_activo_get(actor: str = "ui_shell", db: Session = Depends(_get_db)):
    return JSONResponse(content=get_active_context(db, actor=actor))


@router.post("/api/v1/contexto-activo", response_class=JSONResponse)
async def api_contexto_activo_set(request: Request, db: Session = Depends(_get_db)):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    actor = _safe_text(payload.get("actor")) or "ui_shell"
    context = payload.get("context") if isinstance(payload.get("context"), dict) else payload
    saved = save_active_context(
        db,
        actor=actor,
        context=context,
        source_route=_safe_text(payload.get("source_route")) or _safe_text(getattr(request.url, "path", "")),
    )
    return JSONResponse(content={"status": "ok", "context": saved})
