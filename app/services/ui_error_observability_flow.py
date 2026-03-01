from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Dict, List

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.services.event_log_flow import CLINICAL_EVENT_LOG, emit_event, ensure_event_log_schema

_ALLOWED_UI_ERROR_TYPES = {
    "JS_ERROR",
    "UNHANDLED_REJECTION",
    "RESOURCE_ERROR",
    "FETCH_ERROR",
    "UI_VALIDATION_ERROR",
    "UI_WARNING",
}


def _safe_text(value: Any, *, max_len: int = 400) -> str:
    txt = str(value or "").strip()
    return txt[:max_len]


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def record_ui_error(
    sdb: Session,
    *,
    path: str,
    event_type: str,
    message: str,
    actor: str = "ui_shell",
    source: str = "",
    stack: str = "",
    severity: str = "ERROR",
    context: Dict[str, Any] | None = None,
) -> int | None:
    ensure_event_log_schema(sdb)
    et = _safe_text(event_type, max_len=80).upper() or "JS_ERROR"
    if et not in _ALLOWED_UI_ERROR_TYPES:
        et = "JS_ERROR"
    payload = {
        "message": _safe_text(message, max_len=1200),
        "source": _safe_text(source, max_len=255),
        "stack": _safe_text(stack, max_len=4000),
        "severity": (_safe_text(severity, max_len=20) or "ERROR").upper(),
        "context": _safe_dict(context),
    }
    return emit_event(
        sdb,
        module="ui_error",
        event_type=et,
        entity="page",
        entity_id=_safe_text(path, max_len=255),
        actor=_safe_text(actor, max_len=120) or "ui_shell",
        source_route=_safe_text(path, max_len=255),
        payload=payload,
        commit=True,
    )


def ui_error_summary(sdb: Session, *, days: int = 7, limit: int = 20) -> Dict[str, Any]:
    ensure_event_log_schema(sdb)
    d = max(1, min(int(days or 7), 90))
    lim = max(1, min(int(limit or 20), 100))
    since = utcnow() - timedelta(days=d)

    total = int(
        sdb.execute(
            select(func.count()).select_from(CLINICAL_EVENT_LOG).where(
                CLINICAL_EVENT_LOG.c.module == "ui_error",
                CLINICAL_EVENT_LOG.c.created_at >= since,
            )
        ).scalar()
        or 0
    )

    by_type_rows = sdb.execute(
        select(CLINICAL_EVENT_LOG.c.event_type, func.count())
        .where(
            CLINICAL_EVENT_LOG.c.module == "ui_error",
            CLINICAL_EVENT_LOG.c.created_at >= since,
        )
        .group_by(CLINICAL_EVENT_LOG.c.event_type)
        .order_by(func.count().desc(), CLINICAL_EVENT_LOG.c.event_type.asc())
        .limit(lim)
    ).all()
    by_path_rows = sdb.execute(
        select(CLINICAL_EVENT_LOG.c.entity_id, func.count())
        .where(
            CLINICAL_EVENT_LOG.c.module == "ui_error",
            CLINICAL_EVENT_LOG.c.created_at >= since,
        )
        .group_by(CLINICAL_EVENT_LOG.c.entity_id)
        .order_by(func.count().desc(), CLINICAL_EVENT_LOG.c.entity_id.asc())
        .limit(lim)
    ).all()

    latest_rows = sdb.execute(
        select(
            CLINICAL_EVENT_LOG.c.id,
            CLINICAL_EVENT_LOG.c.event_type,
            CLINICAL_EVENT_LOG.c.entity_id,
            CLINICAL_EVENT_LOG.c.payload_json,
            CLINICAL_EVENT_LOG.c.created_at,
        )
        .where(
            CLINICAL_EVENT_LOG.c.module == "ui_error",
            CLINICAL_EVENT_LOG.c.created_at >= since,
        )
        .order_by(CLINICAL_EVENT_LOG.c.id.desc())
        .limit(max(20, lim * 5))
    ).all()

    msg_counter: Dict[str, int] = {}
    latest: List[Dict[str, Any]] = []
    for rid, etype, path, payload_raw, created_at in latest_rows:
        payload = {}
        if isinstance(payload_raw, str):
            try:
                parsed = json.loads(payload_raw)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}
        msg = _safe_text(payload.get("message"), max_len=240)
        if msg:
            msg_counter[msg] = int(msg_counter.get(msg, 0)) + 1
        if len(latest) < lim:
            latest.append(
                {
                    "id": int(rid or 0),
                    "event_type": _safe_text(etype, max_len=80),
                    "path": _safe_text(path, max_len=255),
                    "message": msg,
                    "severity": _safe_text(payload.get("severity"), max_len=20) or "ERROR",
                    "created_at": created_at.isoformat() if created_at else None,
                }
            )

    top_messages = [
        {"message": m, "total": int(c)}
        for m, c in sorted(msg_counter.items(), key=lambda item: (-item[1], item[0]))[:lim]
    ]

    return {
        "window_days": d,
        "total": total,
        "by_type": [{"event_type": _safe_text(k, max_len=80), "total": int(v or 0)} for k, v in by_type_rows],
        "by_path": [{"path": _safe_text(k, max_len=255), "total": int(v or 0)} for k, v in by_path_rows],
        "top_messages": top_messages,
        "latest": latest,
    }
