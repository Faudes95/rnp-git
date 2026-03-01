from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.services.event_log_flow import CLINICAL_EVENT_LOG, emit_event, ensure_event_log_schema


def _safe_text(value: Any, *, max_len: int = 255) -> str:
    return str(value or "").strip()[:max_len]


def _to_int(value: Any) -> Optional[int]:
    try:
        txt = _safe_text(value, max_len=32)
        if not txt:
            return None
        return int(txt)
    except Exception:
        return None


def _normalize_nss_10(value: Any) -> str:
    txt = "".join(ch for ch in _safe_text(value, max_len=40) if ch.isdigit())
    return txt[:10]


def save_active_context(
    sdb: Session,
    *,
    actor: str = "ui_shell",
    context: Optional[Dict[str, Any]] = None,
    source_route: str = "",
) -> Dict[str, Any]:
    ensure_event_log_schema(sdb)
    ctx = context if isinstance(context, dict) else {}
    payload = {
        "consulta_id": _to_int(ctx.get("consulta_id")),
        "hospitalizacion_id": _to_int(ctx.get("hospitalizacion_id")),
        "nss": _normalize_nss_10(ctx.get("nss")),
        "nombre": _safe_text(ctx.get("nombre"), max_len=220),
        "source": _safe_text(ctx.get("source"), max_len=120) or "ui_wizard",
        "updated_at": utcnow().isoformat(),
    }
    emit_event(
        sdb,
        module="ui_context",
        event_type="CONTEXT_SET",
        entity="active_context",
        entity_id=_safe_text(actor, max_len=120) or "ui_shell",
        actor=_safe_text(actor, max_len=120) or "ui_shell",
        source_route=_safe_text(source_route, max_len=255),
        payload=payload,
        commit=True,
    )
    return payload


def get_active_context(
    sdb: Session,
    *,
    actor: str = "ui_shell",
    max_age_hours: int = 168,
) -> Dict[str, Any]:
    ensure_event_log_schema(sdb)
    hours = max(1, min(int(max_age_hours or 168), 24 * 60))
    since = utcnow() - timedelta(hours=hours)

    row = (
        sdb.execute(
            select(CLINICAL_EVENT_LOG.c.payload_json, CLINICAL_EVENT_LOG.c.created_at)
            .where(
                CLINICAL_EVENT_LOG.c.module == "ui_context",
                CLINICAL_EVENT_LOG.c.event_type == "CONTEXT_SET",
                CLINICAL_EVENT_LOG.c.actor == (_safe_text(actor, max_len=120) or "ui_shell"),
                CLINICAL_EVENT_LOG.c.created_at >= since,
            )
            .order_by(CLINICAL_EVENT_LOG.c.id.desc())
            .limit(1)
        )
        .mappings()
        .first()
    )
    if not row:
        return {
            "actor": _safe_text(actor, max_len=120) or "ui_shell",
            "consulta_id": None,
            "hospitalizacion_id": None,
            "nss": "",
            "nombre": "",
            "updated_at": None,
        }

    payload = {}
    raw = row.get("payload_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}
    return {
        "actor": _safe_text(actor, max_len=120) or "ui_shell",
        "consulta_id": _to_int(payload.get("consulta_id")),
        "hospitalizacion_id": _to_int(payload.get("hospitalizacion_id")),
        "nss": _normalize_nss_10(payload.get("nss")),
        "nombre": _safe_text(payload.get("nombre"), max_len=220),
        "updated_at": payload.get("updated_at") or (row.get("created_at").isoformat() if row.get("created_at") else None),
    }
