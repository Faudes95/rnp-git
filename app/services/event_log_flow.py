from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, func, select
from sqlalchemy.orm import Session

from app.core.request_context import get_correlation_id
from app.core.time_utils import utcnow


EVENT_LOG_METADATA = MetaData()

CLINICAL_EVENT_LOG = Table(
    "clinical_event_log",
    EVENT_LOG_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("correlation_id", String(64), nullable=True, index=True),
    Column("actor", String(120), nullable=True, index=True),
    Column("module", String(80), nullable=False, index=True),
    Column("event_type", String(120), nullable=False, index=True),
    Column("entity", String(120), nullable=True, index=True),
    Column("entity_id", String(120), nullable=True, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("source_route", String(255), nullable=True, index=True),
    Column("payload_json", Text, nullable=True),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)


def ensure_event_log_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    EVENT_LOG_METADATA.create_all(bind=bind, checkfirst=True)


def _dump(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def emit_event(
    sdb: Session,
    *,
    module: str,
    event_type: str,
    entity: str = "",
    entity_id: str = "",
    consulta_id: Optional[int] = None,
    actor: str = "",
    source_route: str = "",
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    commit: bool = True,
) -> Optional[int]:
    ensure_event_log_schema(sdb)
    module_txt = _safe_text(module)
    event_txt = _safe_text(event_type)
    if not module_txt or not event_txt:
        return None

    corr = _safe_text(correlation_id) or get_correlation_id(default="")
    result = sdb.execute(
        CLINICAL_EVENT_LOG.insert().values(
            correlation_id=corr or None,
            actor=_safe_text(actor) or None,
            module=module_txt,
            event_type=event_txt,
            entity=_safe_text(entity) or None,
            entity_id=_safe_text(entity_id) or None,
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            source_route=_safe_text(source_route) or None,
            payload_json=_dump(payload),
            created_at=utcnow(),
        )
    )
    event_id = None
    try:
        event_id = int(getattr(result, "inserted_primary_key", [None])[0])
    except Exception:
        event_id = None
    if commit:
        sdb.commit()
    return event_id


def list_events(
    sdb: Session,
    *,
    limit: int = 200,
    module: str = "",
    event_type: str = "",
    consulta_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_event_log_schema(sdb)
    q = select(CLINICAL_EVENT_LOG)
    if _safe_text(module):
        q = q.where(CLINICAL_EVENT_LOG.c.module == _safe_text(module))
    if _safe_text(event_type):
        q = q.where(CLINICAL_EVENT_LOG.c.event_type == _safe_text(event_type))
    if consulta_id is not None:
        q = q.where(CLINICAL_EVENT_LOG.c.consulta_id == int(consulta_id))
    rows = sdb.execute(
        q.order_by(CLINICAL_EVENT_LOG.c.id.desc()).limit(max(1, min(int(limit or 200), 3000)))
    ).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        payload = {}
        try:
            payload = json.loads(r.get("payload_json") or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        out.append(
            {
                "id": int(r["id"]),
                "correlation_id": r.get("correlation_id"),
                "actor": r.get("actor"),
                "module": r.get("module"),
                "event_type": r.get("event_type"),
                "entity": r.get("entity"),
                "entity_id": r.get("entity_id"),
                "consulta_id": r.get("consulta_id"),
                "source_route": r.get("source_route"),
                "payload": payload,
                "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
            }
        )
    return out


def summary(sdb: Session, *, limit: int = 200) -> Dict[str, Any]:
    ensure_event_log_schema(sdb)
    total = int(sdb.execute(select(func.count()).select_from(CLINICAL_EVENT_LOG)).scalar() or 0)
    by_module = sdb.execute(
        select(CLINICAL_EVENT_LOG.c.module, func.count())
        .group_by(CLINICAL_EVENT_LOG.c.module)
        .order_by(func.count().desc())
    ).all()
    by_type = sdb.execute(
        select(CLINICAL_EVENT_LOG.c.event_type, func.count())
        .group_by(CLINICAL_EVENT_LOG.c.event_type)
        .order_by(func.count().desc())
    ).all()
    return {
        "total": total,
        "por_modulo": [{"module": str(k or ""), "total": int(v)} for k, v in by_module],
        "por_event_type": [{"event_type": str(k or ""), "total": int(v)} for k, v in by_type],
        "latest": list_events(sdb, limit=limit),
    }
