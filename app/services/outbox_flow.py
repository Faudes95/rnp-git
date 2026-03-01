from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, func, select, text, update
from sqlalchemy.orm import Session

from app.core.request_context import get_correlation_id
from app.core.time_utils import utcnow


OUTBOX_METADATA = MetaData()

MODULE_EVENT_OUTBOX = Table(
    "module_event_outbox",
    OUTBOX_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("correlation_id", String(64), nullable=True, index=True),
    Column("actor", String(120), nullable=True, index=True),
    Column("source_route", String(255), nullable=True, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("modulo", String(80), nullable=False, index=True),
    Column("evento", String(80), nullable=False, index=True),
    Column("referencia_id", String(160), nullable=True, index=True),
    Column("payload_json", Text, nullable=True),
    Column("estado", String(30), nullable=False, default="PENDING", index=True),
    Column("error", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    Column("procesado_en", DateTime, nullable=True, index=True),
)


def ensure_outbox_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    OUTBOX_METADATA.create_all(bind=bind, checkfirst=True)
    with bind.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE module_event_outbox ADD COLUMN correlation_id VARCHAR(64)"))
        except Exception:
            pass
        try:
            conn.execute(text("ALTER TABLE module_event_outbox ADD COLUMN actor VARCHAR(120)"))
        except Exception:
            pass
        try:
            conn.execute(text("ALTER TABLE module_event_outbox ADD COLUMN source_route VARCHAR(255)"))
        except Exception:
            pass


def emit_outbox_event(
    sdb: Session,
    *,
    consulta_id: Optional[int],
    modulo: str,
    evento: str,
    referencia_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    actor: str = "",
    source_route: str = "",
    estado: str = "PENDING",
    commit: bool = True,
) -> Optional[int]:
    ensure_outbox_schema(sdb)
    if not str(modulo or "").strip() or not str(evento or "").strip():
        return None

    result = sdb.execute(
        MODULE_EVENT_OUTBOX.insert().values(
            correlation_id=(str(correlation_id or "").strip() or get_correlation_id(default="") or None),
            actor=str(actor or "").strip() or None,
            source_route=str(source_route or "").strip() or None,
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            modulo=str(modulo or "").strip(),
            evento=str(evento or "").strip(),
            referencia_id=str(referencia_id or "").strip() or None,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
            estado=str(estado or "PENDING").strip().upper(),
            creado_en=utcnow(),
        )
    )
    event_id = None
    try:
        event_id = int(getattr(result, "inserted_primary_key", [None])[0])
    except Exception:
        event_id = None

    try:
        from app.services.event_log_flow import emit_event as _emit_event

        _emit_event(
            sdb,
            module=str(modulo or "").strip() or "outbox",
            event_type=str(evento or "").strip() or "OUTBOX_EVENT",
            entity="outbox",
            entity_id=str(event_id or ""),
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            actor=str(actor or "").strip(),
            source_route=str(source_route or "").strip(),
            payload=payload or {},
            correlation_id=str(correlation_id or "").strip() or None,
            commit=False,
        )
    except Exception:
        pass

    if commit:
        sdb.commit()
    return event_id


def mark_outbox_processed(sdb: Session, *, event_id: int, ok: bool = True, error: str = "") -> bool:
    ensure_outbox_schema(sdb)
    event_id_int = int(event_id or 0)
    if event_id_int <= 0:
        return False
    state = "DONE" if ok else "ERROR"
    sdb.execute(
        update(MODULE_EVENT_OUTBOX)
        .where(MODULE_EVENT_OUTBOX.c.id == event_id_int)
        .values(
            estado=state,
            error=(str(error or "")[:2000] or None),
            procesado_en=utcnow(),
        )
    )
    sdb.commit()
    return True


def outbox_summary(sdb: Session, limit: int = 100) -> Dict[str, Any]:
    ensure_outbox_schema(sdb)
    safe_limit = max(1, min(int(limit or 100), 2000))

    total = sdb.execute(select(func.count()).select_from(MODULE_EVENT_OUTBOX)).scalar() or 0

    by_state_rows = sdb.execute(
        select(MODULE_EVENT_OUTBOX.c.estado, func.count())
        .group_by(MODULE_EVENT_OUTBOX.c.estado)
        .order_by(func.count().desc())
    ).all()
    by_module_rows = sdb.execute(
        select(MODULE_EVENT_OUTBOX.c.modulo, func.count())
        .group_by(MODULE_EVENT_OUTBOX.c.modulo)
        .order_by(func.count().desc())
    ).all()

    rows = sdb.execute(
        select(
            MODULE_EVENT_OUTBOX.c.id,
            MODULE_EVENT_OUTBOX.c.correlation_id,
            MODULE_EVENT_OUTBOX.c.actor,
            MODULE_EVENT_OUTBOX.c.source_route,
            MODULE_EVENT_OUTBOX.c.consulta_id,
            MODULE_EVENT_OUTBOX.c.modulo,
            MODULE_EVENT_OUTBOX.c.evento,
            MODULE_EVENT_OUTBOX.c.referencia_id,
            MODULE_EVENT_OUTBOX.c.estado,
            MODULE_EVENT_OUTBOX.c.creado_en,
            MODULE_EVENT_OUTBOX.c.procesado_en,
            MODULE_EVENT_OUTBOX.c.error,
        )
        .order_by(MODULE_EVENT_OUTBOX.c.id.desc())
        .limit(safe_limit)
    ).all()

    latest: List[Dict[str, Any]] = []
    for r in rows:
        latest.append(
            {
                "id": int(r.id),
                "correlation_id": r.correlation_id,
                "actor": r.actor,
                "source_route": r.source_route,
                "consulta_id": int(r.consulta_id) if r.consulta_id is not None else None,
                "modulo": r.modulo,
                "evento": r.evento,
                "referencia_id": r.referencia_id,
                "estado": r.estado,
                "creado_en": r.creado_en.isoformat() if r.creado_en else None,
                "procesado_en": r.procesado_en.isoformat() if r.procesado_en else None,
                "error": r.error,
            }
        )

    return {
        "total": int(total),
        "por_estado": [{"estado": (k or "SIN_ESTADO"), "total": int(v)} for k, v in by_state_rows],
        "por_modulo": [{"modulo": (k or "SIN_MODULO"), "total": int(v)} for k, v in by_module_rows],
        "latest": latest,
    }
