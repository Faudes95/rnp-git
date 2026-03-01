from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, MetaData, String, Table, Text, and_, insert, select, update
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow


GUARDIA_TEMPLATE_METADATA = MetaData()

HOSP_GUARDIA_TEMPLATE_SCHEMA = Table(
    "hospital_guardia_template_schema",
    GUARDIA_TEMPLATE_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("dataset", String(80), nullable=False, unique=True, index=True),
    Column("version", String(32), nullable=False, default="v1", index=True),
    Column("schema_json", Text, nullable=False),
    Column("activo", Boolean, nullable=False, default=True, index=True),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow, index=True),
)


def ensure_guardia_template_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    GUARDIA_TEMPLATE_METADATA.create_all(bind=bind, checkfirst=True)


def _dump(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _default_schema_for_spec(dataset: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    fields = [str(f) for f in (spec.get("fields") or [])]
    field_props: Dict[str, Any] = {}
    for f in fields:
        field_props[f] = {
            "type": "string",
            "title": f,
        }
    return {
        "dataset": dataset,
        "title": spec.get("title") or dataset,
        "version": "v1",
        "file_kind": spec.get("file_kind") or "xlsx",
        "subdatasets": [str(s) for s in (spec.get("subdatasets") or [])],
        "fields": fields,
        "json_schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": field_props,
            "required": [],
        },
    }


def seed_default_guardia_templates(session: Session, *, base_specs: Dict[str, Dict[str, Any]]) -> int:
    ensure_guardia_template_schema(session)
    seeded = 0
    for dataset, spec in base_specs.items():
        row = session.execute(
            select(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.id).where(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.dataset == str(dataset)).limit(1)
        ).first()
        if row:
            continue
        payload = _default_schema_for_spec(str(dataset), spec or {})
        session.execute(
            insert(HOSP_GUARDIA_TEMPLATE_SCHEMA).values(
                dataset=str(dataset),
                version=str(payload.get("version") or "v1"),
                schema_json=_dump(payload),
                activo=True,
                actualizado_en=utcnow(),
            )
        )
        seeded += 1
    if seeded:
        session.commit()
    return seeded


def list_guardia_templates(session: Session, *, base_specs: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    ensure_guardia_template_schema(session)
    seed_default_guardia_templates(session, base_specs=base_specs)

    rows = session.execute(
        select(HOSP_GUARDIA_TEMPLATE_SCHEMA)
        .where(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.activo.is_(True))
        .order_by(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.dataset.asc())
    ).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        schema = _load(r.get("schema_json"))
        out.append(
            {
                "dataset": r.get("dataset"),
                "version": r.get("version"),
                "actualizado_en": r.get("actualizado_en").isoformat() if r.get("actualizado_en") else None,
                "title": schema.get("title") or r.get("dataset"),
                "fields": schema.get("fields") or [],
                "file_kind": schema.get("file_kind") or "xlsx",
                "subdatasets": schema.get("subdatasets") or [],
            }
        )
    return out


def get_guardia_template(session: Session, *, dataset: str, base_specs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ensure_guardia_template_schema(session)
    seed_default_guardia_templates(session, base_specs=base_specs)
    key = str(dataset or "").strip()
    if not key:
        return {}
    row = session.execute(
        select(HOSP_GUARDIA_TEMPLATE_SCHEMA)
        .where(and_(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.dataset == key, HOSP_GUARDIA_TEMPLATE_SCHEMA.c.activo.is_(True)))
        .limit(1)
    ).mappings().first()
    if not row:
        return {}
    schema = _load(row.get("schema_json"))
    return {
        "dataset": row.get("dataset"),
        "version": row.get("version"),
        "actualizado_en": row.get("actualizado_en").isoformat() if row.get("actualizado_en") else None,
        "schema": schema,
    }


def get_effective_guardia_spec(
    session: Session,
    *,
    dataset: str,
    base_specs: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    key = str(dataset or "").strip()
    base = dict(base_specs.get(key) or {})
    if not base:
        return None
    schema = get_guardia_template(session, dataset=key, base_specs=base_specs).get("schema") or {}
    fields = schema.get("fields")
    if isinstance(fields, list) and fields:
        base["fields"] = [str(f) for f in fields]
    title = str(schema.get("title") or "").strip()
    if title:
        base["title"] = title
    file_kind = str(schema.get("file_kind") or "").strip().lower()
    if file_kind:
        base["file_kind"] = file_kind
    subdatasets = schema.get("subdatasets")
    if isinstance(subdatasets, list):
        base["subdatasets"] = [str(s) for s in subdatasets if str(s).strip()]
    return base


def upsert_guardia_template(
    session: Session,
    *,
    dataset: str,
    schema_payload: Dict[str, Any],
    version: str = "v1",
    activo: bool = True,
) -> Dict[str, Any]:
    ensure_guardia_template_schema(session)
    key = str(dataset or "").strip()
    if not key:
        raise ValueError("dataset requerido")
    payload = dict(schema_payload or {})
    payload.setdefault("dataset", key)
    payload.setdefault("version", version)
    payload.setdefault("updated_at", datetime.utcnow().isoformat())
    existing = session.execute(
        select(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.id)
        .where(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.dataset == key)
        .limit(1)
    ).first()
    if existing:
        session.execute(
            update(HOSP_GUARDIA_TEMPLATE_SCHEMA)
            .where(HOSP_GUARDIA_TEMPLATE_SCHEMA.c.dataset == key)
            .values(
                version=str(version or "v1"),
                schema_json=_dump(payload),
                activo=bool(activo),
                actualizado_en=utcnow(),
            )
        )
    else:
        session.execute(
            insert(HOSP_GUARDIA_TEMPLATE_SCHEMA).values(
                dataset=key,
                version=str(version or "v1"),
                schema_json=_dump(payload),
                activo=bool(activo),
                actualizado_en=utcnow(),
            )
        )
    session.commit()
    return get_guardia_template(session, dataset=key, base_specs={key: {"title": key, "fields": []}})

