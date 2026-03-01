from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.core.validators import normalize_nss_10 as _normalize_nss_10


MASTER_IDENTITY_METADATA = MetaData()

PATIENT_MASTER_IDENTITY = Table(
    "patient_master_identity",
    MASTER_IDENTITY_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, unique=True, index=True),
    Column("nss_canonico", String(10), nullable=False, unique=True, index=True),
    Column("curp_canonico", String(20), nullable=True, index=True),
    Column("nombre_canonico", String(255), nullable=True, index=True),
    Column("sexo_canonico", String(20), nullable=True, index=True),
    Column("fecha_primera_atencion", Date, nullable=True, index=True),
    Column("fecha_ultima_atencion", Date, nullable=True, index=True),
    Column("primera_consulta_id", Integer, nullable=True, index=True),
    Column("ultima_consulta_id", Integer, nullable=True, index=True),
    Column("source_count", Integer, nullable=False, default=0),
    Column("hospitalizaciones_acumuladas", Integer, nullable=False, default=0),
    Column("cirugias_acumuladas", Integer, nullable=False, default=0),
    Column("egresos_acumulados", Integer, nullable=False, default=0),
    Column("atenciones_ambulatorias_acumuladas", Integer, nullable=False, default=0),
    Column("conflicto_identidad", Boolean, nullable=False, default=False, index=True),
    Column("detalle_conflicto", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    Column("actualizado_en", DateTime, default=utcnow, nullable=False, index=True),
)

PATIENT_IDENTITY_LINKS = Table(
    "patient_identity_links",
    MASTER_IDENTITY_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("master_id", Integer, nullable=False, index=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss_canonico", String(10), nullable=False, index=True),
    Column("source_system", String(40), nullable=False, default="RNP", index=True),
    Column("source_table", String(120), nullable=False, index=True),
    Column("source_pk", String(120), nullable=False, index=True),
    Column("module", String(80), nullable=True, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("curp_capturado", String(20), nullable=True, index=True),
    Column("nombre_capturado", String(255), nullable=True, index=True),
    Column("sexo_capturado", String(20), nullable=True, index=True),
    Column("fecha_evento", Date, nullable=True, index=True),
    Column("payload_json", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    UniqueConstraint("source_table", "source_pk", name="uq_patient_identity_source_row"),
)

PATIENT_IDENTITY_EVENTS = Table(
    "patient_identity_events",
    MASTER_IDENTITY_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("master_id", Integer, nullable=False, index=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss_canonico", String(10), nullable=False, index=True),
    Column("event_type", String(60), nullable=False, index=True),
    Column("module", String(80), nullable=True, index=True),
    Column("source_table", String(120), nullable=False, index=True),
    Column("source_pk", String(120), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("estatus", String(80), nullable=True, index=True),
    Column("diagnostico", String(255), nullable=True, index=True),
    Column("procedimiento", String(255), nullable=True, index=True),
    Column("medico", String(255), nullable=True, index=True),
    Column("fecha_evento", Date, nullable=True, index=True),
    Column("payload_json", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    UniqueConstraint(
        "source_table",
        "source_pk",
        "event_type",
        name="uq_patient_identity_event_source",
    ),
)


def ensure_master_identity_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    MASTER_IDENTITY_METADATA.create_all(bind=bind, checkfirst=True)


def normalize_nss_10(value: Any) -> str:
    # Master Identity conserva estrategia de últimos 10 para trazabilidad
    # en históricos con 11+ dígitos.
    return _normalize_nss_10(value, strategy="master_right")


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_name(value: Any) -> str:
    txt = _safe_text(value).upper()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _norm_curp(value: Any) -> str:
    return re.sub(r"\s+", "", _safe_text(value).upper())


def _safe_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    txt = _safe_text(value)
    if not txt:
        return None
    try:
        return date.fromisoformat(txt)
    except Exception:
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _event_bucket_increment(source_table: str) -> Dict[str, int]:
    src = _safe_text(source_table).lower()
    if src in {"hospitalizaciones", "hospitalizacion"}:
        return {"hospitalizaciones_acumuladas": 1}
    if src in {"hospital_egresos", "egresos_hospitalarios"}:
        return {"egresos_acumulados": 1}
    if src.startswith("surgical_") or src in {"quirofano", "quirofano_urgencias"}:
        return {"cirugias_acumuladas": 1}
    if src in {"consulta_externa_atenciones", "consultas"}:
        return {"atenciones_ambulatorias_acumuladas": 1}
    return {}


def _event_type_from_source(source_table: str) -> str:
    src = _safe_text(source_table).lower()
    if src in {"consultas"}:
        return "CONSULTA"
    if src in {"hospitalizaciones", "hospitalizacion"}:
        return "HOSP_INGRESO"
    if src in {"hospital_egresos", "egresos_hospitalarios"}:
        return "HOSP_EGRESO"
    if src in {"consulta_externa_atenciones"}:
        return "CONSULTA_SERVICIO"
    if src in {"quirofano", "surgical_programaciones"}:
        return "QX_PROGRAMADA"
    if src in {"quirofano_urgencias", "surgical_urgencias_programaciones"}:
        return "QX_URGENCIA_PROGRAMADA"
    if src in {"surgical_postquirurgicas"}:
        return "QX_POSTQUIRURGICA"
    return "EVENTO"


def _extract_event_fields(
    *,
    source_table: str,
    payload: Optional[Dict[str, Any]],
    fallback_nombre: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    data = payload or {}
    src = _safe_text(source_table).lower()

    diagnostico = (
        data.get("diagnostico")
        or data.get("diagnostico_principal")
        or data.get("patologia")
        or data.get("motivo")
    )
    procedimiento = (
        data.get("procedimiento")
        or data.get("procedimiento_programado")
        or data.get("procedimiento_realizado")
    )
    medico = (
        data.get("medico")
        or data.get("cirujano")
        or data.get("agregado_medico")
        or data.get("medico_cargo")
    )
    estatus = data.get("estatus")

    if src == "hospitalizaciones":
        estatus = estatus or data.get("estado_salud")
    if src == "hospital_egresos":
        estatus = estatus or "EGRESADO"
    if src == "consultas":
        estatus = estatus or data.get("estatus_protocolo")

    return {
        "diagnostico": _safe_text(diagnostico) or None,
        "procedimiento": _safe_text(procedimiento) or None,
        "medico": _safe_text(medico) or _safe_text(fallback_nombre) or None,
        "estatus": _safe_text(estatus) or None,
    }


def _build_uid_from_nss(nss_10: str) -> str:
    return f"PMI-{nss_10}"


def _insert_identity_event(
    db: Session,
    *,
    master_id: int,
    patient_uid: str,
    nss_10: str,
    source_table: str,
    source_pk: str,
    module: Optional[str],
    consulta_id: Optional[int],
    fecha_evento: date,
    payload: Optional[Dict[str, Any]],
    fallback_nombre: Optional[str],
) -> bool:
    event_type = _event_type_from_source(source_table)
    exists = db.execute(
        select(PATIENT_IDENTITY_EVENTS.c.id).where(
            and_(
                PATIENT_IDENTITY_EVENTS.c.source_table == _safe_text(source_table),
                PATIENT_IDENTITY_EVENTS.c.source_pk == _safe_text(source_pk),
                PATIENT_IDENTITY_EVENTS.c.event_type == event_type,
            )
        )
    ).first()
    if exists is not None:
        return False

    extracted = _extract_event_fields(
        source_table=source_table,
        payload=payload,
        fallback_nombre=fallback_nombre,
    )
    db.execute(
        insert(PATIENT_IDENTITY_EVENTS).values(
            master_id=master_id,
            patient_uid=_safe_text(patient_uid),
            nss_canonico=nss_10,
            event_type=event_type,
            module=_safe_text(module) or None,
            source_table=_safe_text(source_table),
            source_pk=_safe_text(source_pk),
            consulta_id=consulta_id,
            estatus=extracted.get("estatus"),
            diagnostico=extracted.get("diagnostico"),
            procedimiento=extracted.get("procedimiento"),
            medico=extracted.get("medico"),
            fecha_evento=fecha_evento,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
            creado_en=utcnow(),
        )
    )
    return True


def upsert_master_identity(
    db: Session,
    *,
    nss: Any,
    curp: Any = None,
    nombre: Any = None,
    sexo: Any = None,
    consulta_id: Optional[int] = None,
    source_table: str,
    source_pk: Any,
    module: Optional[str] = None,
    source_system: str = "RNP",
    fecha_evento: Any = None,
    payload: Optional[Dict[str, Any]] = None,
    commit: bool = True,
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)

    nss_10 = normalize_nss_10(nss)
    if not nss_10:
        return {"ok": False, "reason": "NSS_INVALIDO"}

    source_pk_txt = _safe_text(source_pk) or f"GEN:{nss_10}:{_safe_text(source_table)}"
    now = utcnow()
    fecha_evt = _safe_date(fecha_evento)
    if fecha_evt is None:
        fecha_evt = now.date()

    curp_norm = _norm_curp(curp)
    nombre_norm = _norm_name(nombre)
    sexo_norm = _safe_text(sexo).upper()

    master_row = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.nss_canonico == nss_10)
    ).mappings().first()

    created = False
    if master_row is None:
        created = True
        db.execute(
            insert(PATIENT_MASTER_IDENTITY).values(
                patient_uid=_build_uid_from_nss(nss_10),
                nss_canonico=nss_10,
                curp_canonico=curp_norm or None,
                nombre_canonico=nombre_norm or None,
                sexo_canonico=sexo_norm or None,
                fecha_primera_atencion=fecha_evt,
                fecha_ultima_atencion=fecha_evt,
                primera_consulta_id=consulta_id,
                ultima_consulta_id=consulta_id,
                source_count=0,
                hospitalizaciones_acumuladas=0,
                cirugias_acumuladas=0,
                egresos_acumulados=0,
                atenciones_ambulatorias_acumuladas=0,
                conflicto_identidad=False,
                detalle_conflicto=None,
                creado_en=now,
                actualizado_en=now,
            )
        )
        master_row = db.execute(
            select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.nss_canonico == nss_10)
        ).mappings().first()

    if master_row is None:
        if commit:
            db.rollback()
        return {"ok": False, "reason": "MASTER_UPSERT_FAILED"}

    master_id = int(master_row["id"])
    patient_uid = _safe_text(master_row["patient_uid"])

    # Detecta conflictos de identidad (mismo NSS, CURP muy diferente).
    conflict = bool(master_row.get("conflicto_identidad"))
    conflict_messages: List[str] = []
    current_curp = _norm_curp(master_row.get("curp_canonico"))
    if curp_norm and current_curp and curp_norm != current_curp:
        conflict = True
        conflict_messages.append(f"CURP mismatch: {current_curp} vs {curp_norm}")

    updates: Dict[str, Any] = {
        "actualizado_en": now,
    }
    if not _safe_text(master_row.get("curp_canonico")) and curp_norm:
        updates["curp_canonico"] = curp_norm
    if not _safe_text(master_row.get("nombre_canonico")) and nombre_norm:
        updates["nombre_canonico"] = nombre_norm
    if not _safe_text(master_row.get("sexo_canonico")) and sexo_norm:
        updates["sexo_canonico"] = sexo_norm
    if consulta_id is not None:
        updates["ultima_consulta_id"] = int(consulta_id)
        if master_row.get("primera_consulta_id") is None:
            updates["primera_consulta_id"] = int(consulta_id)
    first_date = _safe_date(master_row.get("fecha_primera_atencion"))
    last_date = _safe_date(master_row.get("fecha_ultima_atencion"))
    if first_date is None or (fecha_evt and fecha_evt < first_date):
        updates["fecha_primera_atencion"] = fecha_evt
    if last_date is None or (fecha_evt and fecha_evt > last_date):
        updates["fecha_ultima_atencion"] = fecha_evt
    if conflict:
        updates["conflicto_identidad"] = True
        previous = _safe_text(master_row.get("detalle_conflicto"))
        merged = "; ".join([x for x in [previous, " | ".join(conflict_messages)] if x])
        updates["detalle_conflicto"] = merged[:2000] if merged else previous or "CONFLICTO_DE_IDENTIDAD"

    if updates:
        db.execute(
            update(PATIENT_MASTER_IDENTITY)
            .where(PATIENT_MASTER_IDENTITY.c.id == master_id)
            .values(**updates)
        )

    # Evita duplicar vínculo de origen.
    existing_link = db.execute(
        select(PATIENT_IDENTITY_LINKS.c.id).where(
            and_(
                PATIENT_IDENTITY_LINKS.c.source_table == _safe_text(source_table),
                PATIENT_IDENTITY_LINKS.c.source_pk == source_pk_txt,
            )
        )
    ).first()
    link_created = existing_link is None
    if link_created:
        db.execute(
            insert(PATIENT_IDENTITY_LINKS).values(
                master_id=master_id,
                patient_uid=patient_uid,
                nss_canonico=nss_10,
                source_system=_safe_text(source_system) or "RNP",
                source_table=_safe_text(source_table) or "NO_REGISTRADO",
                source_pk=source_pk_txt,
                module=_safe_text(module) or None,
                consulta_id=consulta_id,
                curp_capturado=curp_norm or None,
                nombre_capturado=nombre_norm or None,
                sexo_capturado=sexo_norm or None,
                fecha_evento=fecha_evt,
                payload_json=json.dumps(payload or {}, ensure_ascii=False),
                creado_en=now,
            )
        )

        inc = _event_bucket_increment(_safe_text(source_table))
        if inc:
            db.execute(
                update(PATIENT_MASTER_IDENTITY)
                .where(PATIENT_MASTER_IDENTITY.c.id == master_id)
                .values(
                    source_count=(int(master_row.get("source_count") or 0) + 1),
                    **{
                        key: int(master_row.get(key) or 0) + int(value)
                        for key, value in inc.items()
                    },
                    actualizado_en=now,
                )
            )
        else:
            db.execute(
                update(PATIENT_MASTER_IDENTITY)
                .where(PATIENT_MASTER_IDENTITY.c.id == master_id)
                .values(
                    source_count=(int(master_row.get("source_count") or 0) + 1),
                    actualizado_en=now,
                )
            )

    event_created = _insert_identity_event(
        db,
        master_id=master_id,
        patient_uid=patient_uid,
        nss_10=nss_10,
        source_table=_safe_text(source_table),
        source_pk=source_pk_txt,
        module=_safe_text(module) or None,
        consulta_id=consulta_id,
        fecha_evento=fecha_evt,
        payload=payload,
        fallback_nombre=nombre_norm or None,
    )

    if commit:
        db.commit()

    updated_master = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.id == master_id)
    ).mappings().first()

    return {
        "ok": True,
        "created": created,
        "link_created": link_created,
        "event_created": event_created,
        "master_id": master_id,
        "patient_uid": patient_uid,
        "nss_canonico": nss_10,
        "conflicto_identidad": bool((updated_master or {}).get("conflicto_identidad")),
    }


def get_master_identity_snapshot(db: Session, *, nss: Any, include_links: bool = True, links_limit: int = 200) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    nss_10 = normalize_nss_10(nss)
    if not nss_10:
        return {"ok": False, "reason": "NSS_INVALIDO"}
    row = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.nss_canonico == nss_10)
    ).mappings().first()
    if row is None:
        return {"ok": False, "reason": "NO_ENCONTRADO", "nss_canonico": nss_10}
    links: List[Dict[str, Any]] = []
    if include_links:
        links_rows = (
            db.execute(
                select(PATIENT_IDENTITY_LINKS)
                .where(PATIENT_IDENTITY_LINKS.c.master_id == int(row["id"]))
                .order_by(PATIENT_IDENTITY_LINKS.c.creado_en.desc(), PATIENT_IDENTITY_LINKS.c.id.desc())
                .limit(max(1, int(links_limit)))
            )
            .mappings()
            .all()
        )
        links = [_json_safe(dict(x)) for x in links_rows]
    return {
        "ok": True,
        "master": _json_safe(dict(row)),
        "links_total": int(
            db.execute(
                select(func.count(PATIENT_IDENTITY_LINKS.c.id)).where(PATIENT_IDENTITY_LINKS.c.master_id == int(row["id"]))
            ).scalar()
            or 0
        ),
        "links": links,
    }


def get_master_identity_journey(
    db: Session,
    *,
    nss: Any,
    from_date: Optional[Any] = None,
    to_date: Optional[Any] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    nss_10 = normalize_nss_10(nss)
    if not nss_10:
        return {"ok": False, "reason": "NSS_INVALIDO"}

    row = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.nss_canonico == nss_10)
    ).mappings().first()
    if row is None:
        return {"ok": False, "reason": "NO_ENCONTRADO", "nss_canonico": nss_10}

    master_id = int(row["id"])
    f = _safe_date(from_date)
    t = _safe_date(to_date)
    stmt = select(PATIENT_IDENTITY_EVENTS).where(PATIENT_IDENTITY_EVENTS.c.master_id == master_id)
    if f is not None:
        stmt = stmt.where(PATIENT_IDENTITY_EVENTS.c.fecha_evento >= f)
    if t is not None:
        stmt = stmt.where(PATIENT_IDENTITY_EVENTS.c.fecha_evento <= t)
    stmt = stmt.order_by(
        PATIENT_IDENTITY_EVENTS.c.fecha_evento.asc(),
        PATIENT_IDENTITY_EVENTS.c.id.asc(),
    ).limit(max(1, min(int(limit), 10000)))

    events = [dict(x) for x in db.execute(stmt).mappings().all()]
    module_counts: Dict[str, int] = {}
    event_type_counts: Dict[str, int] = {}
    for ev in events:
        module = _safe_text(ev.get("module")) or "NO_ASIGNADO"
        et = _safe_text(ev.get("event_type")) or "EVENTO"
        module_counts[module] = int(module_counts.get(module, 0) + 1)
        event_type_counts[et] = int(event_type_counts.get(et, 0) + 1)

    first_evt = events[0].get("fecha_evento") if events else None
    last_evt = events[-1].get("fecha_evento") if events else None
    span_days: Optional[int] = None
    first_date = _safe_date(first_evt)
    last_date = _safe_date(last_evt)
    if first_date and last_date:
        span_days = max(0, (last_date - first_date).days)

    return {
        "ok": True,
        "nss_canonico": nss_10,
        "master": _json_safe(dict(row)),
        "timeline_total": len(events),
        "timeline_span_days": span_days,
        "module_counts": module_counts,
        "event_type_counts": event_type_counts,
        "timeline": _json_safe(events),
    }


def master_identity_operational_stats(
    db: Session,
    *,
    months: int = 24,
    top_n: int = 20,
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    months = max(1, min(int(months), 120))
    top_n = max(1, min(int(top_n), 200))
    today = utcnow().date()
    # corte aproximado en días para compatibilidad SQLite/Postgres.
    window_start = today.replace(day=1)
    days_back = int(months * 30)
    window_start = date.fromordinal(max(1, window_start.toordinal() - days_back))

    total_master = int(db.execute(select(func.count(PATIENT_MASTER_IDENTITY.c.id))).scalar() or 0)
    total_events_window = int(
        db.execute(
            select(func.count(PATIENT_IDENTITY_EVENTS.c.id)).where(
                PATIENT_IDENTITY_EVENTS.c.fecha_evento >= window_start
            )
        ).scalar()
        or 0
    )
    active_patients_window = int(
        db.execute(
            select(func.count(func.distinct(PATIENT_IDENTITY_EVENTS.c.master_id))).where(
                PATIENT_IDENTITY_EVENTS.c.fecha_evento >= window_start
            )
        ).scalar()
        or 0
    )

    hosp_counts_rows = (
        db.execute(
            select(
                PATIENT_IDENTITY_EVENTS.c.master_id,
                func.count(PATIENT_IDENTITY_EVENTS.c.id).label("hosp_count"),
            )
            .where(
                and_(
                    PATIENT_IDENTITY_EVENTS.c.event_type == "HOSP_INGRESO",
                    PATIENT_IDENTITY_EVENTS.c.fecha_evento >= window_start,
                )
            )
            .group_by(PATIENT_IDENTITY_EVENTS.c.master_id)
        )
        .mappings()
        .all()
    )
    hosp_patients = len(hosp_counts_rows)
    reingresos_patients = sum(1 for r in hosp_counts_rows if int(r.get("hosp_count") or 0) >= 2)
    reingreso_rate = (float(reingresos_patients) / float(hosp_patients) * 100.0) if hosp_patients > 0 else 0.0

    top_recurrent_ids = [int(r["master_id"]) for r in sorted(hosp_counts_rows, key=lambda x: int(x.get("hosp_count") or 0), reverse=True)[:top_n]]
    top_recurrent: List[Dict[str, Any]] = []
    if top_recurrent_ids:
        master_rows = (
            db.execute(
                select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.id.in_(top_recurrent_ids))
            )
            .mappings()
            .all()
        )
        by_id = {int(x["id"]): dict(x) for x in master_rows}
        for r in sorted(hosp_counts_rows, key=lambda x: int(x.get("hosp_count") or 0), reverse=True)[:top_n]:
            mid = int(r["master_id"])
            mrow = by_id.get(mid, {})
            top_recurrent.append(
                {
                    "master_id": mid,
                    "patient_uid": mrow.get("patient_uid"),
                    "nss_canonico": mrow.get("nss_canonico"),
                    "nombre_canonico": mrow.get("nombre_canonico"),
                    "sexo_canonico": mrow.get("sexo_canonico"),
                    "hosp_ingresos_window": int(r.get("hosp_count") or 0),
                    "cirugias_acumuladas": int(mrow.get("cirugias_acumuladas") or 0),
                    "egresos_acumulados": int(mrow.get("egresos_acumulados") or 0),
                }
            )

    event_type_counts_rows = (
        db.execute(
            select(
                PATIENT_IDENTITY_EVENTS.c.event_type,
                func.count(PATIENT_IDENTITY_EVENTS.c.id).label("cnt"),
            )
            .where(PATIENT_IDENTITY_EVENTS.c.fecha_evento >= window_start)
            .group_by(PATIENT_IDENTITY_EVENTS.c.event_type)
            .order_by(func.count(PATIENT_IDENTITY_EVENTS.c.id).desc())
        )
        .mappings()
        .all()
    )
    module_counts_rows = (
        db.execute(
            select(
                PATIENT_IDENTITY_EVENTS.c.module,
                func.count(PATIENT_IDENTITY_EVENTS.c.id).label("cnt"),
            )
            .where(PATIENT_IDENTITY_EVENTS.c.fecha_evento >= window_start)
            .group_by(PATIENT_IDENTITY_EVENTS.c.module)
            .order_by(func.count(PATIENT_IDENTITY_EVENTS.c.id).desc())
        )
        .mappings()
        .all()
    )

    return {
        "ok": True,
        "window_start": window_start.isoformat(),
        "window_months": months,
        "total_master_identity": total_master,
        "active_patients_window": active_patients_window,
        "total_events_window": total_events_window,
        "hospitalizados_window": hosp_patients,
        "pacientes_reingresos_window": int(reingresos_patients),
        "tasa_reingreso_window_pct": round(float(reingreso_rate), 2),
        "event_type_counts": {str(r.get("event_type") or "EVENTO"): int(r.get("cnt") or 0) for r in event_type_counts_rows},
        "module_counts": {str(r.get("module") or "NO_ASIGNADO"): int(r.get("cnt") or 0) for r in module_counts_rows},
        "top_recurrentes": top_recurrent,
    }


def list_master_identity_conflicts(
    db: Session,
    *,
    limit: int = 200,
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    rows = (
        db.execute(
            select(PATIENT_MASTER_IDENTITY)
            .where(PATIENT_MASTER_IDENTITY.c.conflicto_identidad.is_(True))
            .order_by(PATIENT_MASTER_IDENTITY.c.actualizado_en.desc(), PATIENT_MASTER_IDENTITY.c.id.desc())
            .limit(max(1, min(int(limit), 5000)))
        )
        .mappings()
        .all()
    )
    return {
        "ok": True,
        "total_conflictos": len(rows),
        "items": [_json_safe(dict(x)) for x in rows],
    }


def resolve_master_identity_conflict(
    db: Session,
    *,
    master_id: int,
    resolver: str = "system",
    nota: str = "",
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    row = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.id == int(master_id))
    ).mappings().first()
    if row is None:
        return {"ok": False, "reason": "NO_ENCONTRADO", "master_id": int(master_id)}

    nota_txt = _safe_text(nota)
    resolver_txt = _safe_text(resolver) or "system"
    detalle_actual = _safe_text(row.get("detalle_conflicto"))
    marca = f"[RESUELTO {utcnow().isoformat()} por {resolver_txt}]"
    nuevo_detalle = " ".join([x for x in [detalle_actual, marca, nota_txt] if x]).strip()
    db.execute(
        update(PATIENT_MASTER_IDENTITY)
        .where(PATIENT_MASTER_IDENTITY.c.id == int(master_id))
        .values(
            conflicto_identidad=False,
            detalle_conflicto=nuevo_detalle[:2000] if nuevo_detalle else None,
            actualizado_en=utcnow(),
        )
    )
    db.commit()
    updated = db.execute(
        select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.id == int(master_id))
    ).mappings().first()
    return {
        "ok": True,
        "master_id": int(master_id),
        "master": _json_safe(dict(updated or {})),
    }


def backfill_master_identity(
    db: Session,
    m: Any,
    *,
    sdb: Optional[Session] = None,
    limit_per_source: int = 200000,
) -> Dict[str, Any]:
    ensure_master_identity_schema(db)
    summary: Dict[str, Any] = {
        "ok": True,
        "processed": 0,
        "created": 0,
        "linked": 0,
        "invalid_nss": 0,
        "sources": {},
    }

    def _consume(
        *,
        rows: List[Any],
        source_table: str,
        mapper,
    ) -> None:
        source_stats = summary["sources"].setdefault(
            source_table,
            {"rows": 0, "processed": 0, "created": 0, "linked": 0, "invalid_nss": 0, "errors": 0},
        )
        for row in rows:
            source_stats["rows"] += 1
            data = mapper(row)
            nss_10 = normalize_nss_10(data.get("nss"))
            if not nss_10:
                source_stats["invalid_nss"] += 1
                summary["invalid_nss"] += 1
                continue
            try:
                result = upsert_master_identity(
                    db,
                    nss=nss_10,
                    curp=data.get("curp"),
                    nombre=data.get("nombre"),
                    sexo=data.get("sexo"),
                    consulta_id=data.get("consulta_id"),
                    source_table=source_table,
                    source_pk=data.get("source_pk"),
                    module=data.get("module"),
                    fecha_evento=data.get("fecha_evento"),
                    payload=data.get("payload"),
                    commit=False,
                )
                source_stats["processed"] += 1
                summary["processed"] += 1
                if result.get("created"):
                    source_stats["created"] += 1
                    summary["created"] += 1
                if result.get("link_created"):
                    source_stats["linked"] += 1
                    summary["linked"] += 1
            except Exception:
                source_stats["errors"] += 1
                db.rollback()

    consultas = (
        db.query(m.ConsultaDB)
        .filter(m.ConsultaDB.nss.isnot(None))
        .order_by(m.ConsultaDB.id.asc())
        .limit(max(1, int(limit_per_source)))
        .all()
    )
    _consume(
        rows=consultas,
        source_table="consultas",
        mapper=lambda r: {
            "nss": getattr(r, "nss", None),
            "curp": getattr(r, "curp", None),
            "nombre": getattr(r, "nombre", None),
            "sexo": getattr(r, "sexo", None),
            "consulta_id": getattr(r, "id", None),
            "source_pk": getattr(r, "id", None),
            "module": "consulta_externa",
            "fecha_evento": getattr(r, "fecha_registro", None),
        },
    )

    hospitalizaciones = (
        db.query(m.HospitalizacionDB)
        .filter(m.HospitalizacionDB.nss.isnot(None))
        .order_by(m.HospitalizacionDB.id.asc())
        .limit(max(1, int(limit_per_source)))
        .all()
    )
    _consume(
        rows=hospitalizaciones,
        source_table="hospitalizaciones",
        mapper=lambda r: {
            "nss": getattr(r, "nss", None),
            "curp": None,
            "nombre": getattr(r, "nombre_completo", None),
            "sexo": getattr(r, "sexo", None),
            "consulta_id": getattr(r, "consulta_id", None),
            "source_pk": getattr(r, "id", None),
            "module": "hospitalizacion",
            "fecha_evento": getattr(r, "fecha_ingreso", None),
            "payload": {
                "ingreso_tipo": getattr(r, "ingreso_tipo", None),
                "cama": getattr(r, "cama", None),
            },
        },
    )

    # Egresos hospitalarios (tabla aditiva).
    try:
        from app.services.hospitalizacion_egreso_flow import HOSPITAL_EGRESOS, ensure_hospital_egreso_schema

        ensure_hospital_egreso_schema(db)
        egresos_rows = (
            db.execute(
                select(HOSPITAL_EGRESOS)
                .order_by(HOSPITAL_EGRESOS.c.id.asc())
                .limit(max(1, int(limit_per_source)))
            )
            .mappings()
            .all()
        )
        _consume(
            rows=egresos_rows,
            source_table="hospital_egresos",
            mapper=lambda r: {
                "nss": r.get("nss"),
                "curp": None,
                "nombre": r.get("nombre_completo"),
                "sexo": r.get("sexo"),
                "consulta_id": r.get("consulta_id"),
                "source_pk": r.get("id"),
                "module": "hospitalizacion_egreso",
                "fecha_evento": r.get("fecha_egreso"),
                "payload": {
                    "procedimiento_realizado": r.get("procedimiento_realizado"),
                    "dias_estancia": r.get("dias_estancia"),
                },
            },
        )
    except Exception:
        pass

    # Atenciones por submódulo de consulta externa.
    try:
        from app.services.consulta_externa_flow import CONSULTA_EXTERNA_ATENCIONES, ensure_consulta_externa_schema

        ensure_consulta_externa_schema(db)
        att_rows = (
            db.execute(
                select(CONSULTA_EXTERNA_ATENCIONES)
                .order_by(CONSULTA_EXTERNA_ATENCIONES.c.id.asc())
                .limit(max(1, int(limit_per_source)))
            )
            .mappings()
            .all()
        )
        _consume(
            rows=att_rows,
            source_table="consulta_externa_atenciones",
            mapper=lambda r: {
                "nss": r.get("nss"),
                "curp": None,
                "nombre": r.get("nombre"),
                "sexo": r.get("sexo"),
                "consulta_id": r.get("consulta_id"),
                "source_pk": r.get("id"),
                "module": r.get("servicio"),
                "fecha_evento": r.get("fecha_atencion"),
                "payload": {"servicio": r.get("servicio"), "diagnostico": r.get("diagnostico_principal")},
            },
        )
    except Exception:
        pass

    if sdb is not None:
        try:
            prog_rows = (
                sdb.query(m.SurgicalProgramacionDB)
                .filter(m.SurgicalProgramacionDB.nss.isnot(None))
                .order_by(m.SurgicalProgramacionDB.id.asc())
                .limit(max(1, int(limit_per_source)))
                .all()
            )
            _consume(
                rows=prog_rows,
                source_table="surgical_programaciones",
                mapper=lambda r: {
                    "nss": getattr(r, "nss", None),
                    "curp": getattr(r, "curp", None),
                    "nombre": getattr(r, "paciente_nombre", None),
                    "sexo": getattr(r, "sexo", None),
                    "consulta_id": getattr(r, "consulta_id", None),
                    "source_pk": getattr(r, "id", None),
                    "module": getattr(r, "modulo_origen", None) or "quirofano_programada",
                    "fecha_evento": getattr(r, "fecha_programada", None),
                    "payload": {"estatus": getattr(r, "estatus", None)},
                },
            )
        except Exception:
            pass

        try:
            urg_rows = (
                sdb.query(m.SurgicalUrgenciaProgramacionDB)
                .filter(m.SurgicalUrgenciaProgramacionDB.nss.isnot(None))
                .order_by(m.SurgicalUrgenciaProgramacionDB.id.asc())
                .limit(max(1, int(limit_per_source)))
                .all()
            )
            _consume(
                rows=urg_rows,
                source_table="surgical_urgencias_programaciones",
                mapper=lambda r: {
                    "nss": getattr(r, "nss", None),
                    "curp": getattr(r, "curp", None),
                    "nombre": getattr(r, "paciente_nombre", None),
                    "sexo": getattr(r, "sexo", None),
                    "consulta_id": getattr(r, "consulta_id", None),
                    "source_pk": getattr(r, "id", None),
                    "module": "quirofano_urgencias",
                    "fecha_evento": getattr(r, "fecha_urgencia", None),
                    "payload": {"estatus": getattr(r, "estatus", None)},
                },
            )
        except Exception:
            pass

    db.commit()
    summary["total_master"] = int(db.execute(select(func.count(PATIENT_MASTER_IDENTITY.c.id))).scalar() or 0)
    summary["total_links"] = int(db.execute(select(func.count(PATIENT_IDENTITY_LINKS.c.id))).scalar() or 0)
    summary["total_events"] = int(db.execute(select(func.count(PATIENT_IDENTITY_EVENTS.c.id))).scalar() or 0)
    return summary
