from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, func, insert, or_, select, update
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.hospitalization_episode import (
    HOSPITALIZATION_EPISODES,
    ensure_hospitalization_notes_schema,
)
from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
from app.services.event_log_flow import emit_event


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_nss(value: Any) -> str:
    return re.sub(r"\D", "", _safe_text(value))[:10]


def _norm_name(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_text(value).upper())


def _to_date(value: Any, *, fallback: Optional[date] = None) -> date:
    if isinstance(value, date):
        return value
    txt = _safe_text(value)
    if not txt:
        return fallback or date.today()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            continue
    return fallback or date.today()


def _dump(value: Any) -> str:
    try:
        return json.dumps(value or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return {}
    return {}


def _serialize_episode(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "patient_id": _safe_text(row.get("patient_id")),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "service": _safe_text(row.get("service")),
        "location": _safe_text(row.get("location")),
        "shift": _safe_text(row.get("shift")),
        "author_user_id": _safe_text(row.get("author_user_id")),
        "status": _safe_text(row.get("status")).upper() or "ACTIVO",
        "started_on": row.get("started_on").isoformat() if row.get("started_on") else "",
        "ended_on": row.get("ended_on").isoformat() if row.get("ended_on") else None,
        "metrics": _load(row.get("metrics_json")),
        "source_route": _safe_text(row.get("source_route")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        "active": bool(row.get("active", True)),
    }


def _serialize_note(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "episode_id": int(row["episode_id"]),
        "patient_id": _safe_text(row.get("patient_id")),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "note_date": row.get("note_date").isoformat() if row.get("note_date") else "",
        "note_type": _safe_text(row.get("note_type")).upper() or "EVOLUCION",
        "service": _safe_text(row.get("service")),
        "location": _safe_text(row.get("location")),
        "shift": _safe_text(row.get("shift")),
        "author_user_id": _safe_text(row.get("author_user_id")),
        "cie10_codigo": _safe_text(row.get("cie10_codigo")),
        "diagnostico": _safe_text(row.get("diagnostico")),
        "vitals": _load(row.get("vitals_json")),
        "labs": _load(row.get("labs_json")),
        "devices": _load(row.get("devices_json")),
        "events": _load(row.get("events_json")),
        "problem_list": _load(row.get("problem_list_json")),
        "plan_by_problem": _load(row.get("plan_by_problem_json")),
        "devices_snapshot": _load(row.get("devices_snapshot_json")),
        "io_summary": _load(row.get("io_summary_json")),
        "symptoms": _load(row.get("symptoms_json")),
        "events_pending": _load(row.get("events_pending_json")),
        "payload": _load(row.get("payload_json")),
        "note_text": _safe_text(row.get("note_text")),
        "status": _safe_text(row.get("status")).upper() or "BORRADOR",
        "is_final": bool(row.get("is_final")) if row.get("is_final") is not None else False,
        "version": int(row.get("version") or 1),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
    }


def resolve_patient_context(
    db: Session,
    m: Any,
    *,
    patient_id: Optional[str] = None,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
) -> Dict[str, Any]:
    pid = _norm_nss(patient_id or nss)
    consulta = None
    if consulta_id:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(consulta_id)).first()
    if consulta is None and pid:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nss == pid)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    if consulta is None and _safe_text(nombre):
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nombre.contains(_norm_name(nombre)))
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )

    if consulta is not None:
        pid = pid or _norm_nss(getattr(consulta, "nss", ""))
    display_name = _norm_name(nombre or (getattr(consulta, "nombre", "") if consulta else ""))
    consulta_ids: List[int] = []
    q = db.query(m.ConsultaDB)
    if pid:
        q = q.filter(m.ConsultaDB.nss == pid)
    elif display_name:
        q = q.filter(m.ConsultaDB.nombre.contains(display_name))
    for r in q.order_by(m.ConsultaDB.id.desc()).limit(800).all():
        if r.id is not None:
            consulta_ids.append(int(r.id))
    if consulta is not None and int(consulta.id) not in consulta_ids:
        consulta_ids.insert(0, int(consulta.id))

    return {
        "patient_id": pid,
        "consulta": consulta,
        "consulta_id": int(consulta.id) if consulta is not None else (int(consulta_id) if consulta_id else None),
        "consulta_ids": consulta_ids,
        "nombre": display_name,
    }


def create_or_get_active_episode(
    db: Session,
    m: Any,
    *,
    patient_id: Optional[str] = None,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
    service: Optional[str] = None,
    location: Optional[str] = None,
    shift: Optional[str] = None,
    author_user_id: Optional[str] = None,
    started_on: Optional[date] = None,
    source_route: str = "",
    metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_hospitalization_notes_schema(db)
    ctx = resolve_patient_context(
        db,
        m,
        patient_id=patient_id,
        consulta_id=consulta_id,
        nss=nss,
        nombre=nombre,
    )
    pid = _norm_nss(ctx.get("patient_id"))
    if len(pid) != 10:
        raise ValueError("No se pudo resolver NSS de 10 dígitos para crear episodio.")

    start_on = _to_date(started_on, fallback=date.today())
    query = select(HOSPITALIZATION_EPISODES).where(
        and_(
            HOSPITALIZATION_EPISODES.c.patient_id == pid,
            HOSPITALIZATION_EPISODES.c.status == "ACTIVO",
            HOSPITALIZATION_EPISODES.c.active.is_(True),
        )
    )
    if hospitalizacion_id:
        query = query.where(HOSPITALIZATION_EPISODES.c.hospitalizacion_id == int(hospitalizacion_id))
    row = db.execute(query.order_by(desc(HOSPITALIZATION_EPISODES.c.id)).limit(1)).mappings().first()

    values = {
        "consulta_id": ctx.get("consulta_id"),
        "hospitalizacion_id": int(hospitalizacion_id) if hospitalizacion_id is not None else None,
        "service": _safe_text(service).upper(),
        "location": _safe_text(location).upper(),
        "shift": _safe_text(shift).upper(),
        "author_user_id": _safe_text(author_user_id),
        "source_route": _safe_text(source_route),
        "metrics_json": _dump(metrics or {}),
        "updated_at": utcnow(),
    }
    if row:
        db.execute(
            update(HOSPITALIZATION_EPISODES)
            .where(HOSPITALIZATION_EPISODES.c.id == int(row["id"]))
            .values(**values)
        )
        db.flush()
        updated = db.execute(
            select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == int(row["id"])).limit(1)
        ).mappings().first()
        return _serialize_episode(updated) if updated else _serialize_episode(row)

    res = db.execute(
        insert(HOSPITALIZATION_EPISODES).values(
            patient_id=pid,
            consulta_id=ctx.get("consulta_id"),
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            service=_safe_text(service).upper(),
            location=_safe_text(location).upper(),
            shift=_safe_text(shift).upper(),
            author_user_id=_safe_text(author_user_id),
            status="ACTIVO",
            started_on=start_on,
            ended_on=None,
            metrics_json=_dump(metrics or {}),
            source_route=_safe_text(source_route),
            created_at=utcnow(),
            updated_at=utcnow(),
            active=True,
        )
    )
    episode_id = int(res.inserted_primary_key[0])
    row_new = db.execute(
        select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == episode_id).limit(1)
    ).mappings().first()
    if row_new:
        return _serialize_episode(row_new)
    raise ValueError("No se pudo crear episodio de hospitalización.")


def get_episode(db: Session, *, episode_id: int) -> Optional[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    row = db.execute(
        select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == int(episode_id)).limit(1)
    ).mappings().first()
    if not row:
        return None
    return _serialize_episode(row)


def get_active_episode_by_patient(db: Session, *, patient_id: str) -> Optional[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    pid = _norm_nss(patient_id)
    if not pid:
        return None
    row = db.execute(
        select(HOSPITALIZATION_EPISODES)
        .where(
            and_(
                HOSPITALIZATION_EPISODES.c.patient_id == pid,
                HOSPITALIZATION_EPISODES.c.status == "ACTIVO",
                HOSPITALIZATION_EPISODES.c.active.is_(True),
            )
        )
        .order_by(desc(HOSPITALIZATION_EPISODES.c.id))
        .limit(1)
    ).mappings().first()
    return _serialize_episode(row) if row else None


def list_patient_episodes(
    db: Session,
    *,
    patient_id: str,
    consulta_ids: Optional[List[int]] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    pid = _norm_nss(patient_id)
    q = select(HOSPITALIZATION_EPISODES)
    cond = []
    if pid:
        cond.append(HOSPITALIZATION_EPISODES.c.patient_id == pid)
    if consulta_ids:
        cond.append(HOSPITALIZATION_EPISODES.c.consulta_id.in_([int(x) for x in consulta_ids if x is not None]))
    if cond:
        q = q.where(or_(*cond))
    rows = db.execute(
        q.order_by(desc(HOSPITALIZATION_EPISODES.c.started_on), desc(HOSPITALIZATION_EPISODES.c.id)).limit(max(1, min(limit, 2000)))
    ).mappings().all()
    return [_serialize_episode(r) for r in rows]


def close_episode(
    db: Session,
    *,
    episode_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    patient_id: Optional[str] = None,
    ended_on: Optional[date] = None,
    summary_metrics: Optional[Dict[str, Any]] = None,
    author_user_id: str = "",
) -> Optional[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    q = select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.status == "ACTIVO")
    if episode_id:
        q = q.where(HOSPITALIZATION_EPISODES.c.id == int(episode_id))
    elif hospitalizacion_id:
        q = q.where(HOSPITALIZATION_EPISODES.c.hospitalizacion_id == int(hospitalizacion_id))
    elif patient_id:
        q = q.where(HOSPITALIZATION_EPISODES.c.patient_id == _norm_nss(patient_id))
    else:
        return None
    row = db.execute(q.order_by(desc(HOSPITALIZATION_EPISODES.c.id)).limit(1)).mappings().first()
    if not row:
        return None
    existing_metrics = _load(row.get("metrics_json"))
    if summary_metrics:
        existing_metrics.update(summary_metrics)
    db.execute(
        update(HOSPITALIZATION_EPISODES)
        .where(HOSPITALIZATION_EPISODES.c.id == int(row["id"]))
        .values(
            status="CERRADO",
            ended_on=_to_date(ended_on, fallback=date.today()),
            metrics_json=_dump(existing_metrics),
            author_user_id=_safe_text(author_user_id) or _safe_text(row.get("author_user_id")),
            updated_at=utcnow(),
            active=False,
        )
    )
    updated = db.execute(
        select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == int(row["id"])).limit(1)
    ).mappings().first()
    return _serialize_episode(updated) if updated else _serialize_episode(row)


def sync_episode_from_hospitalizacion(
    db: Session,
    m: Any,
    *,
    hospitalizacion_row: Any,
    actor: str = "system",
    source_route: str = "",
) -> Dict[str, Any]:
    return create_or_get_active_episode(
        db,
        m,
        patient_id=_safe_text(getattr(hospitalizacion_row, "nss", "")),
        consulta_id=getattr(hospitalizacion_row, "consulta_id", None),
        hospitalizacion_id=getattr(hospitalizacion_row, "id", None),
        service=_safe_text(getattr(hospitalizacion_row, "servicio", "")),
        location=_safe_text(getattr(hospitalizacion_row, "cama", "")),
        shift="",
        author_user_id=actor,
        started_on=getattr(hospitalizacion_row, "fecha_ingreso", None) or date.today(),
        source_route=source_route,
        metrics={
            "ingreso_tipo": _safe_text(getattr(hospitalizacion_row, "ingreso_tipo", "")),
            "urgencia_tipo": _safe_text(getattr(hospitalizacion_row, "urgencia_tipo", "")),
            "estado_clinico": _safe_text(getattr(hospitalizacion_row, "estado_clinico", "")),
            "estatus_hospitalizacion": _safe_text(getattr(hospitalizacion_row, "estatus", "")),
        },
    )


def sync_episodes_from_hospitalizaciones(db: Session, m: Any, *, hospitalizaciones: List[Any]) -> int:
    ensure_hospitalization_notes_schema(db)
    updated = 0
    for h in hospitalizaciones or []:
        try:
            episode = sync_episode_from_hospitalizacion(
                db,
                m,
                hospitalizacion_row=h,
                actor="sync",
                source_route="expediente_sync",
            )
            if _safe_text(getattr(h, "estatus", "")).upper() == "EGRESADO":
                close_episode(
                    db,
                    episode_id=episode.get("id"),
                    ended_on=getattr(h, "fecha_egreso", None) or date.today(),
                    summary_metrics={
                        "dias_estancia": getattr(h, "dias_hospitalizacion", None),
                        "estado_clinico_egreso": _safe_text(getattr(h, "estado_clinico", "")),
                    },
                    author_user_id="sync",
                )
            updated += 1
        except Exception:
            continue
    return updated


def _mirror_note_to_legacy(
    db: Session,
    *,
    note: Dict[str, Any],
    author_user_id: str,
) -> None:
    from app.services.expediente_nota_medica_flow import EXPEDIENTE_NOTAS_DIARIAS, ensure_expediente_nota_schema

    if note.get("consulta_id") is None:
        # Ruta API v1 puede crear notas sin consulta/hospitalización vinculada.
        # En ese caso no hay destino legacy compatible (consulta_id es obligatorio).
        return
    ensure_expediente_nota_schema(db)
    legacy_q = select(EXPEDIENTE_NOTAS_DIARIAS).where(
        and_(
            EXPEDIENTE_NOTAS_DIARIAS.c.hospitalizacion_id == note.get("hospitalizacion_id"),
            EXPEDIENTE_NOTAS_DIARIAS.c.fecha_nota == _to_date(note.get("note_date"), fallback=date.today()),
            EXPEDIENTE_NOTAS_DIARIAS.c.servicio_nota == _safe_text(note.get("service")),
        )
    )
    legacy = db.execute(legacy_q.order_by(desc(EXPEDIENTE_NOTAS_DIARIAS.c.id)).limit(1)).mappings().first()
    values = {
        "consulta_id": note.get("consulta_id"),
        "hospitalizacion_id": note.get("hospitalizacion_id"),
        "fecha_nota": _to_date(note.get("note_date"), fallback=date.today()),
        "nss": _norm_nss(note.get("patient_id")),
        "nombre": _norm_name((note.get("payload") or {}).get("nombre") or ""),
        "cama": _safe_text(note.get("location")),
        "servicio_nota": _safe_text(note.get("service")) or "HOSPITALIZACION",
        "cie10_codigo": _safe_text(note.get("cie10_codigo")),
        "diagnostico_cie10": _safe_text(note.get("diagnostico")),
        "hr": (note.get("vitals") or {}).get("hr"),
        "sbp": (note.get("vitals") or {}).get("sbp"),
        "dbp": (note.get("vitals") or {}).get("dbp"),
        "temp": (note.get("vitals") or {}).get("temp"),
        "peso": (note.get("vitals") or {}).get("peso"),
        "talla": (note.get("vitals") or {}).get("talla"),
        "imc": (note.get("vitals") or {}).get("imc"),
        "labs_json": _dump(note.get("labs") or {}),
        "nota_texto": _safe_text(note.get("note_text")),
        "creado_por": _safe_text(author_user_id) or "system",
        "creado_en": utcnow(),
    }
    if legacy:
        db.execute(
            update(EXPEDIENTE_NOTAS_DIARIAS)
            .where(EXPEDIENTE_NOTAS_DIARIAS.c.id == int(legacy["id"]))
            .values(**values)
        )
    else:
        db.execute(insert(EXPEDIENTE_NOTAS_DIARIAS).values(**values))


def upsert_daily_note(
    db: Session,
    *,
    episode_id: int,
    note_date: Optional[date] = None,
    note_type: str = "EVOLUCION",
    service: str = "UROLOGIA",
    location: str = "",
    shift: str = "",
    author_user_id: str = "system",
    cie10_codigo: str = "",
    diagnostico: str = "",
    vitals: Optional[Dict[str, Any]] = None,
    labs: Optional[Dict[str, Any]] = None,
    devices: Optional[Dict[str, Any]] = None,
    events: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    note_text: str = "",
    status: str = "BORRADOR",
    source_route: str = "",
    mirror_legacy: bool = True,
) -> Dict[str, Any]:
    ensure_hospitalization_notes_schema(db)
    ep = db.execute(
        select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == int(episode_id)).limit(1)
    ).mappings().first()
    if not ep:
        raise ValueError("episode_id no encontrado")

    nd = _to_date(note_date, fallback=date.today())
    srv = _safe_text(service).upper() or "UROLOGIA"
    existing = db.execute(
        select(INPATIENT_DAILY_NOTES).where(
            and_(
                INPATIENT_DAILY_NOTES.c.episode_id == int(episode_id),
                INPATIENT_DAILY_NOTES.c.note_date == nd,
                INPATIENT_DAILY_NOTES.c.service == srv,
            )
        ).limit(1)
    ).mappings().first()

    values = {
        "episode_id": int(episode_id),
        "patient_id": _norm_nss(ep.get("patient_id")),
        "consulta_id": ep.get("consulta_id"),
        "hospitalizacion_id": ep.get("hospitalizacion_id"),
        "note_date": nd,
        "note_type": _safe_text(note_type).upper() or "EVOLUCION",
        "service": srv,
        "location": _safe_text(location).upper(),
        "shift": _safe_text(shift).upper(),
        "author_user_id": _safe_text(author_user_id),
        "cie10_codigo": _safe_text(cie10_codigo).upper(),
        "diagnostico": _safe_text(diagnostico).upper(),
        "vitals_json": _dump(vitals or {}),
        "labs_json": _dump(labs or {}),
        "devices_json": _dump(devices or {}),
        "events_json": _dump(events or {}),
        "payload_json": _dump(payload or {}),
        "note_text": _safe_text(note_text),
        "status": _safe_text(status).upper() or "BORRADOR",
        "updated_at": utcnow(),
    }

    if existing:
        db.execute(
            update(INPATIENT_DAILY_NOTES)
            .where(INPATIENT_DAILY_NOTES.c.id == int(existing["id"]))
            .values(**values)
        )
        note_id = int(existing["id"])
    else:
        values["created_at"] = utcnow()
        res = db.execute(insert(INPATIENT_DAILY_NOTES).values(**values))
        note_id = int(res.inserted_primary_key[0])

    row = db.execute(select(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.id == note_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No se pudo guardar la nota intrahospitalaria")
    note = _serialize_note(row)
    if mirror_legacy:
        _mirror_note_to_legacy(db, note=note, author_user_id=author_user_id)

    try:
        emit_event(
            db,
            module="hospitalizacion_notes",
            event_type="INPATIENT_DAILY_NOTE_UPSERTED",
            entity="inpatient_daily_notes",
            entity_id=str(note_id),
            consulta_id=note.get("consulta_id"),
            actor=author_user_id or "system",
            source_route=source_route,
            payload={
                "episode_id": int(episode_id),
                "note_date": note.get("note_date"),
                "note_type": note.get("note_type"),
                "service": note.get("service"),
                "hospitalizacion_id": note.get("hospitalizacion_id"),
                "patient_id": note.get("patient_id"),
            },
            commit=False,
        )
    except Exception:
        pass
    return note


def get_daily_note(db: Session, *, note_id: int) -> Optional[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    row = db.execute(
        select(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.id == int(note_id)).limit(1)
    ).mappings().first()
    return _serialize_note(row) if row else None


def list_daily_notes(
    db: Session,
    *,
    episode_id: int,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    q = select(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.episode_id == int(episode_id))
    if date_from is not None:
        q = q.where(INPATIENT_DAILY_NOTES.c.note_date >= _to_date(date_from))
    if date_to is not None:
        q = q.where(INPATIENT_DAILY_NOTES.c.note_date <= _to_date(date_to))
    rows = db.execute(
        q.order_by(INPATIENT_DAILY_NOTES.c.note_date.asc(), INPATIENT_DAILY_NOTES.c.id.asc()).limit(max(1, min(limit, 5000)))
    ).mappings().all()
    return [_serialize_note(r) for r in rows]


def list_patient_daily_notes(
    db: Session,
    *,
    patient_id: str,
    consulta_ids: Optional[List[int]] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    pid = _norm_nss(patient_id)
    q = select(INPATIENT_DAILY_NOTES)
    cond = []
    if pid:
        cond.append(INPATIENT_DAILY_NOTES.c.patient_id == pid)
    if consulta_ids:
        cond.append(INPATIENT_DAILY_NOTES.c.consulta_id.in_([int(x) for x in consulta_ids if x is not None]))
    if cond:
        q = q.where(or_(*cond))
    rows = db.execute(
        q.order_by(desc(INPATIENT_DAILY_NOTES.c.note_date), desc(INPATIENT_DAILY_NOTES.c.id)).limit(max(1, min(limit, 5000)))
    ).mappings().all()
    return [_serialize_note(r) for r in rows]


def summarize_patient_episodes(
    db: Session,
    m: Any,
    *,
    patient_id: str,
    consulta_ids: Optional[List[int]] = None,
    episodes: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    ensure_hospitalization_notes_schema(db)
    eps = episodes or list_patient_episodes(db, patient_id=patient_id, consulta_ids=consulta_ids, limit=800)
    if not eps:
        return []

    note_counts: Dict[int, int] = defaultdict(int)
    first_note: Dict[int, str] = {}
    last_note: Dict[int, str] = {}
    notes = list_patient_daily_notes(db, patient_id=patient_id, consulta_ids=consulta_ids, limit=4000)
    for n in notes:
        eid = int(n.get("episode_id") or 0)
        if eid <= 0:
            continue
        note_counts[eid] += 1
        nd = _safe_text(n.get("note_date"))
        if not first_note.get(eid) or nd < first_note[eid]:
            first_note[eid] = nd
        if not last_note.get(eid) or nd > last_note[eid]:
            last_note[eid] = nd

    consulta_id_set = {int(x) for x in (consulta_ids or []) if x is not None}
    qx_by_episode: Dict[int, Dict[str, Any]] = defaultdict(lambda: {"opero": False, "procedimientos": set(), "cirujanos": set(), "sangrado_total_ml": 0.0})
    if consulta_id_set:
        sdb = m._new_surgical_session(enable_dual_write=True)
        try:
            surg_rows = (
                sdb.query(m.SurgicalProgramacionDB)
                .filter(m.SurgicalProgramacionDB.consulta_id.in_(list(consulta_id_set)))
                .all()
            )
            postq_rows = (
                sdb.query(m.SurgicalPostquirurgicaDB)
                .filter(
                    m.SurgicalPostquirurgicaDB.surgical_programacion_id.in_([r.id for r in surg_rows if r.id is not None])
                )
                .all()
                if surg_rows
                else []
            )
            postq_by_sp: Dict[int, Any] = {}
            for pq in postq_rows:
                sid = int(getattr(pq, "surgical_programacion_id", 0) or 0)
                if sid <= 0:
                    continue
                prev = postq_by_sp.get(sid)
                if prev is None or (getattr(pq, "fecha_realizacion", None) or date.min) >= (getattr(prev, "fecha_realizacion", None) or date.min):
                    postq_by_sp[sid] = pq

            for ep in eps:
                eid = int(ep.get("id") or 0)
                if eid <= 0:
                    continue
                ep_start = _to_date(ep.get("started_on"), fallback=date.today())
                ep_end = _to_date(ep.get("ended_on"), fallback=ep_start)
                ep_consulta = int(ep.get("consulta_id") or 0)
                for sp in surg_rows:
                    if ep_consulta and int(getattr(sp, "consulta_id", 0) or 0) != ep_consulta:
                        continue
                    sp_date = getattr(sp, "fecha_realizacion", None) or getattr(sp, "fecha_programada", None)
                    if sp_date is None or sp_date < ep_start or sp_date > ep_end:
                        continue
                    info = qx_by_episode[eid]
                    info["opero"] = True
                    proc = _safe_text(getattr(sp, "procedimiento_realizado", None) or getattr(sp, "procedimiento_programado", None) or getattr(sp, "procedimiento", None))
                    if proc:
                        info["procedimientos"].add(proc.upper())
                    cir = _safe_text(getattr(sp, "cirujano", None))
                    if cir:
                        info["cirujanos"].add(cir.upper())
                    sangrado = getattr(sp, "sangrado_ml", None)
                    if sangrado is not None:
                        try:
                            info["sangrado_total_ml"] += float(sangrado)
                        except Exception:
                            pass
                    pq = postq_by_sp.get(int(sp.id or 0))
                    if pq is not None:
                        cir2 = _safe_text(getattr(pq, "cirujano", None))
                        if cir2:
                            info["cirujanos"].add(cir2.upper())
                        sang2 = getattr(pq, "sangrado_ml", None)
                        if sang2 is not None:
                            try:
                                info["sangrado_total_ml"] += float(sang2)
                            except Exception:
                                pass
                        proc2 = _safe_text(getattr(pq, "procedimiento_realizado", None))
                        if proc2:
                            info["procedimientos"].add(proc2.upper())
        finally:
            sdb.close()

    out: List[Dict[str, Any]] = []
    for idx, ep in enumerate(sorted(eps, key=lambda x: x.get("started_on") or "", reverse=True), start=1):
        eid = int(ep.get("id") or 0)
        start_on = _to_date(ep.get("started_on"), fallback=date.today())
        ended_txt = ep.get("ended_on")
        end_on = _to_date(ended_txt, fallback=date.today()) if ended_txt else date.today()
        qx = qx_by_episode.get(eid, {})
        out.append(
            {
                "episode_id": eid,
                "tipo": "HOSPITALIZACION ACTIVA" if _safe_text(ep.get("status")).upper() == "ACTIVO" else ("ULTIMA HOSPITALIZACION" if idx == 1 else f"HOSPITALIZACION PREVIA #{idx-1}"),
                "started_on": ep.get("started_on"),
                "ended_on": ep.get("ended_on"),
                "duracion_dias": max((end_on - start_on).days, 0) if end_on and start_on else None,
                "service": ep.get("service"),
                "location": ep.get("location"),
                "status": ep.get("status"),
                "consulta_id": ep.get("consulta_id"),
                "hospitalizacion_id": ep.get("hospitalizacion_id"),
                "note_count": int(note_counts.get(eid, 0)),
                "first_note_date": first_note.get(eid),
                "last_note_date": last_note.get(eid),
                "se_opero": bool(qx.get("opero")),
                "procedimientos": sorted(list(qx.get("procedimientos") or []))[:6],
                "cirujanos": sorted(list(qx.get("cirujanos") or []))[:6],
                "sangrado_total_ml": round(float(qx.get("sangrado_total_ml") or 0.0), 2),
            }
        )
    return out
