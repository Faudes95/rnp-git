from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, insert, select, update
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.hospitalization_episode import HOSPITALIZATION_EPISODES, ensure_hospitalization_notes_schema
from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
from app.models.inpatient_ai_models import (
    ALERT_ACTION_METADATA,
    CLINICAL_TAGS,
    LAB_RESULTS,
    ensure_inpatient_time_series_schema,
)
from app.services.hospitalization_notes_flow import create_or_get_active_episode


LAB_TESTS_MINIMAL = {
    "creatinine",
    "bun",
    "wbc",
    "lactate",
    "hb",
    "platelets",
    "crp",
    "pct",
}
RESOLUTION_REASONS = {"true_positive", "false_positive", "handled", ""}


class DailyNoteConflictError(ValueError):
    pass


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_lower(value: Any) -> str:
    return _safe_text(value).lower()


def _safe_upper(value: Any) -> str:
    return _safe_text(value).upper()


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
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _to_dt(value: Any, *, fallback: Optional[datetime] = None) -> datetime:
    if isinstance(value, datetime):
        return value
    txt = _safe_text(value)
    if not txt:
        return fallback or utcnow()
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return fallback or utcnow()


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


def _ensure_schema(db: Session) -> None:
    ensure_hospitalization_notes_schema(db)
    ensure_inpatient_time_series_schema(db)


def _serialize_lab(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "collected_at": row.get("collected_at").isoformat() if row.get("collected_at") else "",
        "test_name": _safe_lower(row.get("test_name")),
        "value_num": row.get("value_num"),
        "value_text": _safe_text(row.get("value_text")),
        "unit": _safe_text(row.get("unit")),
        "source": _safe_text(row.get("source")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def add_lab(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    collected_at: Optional[datetime] = None,
    test_name: str,
    value_num: Optional[float] = None,
    value_text: Optional[str] = None,
    unit: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    test = _safe_lower(test_name)
    if not test:
        raise ValueError("test_name es obligatorio.")
    # Mantiene compatibilidad: acepta otros analitos, pero normaliza los mínimos.
    if test in LAB_TESTS_MINIMAL and value_num is None and not _safe_text(value_text):
        raise ValueError("Para analitos mínimos debe enviar value_num o value_text.")

    ins = db.execute(
        insert(LAB_RESULTS).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            collected_at=_to_dt(collected_at, fallback=utcnow()),
            test_name=test,
            value_num=float(value_num) if value_num is not None else None,
            value_text=_safe_text(value_text) or None,
            unit=_safe_text(unit) or None,
            source=_safe_text(source) or "API_INPATIENT_LABS",
            created_at=utcnow(),
        )
    )
    lab_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(LAB_RESULTS).where(LAB_RESULTS.c.id == lab_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar laboratorio.")
    return _serialize_lab(row)


def list_labs(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    _ensure_schema(db)
    q = select(LAB_RESULTS)
    filters = []
    if consulta_id is not None:
        filters.append(LAB_RESULTS.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(LAB_RESULTS.c.hospitalizacion_id == int(hospitalizacion_id))
    if date_from is not None:
        filters.append(LAB_RESULTS.c.collected_at >= _to_dt(date_from))
    if date_to is not None:
        filters.append(LAB_RESULTS.c.collected_at <= _to_dt(date_to))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(LAB_RESULTS.c.collected_at.asc(), LAB_RESULTS.c.id.asc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_lab(r) for r in rows]


def _resolve_episode_row(
    db: Session,
    *,
    consulta_id: Optional[int],
    hospitalizacion_id: Optional[int],
    consulta_patient_id: Optional[str] = None,
) -> Dict[str, Any]:
    row = None
    if hospitalizacion_id is not None:
        row = db.execute(
            select(HOSPITALIZATION_EPISODES)
            .where(HOSPITALIZATION_EPISODES.c.hospitalizacion_id == int(hospitalizacion_id))
            .order_by(desc(HOSPITALIZATION_EPISODES.c.active), desc(HOSPITALIZATION_EPISODES.c.id))
            .limit(1)
        ).mappings().first()
    if row is None and consulta_id is not None:
        row = db.execute(
            select(HOSPITALIZATION_EPISODES)
            .where(HOSPITALIZATION_EPISODES.c.consulta_id == int(consulta_id))
            .order_by(desc(HOSPITALIZATION_EPISODES.c.active), desc(HOSPITALIZATION_EPISODES.c.id))
            .limit(1)
        ).mappings().first()
    if row is not None:
        return row

    from app.core.app_context import main_proxy as m

    episode = create_or_get_active_episode(
        db,
        m,
        patient_id=_safe_text(consulta_patient_id) or None,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        service="HOSPITALIZACION",
        source_route="/api/inpatient/daily-note",
    )
    episode_id = int(episode["id"])
    row = db.execute(
        select(HOSPITALIZATION_EPISODES).where(HOSPITALIZATION_EPISODES.c.id == episode_id).limit(1)
    ).mappings().first()
    if row is None:
        raise ValueError("No fue posible resolver episodio de hospitalización.")
    return row


def _serialize_daily_note(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "episode_id": int(row["episode_id"]),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "consulta_id": row.get("consulta_id"),
        "note_date": row.get("note_date").isoformat() if row.get("note_date") else "",
        "service": _safe_text(row.get("service")),
        "location": _safe_text(row.get("location")),
        "cie10_codigo": _safe_text(row.get("cie10_codigo")),
        "diagnostico": _safe_text(row.get("diagnostico")),
        "author_user_id": _safe_text(row.get("author_user_id")),
        "vitals_json": _load(row.get("vitals_json")),
        "labs_json": _load(row.get("labs_json")),
        "problem_list_json": _load(row.get("problem_list_json")),
        "plan_by_problem_json": _load(row.get("plan_by_problem_json")),
        "devices_snapshot_json": _load(row.get("devices_snapshot_json")),
        "io_summary_json": _load(row.get("io_summary_json")),
        "symptoms_json": _load(row.get("symptoms_json")),
        "events_pending_json": _load(row.get("events_pending_json")),
        "free_text": _safe_text(row.get("free_text")),
        "is_final": bool(row.get("is_final", False)),
        "version": int(row.get("version") or 1),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
    }


def upsert_daily_note(
    db: Session,
    *,
    hospitalizacion_id: Optional[int],
    consulta_id: Optional[int],
    note_date: date,
    author_user_id: Optional[str],
    problem_list_json: Optional[Dict[str, Any]],
    plan_by_problem_json: Optional[Dict[str, Any]],
    devices_snapshot_json: Optional[Dict[str, Any]],
    io_summary_json: Optional[Dict[str, Any]],
    symptoms_json: Optional[Dict[str, Any]],
    events_pending_json: Optional[Dict[str, Any]],
    free_text: Optional[str],
    is_final: bool,
    upsert: bool = False,
    consulta_patient_id: Optional[str] = None,
    note_type: str = "EVOLUCION",
    service: Optional[str] = None,
    location: Optional[str] = None,
    cie10_codigo: Optional[str] = None,
    diagnostico: Optional[str] = None,
    vitals_json: Optional[Dict[str, Any]] = None,
    labs_json: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure_schema(db)
    if hospitalizacion_id is None and consulta_id is None:
        raise ValueError("Debe enviar hospitalizacion_id o consulta_id.")

    episode = _resolve_episode_row(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        consulta_patient_id=consulta_patient_id,
    )
    nd = _to_date(note_date, fallback=date.today())
    hosp_id = hospitalizacion_id if hospitalizacion_id is not None else episode.get("hospitalizacion_id")
    cons_id = consulta_id if consulta_id is not None else episode.get("consulta_id")
    episode_id = int(episode["id"])

    existing_q = select(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.note_date == nd)
    if hosp_id is not None:
        existing_q = existing_q.where(INPATIENT_DAILY_NOTES.c.hospitalizacion_id == int(hosp_id))
    else:
        existing_q = existing_q.where(INPATIENT_DAILY_NOTES.c.episode_id == episode_id)
    existing = db.execute(existing_q.order_by(desc(INPATIENT_DAILY_NOTES.c.id)).limit(1)).mappings().first()

    if existing is not None and not upsert:
        raise DailyNoteConflictError("Ya existe una nota diaria para hospitalizacion_id/note_date.")

    version = 1
    if existing is not None:
        version = int(existing.get("version") or 1) + 1

    values = {
        "episode_id": episode_id,
        "patient_id": _safe_text(episode.get("patient_id")),
        "consulta_id": int(cons_id) if cons_id is not None else None,
        "hospitalizacion_id": int(hosp_id) if hosp_id is not None else None,
        "note_date": nd,
        "note_type": _safe_upper(note_type) or "EVOLUCION",
        "service": _safe_upper(service or episode.get("service")) or "HOSPITALIZACION",
        "location": _safe_upper(location or episode.get("location")),
        "shift": _safe_upper(episode.get("shift")),
        "author_user_id": _safe_text(author_user_id) or "system",
        "cie10_codigo": _safe_upper(cie10_codigo),
        "diagnostico": _safe_upper(diagnostico),
        "vitals_json": _dump(vitals_json or {}),
        "labs_json": _dump(labs_json or {}),
        "problem_list_json": _dump(problem_list_json or {}),
        "plan_by_problem_json": _dump(plan_by_problem_json or {}),
        "devices_snapshot_json": _dump(devices_snapshot_json or {}),
        "io_summary_json": _dump(io_summary_json or {}),
        "symptoms_json": _dump(symptoms_json or {}),
        "events_pending_json": _dump(events_pending_json or {}),
        "free_text": _safe_text(free_text),
        "is_final": bool(is_final),
        "version": version,
        "note_text": _safe_text(free_text),
        "payload_json": _dump({"source": "api_inpatient_daily_note"}),
        "status": "FINAL" if is_final else "BORRADOR",
        "updated_at": utcnow(),
    }

    if existing is not None:
        db.execute(update(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.id == int(existing["id"])).values(**values))
        note_id = int(existing["id"])
    else:
        values["created_at"] = utcnow()
        ins = db.execute(insert(INPATIENT_DAILY_NOTES).values(**values))
        note_id = int(ins.inserted_primary_key[0])

    row = db.execute(select(INPATIENT_DAILY_NOTES).where(INPATIENT_DAILY_NOTES.c.id == note_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar nota diaria.")
    return _serialize_daily_note(row)


def list_daily_notes(
    db: Session,
    *,
    hospitalizacion_id: Optional[int] = None,
    consulta_id: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    _ensure_schema(db)
    q = select(INPATIENT_DAILY_NOTES)
    filters = []
    if hospitalizacion_id is not None:
        filters.append(INPATIENT_DAILY_NOTES.c.hospitalizacion_id == int(hospitalizacion_id))
    if consulta_id is not None:
        filters.append(INPATIENT_DAILY_NOTES.c.consulta_id == int(consulta_id))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(INPATIENT_DAILY_NOTES.c.note_date.asc(), INPATIENT_DAILY_NOTES.c.id.asc()).limit(max(1, min(limit, 5000)))
    ).mappings().all()
    return [_serialize_daily_note(r) for r in rows]


def _serialize_alert_action(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "alert_id": int(row["alert_id"]),
        "ack_by": _safe_text(row.get("ack_by")),
        "ack_at": row.get("ack_at").isoformat() if row.get("ack_at") else None,
        "resolved_by": _safe_text(row.get("resolved_by")),
        "resolved_at": row.get("resolved_at").isoformat() if row.get("resolved_at") else None,
        "resolution_reason": _safe_text(row.get("resolution_reason")),
        "action_taken_json": _load(row.get("action_taken_json")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
    }


def ack_alert(db: Session, *, alert_id: int, ack_by: Optional[str] = None) -> Dict[str, Any]:
    _ensure_schema(db)
    row = db.execute(
        select(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.alert_id == int(alert_id)).limit(1)
    ).mappings().first()
    values = {
        "ack_by": _safe_text(ack_by) or "system",
        "ack_at": utcnow(),
        "updated_at": utcnow(),
    }
    if row is None:
        ins = db.execute(
            insert(ALERT_ACTION_METADATA).values(
                alert_id=int(alert_id),
                ack_by=values["ack_by"],
                ack_at=values["ack_at"],
                resolved_by=None,
                resolved_at=None,
                resolution_reason=None,
                action_taken_json=_dump({}),
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
        meta_id = int(ins.inserted_primary_key[0])
    else:
        meta_id = int(row["id"])
        db.execute(update(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.id == meta_id).values(**values))

    latest = db.execute(select(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.id == meta_id).limit(1)).mappings().first()
    if not latest:
        raise ValueError("No fue posible registrar ACK.")
    return _serialize_alert_action(latest)


def resolve_alert(
    db: Session,
    *,
    alert_id: int,
    resolved_by: Optional[str] = None,
    resolution_reason: Optional[str] = None,
    action_taken_json: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure_schema(db)
    reason = _safe_lower(resolution_reason)
    if reason not in RESOLUTION_REASONS:
        raise ValueError("resolution_reason inválido. Use true_positive/false_positive/handled.")
    row = db.execute(
        select(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.alert_id == int(alert_id)).limit(1)
    ).mappings().first()
    values = {
        "resolved_by": _safe_text(resolved_by) or "system",
        "resolved_at": utcnow(),
        "resolution_reason": reason or "handled",
        "action_taken_json": _dump(action_taken_json or {}),
        "updated_at": utcnow(),
    }
    if row is None:
        ins = db.execute(
            insert(ALERT_ACTION_METADATA).values(
                alert_id=int(alert_id),
                ack_by=None,
                ack_at=None,
                resolved_by=values["resolved_by"],
                resolved_at=values["resolved_at"],
                resolution_reason=values["resolution_reason"],
                action_taken_json=values["action_taken_json"],
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
        meta_id = int(ins.inserted_primary_key[0])
    else:
        meta_id = int(row["id"])
        db.execute(update(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.id == meta_id).values(**values))

    latest = db.execute(select(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.id == meta_id).limit(1)).mappings().first()
    if not latest:
        raise ValueError("No fue posible resolver alerta.")
    return _serialize_alert_action(latest)


def list_alert_actions(
    db: Session,
    *,
    alert_ids: Optional[List[int]] = None,
    limit: int = 2000,
) -> Dict[int, Dict[str, Any]]:
    _ensure_schema(db)
    q = select(ALERT_ACTION_METADATA)
    if alert_ids:
        clean_ids = [int(x) for x in alert_ids if x is not None]
        if clean_ids:
            q = q.where(ALERT_ACTION_METADATA.c.alert_id.in_(clean_ids))
    rows = db.execute(
        q.order_by(ALERT_ACTION_METADATA.c.updated_at.desc(), ALERT_ACTION_METADATA.c.id.desc())
        .limit(max(1, min(limit, 5000)))
    ).mappings().all()
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        key = int(r["alert_id"])
        if key in out:
            continue
        out[key] = _serialize_alert_action(r)
    return out


def _serialize_tag(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "tag_type": _safe_upper(row.get("tag_type")),
        "tag_value": _safe_text(row.get("tag_value")),
        "laterality": _safe_upper(row.get("laterality")),
        "severity": _safe_upper(row.get("severity")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def add_tag(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    tag_type: str,
    tag_value: str,
    laterality: Optional[str] = None,
    severity: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    ttype = _safe_upper(tag_type)
    tvalue = _safe_text(tag_value)
    if not ttype or not tvalue:
        raise ValueError("tag_type y tag_value son obligatorios.")

    ins = db.execute(
        insert(CLINICAL_TAGS).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            tag_type=ttype,
            tag_value=tvalue,
            laterality=_safe_upper(laterality) or None,
            severity=_safe_upper(severity) or None,
            created_at=utcnow(),
        )
    )
    tag_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(CLINICAL_TAGS).where(CLINICAL_TAGS.c.id == tag_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar tag.")
    return _serialize_tag(row)


def list_tags(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    tag_type: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    _ensure_schema(db)
    q = select(CLINICAL_TAGS)
    filters = []
    if consulta_id is not None:
        filters.append(CLINICAL_TAGS.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(CLINICAL_TAGS.c.hospitalizacion_id == int(hospitalizacion_id))
    if _safe_text(tag_type):
        filters.append(CLINICAL_TAGS.c.tag_type == _safe_upper(tag_type))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(CLINICAL_TAGS.c.created_at.desc(), CLINICAL_TAGS.c.id.desc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_tag(r) for r in rows]
