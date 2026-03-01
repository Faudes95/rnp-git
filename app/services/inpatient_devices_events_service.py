from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, insert, select, update
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.inpatient_ai_models import (
    CLINICAL_EVENT_LOG,
    DEVICES_TS,
    UROLOGY_DEVICES,
    ensure_inpatient_time_series_schema,
)

DEVICE_TYPES_ALLOWED = {
    # Legacy
    "FOLEY",
    "JJ_STENT",
    "NEPHROSTOMY",
    "SURGICAL_DRAIN",
    # Dispositivos clínicos (UI español)
    "SONDA FOLEY",
    "CATETER JJ",
    "CATETER URETERAL",
    "CATETER URETERAL POR REPARACION POR FISTULA VESICOVAGINAL",
    # Drenajes clínicos (UI español)
    "PENROSE",
    "SARATOGA",
    "JACKSON",
    "NEFROSTOMIA",
    "CONDUCTO ILEAL",
    "URETEROSTOMA",
    "DRENAJE PELVICO",
}
SIDE_ALLOWED = {"L", "R", "BILAT", "NA", ""}
DIFFICULTY_ALLOWED = {"EASY", "MODERATE", "HARD", ""}
EVENT_TYPES_ALLOWED = {
    "ABX_STARTED",
    "ANALGESIA_LEVEL_SET",
    "ANTICOAG_FLAG_SET",
    "US_DOPPLER_SCROTUM_ORDERED",
    "US_DOPPLER_SCROTUM_DONE",
    "CT_UROGRAM_ORDERED",
    "CT_UROGRAM_DONE",
    "CYSTOSCOPY_DONE",
    "URS_DONE",
    "PCNL_DONE",
    "URINALYSIS_ORDERED",
    "URINALYSIS_RESULT",
    "URINE_CULTURE_ORDERED",
    "URINE_CULTURE_RESULT",
    "ICU_TRANSFER",
    "RETURN_TO_OR",
    "DISCHARGE",
    # Aditivo: control estructurado de drenajes/dispositivos
    "DRAINAGE_STATUS_SET",
    "DEVICE_STATUS_SET",
    "DRAIN_OUTPUT_RECORDED",
    "FOLEY_URESIS_RECORDED",
}
INPATIENT_EVENTS_MODULE = "INPATIENT_EVENTS"


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_upper(value: Any) -> str:
    return _safe_text(value).upper()


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
            out = json.loads(value)
            if isinstance(out, dict):
                return out
        except Exception:
            return {}
    return {}


def _serialize_device(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "device_type": _safe_upper(row.get("device_type")),
        "present": bool(row.get("present", True)),
        "inserted_at": row.get("inserted_at").isoformat() if row.get("inserted_at") else None,
        "removed_at": row.get("removed_at").isoformat() if row.get("removed_at") else None,
        "side": _safe_upper(row.get("side")),
        "location": _safe_text(row.get("location")),
        "size_fr": _safe_text(row.get("size_fr")),
        "difficulty": _safe_upper(row.get("difficulty")),
        "irrigation": row.get("irrigation"),
        "planned_removal_at": row.get("planned_removal_at").isoformat() if row.get("planned_removal_at") else None,
        "planned_change_at": row.get("planned_change_at").isoformat() if row.get("planned_change_at") else None,
        "notes": _safe_text(row.get("notes")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def _serialize_event(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "event_time": row.get("event_time").isoformat() if row.get("event_time") else "",
        "event_type": _safe_upper(row.get("event_type")),
        "payload": _load(row.get("payload_json")),
        "module": _safe_text(row.get("module")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def _device_family_from_type(device_type: str) -> str:
    dt = _safe_upper(device_type)
    if dt in {"PENROSE", "SARATOGA", "JACKSON", "NEFROSTOMIA", "CONDUCTO ILEAL", "URETEROSTOMA", "DRENAJE PELVICO", "SURGICAL_DRAIN"}:
        return "DRENAJE"
    return "DISPOSITIVO"


def _insert_device_ts(
    db: Session,
    *,
    consulta_id: Optional[int],
    hospitalizacion_id: Optional[int],
    ts: Optional[datetime],
    device_type: str,
    present: Optional[bool],
    side: Optional[str] = None,
    size_fr: Optional[str] = None,
    flow_ml: Optional[float] = None,
    notes: Optional[str] = None,
    source: str = "inpatient_devices_events_service",
) -> None:
    db.execute(
        insert(DEVICES_TS).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            ts=_to_dt(ts, fallback=utcnow()),
            device_family=_device_family_from_type(device_type),
            device_type=_safe_upper(device_type),
            present=present if present is None else bool(present),
            side=_safe_upper(side) or None,
            size_fr=_safe_text(size_fr) or None,
            flow_ml=flow_ml,
            notes=_safe_text(notes) or None,
            source=_safe_text(source) or "inpatient_devices_events_service",
            created_at=utcnow(),
        )
    )


def add_device(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    device_type: str,
    present: bool = True,
    inserted_at: Optional[datetime] = None,
    removed_at: Optional[datetime] = None,
    side: Optional[str] = None,
    location: Optional[str] = None,
    size_fr: Optional[str] = None,
    difficulty: Optional[str] = None,
    irrigation: Optional[bool] = None,
    planned_removal_at: Optional[datetime] = None,
    planned_change_at: Optional[datetime] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_inpatient_time_series_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    dtype = _safe_upper(device_type)
    if dtype not in DEVICE_TYPES_ALLOWED:
        raise ValueError(f"device_type inválido: {dtype}")
    side_n = _safe_upper(side)
    if side_n not in SIDE_ALLOWED:
        raise ValueError("side inválido. Use L/R/BILAT/NA.")
    diff_n = _safe_upper(difficulty)
    if diff_n not in DIFFICULTY_ALLOWED:
        raise ValueError("difficulty inválido. Use EASY/MODERATE/HARD.")

    ins = db.execute(
        insert(UROLOGY_DEVICES).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            device_type=dtype,
            present=bool(present),
            inserted_at=_to_dt(inserted_at, fallback=utcnow()) if inserted_at is not None else None,
            removed_at=_to_dt(removed_at) if removed_at is not None else None,
            side=side_n or None,
            location=_safe_text(location) or None,
            size_fr=_safe_text(size_fr) or None,
            difficulty=diff_n or None,
            irrigation=irrigation,
            planned_removal_at=_to_dt(planned_removal_at) if planned_removal_at is not None else None,
            planned_change_at=_to_dt(planned_change_at) if planned_change_at is not None else None,
            notes=_safe_text(notes) or None,
            created_at=utcnow(),
        )
    )
    device_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(UROLOGY_DEVICES).where(UROLOGY_DEVICES.c.id == device_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar device.")
    try:
        _insert_device_ts(
            db,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            ts=(inserted_at or utcnow()),
            device_type=dtype,
            present=bool(present),
            side=side_n or None,
            size_fr=_safe_text(size_fr) or None,
            notes=_safe_text(notes) or None,
            source="add_device",
        )
    except Exception:
        # Aditivo: no bloquear captura clínica principal por traza temporal.
        pass
    return _serialize_device(row)


def list_devices(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_inpatient_time_series_schema(db)
    q = select(UROLOGY_DEVICES)
    filters = []
    if consulta_id is not None:
        filters.append(UROLOGY_DEVICES.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(UROLOGY_DEVICES.c.hospitalizacion_id == int(hospitalizacion_id))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(UROLOGY_DEVICES.c.created_at.desc(), UROLOGY_DEVICES.c.id.desc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_device(r) for r in rows]


def update_device(
    db: Session,
    *,
    device_id: int,
    present: Optional[bool] = None,
    removed_at: Optional[datetime] = None,
    planned_removal_at: Optional[datetime] = None,
    planned_change_at: Optional[datetime] = None,
    notes: Optional[str] = None,
    irrigation: Optional[bool] = None,
    side: Optional[str] = None,
    location: Optional[str] = None,
    size_fr: Optional[str] = None,
    difficulty: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_inpatient_time_series_schema(db)
    row = db.execute(select(UROLOGY_DEVICES).where(UROLOGY_DEVICES.c.id == int(device_id)).limit(1)).mappings().first()
    if not row:
        raise ValueError("Device no encontrado.")
    values: Dict[str, Any] = {}
    if present is not None:
        values["present"] = bool(present)
    if removed_at is not None:
        values["removed_at"] = _to_dt(removed_at)
    if planned_removal_at is not None:
        values["planned_removal_at"] = _to_dt(planned_removal_at)
    if planned_change_at is not None:
        values["planned_change_at"] = _to_dt(planned_change_at)
    if notes is not None:
        values["notes"] = _safe_text(notes) or None
    if irrigation is not None:
        values["irrigation"] = irrigation
    if side is not None:
        side_n = _safe_upper(side)
        if side_n not in SIDE_ALLOWED:
            raise ValueError("side inválido. Use L/R/BILAT/NA.")
        values["side"] = side_n or None
    if location is not None:
        values["location"] = _safe_text(location) or None
    if size_fr is not None:
        values["size_fr"] = _safe_text(size_fr) or None
    if difficulty is not None:
        diff_n = _safe_upper(difficulty)
        if diff_n not in DIFFICULTY_ALLOWED:
            raise ValueError("difficulty inválido. Use EASY/MODERATE/HARD.")
        values["difficulty"] = diff_n or None

    if values:
        db.execute(update(UROLOGY_DEVICES).where(UROLOGY_DEVICES.c.id == int(device_id)).values(**values))
    row2 = db.execute(select(UROLOGY_DEVICES).where(UROLOGY_DEVICES.c.id == int(device_id)).limit(1)).mappings().first()
    if not row2:
        raise ValueError("Device no encontrado tras update.")
    try:
        _insert_device_ts(
            db,
            consulta_id=row2.get("consulta_id"),
            hospitalizacion_id=row2.get("hospitalizacion_id"),
            ts=values.get("removed_at") or utcnow(),
            device_type=_safe_text(row2.get("device_type")),
            present=values.get("present", row2.get("present")),
            side=values.get("side") if "side" in values else row2.get("side"),
            size_fr=values.get("size_fr") if "size_fr" in values else row2.get("size_fr"),
            notes=values.get("notes") if "notes" in values else row2.get("notes"),
            source="update_device",
        )
    except Exception:
        pass
    return _serialize_device(row2)


def add_event(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    event_time: Optional[datetime] = None,
    event_type: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_inpatient_time_series_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    ev_type = _safe_upper(event_type)
    if ev_type not in EVENT_TYPES_ALLOWED:
        raise ValueError(f"event_type inválido: {ev_type}")

    ins = db.execute(
        insert(CLINICAL_EVENT_LOG).values(
            correlation_id=None,
            actor="api_inpatient_events",
            module=INPATIENT_EVENTS_MODULE,
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            entity="INPATIENT_EVENT",
            entity_id=None,
            source_route="/api/inpatient/events",
            event_time=_to_dt(event_time, fallback=utcnow()),
            event_type=ev_type,
            payload_json=_dump(payload or {}),
            created_at=utcnow(),
        )
    )
    event_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(CLINICAL_EVENT_LOG).where(CLINICAL_EVENT_LOG.c.id == event_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar evento.")
    try:
        flow_ml = None
        ev_payload = payload or {}
        if ev_type == "DRAIN_OUTPUT_RECORDED":
            flow_ml = float(ev_payload.get("output_ml")) if ev_payload.get("output_ml") is not None else None
            _insert_device_ts(
                db,
                consulta_id=consulta_id,
                hospitalizacion_id=hospitalizacion_id,
                ts=_to_dt(event_time, fallback=utcnow()),
                device_type=_safe_text(ev_payload.get("drain_type") or "DRENAJE"),
                present=True,
                side=_safe_text(ev_payload.get("side")),
                flow_ml=flow_ml,
                notes="GASTO DRENAJE",
                source="event_drain_output",
            )
        elif ev_type == "FOLEY_URESIS_RECORDED":
            flow_ml = float(ev_payload.get("output_ml")) if ev_payload.get("output_ml") is not None else None
            _insert_device_ts(
                db,
                consulta_id=consulta_id,
                hospitalizacion_id=hospitalizacion_id,
                ts=_to_dt(event_time, fallback=utcnow()),
                device_type="SONDA FOLEY",
                present=True,
                size_fr=_safe_text(ev_payload.get("foley_fr")),
                flow_ml=flow_ml,
                notes="URESIS FOLEY",
                source="event_foley_uresis",
            )
    except Exception:
        pass
    return _serialize_event(row)


def list_events(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_inpatient_time_series_schema(db)
    q = select(CLINICAL_EVENT_LOG)
    filters = [CLINICAL_EVENT_LOG.c.module == INPATIENT_EVENTS_MODULE]
    if consulta_id is not None:
        filters.append(CLINICAL_EVENT_LOG.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(CLINICAL_EVENT_LOG.c.hospitalizacion_id == int(hospitalizacion_id))
    if date_from is not None:
        filters.append(CLINICAL_EVENT_LOG.c.event_time >= _to_dt(date_from))
    if date_to is not None:
        filters.append(CLINICAL_EVENT_LOG.c.event_time <= _to_dt(date_to))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(CLINICAL_EVENT_LOG.c.event_time.asc(), CLINICAL_EVENT_LOG.c.id.asc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_event(r) for r in rows]


def list_devices_ts(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_inpatient_time_series_schema(db)
    q = select(DEVICES_TS)
    filters = []
    if consulta_id is not None:
        filters.append(DEVICES_TS.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(DEVICES_TS.c.hospitalizacion_id == int(hospitalizacion_id))
    if date_from is not None:
        filters.append(DEVICES_TS.c.ts >= _to_dt(date_from))
    if date_to is not None:
        filters.append(DEVICES_TS.c.ts <= _to_dt(date_to))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(DEVICES_TS.c.ts.asc(), DEVICES_TS.c.id.asc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "consulta_id": r.get("consulta_id"),
                "hospitalizacion_id": r.get("hospitalizacion_id"),
                "ts": r.get("ts").isoformat() if r.get("ts") else "",
                "device_family": _safe_text(r.get("device_family")),
                "device_type": _safe_text(r.get("device_type")),
                "present": r.get("present"),
                "side": _safe_text(r.get("side")),
                "size_fr": _safe_text(r.get("size_fr")),
                "flow_ml": r.get("flow_ml"),
                "notes": _safe_text(r.get("notes")),
                "source": _safe_text(r.get("source")),
            }
        )
    return out
