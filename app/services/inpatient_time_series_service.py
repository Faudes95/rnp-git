from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, insert, select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.inpatient_ai_models import IO_BLOCKS, VITALS_TS, ensure_inpatient_time_series_schema


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


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


def _serialize_vitals(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "recorded_at": row.get("recorded_at").isoformat() if row.get("recorded_at") else "",
        "heart_rate": row.get("heart_rate"),
        "sbp": row.get("sbp"),
        "dbp": row.get("dbp"),
        "map": row.get("map"),
        "temperature": row.get("temperature"),
        "spo2": row.get("spo2"),
        "resp_rate": row.get("resp_rate"),
        "mental_status_avpu": row.get("mental_status_avpu"),
        "gcs": row.get("gcs"),
        "o2_device": row.get("o2_device"),
        "o2_flow_lpm": row.get("o2_flow_lpm"),
        "pain_score_0_10": row.get("pain_score_0_10"),
        "source": row.get("source"),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def _serialize_io_block(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "consulta_id": row.get("consulta_id"),
        "hospitalizacion_id": row.get("hospitalizacion_id"),
        "interval_start": row.get("interval_start").isoformat() if row.get("interval_start") else "",
        "interval_end": row.get("interval_end").isoformat() if row.get("interval_end") else "",
        "urine_output_ml": row.get("urine_output_ml"),
        "intake_ml": row.get("intake_ml"),
        "net_balance_ml": row.get("net_balance_ml"),
        "weight_kg": row.get("weight_kg"),
        "height_cm": row.get("height_cm"),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def add_vitals_ts(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    recorded_at: Optional[datetime] = None,
    heart_rate: Optional[int] = None,
    sbp: Optional[int] = None,
    dbp: Optional[int] = None,
    map_value: Optional[float] = None,
    temperature: Optional[float] = None,
    spo2: Optional[float] = None,
    resp_rate: Optional[int] = None,
    mental_status_avpu: Optional[str] = None,
    gcs: Optional[int] = None,
    o2_device: Optional[str] = None,
    o2_flow_lpm: Optional[float] = None,
    pain_score_0_10: Optional[int] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_inpatient_time_series_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    if pain_score_0_10 is not None and (int(pain_score_0_10) < 0 or int(pain_score_0_10) > 10):
        raise ValueError("pain_score_0_10 fuera de rango (0..10).")
    if o2_flow_lpm is not None and float(o2_flow_lpm) < 0:
        raise ValueError("o2_flow_lpm no puede ser negativo.")

    map_final = float(map_value) if map_value is not None else None
    if map_final is None and sbp is not None and dbp is not None:
        map_final = round((float(sbp) + (2.0 * float(dbp))) / 3.0, 2)

    ins = db.execute(
        insert(VITALS_TS).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            recorded_at=_to_dt(recorded_at, fallback=utcnow()),
            heart_rate=int(heart_rate) if heart_rate is not None else None,
            sbp=int(sbp) if sbp is not None else None,
            dbp=int(dbp) if dbp is not None else None,
            map=map_final,
            temperature=float(temperature) if temperature is not None else None,
            spo2=float(spo2) if spo2 is not None else None,
            resp_rate=int(resp_rate) if resp_rate is not None else None,
            mental_status_avpu=_safe_text(mental_status_avpu).upper() or None,
            gcs=int(gcs) if gcs is not None else None,
            o2_device=_safe_text(o2_device).upper() or None,
            o2_flow_lpm=float(o2_flow_lpm) if o2_flow_lpm is not None else None,
            pain_score_0_10=int(pain_score_0_10) if pain_score_0_10 is not None else None,
            source=_safe_text(source) or "API_INPATIENT_VITALS",
            created_at=utcnow(),
        )
    )
    vital_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(VITALS_TS).where(VITALS_TS.c.id == vital_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar vitals_ts.")
    return _serialize_vitals(row)


def list_vitals(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_inpatient_time_series_schema(db)
    q = select(VITALS_TS)
    filters = []
    if consulta_id is not None:
        filters.append(VITALS_TS.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(VITALS_TS.c.hospitalizacion_id == int(hospitalizacion_id))
    if date_from is not None:
        filters.append(VITALS_TS.c.recorded_at >= _to_dt(date_from))
    if date_to is not None:
        filters.append(VITALS_TS.c.recorded_at <= _to_dt(date_to))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(VITALS_TS.c.recorded_at.asc(), VITALS_TS.c.id.asc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_vitals(r) for r in rows]


def add_io_block(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    interval_start: datetime,
    interval_end: datetime,
    urine_output_ml: Optional[float] = None,
    intake_ml: Optional[float] = None,
    net_balance_ml: Optional[float] = None,
    weight_kg: Optional[float] = None,
    height_cm: Optional[float] = None,
) -> Dict[str, Any]:
    ensure_inpatient_time_series_schema(db)
    if consulta_id is None and hospitalizacion_id is None:
        raise ValueError("Debe enviar consulta_id o hospitalizacion_id.")
    start_dt = _to_dt(interval_start, fallback=utcnow())
    end_dt = _to_dt(interval_end, fallback=utcnow())
    if end_dt <= start_dt:
        raise ValueError("interval_end debe ser mayor a interval_start.")

    net_final = float(net_balance_ml) if net_balance_ml is not None else None
    if net_final is None and intake_ml is not None and urine_output_ml is not None:
        net_final = float(intake_ml) - float(urine_output_ml)

    ins = db.execute(
        insert(IO_BLOCKS).values(
            consulta_id=int(consulta_id) if consulta_id is not None else None,
            hospitalizacion_id=int(hospitalizacion_id) if hospitalizacion_id is not None else None,
            interval_start=start_dt,
            interval_end=end_dt,
            urine_output_ml=float(urine_output_ml) if urine_output_ml is not None else None,
            intake_ml=float(intake_ml) if intake_ml is not None else None,
            net_balance_ml=net_final,
            weight_kg=float(weight_kg) if weight_kg is not None else None,
            height_cm=float(height_cm) if height_cm is not None else None,
            created_at=utcnow(),
        )
    )
    block_id = int(ins.inserted_primary_key[0])
    row = db.execute(select(IO_BLOCKS).where(IO_BLOCKS.c.id == block_id).limit(1)).mappings().first()
    if not row:
        raise ValueError("No fue posible guardar io_block.")
    return _serialize_io_block(row)


def list_io_blocks(
    db: Session,
    *,
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_inpatient_time_series_schema(db)
    q = select(IO_BLOCKS)
    filters = []
    if consulta_id is not None:
        filters.append(IO_BLOCKS.c.consulta_id == int(consulta_id))
    if hospitalizacion_id is not None:
        filters.append(IO_BLOCKS.c.hospitalizacion_id == int(hospitalizacion_id))
    if date_from is not None:
        filters.append(IO_BLOCKS.c.interval_start >= _to_dt(date_from))
    if date_to is not None:
        filters.append(IO_BLOCKS.c.interval_end <= _to_dt(date_to))
    if filters:
        q = q.where(and_(*filters))
    rows = db.execute(
        q.order_by(IO_BLOCKS.c.interval_start.asc(), IO_BLOCKS.c.id.asc()).limit(max(1, min(limit, 20000)))
    ).mappings().all()
    return [_serialize_io_block(r) for r in rows]

