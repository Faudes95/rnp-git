from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.schemas.inpatient_time_series import IOBlockCreate, VitalsTSCreate
from app.services.inpatient_time_series_service import (
    add_io_block,
    add_vitals_ts,
    list_io_blocks,
    list_vitals,
)

router = APIRouter(tags=["inpatient-time-series"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.post("/api/inpatient/vitals", response_class=JSONResponse)
def api_inpatient_add_vitals(payload: VitalsTSCreate, db: Session = Depends(_get_db)):
    if payload.pain_score_0_10 is not None and (payload.pain_score_0_10 < 0 or payload.pain_score_0_10 > 10):
        raise HTTPException(status_code=422, detail="pain_score_0_10 fuera de rango (0..10).")
    if payload.o2_flow_lpm is not None and payload.o2_flow_lpm < 0:
        raise HTTPException(status_code=422, detail="o2_flow_lpm no puede ser negativo.")
    try:
        row = add_vitals_ts(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            recorded_at=payload.recorded_at,
            heart_rate=payload.heart_rate,
            sbp=payload.sbp,
            dbp=payload.dbp,
            map_value=payload.map,
            temperature=payload.temperature,
            spo2=payload.spo2,
            resp_rate=payload.resp_rate,
            mental_status_avpu=payload.mental_status_avpu,
            gcs=payload.gcs,
            o2_device=payload.o2_device,
            o2_flow_lpm=payload.o2_flow_lpm,
            pain_score_0_10=payload.pain_score_0_10,
            source=payload.source,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "vitals": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar vitals_ts: {exc}") from exc


@router.get("/api/inpatient/vitals", response_class=JSONResponse)
def api_inpatient_list_vitals(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_vitals(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        date_from=from_ts,
        date_to=to_ts,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})


@router.post("/api/inpatient/io-block", response_class=JSONResponse)
def api_inpatient_add_io_block(payload: IOBlockCreate, db: Session = Depends(_get_db)):
    if payload.interval_end <= payload.interval_start:
        raise HTTPException(status_code=422, detail="interval_end debe ser mayor a interval_start.")
    try:
        row = add_io_block(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            interval_start=payload.interval_start,
            interval_end=payload.interval_end,
            urine_output_ml=payload.urine_output_ml,
            intake_ml=payload.intake_ml,
            net_balance_ml=payload.net_balance_ml,
            weight_kg=payload.weight_kg,
            height_cm=payload.height_cm,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "io_block": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar io_block: {exc}") from exc


@router.get("/api/inpatient/io-blocks", response_class=JSONResponse)
def api_inpatient_list_io_blocks(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_io_blocks(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        date_from=from_ts,
        date_to=to_ts,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})

