from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.schemas.inpatient_devices_events import ClinicalEventCreate, UrologyDeviceCreate, UrologyDevicePatch
from app.services.inpatient_devices_events_service import (
    EVENT_TYPES_ALLOWED,
    add_device,
    add_event,
    list_devices,
    list_devices_ts,
    list_events,
    update_device,
)

router = APIRouter(tags=["urology-devices-events"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.post("/api/inpatient/devices", response_class=JSONResponse)
def api_inpatient_add_device(payload: UrologyDeviceCreate, db: Session = Depends(_get_db)):
    try:
        row = add_device(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            device_type=payload.device_type,
            present=payload.present,
            inserted_at=payload.inserted_at,
            removed_at=payload.removed_at,
            side=payload.side,
            location=payload.location,
            size_fr=payload.size_fr,
            difficulty=payload.difficulty,
            irrigation=payload.irrigation,
            planned_removal_at=payload.planned_removal_at,
            planned_change_at=payload.planned_change_at,
            notes=payload.notes,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "device": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar device: {exc}") from exc


@router.get("/api/inpatient/devices", response_class=JSONResponse)
def api_inpatient_list_devices(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_devices(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})


@router.get("/api/inpatient/devices-ts", response_class=JSONResponse)
def api_inpatient_list_devices_ts(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_devices_ts(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        date_from=from_ts,
        date_to=to_ts,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})


@router.patch("/api/inpatient/devices/{device_id}", response_class=JSONResponse)
async def api_inpatient_patch_device(
    device_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    payload: Dict[str, Any] = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        payload = await request.json()
    else:
        form = await request.form()
        payload = {k: v for k, v in form.items()}
    patch = UrologyDevicePatch(**payload)
    try:
        row = update_device(
            db,
            device_id=device_id,
            present=patch.present,
            removed_at=patch.removed_at,
            planned_removal_at=patch.planned_removal_at,
            planned_change_at=patch.planned_change_at,
            notes=patch.notes,
            irrigation=patch.irrigation,
            side=patch.side,
            location=patch.location,
            size_fr=patch.size_fr,
            difficulty=patch.difficulty,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "device": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible actualizar device: {exc}") from exc


@router.post("/api/inpatient/events", response_class=JSONResponse)
def api_inpatient_add_event(payload: ClinicalEventCreate, db: Session = Depends(_get_db)):
    if str(payload.event_type or "").upper() not in EVENT_TYPES_ALLOWED:
        raise HTTPException(
            status_code=422,
            detail={"message": "event_type inválido", "allowed": sorted(EVENT_TYPES_ALLOWED)},
        )
    try:
        row = add_event(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            event_time=payload.event_time,
            event_type=payload.event_type,
            payload=payload.payload,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "event": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar evento: {exc}") from exc


@router.get("/api/inpatient/events", response_class=JSONResponse)
def api_inpatient_list_events(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_events(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        date_from=from_ts,
        date_to=to_ts,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})
