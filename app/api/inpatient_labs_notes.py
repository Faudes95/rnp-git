from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.schemas.inpatient_labs_notes import (
    AlertAckPayload,
    AlertResolvePayload,
    ClinicalTagCreate,
    InpatientDailyNoteCreate,
    LabResultCreate,
)
from app.services.inpatient_labs_notes_service import (
    DailyNoteConflictError,
    ack_alert,
    add_lab,
    add_tag,
    list_daily_notes,
    list_labs,
    list_tags,
    resolve_alert,
    upsert_daily_note,
)

router = APIRouter(tags=["inpatient-labs-notes"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.post("/api/inpatient/labs", response_class=JSONResponse)
def api_inpatient_add_lab(payload: LabResultCreate, db: Session = Depends(_get_db)):
    try:
        row = add_lab(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            collected_at=payload.collected_at,
            test_name=payload.test_name,
            value_num=payload.value_num,
            value_text=payload.value_text,
            unit=payload.unit,
            source=payload.source,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "lab": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar laboratorio: {exc}") from exc


@router.get("/api/inpatient/labs", response_class=JSONResponse)
def api_inpatient_list_labs(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    from_ts: Optional[datetime] = Query(default=None, alias="from"),
    to_ts: Optional[datetime] = Query(default=None, alias="to"),
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_labs(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        date_from=from_ts,
        date_to=to_ts,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})


@router.post("/api/inpatient/daily-note", response_class=JSONResponse)
def api_inpatient_daily_note(
    payload: InpatientDailyNoteCreate,
    upsert: int = Query(default=0, ge=0, le=1),
    db: Session = Depends(_get_db),
):
    try:
        row = upsert_daily_note(
            db,
            hospitalizacion_id=payload.hospitalizacion_id,
            consulta_id=payload.consulta_id,
            note_date=payload.note_date,
            author_user_id=payload.author_user_id,
            problem_list_json=payload.problem_list_json,
            plan_by_problem_json=payload.plan_by_problem_json,
            devices_snapshot_json=payload.devices_snapshot_json,
            io_summary_json=payload.io_summary_json,
            symptoms_json=payload.symptoms_json,
            events_pending_json=payload.events_pending_json,
            free_text=payload.free_text,
            is_final=payload.is_final,
            upsert=bool(upsert),
            consulta_patient_id=payload.consulta_patient_id,
            note_type=payload.note_type or "EVOLUCION",
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "daily_note": row})
    except DailyNoteConflictError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar nota diaria: {exc}") from exc


@router.get("/api/inpatient/daily-notes", response_class=JSONResponse)
def api_inpatient_daily_notes(
    hospitalizacion_id: Optional[int] = None,
    consulta_id: Optional[int] = None,
    limit: int = 1000,
    db: Session = Depends(_get_db),
):
    rows = list_daily_notes(
        db,
        hospitalizacion_id=hospitalizacion_id,
        consulta_id=consulta_id,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})


@router.post("/api/inpatient/alerts/{alert_id}/ack", response_class=JSONResponse)
def api_inpatient_alert_ack(alert_id: int, payload: AlertAckPayload, db: Session = Depends(_get_db)):
    try:
        row = ack_alert(db, alert_id=alert_id, ack_by=payload.ack_by)
        db.commit()
        return JSONResponse(content={"status": "ok", "metadata": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible hacer ACK: {exc}") from exc


@router.post("/api/inpatient/alerts/{alert_id}/resolve", response_class=JSONResponse)
def api_inpatient_alert_resolve(alert_id: int, payload: AlertResolvePayload, db: Session = Depends(_get_db)):
    try:
        row = resolve_alert(
            db,
            alert_id=alert_id,
            resolved_by=payload.resolved_by,
            resolution_reason=payload.resolution_reason,
            action_taken_json=payload.action_taken_json,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "metadata": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible resolver alerta: {exc}") from exc


@router.post("/api/inpatient/tags", response_class=JSONResponse)
def api_inpatient_add_tag(payload: ClinicalTagCreate, db: Session = Depends(_get_db)):
    try:
        row = add_tag(
            db,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            tag_type=payload.tag_type,
            tag_value=payload.tag_value,
            laterality=payload.laterality,
            severity=payload.severity,
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "tag": row})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar tag: {exc}") from exc


@router.get("/api/inpatient/tags", response_class=JSONResponse)
def api_inpatient_list_tags(
    consulta_id: Optional[int] = None,
    hospitalizacion_id: Optional[int] = None,
    tag_type: Optional[str] = None,
    limit: int = 5000,
    db: Session = Depends(_get_db),
):
    rows = list_tags(
        db,
        consulta_id=consulta_id,
        hospitalizacion_id=hospitalizacion_id,
        tag_type=tag_type,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "items": rows})

