"""
Router ADITIVO para Ward Round Dashboard y Smart Expediente.
No modifica ningún router existente.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from app.services.ward_round_flow import (
    ward_round_autofill_vitals_flow,
    ward_round_dashboard_flow,
    ward_round_save_inline_note_flow,
)
from app.services.smart_expediente_flow import (
    smart_expediente_flow,
)
from app.services.command_center_flow import (
    command_center_flow,
)

router = APIRouter(tags=["ward-smart"])


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


# ── Ward Round Dashboard ──────────────────────────────────────────
@router.get("/ward-round", response_class=HTMLResponse)
async def ward_round_dashboard(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await ward_round_dashboard_flow(request, db, fecha=fecha)


@router.post("/ward-round/nota", response_class=JSONResponse)
async def ward_round_save_note(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await ward_round_save_inline_note_flow(request, db)


@router.get("/api/ward-round/vitals", response_class=JSONResponse)
async def ward_round_vitals(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await ward_round_autofill_vitals_flow(request, db)


# ── Smart Expediente ──────────────────────────────────────────────
@router.get("/expediente-smart", response_class=HTMLResponse)
async def smart_expediente(
    request: Request,
    consulta_id: int = 0,
    db: Session = Depends(_get_db),
):
    return await smart_expediente_flow(request, db, consulta_id=consulta_id)


# ── Patient Command Center ───────────────────────────────────────
@router.get("/command-center", response_class=HTMLResponse)
async def patient_command_center(
    request: Request,
    consulta_id: int = 0,
    db: Session = Depends(_get_db),
):
    return await command_center_flow(request, db, consulta_id=consulta_id)
