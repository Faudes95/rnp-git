from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.services.quirofano_flow import (
    cancelar_programacion_flow,
    guardar_postquirurgica_flow,
    guardar_quirofano_urgencia_flow,
    listar_quirofanos_urgencias_flow,
    render_postquirurgica_flow,
    render_quirofano_urgencias_flow,
    render_quirofano_urgencias_solicitud_flow,
)

router = APIRouter(tags=["quirofano-urgencias"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_surgical_db()


@router.get("/quirofano/urgencias", response_class=HTMLResponse)
async def quirofano_urgencias(request: Request):
    return await render_quirofano_urgencias_flow(request)


@router.get("/quirofano/urgencias/solicitud", response_class=HTMLResponse)
async def quirofano_urgencias_solicitud(request: Request):
    return await render_quirofano_urgencias_solicitud_flow(request)


@router.get("/quirofano/urgencias/postquirurgica", response_class=HTMLResponse)
async def quirofano_urgencias_postquirurgica(
    request: Request,
    surgical_programacion_id: Optional[int] = None,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_postquirurgica_flow(
        request,
        db,
        sdb,
        selected_programacion_id=surgical_programacion_id,
        urgencias_only=True,
        form_action="/quirofano/urgencias/postquirurgica",
        back_href="/quirofano/urgencias",
        titulo="🩺 Nota Postquirúrgica de Urgencias",
    )


@router.post("/quirofano/urgencias", response_class=HTMLResponse)
@router.post("/quirofano/urgencias/solicitud", response_class=HTMLResponse)
async def guardar_quirofano_urgencias(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await guardar_quirofano_urgencia_flow(request, db, sdb)


@router.post("/quirofano/urgencias/postquirurgica", response_class=HTMLResponse)
async def guardar_postquirurgica_urgencias(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await guardar_postquirurgica_flow(
        request,
        db,
        sdb,
        urgencias_only=True,
        form_action="/quirofano/urgencias/postquirurgica",
        back_href="/quirofano/urgencias",
        titulo="🩺 Nota Postquirúrgica de Urgencias",
    )


@router.post("/quirofano/urgencias/cancelar", response_class=HTMLResponse)
async def cancelar_quirofano_urgencias(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await cancelar_programacion_flow(
        request,
        db,
        sdb,
        urgencias_only=True,
        back_href="/quirofano/urgencias/lista",
    )


@router.get("/quirofano/urgencias/lista", response_class=HTMLResponse)
async def listar_quirofanos_urgencias(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
    campo: Optional[str] = None,
    q: Optional[str] = None,
):
    return await listar_quirofanos_urgencias_flow(request, sdb, campo=campo, q=q)


@router.get("/quirofano/urgencias/{surgical_programacion_id}/postquirurgica", response_class=HTMLResponse)
async def quirofano_urgencias_postquirurgica_direct(
    request: Request,
    surgical_programacion_id: int,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_postquirurgica_flow(
        request,
        db,
        sdb,
        selected_programacion_id=surgical_programacion_id,
        urgencias_only=True,
        form_action="/quirofano/urgencias/postquirurgica",
        back_href="/quirofano/urgencias",
        titulo="🩺 Nota Postquirúrgica de Urgencias",
    )
