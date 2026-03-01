from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter(tags=["compat"])


@router.get("/consulta_externa/home")
async def consulta_externa_home_compat():
    # Alias histórico para mantener navegación funcional.
    return RedirectResponse(url="/consulta_externa", status_code=307)


@router.get("/quirofano/urgencias/nuevo")
async def quirofano_urgencias_nuevo_compat():
    # Alias histórico para mantener navegación funcional.
    return RedirectResponse(url="/quirofano/urgencias/solicitud", status_code=307)


@router.get("/fau-bot/dashboard")
async def fau_bot_dashboard_compat():
    # Alias histórico para mantener navegación funcional.
    return RedirectResponse(url="/ai/fau-bot", status_code=307)


@router.get("/expediente_fase1")
async def expediente_fase1_compat(request: Request):
    consulta_id = request.query_params.get("consulta_id")
    if consulta_id:
        return RedirectResponse(url=f"/expediente/fase1?consulta_id={consulta_id}", status_code=307)
    return RedirectResponse(url="/expediente", status_code=307)

