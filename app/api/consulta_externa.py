from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.services.consulta_externa_flow import (
    api_consulta_externa_recetas_ingest_flow,
    api_consulta_externa_servicios_stats_flow,
    consulta_externa_home_flow,
    consulta_externa_leoch_form_flow,
    consulta_externa_leoch_guardar_flow,
    consulta_externa_recetas_placeholder_flow,
    consulta_externa_uroendoscopia_form_flow,
    consulta_externa_uroendoscopia_guardar_flow,
)


router = APIRouter(tags=["consulta-externa"])


# ── Rutas de Perfil Clínico (consultas subsecuentes) ──

@router.get("/consulta_externa/subsecuente/consulta")
async def consulta_subsecuente_consulta(request: Request):
    """Redirige a perfil clínico de consulta externa para consultas subsecuentes."""
    return RedirectResponse(url="/perfil-clinico/consulta-externa", status_code=307)


@router.get("/consulta_externa/subsecuente/leoch")
async def consulta_subsecuente_leoch(request: Request):
    """Redirige a perfil clínico de LEOCH para consultas subsecuentes."""
    return RedirectResponse(url="/perfil-clinico/leoch", status_code=307)


@router.get("/consulta_externa/subsecuente/uroendoscopia")
async def consulta_subsecuente_uroendoscopia(request: Request):
    """Redirige a perfil clínico de Uroendoscopia para consultas subsecuentes."""
    return RedirectResponse(url="/perfil-clinico/uroendoscopia", status_code=307)


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.get("/consulta_externa")
async def consulta_externa_home(request: Request):
    return consulta_externa_home_flow(request)


@router.get("/consulta_externa/consulta")
async def consulta_externa_consulta_redirect(request: Request):
    return_to = request.query_params.get("return_to") or "/consulta_externa"
    if not str(return_to).startswith("/") or str(return_to).startswith("//"):
        return_to = "/consulta_externa"
    return RedirectResponse(url=f"/consulta?return_to={quote(str(return_to), safe='/_-')}", status_code=307)


@router.get("/consulta_externa/uroendoscopia")
async def consulta_externa_uroendoscopia_form(request: Request):
    return consulta_externa_uroendoscopia_form_flow(request)


@router.post("/consulta_externa/uroendoscopia/guardar")
async def consulta_externa_uroendoscopia_guardar(request: Request, db: Session = Depends(_get_db)):
    return await consulta_externa_uroendoscopia_guardar_flow(request, db)


@router.get("/consulta_externa/leoch")
async def consulta_externa_leoch_form(request: Request):
    return consulta_externa_leoch_form_flow(request)


@router.post("/consulta_externa/leoch/guardar")
async def consulta_externa_leoch_guardar(request: Request, db: Session = Depends(_get_db)):
    return await consulta_externa_leoch_guardar_flow(request, db)


@router.get("/consulta_externa/recetas")
async def consulta_externa_recetas(request: Request):
    return consulta_externa_recetas_placeholder_flow(request)


@router.get("/api/stats/consulta-externa/servicios")
def api_consulta_externa_servicios_stats(db: Session = Depends(_get_db)):
    return api_consulta_externa_servicios_stats_flow(db)


@router.post("/api/consulta_externa/recetas/ingest")
async def api_consulta_externa_recetas_ingest(request: Request, db: Session = Depends(_get_db)):
    return await api_consulta_externa_recetas_ingest_flow(request, db)
