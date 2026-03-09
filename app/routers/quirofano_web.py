from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.api import legacy_web as legacy

router = APIRouter(tags=["quirofano-web"])


@router.get("/quirofano", response_class=HTMLResponse)
async def quirofano_home(request: Request):
    return await legacy.quirofano_home(request)


@router.get("/quirofano/programada", response_class=HTMLResponse)
async def quirofano_programada_home(request: Request):
    return await legacy.quirofano_programada_home(request)


@router.get("/quirofano/programada/lista", response_class=HTMLResponse)
@router.get("/quirofano/lista", response_class=HTMLResponse)
async def listar_quirofanos(
    request: Request,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
    campo: Optional[str] = None,
    q: Optional[str] = None,
):
    return await legacy.listar_quirofanos(request, db, sdb, campo=campo, q=q)


@router.get("/quirofano/lista-espera-programacion", response_class=HTMLResponse)
async def listar_espera_programacion_quirurgica(
    request: Request,
    sdb: Session = Depends(legacy._get_surgical_db),
    q: Optional[str] = None,
    mostrar_todos: Optional[int] = 0,
):
    return await legacy.listar_espera_programacion_quirurgica(request, sdb, q=q, mostrar_todos=mostrar_todos)


@router.get("/quirofano/lista-espera")
async def quirofano_lista_espera_legacy_alias():
    return await legacy.quirofano_lista_espera_legacy_alias()


@router.get("/quirofano/lista-espera/ingresar", response_class=HTMLResponse)
async def quirofano_lista_espera_ingresar(
    request: Request,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    saved: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.quirofano_lista_espera_ingresar(request, consulta_id, nss, saved, error, db, sdb)


@router.post("/quirofano/lista-espera/ingresar", response_class=HTMLResponse)
async def guardar_quirofano_lista_espera_ingresar(
    request: Request,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.guardar_quirofano_lista_espera_ingresar(request, db, sdb)


@router.get("/quirofano/programada/postquirurgica", response_class=HTMLResponse)
async def quirofano_postquirurgica(
    request: Request,
    surgical_programacion_id: Optional[int] = None,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.quirofano_postquirurgica(request, surgical_programacion_id, db, sdb)


@router.get("/quirofano/programada/{surgical_programacion_id}/postquirurgica", response_class=HTMLResponse)
async def quirofano_postquirurgica_direct(
    request: Request,
    surgical_programacion_id: int,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.quirofano_postquirurgica_direct(request, surgical_programacion_id, db, sdb)


@router.post("/quirofano/programada/postquirurgica", response_class=HTMLResponse)
async def guardar_postquirurgica(
    request: Request,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.guardar_postquirurgica(request, db, sdb)


@router.post("/quirofano/programada/cancelar", response_class=HTMLResponse)
async def cancelar_quirofano_programada(
    request: Request,
    db: Session = Depends(legacy._get_db),
    sdb: Session = Depends(legacy._get_surgical_db),
):
    return await legacy.cancelar_quirofano_programada(request, db, sdb)


@router.get("/quirofano/nuevo", response_class=HTMLResponse)
async def nuevo_quirofano_form(request: Request):
    return await legacy.nuevo_quirofano_form(request)


@router.post("/quirofano/nuevo", response_class=HTMLResponse)
async def guardar_quirofano(
    request: Request,
    db: Session = Depends(legacy._get_db),
):
    return await legacy.guardar_quirofano(request, db)
