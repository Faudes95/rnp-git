from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.api import legacy_web as legacy

router = APIRouter(tags=["expediente-web"])


@router.get("/expediente", response_class=HTMLResponse)
async def ver_expediente(
    request: Request,
    consulta_id: Optional[int] = None,
    curp: Optional[str] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(legacy._get_db),
):
    return await legacy.ver_expediente(request, consulta_id, curp, nss, nombre, q, db)
