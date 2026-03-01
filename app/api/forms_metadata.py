from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.services.form_metadata_flow import (
    get_form_schema,
    list_forms,
    seed_default_form_metadata,
    validate_form_payload,
)

router = APIRouter(prefix="/api/forms", tags=["forms-metadata"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.get("", response_class=JSONResponse)
def forms_list(db: Session = Depends(_get_db)):
    seed_default_form_metadata(db)
    return JSONResponse(content={"forms": list_forms(db)})


@router.get("/{form_code}/schema", response_class=JSONResponse)
def forms_schema(form_code: str, db: Session = Depends(_get_db)):
    seed_default_form_metadata(db)
    try:
        return JSONResponse(content=get_form_schema(db, form_code))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{form_code}/validate", response_class=JSONResponse)
async def forms_validate(form_code: str, request: Request, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    body: Dict[str, Any] = await request.json()
    csrf_token = str(body.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    section_code = str(body.get("section_code") or "").strip()
    payload = body.get("payload") or {}
    if not section_code:
        raise HTTPException(status_code=400, detail="section_code requerido")
    if not isinstance(payload, dict):
        payload = {}

    seed_default_form_metadata(db)
    result = validate_form_payload(
        db,
        form_code=form_code,
        section_code=section_code,
        payload=payload,
    )
    return JSONResponse(content={"ok": True, "validation": result.as_dict()})


@router.post("/{form_code}/seed-defaults", response_class=JSONResponse)
def forms_seed_defaults(form_code: str, db: Session = Depends(_get_db)):
    if form_code not in {"consulta_externa", "hospitalizacion_ingreso", "urgencias_solicitud_qx"}:
        raise HTTPException(status_code=400, detail="form_code no soportado para seed por defecto")
    result = seed_default_form_metadata(db)
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("reason") or "No se pudo inicializar metadata")
    return JSONResponse(content={"status": "ok", "result": result})
