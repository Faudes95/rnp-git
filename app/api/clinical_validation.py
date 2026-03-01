"""
API de Validación Clínica — FASE 3.

ADITIVO: No modifica validaciones existentes.
Endpoint para validar datos clínicos antes de guardar.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.core.clinical_validations import validate_all

router = APIRouter(tags=["clinical_validation"])


@router.post("/api/clinical/validate", response_class=JSONResponse)
async def validate_clinical_data(request: Request):
    """Valida datos clínicos y retorna errores/warnings.

    Body JSON:
      Formato 1: { "context": "consulta", ...datos... }
      Formato 2: { "data": { ...datos... }, "context": "consulta" }
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    # Support both formats
    if "data" in body and isinstance(body["data"], dict):
        data = body["data"]
        context = str(body.get("context", "consulta")).strip().lower()
    else:
        data = body
        context = str(body.pop("context", "consulta") if isinstance(body, dict) else "consulta").strip().lower()

    result = validate_all(data, context=context)

    return JSONResponse(content={
        "ok": True,
        **result.to_dict(),
    })
