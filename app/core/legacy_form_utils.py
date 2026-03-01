from __future__ import annotations

from typing import Any, Dict, Iterable

from fastapi import HTTPException, Request


def extract_protocolo_detalles(data: Dict[str, Any], protocol_fields: Iterable[str]) -> Dict[str, Any]:
    """Normaliza los campos de protocolo manteniendo contrato legacy."""
    detalles: Dict[str, Any] = {}
    for key in protocol_fields:
        value = data.get(key)
        detalles[str(key)] = value if value not in [None, ""] else "NO_APLICA"
    return detalles


def validate_csrf_token(form_data: Dict[str, Any], request: Request, csrf_cookie_name: str) -> None:
    token_form = form_data.get("csrf_token")
    token_cookie = request.cookies.get(csrf_cookie_name)
    if not token_form or not token_cookie or token_form != token_cookie:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
