from __future__ import annotations

from typing import Any, Dict, List


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_nss_10(value: Any) -> str:
    return "".join(ch for ch in _safe_text(value) if ch.isdigit())[:10]


def _norm_sexo(value: Any) -> str:
    return _safe_text(value).upper()


def validate_identity_fields(*, nss: Any, edad: Any, sexo: Any, nombre: Any = "") -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    nss10 = _norm_nss_10(nss)
    if len(nss10) != 10:
        errors.append("NSS debe tener exactamente 10 dígitos.")

    try:
        edad_i = int(edad)
        if edad_i < 0 or edad_i > 120:
            errors.append("Edad fuera de rango clínico (0-120).")
    except Exception:
        errors.append("Edad inválida.")

    sx = _norm_sexo(sexo)
    if sx not in {"MASCULINO", "FEMENINO"}:
        errors.append("Sexo inválido (MASCULINO/FEMENINO).")

    nm = _safe_text(nombre)
    if nm and len(nm.split()) < 2:
        warnings.append("Nombre clínico con baja completitud (sugerido: apellidos + nombre).")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "normalized": {
            "nss": nss10,
            "sexo": sx,
            "edad": int(edad) if str(edad).strip().isdigit() else None,
            "nombre": nm,
        },
    }
