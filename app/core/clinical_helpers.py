from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from sqlalchemy.orm import Session


def registrar_auditoria_row(
    sdb: Session,
    *,
    audit_model: Type[Any],
    tabla: str,
    registro_id: int,
    operacion: str,
    usuario: str,
    datos_anteriores: Optional[Dict[str, Any]] = None,
    datos_nuevos: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        sdb.add(
            audit_model(
                tabla=tabla,
                registro_id=registro_id,
                operacion=operacion,
                usuario=usuario or "system",
                datos_anteriores=datos_anteriores or {},
                datos_nuevos=datos_nuevos or {},
            )
        )
        sdb.commit()
    except Exception:
        sdb.rollback()


def get_code_from_map(value: Optional[str], *, normalize_fn, code_map: Dict[str, str]) -> Optional[str]:
    if not value:
        return None
    key = normalize_fn(value)
    return code_map.get(key)


def build_patologia_cie10_catalog(
    patologias: List[str],
    *,
    get_cie10_fn,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for pat in patologias:
        out.append({"patologia": pat, "cie10": get_cie10_fn(pat) or "NO_REGISTRADO"})
    return out


def edad_quinquenio(edad: Optional[int]) -> Optional[str]:
    if edad is None:
        return None
    low = (edad // 5) * 5
    high = low + 4
    return f"{low}-{high}"


def edad_grupo_epidemiologico(edad: Optional[int]) -> Optional[str]:
    if edad is None:
        return None
    if edad < 18:
        return "PEDIATRIA"
    if edad < 60:
        return "ADULTO"
    return "GERIATRIA"
