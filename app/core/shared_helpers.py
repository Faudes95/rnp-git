"""
Helpers compartidos consolidados — FASE 3.

ADITIVO: No modifica helpers existentes.
Consolida funciones duplicadas (_safe_text, _safe_int, _safe_float, etc.)
en un solo módulo reutilizable. Los archivos existentes pueden importar
de aquí en lugar de re-definir las mismas funciones.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional


def safe_text(value: Any, default: str = "") -> str:
    """Convierte cualquier valor a string limpio. Reemplaza 10+ definiciones de _safe_text."""
    return str(value or "").strip() if value is not None else default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Convierte a int o retorna default. Reemplaza 10+ definiciones de _safe_int."""
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convierte a float o retorna default. Reemplaza 10+ definiciones de _safe_float."""
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    """Convierte a boolean con interpretación flexible."""
    if isinstance(value, bool):
        return value
    text = safe_text(value).upper()
    if text in ("SI", "SÍ", "YES", "TRUE", "1", "S"):
        return True
    if text in ("NO", "FALSE", "0", "N"):
        return False
    return default


def normalize_upper(value: Any) -> str:
    """Normaliza texto a mayúsculas, eliminando espacios duplicados."""
    return re.sub(r"\s+", " ", safe_text(value).upper())


def normalize_nss(value: Any) -> str:
    """Normaliza NSS a 10 dígitos."""
    return re.sub(r"\D", "", safe_text(value))[:10]


def normalize_curp(value: Any) -> str:
    """Normaliza CURP a 18 caracteres alfanuméricos."""
    return re.sub(r"[^A-Z0-9]", "", safe_text(value).upper())[:18]


def load_json(value: Any, default: Any = None) -> Any:
    """Carga JSON de forma segura. Reemplaza 10+ definiciones de _load_json."""
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return default


def iso_date(value: Any) -> Optional[str]:
    """Convierte fecha a formato ISO. Reemplaza múltiples _iso_date."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    txt = safe_text(value)
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(txt[:10], fmt[:len(fmt.split("T")[0]) + 2 if "T" in fmt else len(fmt)]).date().isoformat()
        except Exception:
            continue
    return None


def serialize_for_json(obj: Any) -> Any:
    """Serializa datetime/date a ISO string recursivamente."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [serialize_for_json(i) for i in obj]
    return obj


def chunk_list(lst: List, size: int) -> List[List]:
    """Divide una lista en chunks de tamaño fijo."""
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Limita un valor entre min y max."""
    return max(min_val, min(max_val, value))
