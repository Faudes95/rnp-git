from typing import Any, Optional

from app.core.validators import normalize_curp as _normalize_curp
from app.core.validators import normalize_nss_10 as _normalize_nss_10


def normalize_upper(value: Optional[str]) -> str:
    return value.strip().upper() if isinstance(value, str) else ""


def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_curp(value: str) -> str:
    return _normalize_curp(value)


def normalize_nss(value: str) -> str:
    # Compatibilidad aditiva: se mantiene la estrategia legacy (primeros 10)
    # para no romper cruces históricos.
    return _normalize_nss_10(value, strategy="legacy_left")


def classify_age_group(age: Optional[int]) -> str:
    if age is None:
        return "SIN_EDAD"
    if age < 18:
        return "MENOR_18"
    if age <= 25:
        return "18-25"
    if age <= 35:
        return "26-35"
    if age <= 45:
        return "36-45"
    if age <= 55:
        return "46-55"
    if age <= 60:
        return "56-60"
    if age <= 80:
        return str(age)
    return "MAS DE 80"
