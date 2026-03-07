from typing import Any, Optional

from sqlalchemy import String, cast, func, or_

from app.core.validators import normalize_curp as _normalize_curp
from app.core.validators import nss_aliases as _nss_aliases
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


def nss_aliases(value: Any) -> list[str]:
    return _nss_aliases(value)


def nss_matches(left: Any, right: Any) -> bool:
    left_aliases = set(_nss_aliases(left))
    right_aliases = set(_nss_aliases(right))
    return bool(left_aliases and right_aliases and left_aliases.intersection(right_aliases))


def nss_compat_expr(column: Any, target_nss: Any) -> Optional[Any]:
    nss = normalize_nss(str(target_nss or ""))
    if not nss:
        return None
    col_txt = func.coalesce(cast(column, String), "")
    col_digits = func.replace(
        func.replace(
            func.replace(
                func.replace(col_txt, " ", ""),
                "-",
                "",
            ),
            "/",
            "",
        ),
        ".",
        "",
    )
    return or_(
        func.substr(col_digits, 1, 10) == nss,
        col_txt == nss,
        col_txt.like(f"{nss}%"),
        col_txt.like(f"%{nss}"),
        col_txt.like(f"%{nss}%"),
    )


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
