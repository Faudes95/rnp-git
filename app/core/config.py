import os
from typing import Set

# Campos sensibles para redacción en logs
PII_FIELDS: Set[str] = {
    "curp",
    "nss",
    "nombre",
    "nombre_completo",
    "telefono",
    "email",
}


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return raw if raw is not None else default


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default

