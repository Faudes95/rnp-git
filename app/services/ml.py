"""Servicios de ML extraídos progresivamente (sin dependencias de FastAPI)."""

from typing import Optional


def parse_numeric_token(value: Optional[str]) -> int:
    if not value:
        return 0
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else 0

