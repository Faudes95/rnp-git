from __future__ import annotations

from contextvars import ContextVar
from typing import Optional
from uuid import uuid4


_correlation_id_ctx: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def generate_correlation_id() -> str:
    return uuid4().hex


def set_correlation_id(value: Optional[str]) -> object:
    normalized = str(value or "").strip() or None
    return _correlation_id_ctx.set(normalized)


def get_correlation_id(default: str = "") -> str:
    value = _correlation_id_ctx.get()
    if value:
        return value
    return str(default or "")


def reset_correlation_id(token: object) -> None:
    _correlation_id_ctx.reset(token)
