"""Wrappers HTTP del dominio de quirófano para migración incremental."""

from app.api.quirofano import router as api_router
from app.api.quirofano_jefatura_web import router as jefatura_quirofano_router
from app.api.urgencias import router as urgencias_router

__all__ = ["api_router", "jefatura_quirofano_router", "urgencias_router"]
