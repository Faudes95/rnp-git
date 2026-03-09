"""Wrappers HTTP del dominio de consulta para migración incremental."""

from app.api.consulta import router as consulta_router
from app.api.consulta_externa import router as consulta_externa_router
from app.api.interconsultas import router as interconsultas_router

__all__ = ["consulta_router", "consulta_externa_router", "interconsultas_router"]
