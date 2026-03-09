"""Compatibilidad legacy hacia el registro real en app.routers."""

from app.routers.registry import RouterSpec, get_api_routers

__all__ = ["RouterSpec", "get_api_routers"]
