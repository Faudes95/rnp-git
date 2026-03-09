"""Routers API por dominio (compatibilidad hacia app.routers)."""

from app.routers.registry import get_api_routers

__all__ = ["get_api_routers"]
