"""Wrappers de infraestructura para el composition root legacy."""

from app.core.composition_root import attach_lifespan, create_app_instance

__all__ = ["attach_lifespan", "create_app_instance"]
