from __future__ import annotations

import importlib
from functools import lru_cache
from types import ModuleType

from app.core.errors import InfrastructureDomainError


@lru_cache(maxsize=1)
def get_main_module() -> ModuleType:
    """Acceso centralizado y cacheado al módulo principal legacy."""
    try:
        return importlib.import_module("main")
    except Exception as exc:
        raise InfrastructureDomainError(
            "No se pudo cargar el módulo principal",
            details={"module": "main", "error": str(exc)},
        ) from exc


class _MainModuleProxy:
    def __getattr__(self, name: str):
        module = get_main_module()
        try:
            return getattr(module, name)
        except AttributeError as exc:
            raise InfrastructureDomainError(
                f"El símbolo '{name}' no está disponible en main.py",
                details={"symbol": name},
            ) from exc


main_proxy = _MainModuleProxy()

