from __future__ import annotations

import importlib
import os
from functools import lru_cache
from types import ModuleType

from app.core.errors import InfrastructureDomainError


@lru_cache(maxsize=1)
def get_main_module() -> ModuleType:
    """Acceso centralizado y cacheado al módulo principal legacy."""
    module_name = (os.getenv("RNP_APP_CONTEXT_MODULE", "") or "").strip()
    if not module_name:
        boot_profile = (os.getenv("APP_BOOT_PROFILE", "full") or "full").strip().lower()
        if boot_profile == "minimal_jefatura":
            module_name = "app.entrypoints.minimal_jefatura_main"
        else:
            module_name = "main_full"
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        raise InfrastructureDomainError(
            "No se pudo cargar el módulo principal",
            details={"module": module_name, "error": str(exc)},
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
