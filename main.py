# -*- coding: utf-8 -*-
import importlib
import os

from app.core.boot_profile import (
    BOOT_PROFILE_FULL,
    normalize_app_boot_profile,
)
from app.core.profile_manifest import get_profile_manifest


APP_BOOT_PROFILE = normalize_app_boot_profile(os.getenv("APP_BOOT_PROFILE", BOOT_PROFILE_FULL))
_profile_manifest = get_profile_manifest(APP_BOOT_PROFILE)
os.environ.setdefault("RNP_APP_CONTEXT_MODULE", _profile_manifest.entrypoint_module)
_entrypoint_module = importlib.import_module(_profile_manifest.entrypoint_module)

for _symbol in dir(_entrypoint_module):
    if _symbol.startswith("_"):
        continue
    globals()[_symbol] = getattr(_entrypoint_module, _symbol)


def __getattr__(name: str):
    """Reexpone atributos perezosos del entrypoint real sin depender de import *."""
    return getattr(_entrypoint_module, name)
