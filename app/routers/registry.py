from __future__ import annotations

import importlib
from typing import List

from fastapi import APIRouter

from app.core.boot_profile import BOOT_PROFILE_FULL
from app.core.profile_manifest import get_profile_manifest
from app.routers.module_catalog import ALL_ROUTER_MODULE_IDS, ROUTER_MODULE_INDEX, RouterSpec


def iter_router_specs_for_profile(profile: object) -> tuple[RouterSpec, ...]:
    manifest = get_profile_manifest(profile)
    specs: list[RouterSpec] = []
    active_modules = manifest.active_modules
    for module_id in ALL_ROUTER_MODULE_IDS:
        if module_id not in active_modules:
            continue
        specs.extend(ROUTER_MODULE_INDEX[module_id].specs)
    return tuple(specs)


def get_api_routers(profile: str = BOOT_PROFILE_FULL) -> List[APIRouter]:
    routers: List[APIRouter] = []
    for spec in iter_router_specs_for_profile(profile):
        module = importlib.import_module(spec.module_name)
        routers.append(getattr(module, spec.attr_name))
    return routers
