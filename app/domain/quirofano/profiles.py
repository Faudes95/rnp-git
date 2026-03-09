from __future__ import annotations

from app.core.boot_profile import BOOT_PROFILE_MINIMAL_JEFATURA, BOOT_PROFILE_QUIROFANO

QUIROFANO_ROUTER_MODULE_IDS = frozenset({"quirofano", "quirofano_web", "urgencias", "jefatura_quirofano"})
QUIROFANO_BOOT_PROFILES = frozenset({BOOT_PROFILE_QUIROFANO, BOOT_PROFILE_MINIMAL_JEFATURA})
