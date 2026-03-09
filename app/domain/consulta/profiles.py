from __future__ import annotations

from app.core.boot_profile import BOOT_PROFILE_CONSULTA

CONSULTA_ROUTER_MODULE_IDS = frozenset({"consulta", "consulta_externa", "interconsultas"})
CONSULTA_BOOT_PROFILES = frozenset({BOOT_PROFILE_CONSULTA})
