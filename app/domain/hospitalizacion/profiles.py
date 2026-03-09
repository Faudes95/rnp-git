from __future__ import annotations

from app.core.boot_profile import BOOT_PROFILE_HOSPITALIZACION

HOSPITALIZACION_ROUTER_MODULE_IDS = frozenset(
    {
        "hospitalizacion",
        "inpatient_notes",
        "inpatient_labs_notes",
        "inpatient_time_series",
        "ward_smart",
        "enfermeria",
    }
)
HOSPITALIZACION_BOOT_PROFILES = frozenset({BOOT_PROFILE_HOSPITALIZACION})
