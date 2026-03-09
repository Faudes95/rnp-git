from __future__ import annotations

from app.core.boot_profile import BOOT_PROFILE_EXPEDIENTE

EXPEDIENTE_ROUTER_MODULE_IDS = frozenset(
    {"expediente_web", "expediente_plus", "perfil_clinico", "ehr_integrado", "master_identity", "patient_autofill"}
)
EXPEDIENTE_BOOT_PROFILES = frozenset({BOOT_PROFILE_EXPEDIENTE})
