from __future__ import annotations

from typing import FrozenSet

BOOT_PROFILE_FULL = "full"
BOOT_PROFILE_MINIMAL_JEFATURA = "minimal_jefatura"
BOOT_PROFILE_CONSULTA = "consulta"
BOOT_PROFILE_HOSPITALIZACION = "hospitalizacion"
BOOT_PROFILE_QUIROFANO = "quirofano"
BOOT_PROFILE_EXPEDIENTE = "expediente"
BOOT_PROFILE_INVESTIGACION = "investigacion"
BOOT_PROFILE_JEFATURA_UROLOGIA = "jefatura_urologia"
BOOT_PROFILE_RESIDENTES_UROLOGIA = "residentes_urologia"

_KNOWN_BOOT_PROFILES = {
    BOOT_PROFILE_FULL,
    BOOT_PROFILE_MINIMAL_JEFATURA,
    BOOT_PROFILE_CONSULTA,
    BOOT_PROFILE_HOSPITALIZACION,
    BOOT_PROFILE_QUIROFANO,
    BOOT_PROFILE_EXPEDIENTE,
    BOOT_PROFILE_INVESTIGACION,
    BOOT_PROFILE_JEFATURA_UROLOGIA,
    BOOT_PROFILE_RESIDENTES_UROLOGIA,
}

_INTERNAL_VALIDATION_BOOT_PROFILES: FrozenSet[str] = frozenset(
    {
        BOOT_PROFILE_MINIMAL_JEFATURA,
        BOOT_PROFILE_CONSULTA,
        BOOT_PROFILE_HOSPITALIZACION,
        BOOT_PROFILE_QUIROFANO,
        BOOT_PROFILE_EXPEDIENTE,
        BOOT_PROFILE_INVESTIGACION,
        BOOT_PROFILE_JEFATURA_UROLOGIA,
        BOOT_PROFILE_RESIDENTES_UROLOGIA,
    }
)


def normalize_app_boot_profile(raw_value: object) -> str:
    value = str(raw_value or BOOT_PROFILE_FULL).strip().lower()
    return value if value in _KNOWN_BOOT_PROFILES else BOOT_PROFILE_FULL


def is_minimal_jefatura_profile(profile: object) -> bool:
    return normalize_app_boot_profile(profile) == BOOT_PROFILE_MINIMAL_JEFATURA


def is_internal_validation_profile(profile: object) -> bool:
    return normalize_app_boot_profile(profile) in _INTERNAL_VALIDATION_BOOT_PROFILES


def uses_main_full_entrypoint(profile: object) -> bool:
    return not is_minimal_jefatura_profile(profile)


def list_known_boot_profiles() -> tuple[str, ...]:
    return tuple(sorted(_KNOWN_BOOT_PROFILES))
