from __future__ import annotations

BOOT_PROFILE_FULL = "full"
BOOT_PROFILE_MINIMAL_JEFATURA = "minimal_jefatura"

_KNOWN_BOOT_PROFILES = {
    BOOT_PROFILE_FULL,
    BOOT_PROFILE_MINIMAL_JEFATURA,
}


def normalize_app_boot_profile(raw_value: object) -> str:
    value = str(raw_value or BOOT_PROFILE_FULL).strip().lower()
    return value if value in _KNOWN_BOOT_PROFILES else BOOT_PROFILE_FULL


def is_minimal_jefatura_profile(profile: object) -> bool:
    return normalize_app_boot_profile(profile) == BOOT_PROFILE_MINIMAL_JEFATURA
