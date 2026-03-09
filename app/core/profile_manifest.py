from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet, Tuple

from app.core.boot_profile import (
    BOOT_PROFILE_CONSULTA,
    BOOT_PROFILE_EXPEDIENTE,
    BOOT_PROFILE_FULL,
    BOOT_PROFILE_HOSPITALIZACION,
    BOOT_PROFILE_INVESTIGACION,
    BOOT_PROFILE_JEFATURA_UROLOGIA,
    BOOT_PROFILE_MINIMAL_JEFATURA,
    BOOT_PROFILE_QUIROFANO,
    BOOT_PROFILE_RESIDENTES_UROLOGIA,
    normalize_app_boot_profile,
)
from app.domain.consulta.profiles import CONSULTA_ROUTER_MODULE_IDS
from app.domain.expediente.profiles import EXPEDIENTE_ROUTER_MODULE_IDS
from app.domain.hospitalizacion.profiles import HOSPITALIZACION_ROUTER_MODULE_IDS
from app.domain.jefaturas.profiles import JEFATURAS_ROUTER_MODULE_IDS
from app.domain.quirofano.profiles import QUIROFANO_ROUTER_MODULE_IDS
from app.routers.module_catalog import ALL_ROUTER_MODULE_IDS

FULL_ENTRYPOINT_MODULE = "main_full"
MINIMAL_JEFATURA_ENTRYPOINT_MODULE = "app.entrypoints.minimal_jefatura_main"


@dataclass(frozen=True)
class ProfileManifest:
    profile: str
    entrypoint_module: str
    active_modules: FrozenSet[str]
    internal_only: bool = True
    description: str = ""


PROFILE_MANIFESTS: Dict[str, ProfileManifest] = {
    BOOT_PROFILE_FULL: ProfileManifest(
        profile=BOOT_PROFILE_FULL,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(ALL_ROUTER_MODULE_IDS),
        internal_only=False,
        description="Perfil integral de la plataforma UROMED.",
    ),
    BOOT_PROFILE_MINIMAL_JEFATURA: ProfileManifest(
        profile=BOOT_PROFILE_MINIMAL_JEFATURA,
        entrypoint_module=MINIMAL_JEFATURA_ENTRYPOINT_MODULE,
        active_modules=frozenset({"jefaturas", "jefatura_quirofano", "shell"}),
        description="Perfil técnico acotado para Jefatura de Quirófano.",
    ),
    BOOT_PROFILE_CONSULTA: ProfileManifest(
        profile=BOOT_PROFILE_CONSULTA,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(
            {
                "compat",
                "legacy_core",
                "shell",
                *CONSULTA_ROUTER_MODULE_IDS,
                "forms_metadata",
                "master_identity",
                "patient_autofill",
                "auth_login",
                "governance",
                "clinical_validation",
            }
        ),
        description="Perfil interno para refactor de consulta y captura ambulatoria.",
    ),
    BOOT_PROFILE_HOSPITALIZACION: ProfileManifest(
        profile=BOOT_PROFILE_HOSPITALIZACION,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(
            {
                "compat",
                "legacy_core",
                "shell",
                *HOSPITALIZACION_ROUTER_MODULE_IDS,
                "forms_metadata",
                "master_identity",
                "patient_autofill",
                "auth_login",
                "governance",
                "clinical_validation",
            }
        ),
        description="Perfil interno para hospitalización, guardia y censo.",
    ),
    BOOT_PROFILE_QUIROFANO: ProfileManifest(
        profile=BOOT_PROFILE_QUIROFANO,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(
            {
                "compat",
                "legacy_core",
                "shell",
                *QUIROFANO_ROUTER_MODULE_IDS,
                "forms_metadata",
                "master_identity",
                "api_v1",
                "urology_devices_events",
                "auth_login",
                "governance",
                "clinical_validation",
            }
        ),
        description="Perfil interno para quirófano clínico y jefatura quirúrgica.",
    ),
    BOOT_PROFILE_EXPEDIENTE: ProfileManifest(
        profile=BOOT_PROFILE_EXPEDIENTE,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(
            {
                "compat",
                "legacy_core",
                "shell",
                *EXPEDIENTE_ROUTER_MODULE_IDS,
                "forms_metadata",
                "auth_login",
                "governance",
            }
        ),
        description="Perfil interno para expediente clínico único.",
    ),
    BOOT_PROFILE_INVESTIGACION: ProfileManifest(
        profile=BOOT_PROFILE_INVESTIGACION,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset(
            {
                "compat",
                "legacy_core",
                "shell",
                "dashboard",
                "reporte",
                "reporte_stats",
                "fhir",
                "api_v1",
                "governance",
            }
        ),
        description="Perfil interno para investigación, reporte y analítica.",
    ),
    BOOT_PROFILE_JEFATURA_UROLOGIA: ProfileManifest(
        profile=BOOT_PROFILE_JEFATURA_UROLOGIA,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset({"shell", *JEFATURAS_ROUTER_MODULE_IDS, "auth_login", "api_v1"}),
        description="Perfil interno para la gobernanza de Jefatura de Urología.",
    ),
    BOOT_PROFILE_RESIDENTES_UROLOGIA: ProfileManifest(
        profile=BOOT_PROFILE_RESIDENTES_UROLOGIA,
        entrypoint_module=FULL_ENTRYPOINT_MODULE,
        active_modules=frozenset({"shell", *JEFATURAS_ROUTER_MODULE_IDS, "auth_login", "api_v1"}),
        description="Perfil interno para perfiles longitudinales de residentes.",
    ),
}


def get_profile_manifest(profile: object) -> ProfileManifest:
    resolved_profile = normalize_app_boot_profile(profile)
    return PROFILE_MANIFESTS[resolved_profile]


def list_profile_manifests() -> Tuple[ProfileManifest, ...]:
    return tuple(PROFILE_MANIFESTS[key] for key in sorted(PROFILE_MANIFESTS))
