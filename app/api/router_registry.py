from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import FrozenSet, List

from fastapi import APIRouter

from app.core.boot_profile import BOOT_PROFILE_FULL, BOOT_PROFILE_MINIMAL_JEFATURA, normalize_app_boot_profile


@dataclass(frozen=True)
class RouterSpec:
    module_name: str
    attr_name: str = "router"
    profiles: FrozenSet[str] = frozenset({BOOT_PROFILE_FULL})


ROUTER_SPECS = (
    RouterSpec(".admin"),
    RouterSpec(".catalogs"),
    RouterSpec(".compat_routes"),
    RouterSpec(".legacy_core"),
    RouterSpec(".consulta"),
    RouterSpec(".consulta_sections"),
    RouterSpec(".consulta_sections", attr_name="metadata_router"),
    RouterSpec(".consulta_externa"),
    RouterSpec(".reporte"),
    RouterSpec(".reporte_stats"),
    RouterSpec(".jefatura_web", profiles=frozenset({BOOT_PROFILE_FULL, BOOT_PROFILE_MINIMAL_JEFATURA})),
    RouterSpec(".quirofano_jefatura_web", profiles=frozenset({BOOT_PROFILE_FULL, BOOT_PROFILE_MINIMAL_JEFATURA})),
    RouterSpec(".legacy_web"),
    RouterSpec(".quirofano"),
    RouterSpec(".urgencias"),
    RouterSpec(".ui_nav", profiles=frozenset({BOOT_PROFILE_FULL, BOOT_PROFILE_MINIMAL_JEFATURA})),
    RouterSpec(".v1"),
    RouterSpec(".hospitalizacion"),
    RouterSpec(".inpatient_notes"),
    RouterSpec(".inpatient_labs_notes"),
    RouterSpec(".inpatient_time_series"),
    RouterSpec(".master_identity"),
    RouterSpec(".fhir"),
    RouterSpec(".urology_devices_events"),
    RouterSpec(".dashboard"),
    RouterSpec(".forms_metadata"),
    RouterSpec(".expediente_plus"),
    RouterSpec(".fau_bot"),
    RouterSpec(".fau_bot_core"),
    RouterSpec(".ward_smart"),
    RouterSpec(".perfil_clinico"),
    RouterSpec(".ehr_integrado"),
    RouterSpec(".patient_autofill"),
    RouterSpec(".governance"),
    RouterSpec(".clinical_validation"),
    RouterSpec(".interconsultas"),
    RouterSpec(".auth_login"),
    RouterSpec(".firma_electronica"),
    RouterSpec(".enfermeria"),
    RouterSpec(".notificaciones"),
)


def get_api_routers(profile: str = BOOT_PROFILE_FULL) -> List[APIRouter]:
    resolved_profile = normalize_app_boot_profile(profile)
    routers: List[APIRouter] = []
    for spec in ROUTER_SPECS:
        if resolved_profile not in spec.profiles:
            continue
        module = importlib.import_module(spec.module_name, package=__package__)
        routers.append(getattr(module, spec.attr_name))
    return routers
