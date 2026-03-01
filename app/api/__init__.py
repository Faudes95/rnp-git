"""Routers API por dominio (migración progresiva)."""

from typing import List

from fastapi import APIRouter

from .admin import router as admin_router
from .catalogs import router as catalogs_router
from .consulta import router as consulta_router
from .consulta_sections import router as consulta_sections_router
from .consulta_sections import metadata_router as consulta_metadata_router
from .consulta_externa import router as consulta_externa_router
from .compat_routes import router as compat_routes_router
from .dashboard import router as dashboard_router
from .forms_metadata import router as forms_metadata_router
from .fau_bot import router as fau_bot_router
from .fau_bot_core import router as fau_bot_core_router
from .expediente_plus import router as expediente_plus_router
from .fhir import router as fhir_router
from .hospitalizacion import router as hospitalizacion_router
from .master_identity import router as master_identity_router
from .inpatient_notes import router as inpatient_notes_router
from .inpatient_labs_notes import router as inpatient_labs_notes_router
from .inpatient_time_series import router as inpatient_time_series_router
from .legacy_core import router as legacy_core_router
from .perfil_clinico import router as perfil_clinico_router
from .legacy_web import router as legacy_web_router
from .quirofano import router as quirofano_router
from .reporte import router as reporte_router
from .reporte_stats import router as reporte_stats_router
from .urgencias import router as urgencias_router
from .urology_devices_events import router as urology_devices_events_router
from .ui_nav import router as ui_nav_router
from .v1 import router as v1_router
from .ward_smart import router as ward_smart_router
from .ehr_integrado import router as ehr_integrado_router
from .patient_autofill import router as patient_autofill_router
from .governance import router as governance_router
from .clinical_validation import router as clinical_validation_router
from .interconsultas import router as interconsultas_router
from .auth_login import router as auth_login_router
from .firma_electronica import router as firma_electronica_router
from .enfermeria import router as enfermeria_router
from .notificaciones import router as notificaciones_router


def get_api_routers() -> List[APIRouter]:
    return [
        admin_router,
        catalogs_router,
        compat_routes_router,
        legacy_core_router,
        consulta_router,
        consulta_sections_router,
        consulta_metadata_router,
        consulta_externa_router,
        reporte_router,
        reporte_stats_router,
        legacy_web_router,
        quirofano_router,
        urgencias_router,
        ui_nav_router,
        v1_router,
        hospitalizacion_router,
        inpatient_notes_router,
        inpatient_labs_notes_router,
        inpatient_time_series_router,
        master_identity_router,
        fhir_router,
        urology_devices_events_router,
        dashboard_router,
        forms_metadata_router,
        expediente_plus_router,
        fau_bot_router,
        fau_bot_core_router,
        ward_smart_router,
        perfil_clinico_router,
        ehr_integrado_router,
        patient_autofill_router,
        governance_router,
        clinical_validation_router,
        interconsultas_router,
        auth_login_router,
        firma_electronica_router,
        enfermeria_router,
        notificaciones_router,
    ]
