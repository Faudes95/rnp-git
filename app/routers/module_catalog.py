from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class RouterSpec:
    module_name: str
    attr_name: str = "router"


@dataclass(frozen=True)
class RouterModule:
    module_id: str
    domain: str
    label: str
    specs: Tuple[RouterSpec, ...]


ROUTER_MODULES: Tuple[RouterModule, ...] = (
    RouterModule("admin", "infra", "Administración", (RouterSpec("app.api.admin"),)),
    RouterModule("catalogs", "infra", "Catálogos", (RouterSpec("app.api.catalogs"),)),
    RouterModule("compat", "infra", "Compatibilidad HTTP", (RouterSpec("app.api.compat_routes"),)),
    RouterModule("legacy_core", "infra", "Core legacy", (RouterSpec("app.api.legacy_core"),)),
    RouterModule("consulta", "consulta", "Consulta", (RouterSpec("app.routers.consulta", attr_name="consulta_router"), RouterSpec("app.api.consulta_sections"), RouterSpec("app.api.consulta_sections", attr_name="metadata_router"))),
    RouterModule("consulta_externa", "consulta", "Consulta externa", (RouterSpec("app.routers.consulta", attr_name="consulta_externa_router"),)),
    RouterModule("reporte", "analytics", "Reportes", (RouterSpec("app.api.reporte"),)),
    RouterModule("reporte_stats", "analytics", "Reportes estadísticos", (RouterSpec("app.api.reporte_stats"),)),
    RouterModule("jefaturas", "jefaturas", "Jefatura de Urología", (RouterSpec("app.routers.jefaturas", attr_name="jefatura_urologia_router"),)),
    RouterModule("jefatura_quirofano", "quirofano", "Jefatura de Quirófano", (RouterSpec("app.routers.quirofano", attr_name="jefatura_quirofano_router"),)),
    RouterModule("legacy_web", "infra", "Navegación legacy", (RouterSpec("app.api.legacy_web"),)),
    RouterModule("quirofano", "quirofano", "Quirófano", (RouterSpec("app.routers.quirofano", attr_name="api_router"),)),
    RouterModule("urgencias", "quirofano", "Urgencias", (RouterSpec("app.routers.quirofano", attr_name="urgencias_router"),)),
    RouterModule("shell", "infra", "Shell UI", (RouterSpec("app.api.ui_nav"),)),
    RouterModule("api_v1", "platform", "API v1", (RouterSpec("app.api.v1"),)),
    RouterModule("hospitalizacion", "hospitalizacion", "Hospitalización", (RouterSpec("app.routers.hospitalizacion", attr_name="hospitalizacion_router"),)),
    RouterModule("inpatient_notes", "hospitalizacion", "Notas hospitalarias", (RouterSpec("app.routers.hospitalizacion", attr_name="inpatient_notes_router"),)),
    RouterModule("inpatient_labs_notes", "hospitalizacion", "Labs y notas hospitalarias", (RouterSpec("app.routers.hospitalizacion", attr_name="inpatient_labs_notes_router"),)),
    RouterModule("inpatient_time_series", "hospitalizacion", "Series de tiempo inpatient", (RouterSpec("app.routers.hospitalizacion", attr_name="inpatient_time_series_router"),)),
    RouterModule("master_identity", "expediente", "Identidad maestra", (RouterSpec("app.routers.expediente", attr_name="master_identity_router"),)),
    RouterModule("fhir", "fhir", "FHIR", (RouterSpec("app.routers.fhir", attr_name="fhir_router"),)),
    RouterModule("urology_devices_events", "quirofano", "Eventos de dispositivos", (RouterSpec("app.api.urology_devices_events"),)),
    RouterModule("dashboard", "analytics", "Dashboard", (RouterSpec("app.api.dashboard"),)),
    RouterModule("forms_metadata", "platform", "Metadatos de formularios", (RouterSpec("app.api.forms_metadata"),)),
    RouterModule("expediente_plus", "expediente", "Expediente clínico único", (RouterSpec("app.routers.expediente", attr_name="expediente_plus_router"),)),
    RouterModule("ai_fau_bot", "ia", "FAU-BOT", (RouterSpec("app.routers.ia", attr_name="fau_bot_router"),)),
    RouterModule("ai_fau_bot_core", "ia", "FAU-BOT Core", (RouterSpec("app.routers.ia", attr_name="fau_bot_core_router"),)),
    RouterModule("ward_smart", "hospitalizacion", "Ward smart", (RouterSpec("app.routers.hospitalizacion", attr_name="ward_smart_router"),)),
    RouterModule("perfil_clinico", "expediente", "Perfil clínico", (RouterSpec("app.routers.expediente", attr_name="perfil_clinico_router"),)),
    RouterModule("ehr_integrado", "expediente", "EHR integrado", (RouterSpec("app.routers.expediente", attr_name="ehr_integrado_router"),)),
    RouterModule("patient_autofill", "expediente", "Autollenado paciente", (RouterSpec("app.routers.expediente", attr_name="patient_autofill_router"),)),
    RouterModule("governance", "infra", "Gobernanza", (RouterSpec("app.api.governance"),)),
    RouterModule("clinical_validation", "infra", "Validación clínica", (RouterSpec("app.api.clinical_validation"),)),
    RouterModule("interconsultas", "consulta", "Interconsultas", (RouterSpec("app.routers.consulta", attr_name="interconsultas_router"),)),
    RouterModule("auth_login", "infra", "Auth login", (RouterSpec("app.api.auth_login"),)),
    RouterModule("firma_electronica", "infra", "Firma electrónica", (RouterSpec("app.api.firma_electronica"),)),
    RouterModule("enfermeria", "hospitalizacion", "Enfermería", (RouterSpec("app.routers.hospitalizacion", attr_name="enfermeria_router"),)),
    RouterModule("notificaciones", "infra", "Notificaciones", (RouterSpec("app.api.notificaciones"),)),
)

ROUTER_MODULE_INDEX: Dict[str, RouterModule] = {module.module_id: module for module in ROUTER_MODULES}
ALL_ROUTER_MODULE_IDS = tuple(module.module_id for module in ROUTER_MODULES)
