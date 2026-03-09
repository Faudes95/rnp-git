"""Fachada de servicios para consulta y consulta externa."""

from app.services.consulta import mensaje_estatus_consulta
from app.services.consulta_externa_flow import (
    CONSULTA_EXTERNA_ATENCIONES,
    api_consulta_externa_recetas_ingest_flow,
    api_consulta_externa_servicios_stats_flow,
    consulta_externa_home_flow,
    consulta_externa_leoch_form_flow,
    consulta_externa_leoch_guardar_flow,
    consulta_externa_recetas_placeholder_flow,
    consulta_externa_uroendoscopia_form_flow,
    consulta_externa_uroendoscopia_guardar_flow,
    ensure_consulta_externa_schema,
)

__all__ = [
    "CONSULTA_EXTERNA_ATENCIONES",
    "api_consulta_externa_recetas_ingest_flow",
    "api_consulta_externa_servicios_stats_flow",
    "consulta_externa_home_flow",
    "consulta_externa_leoch_form_flow",
    "consulta_externa_leoch_guardar_flow",
    "consulta_externa_recetas_placeholder_flow",
    "consulta_externa_uroendoscopia_form_flow",
    "consulta_externa_uroendoscopia_guardar_flow",
    "ensure_consulta_externa_schema",
    "mensaje_estatus_consulta",
]
