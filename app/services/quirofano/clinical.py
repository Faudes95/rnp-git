"""Fachada de servicios para quirófano clínico legacy."""

from app.services.quirofano_flow import (
    cancelar_programacion_flow,
    guardar_postquirurgica_flow,
    guardar_quirofano_flow,
    guardar_quirofano_urgencia_flow,
    listar_quirofanos_flow,
    listar_quirofanos_urgencias_flow,
    render_postquirurgica_flow,
    render_quirofano_urgencias_flow,
    render_quirofano_urgencias_solicitud_flow,
)
from app.services.quirofano_waitlist_flow import (
    render_waitlist_ingreso_flow,
    render_waitlist_lista_flow,
    save_waitlist_ingreso_flow,
)

__all__ = [
    "cancelar_programacion_flow",
    "guardar_postquirurgica_flow",
    "guardar_quirofano_flow",
    "guardar_quirofano_urgencia_flow",
    "listar_quirofanos_flow",
    "listar_quirofanos_urgencias_flow",
    "render_postquirurgica_flow",
    "render_quirofano_urgencias_flow",
    "render_quirofano_urgencias_solicitud_flow",
    "render_waitlist_ingreso_flow",
    "render_waitlist_lista_flow",
    "save_waitlist_ingreso_flow",
]
