"""Fachada de servicios para alta y egreso hospitalario."""

from app.services.hospitalizacion_egreso_flow import (
    api_hospitalizacion_egresos_flow,
    hospitalizacion_alta_form_flow,
    hospitalizacion_alta_guardar_flow,
    hospitalizacion_alta_imprimir_docx_flow,
    hospitalizacion_egresos_reporte_flow,
)

__all__ = [
    "api_hospitalizacion_egresos_flow",
    "hospitalizacion_alta_form_flow",
    "hospitalizacion_alta_guardar_flow",
    "hospitalizacion_alta_imprimir_docx_flow",
    "hospitalizacion_egresos_reporte_flow",
]
