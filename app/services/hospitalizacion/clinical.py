"""Fachada de servicios para ingreso hospitalario y censo."""

from app.services.hospitalizacion_flow import (
    api_ingresos_hospitalizacion_flow,
    buscar_paciente_hospitalizacion_flow,
    cerrar_hospitalizacion_activa_flow,
    guardar_censo_cambios_flow,
    guardar_guardia_flow,
    guardar_hospitalizacion_flow,
    hospitalizacion_incapacidades_flow,
    hospitalizacion_ingresar_entry_flow,
    hospitalizacion_ingreso_preop_imprimir_docx_flow,
    hospitalizacion_urgencias_finalizar_draft_flow,
    imprimir_censo_excel_flow,
    listar_hospitalizaciones_flow,
    nuevo_hospitalizacion_form_flow,
    precheck_hospitalizacion_ingreso_flow,
    reporte_estadistico_hospitalizacion_flow,
    ver_censo_diario_flow,
)

__all__ = [
    "api_ingresos_hospitalizacion_flow",
    "buscar_paciente_hospitalizacion_flow",
    "cerrar_hospitalizacion_activa_flow",
    "guardar_censo_cambios_flow",
    "guardar_guardia_flow",
    "guardar_hospitalizacion_flow",
    "hospitalizacion_incapacidades_flow",
    "hospitalizacion_ingresar_entry_flow",
    "hospitalizacion_ingreso_preop_imprimir_docx_flow",
    "hospitalizacion_urgencias_finalizar_draft_flow",
    "imprimir_censo_excel_flow",
    "listar_hospitalizaciones_flow",
    "nuevo_hospitalizacion_form_flow",
    "precheck_hospitalizacion_ingreso_flow",
    "reporte_estadistico_hospitalizacion_flow",
    "ver_censo_diario_flow",
]
