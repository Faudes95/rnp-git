"""Fachada de servicios para guardia hospitalaria y datasets operativos."""

from app.services.guardia_template_flow import get_guardia_template, list_guardia_templates, upsert_guardia_template
from app.services.hospital_guardia_flow import (
    DATASET_SPECS,
    eliminar_hospitalizacion_guardia_dataset_flow,
    guardar_hospitalizacion_guardia_dataset_flow,
    hospitalizacion_guardia_dataset_export_docx_flow,
    hospitalizacion_guardia_dataset_flow,
    hospitalizacion_guardia_exportar_flow,
    hospitalizacion_guardia_home_flow,
    hospitalizacion_guardia_importar_form_flow,
    hospitalizacion_guardia_importar_submit_flow,
    hospitalizacion_guardia_reporte_flow,
    hospitalizacion_guardia_reporte_json_flow,
)

__all__ = [
    "DATASET_SPECS",
    "eliminar_hospitalizacion_guardia_dataset_flow",
    "get_guardia_template",
    "guardar_hospitalizacion_guardia_dataset_flow",
    "hospitalizacion_guardia_dataset_export_docx_flow",
    "hospitalizacion_guardia_dataset_flow",
    "hospitalizacion_guardia_exportar_flow",
    "hospitalizacion_guardia_home_flow",
    "hospitalizacion_guardia_importar_form_flow",
    "hospitalizacion_guardia_importar_submit_flow",
    "hospitalizacion_guardia_reporte_flow",
    "hospitalizacion_guardia_reporte_json_flow",
    "list_guardia_templates",
    "upsert_guardia_template",
]
