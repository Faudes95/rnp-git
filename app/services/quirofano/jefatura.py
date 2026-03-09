"""Fachada de servicios para Jefatura de Quirófano."""

from app.services.quirofano_jefatura_flow import render_jefatura_quirofano_waiting_flow
from app.services.quirofano_jefatura_import_flow import (
    confirm_import_batch_from_request,
    create_import_batch_from_upload,
    render_jefatura_quirofano_import_review_flow,
    render_jefatura_quirofano_imports_flow,
    save_import_review_from_request,
)
from app.services.quirofano_jefatura_programacion_flow import (
    add_case_event_from_request,
    add_case_incidence_from_request,
    add_case_staff_from_request,
    build_dashboard_payload,
    render_jefatura_quirofano_case_detail_flow,
    render_jefatura_quirofano_day_flow,
    render_jefatura_quirofano_programacion_index_flow,
    render_jefatura_quirofano_publication_flow,
    render_jefatura_quirofano_template_flow,
    save_service_lines_from_request,
    save_template_version_from_request,
    update_daily_blocks_from_request,
    upsert_daily_case_from_request,
)
from app.services.quirofano_jefatura_shared import build_day_overview, serialize_case, serialize_daily_block

__all__ = [
    "add_case_event_from_request",
    "add_case_incidence_from_request",
    "add_case_staff_from_request",
    "build_dashboard_payload",
    "build_day_overview",
    "confirm_import_batch_from_request",
    "create_import_batch_from_upload",
    "render_jefatura_quirofano_case_detail_flow",
    "render_jefatura_quirofano_day_flow",
    "render_jefatura_quirofano_import_review_flow",
    "render_jefatura_quirofano_imports_flow",
    "render_jefatura_quirofano_programacion_index_flow",
    "render_jefatura_quirofano_publication_flow",
    "render_jefatura_quirofano_template_flow",
    "render_jefatura_quirofano_waiting_flow",
    "save_import_review_from_request",
    "save_service_lines_from_request",
    "save_template_version_from_request",
    "serialize_case",
    "serialize_daily_block",
    "update_daily_blocks_from_request",
    "upsert_daily_case_from_request",
]
