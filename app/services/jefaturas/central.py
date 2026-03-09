"""Fachada de servicios para Central académica de Urología."""

from app.services.jefatura_central_flow import (
    assign_central_exam_from_request,
    create_central_exam_template_from_request,
    render_jefatura_central_exam_assignment_flow,
    render_jefatura_central_exams_flow,
    render_jefatura_central_home_flow,
    render_resident_exam_flow,
    submit_resident_exam_response,
)
from app.services.jefatura_central_insumos_flow import render_jefatura_central_insumos_flow
from app.services.jefatura_central_records_flow import (
    create_central_case_from_request,
    create_central_incidence_from_request,
    render_jefatura_central_cases_flow,
    render_jefatura_central_incidences_flow,
    update_central_case_from_request,
    update_central_incidence_from_request,
)

__all__ = [
    "assign_central_exam_from_request",
    "create_central_case_from_request",
    "create_central_exam_template_from_request",
    "create_central_incidence_from_request",
    "render_jefatura_central_cases_flow",
    "render_jefatura_central_exam_assignment_flow",
    "render_jefatura_central_exams_flow",
    "render_jefatura_central_home_flow",
    "render_jefatura_central_incidences_flow",
    "render_jefatura_central_insumos_flow",
    "render_resident_exam_flow",
    "submit_resident_exam_response",
    "update_central_case_from_request",
    "update_central_incidence_from_request",
]
