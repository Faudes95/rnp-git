"""Fachada de servicios para Jefatura de Urología y perfiles de residentes."""

from app.services.jefatura_urologia_flow import (
    render_jefatura_urologia_home_flow,
    render_jefatura_urologia_module_flow,
    render_jefatura_urologia_programa_submodule_flow,
    render_jefatura_urologia_residente_profile_flow,
)
from app.services.resident_profiles_flow import resident_profile_photo_response, update_resident_profile_from_request

__all__ = [
    "render_jefatura_urologia_home_flow",
    "render_jefatura_urologia_module_flow",
    "render_jefatura_urologia_programa_submodule_flow",
    "render_jefatura_urologia_residente_profile_flow",
    "resident_profile_photo_response",
    "update_resident_profile_from_request",
]
