"""Wrappers HTTP del dominio de hospitalización para migración incremental."""

from app.api.enfermeria import router as enfermeria_router
from app.api.hospitalizacion import router as hospitalizacion_router
from app.api.inpatient_labs_notes import router as inpatient_labs_notes_router
from app.api.inpatient_notes import router as inpatient_notes_router
from app.api.inpatient_time_series import router as inpatient_time_series_router
from app.api.ward_smart import router as ward_smart_router

__all__ = [
    "enfermeria_router",
    "hospitalizacion_router",
    "inpatient_labs_notes_router",
    "inpatient_notes_router",
    "inpatient_time_series_router",
    "ward_smart_router",
]
