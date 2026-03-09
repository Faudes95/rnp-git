"""Wrappers HTTP del dominio de expediente para migración incremental."""

from app.api.ehr_integrado import router as ehr_integrado_router
from app.api.expediente_plus import router as expediente_plus_router
from app.api.master_identity import router as master_identity_router
from app.api.patient_autofill import router as patient_autofill_router
from app.api.perfil_clinico import router as perfil_clinico_router

__all__ = [
    "ehr_integrado_router",
    "expediente_plus_router",
    "master_identity_router",
    "patient_autofill_router",
    "perfil_clinico_router",
]
