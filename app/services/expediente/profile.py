"""Fachada de servicios para perfil clínico y expediente integrado."""

from app.services.ehr_indexer import reindex_last_days, reindex_patient

__all__ = ["reindex_last_days", "reindex_patient"]
