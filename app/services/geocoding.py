"""Servicios de geocodificación extraídos progresivamente."""

from typing import Optional, Tuple


def build_geocoding_query(alcaldia: str, colonia: str, cp: str) -> Optional[str]:
    if not alcaldia and not colonia:
        return None
    query = f"{colonia or ''}, {alcaldia or ''}, CDMX, Mexico".strip(", ")
    if cp:
        query = f"{colonia or ''}, {alcaldia or ''}, {cp}, CDMX, Mexico".strip(", ")
    return query


def safe_latlon(lat: Optional[float], lon: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    try:
        return (float(lat), float(lon)) if lat is not None and lon is not None else (None, None)
    except Exception:
        return None, None

