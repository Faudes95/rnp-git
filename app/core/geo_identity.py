from __future__ import annotations

import hashlib
from typing import Any, Optional, Tuple


def build_patient_hash(nss: Optional[str], curp: Optional[str], consulta_id: Optional[int]) -> Optional[str]:
    seed = f"{(nss or '').strip()}|{(curp or '').strip()}|{consulta_id or ''}"
    if not seed.strip("|"):
        return None
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def geocode_address(
    *,
    requests_module: Any,
    geocoder_url: str,
    geocoder_user_agent: str,
    offline_strict_mode: bool,
    alcaldia: str,
    colonia: str,
    cp: str,
) -> Tuple[Optional[float], Optional[float]]:
    if requests_module is None or offline_strict_mode:
        return None, None
    if not alcaldia and not colonia:
        return None, None
    query = f"{colonia or ''}, {alcaldia or ''}, CDMX, Mexico".strip(", ")
    if cp:
        query = f"{colonia or ''}, {alcaldia or ''}, {cp}, CDMX, Mexico".strip(", ")
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "addressdetails": 0,
    }
    headers = {"User-Agent": geocoder_user_agent}
    try:
        resp = requests_module.get(geocoder_url, params=params, headers=headers, timeout=5)
        if resp.status_code != 200:
            return None, None
        data = resp.json() or []
        if not data:
            return None, None
        lat = float(data[0].get("lat"))
        lon = float(data[0].get("lon"))
        return lat, lon
    except Exception:
        return None, None
