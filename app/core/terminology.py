from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Any, Dict, Optional

from app.core.validators import normalize_whitespace


def _norm_key(value: Any) -> str:
    txt = normalize_whitespace(value).upper()
    txt = unicodedata.normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")
    txt = re.sub(r"[^A-Z0-9]+", " ", txt).strip()
    return re.sub(r"\s+", " ", txt)


_DX_ALIAS_TO_KEY = {
    "CANCER DE PROSTATA": "ca_prostata",
    "CA PROSTATA": "ca_prostata",
    "CANCER DE VEJIGA": "ca_vejiga",
    "CA VEJIGA": "ca_vejiga",
    "CANCER RENAL": "ca_rinon",
    "CANCER DE RINON": "ca_rinon",
    "CA RINON": "ca_rinon",
    "CALCULO DEL RINON": "litiasis_rinon",
    "LITIASIS RENAL": "litiasis_rinon",
    "CALCULO DEL URETER": "litiasis_ureter",
    "LITIASIS URETERAL": "litiasis_ureter",
    "CALCULO DE VEJIGA": "litiasis_vejiga",
    "LITIASIS VESICAL": "litiasis_vejiga",
    "HIPERPLASIA PROSTATICA BENIGNA": "hpb",
    "HPB": "hpb",
}


_DX_KEY_TO_CIE10 = {
    "ca_prostata": "C61",
    "ca_vejiga": "C67.9",
    "ca_rinon": "C64",
    "litiasis_rinon": "N20.0",
    "litiasis_ureter": "N20.1",
    "litiasis_vejiga": "N21.0",
    "hpb": "N40",
}


_PROC_ALIAS_TO_SNOMED_KEY = {
    "PROSTATECTOMIA RADICAL": "ca_prostata",
    "RESECCION TRANSURETRAL DE VEJIGA": "ca_vejiga",
}


_LAB_ALIAS_TO_LOINC_KEY = {
    "PESO": "peso",
    "TALLA": "talla",
    "IMC": "imc",
    "FRECUENCIA CARDIACA": "fc",
    "FC": "fc",
    "TEMPERATURA": "temp",
    "TEMP": "temp",
}


@lru_cache(maxsize=1)
def _catalog_maps() -> Dict[str, Dict[str, Any]]:
    try:
        from catalogs import get_icd11_map, get_loinc_map, get_snomed_map

        return {
            "cie11": get_icd11_map() or {},
            "loinc": get_loinc_map() or {},
            "snomed": get_snomed_map() or {},
        }
    except Exception:
        return {"cie11": {}, "loinc": {}, "snomed": {}}


def normalize_diagnostico(
    diagnostico: Any,
    *,
    cie10_codigo: Any = "",
) -> Dict[str, Any]:
    raw = normalize_whitespace(diagnostico).upper()
    key = _DX_ALIAS_TO_KEY.get(_norm_key(raw), "")
    maps = _catalog_maps()
    cie11_code = ""
    cie11_display = ""
    if key and key in maps["cie11"]:
        cie11_code, cie11_display = maps["cie11"][key]
    cie10_norm = normalize_whitespace(cie10_codigo).upper() or _DX_KEY_TO_CIE10.get(key, "")
    return {
        "raw": raw,
        "normalized": raw,
        "catalog_key": key or None,
        "cie10_codigo": cie10_norm or None,
        "cie11_codigo": (str(cie11_code).upper() if cie11_code else None),
        "cie11_display": (str(cie11_display).upper() if cie11_display else None),
    }


def normalize_procedimiento(procedimiento: Any) -> Dict[str, Any]:
    raw = normalize_whitespace(procedimiento).upper()
    snomed_key = _PROC_ALIAS_TO_SNOMED_KEY.get(_norm_key(raw), "")
    snomed_code: Optional[str] = None
    if snomed_key:
        snomed_code = _catalog_maps()["snomed"].get(snomed_key)
    return {
        "raw": raw,
        "normalized": raw,
        "snomed_codigo": (str(snomed_code).upper() if snomed_code else None),
        "catalog_key": snomed_key or None,
    }


def normalize_lab_name(test_name: Any, *, test_code: Any = "") -> Dict[str, Any]:
    raw_name = normalize_whitespace(test_name).upper()
    raw_code = normalize_whitespace(test_code).upper()
    loinc_key = _LAB_ALIAS_TO_LOINC_KEY.get(_norm_key(raw_name), "")
    loinc_code = _catalog_maps()["loinc"].get(loinc_key) if loinc_key else None
    if raw_code and not loinc_code:
        loinc_code = raw_code
    return {
        "raw_name": raw_name,
        "raw_code": raw_code,
        "normalized_name": raw_name,
        "loinc_codigo": (str(loinc_code).upper() if loinc_code else None),
        "catalog_key": loinc_key or None,
    }
