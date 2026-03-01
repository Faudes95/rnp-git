import csv
import os
from typing import Dict, Tuple

CATALOG_DIR = os.getenv("CATALOG_DIR", os.path.join(os.path.dirname(__file__), "catalogs"))

_cached_icd11: Dict[str, Tuple[str, str]] = {}
_cached_loinc: Dict[str, str] = {}
_cached_snomed: Dict[str, str] = {}

DEFAULT_ICD11 = {
    "ca_prostata": ("2C82.0", "Cáncer de próstata"),
    "ca_vejiga": ("2C90.0", "Cáncer de vejiga"),
    "ca_rinon": ("2C80.0", "Cáncer de riñón"),
    "ca_testiculo": ("2C60.0", "Cáncer de testículo"),
    "ca_pene": ("2C70.0", "Cáncer de pene"),
    "ca_urotelial_alto": ("2C91.0", "Cáncer urotelial tracto superior"),
    "litiasis_rinon": ("GB60.0", "Cálculo renal"),
    "litiasis_ureter": ("GB61.0", "Cálculo ureteral"),
    "litiasis_vejiga": ("GB62.0", "Cálculo vesical"),
    "hpb": ("GA70.0", "Hiperplasia prostática benigna"),
}

DEFAULT_LOINC = {
    "peso": "29463-7",
    "talla": "8302-2",
    "imc": "39156-5",
    "ta": "8480-6",
    "fc": "8867-4",
    "temp": "8310-5",
}


def _load_csv_map(path: str, key_col: str, code_col: str, display_col: str = "display"):
    data: Dict[str, Tuple[str, str]] = {}
    if not os.path.exists(path):
        return data
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_col)
            code = row.get(code_col)
            display = row.get(display_col) or ""
            if key and code:
                data[key] = (code, display)
    return data


def _load_csv_simple(path: str, key_col: str, code_col: str):
    data: Dict[str, str] = {}
    if not os.path.exists(path):
        return data
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_col)
            code = row.get(code_col)
            if key and code:
                data[key] = code
    return data


def get_icd11_map() -> Dict[str, Tuple[str, str]]:
    global _cached_icd11
    if _cached_icd11:
        return _cached_icd11
    path = os.path.join(CATALOG_DIR, "cie11.csv")
    data = _load_csv_map(path, "key", "code", "display")
    if not data:
        data = DEFAULT_ICD11.copy()
    _cached_icd11 = data
    return _cached_icd11


def get_loinc_map() -> Dict[str, str]:
    global _cached_loinc
    if _cached_loinc:
        return _cached_loinc
    path = os.path.join(CATALOG_DIR, "loinc.csv")
    data = _load_csv_simple(path, "key", "code")
    if not data:
        data = DEFAULT_LOINC.copy()
    _cached_loinc = data
    return _cached_loinc


def get_snomed_map() -> Dict[str, str]:
    global _cached_snomed
    if _cached_snomed:
        return _cached_snomed
    path = os.path.join(CATALOG_DIR, "snomed.csv")
    data = _load_csv_simple(path, "key", "code")
    _cached_snomed = data
    return _cached_snomed
