from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


_LOCK = threading.Lock()
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL_SEC = max(10, int(os.getenv("CATALOG_CACHE_TTL_SEC", "300") or 300))

_ROOT_DIR = Path(__file__).resolve().parents[2]
_CATALOGS_DIR = Path(os.getenv("CATALOG_DIR", str(_ROOT_DIR / "catalogs")))


def _safe_name(name: Any) -> str:
    value = str(name or "").strip().lower().replace("-", "_")
    return "".join(ch for ch in value if ch.isalnum() or ch in {"_", "."})


def _normalize_items(name: str, raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        raw_items = raw.get("items") or []
    elif isinstance(raw, list):
        raw_items = raw
    elif isinstance(raw, dict):
        raw_items = []
        for k, v in raw.items():
            if isinstance(v, dict):
                node = {"key": str(k)}
                node.update(v)
                raw_items.append(node)
            else:
                raw_items.append({"key": str(k), "code": str(v)})
    else:
        raw_items = []

    items: List[Dict[str, Any]] = []
    for idx, node in enumerate(raw_items):
        if not isinstance(node, dict):
            continue
        normalized = {
            "id": int(node.get("id") or idx + 1),
            "key": str(node.get("key") or "").strip(),
            "code": str(node.get("code") or "").strip(),
            "label": str(node.get("label") or node.get("display") or "").strip(),
        }
        if not normalized["label"] and normalized["key"]:
            normalized["label"] = normalized["key"]
        if not normalized["key"] and normalized["code"]:
            normalized["key"] = normalized["code"].lower()
        if normalized["key"] or normalized["code"] or normalized["label"]:
            items.append(normalized)
    return items


def _load_from_json(name: str) -> Optional[Dict[str, Any]]:
    path = _CATALOGS_DIR / f"{name}.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    items = _normalize_items(name, raw)
    version = ""
    if isinstance(raw, dict):
        version = str(raw.get("version") or "")
    return {
        "name": name,
        "source": str(path),
        "version": version or "json",
        "items": items,
        "count": len(items),
        "loaded_at": time.time(),
    }


def _load_from_legacy_maps(name: str) -> Optional[Dict[str, Any]]:
    try:
        from catalogs import get_icd11_map, get_loinc_map, get_snomed_map
    except Exception:
        return None

    if name == "cie11":
        mapped = get_icd11_map()
        items = [{"id": i + 1, "key": str(k), "code": str(v[0]), "label": str(v[1] or k)} for i, (k, v) in enumerate(mapped.items())]
    elif name == "loinc":
        mapped = get_loinc_map()
        items = [{"id": i + 1, "key": str(k), "code": str(v), "label": str(k)} for i, (k, v) in enumerate(mapped.items())]
    elif name == "snomed":
        mapped = get_snomed_map()
        items = [{"id": i + 1, "key": str(k), "code": str(v), "label": str(k)} for i, (k, v) in enumerate(mapped.items())]
    else:
        return None
    return {
        "name": name,
        "source": "legacy_csv",
        "version": "legacy",
        "items": items,
        "count": len(items),
        "loaded_at": time.time(),
    }


def list_catalog_names() -> List[str]:
    names = set()
    try:
        for path in _CATALOGS_DIR.glob("*.json"):
            names.add(_safe_name(path.stem))
    except Exception:
        pass
    names.update({"cie11", "loinc", "snomed"})
    return sorted(n for n in names if n)


def get_catalog(name: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    normalized = _safe_name(name)
    if not normalized:
        raise ValueError("catalogo inválido")
    now = time.time()
    with _LOCK:
        cached = _CACHE.get(normalized)
        if cached and not force_refresh and (now - float(cached.get("loaded_at") or 0.0) <= _CACHE_TTL_SEC):
            return dict(cached)

    payload = _load_from_json(normalized) or _load_from_legacy_maps(normalized)
    if payload is None:
        raise KeyError(f"Catálogo no encontrado: {normalized}")
    with _LOCK:
        _CACHE[normalized] = dict(payload)
    return payload


def validate_catalog_value(name: str, *, key: Optional[str] = None, code: Optional[str] = None) -> Dict[str, Any]:
    catalog = get_catalog(name)
    key_norm = str(key or "").strip().lower()
    code_norm = str(code or "").strip().upper()
    match: Optional[Dict[str, Any]] = None
    for item in catalog.get("items") or []:
        i_key = str(item.get("key") or "").strip().lower()
        i_code = str(item.get("code") or "").strip().upper()
        if key_norm and i_key == key_norm:
            match = item
            break
        if code_norm and i_code == code_norm:
            match = item
            break
    return {
        "catalog": catalog.get("name"),
        "valid": match is not None,
        "match": match,
        "count": int(catalog.get("count") or 0),
    }

