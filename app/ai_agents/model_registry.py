from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional

try:
    import joblib
except Exception:
    joblib = None


_LOCK = threading.Lock()
_MODEL_CACHE: Dict[str, Any] = {}
_MODEL_META: Dict[str, Dict[str, Any]] = {}


def _norm_path(path: str) -> str:
    return os.path.abspath(os.path.expanduser(str(path or "").strip()))


def load_model_cached(path: str, *, force_reload: bool = False) -> Any:
    """Carga un modelo joblib una sola vez por proceso.

    Diseño aditivo: si falla, retorna None sin romper flujo clínico.
    """
    if joblib is None:
        return None
    full = _norm_path(path)
    if not full:
        return None
    if not force_reload:
        cached = _MODEL_CACHE.get(full)
        if cached is not None:
            meta = _MODEL_META.setdefault(full, {})
            meta["hits"] = int(meta.get("hits") or 0) + 1
            meta["last_access_ts"] = time.time()
            return cached
    meta = _MODEL_META.setdefault(full, {})
    meta["misses"] = int(meta.get("misses") or 0) + 1
    meta["last_access_ts"] = time.time()
    if not os.path.exists(full):
        meta["last_error"] = "missing_file"
        return None
    with _LOCK:
        if not force_reload and full in _MODEL_CACHE:
            meta["hits"] = int(meta.get("hits") or 0) + 1
            meta["last_access_ts"] = time.time()
            return _MODEL_CACHE[full]
        try:
            start = time.perf_counter()
            model = joblib.load(full)
            elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
        except Exception:
            meta["errors"] = int(meta.get("errors") or 0) + 1
            meta["last_error"] = "joblib_load_failed"
            return None
        _MODEL_CACHE[full] = model
        meta["loads"] = int(meta.get("loads") or 0) + 1
        meta["last_load_ms"] = elapsed_ms
        meta["last_loaded_ts"] = time.time()
        meta["last_error"] = None
        return model


def warmup_models(paths: List[str]) -> Dict[str, bool]:
    status: Dict[str, bool] = {}
    for raw in paths:
        p = _norm_path(raw)
        if not p:
            continue
        status[p] = load_model_cached(p) is not None
    return status


def model_cache_status() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    all_paths = set(_MODEL_META.keys()) | set(_MODEL_CACHE.keys())
    for path in sorted(all_paths):
        model = _MODEL_CACHE.get(path)
        meta = _MODEL_META.get(path, {})
        out[path] = {
            "loaded": model is not None,
            "type": type(model).__name__ if model is not None else "None",
            "hits": int(meta.get("hits") or 0),
            "misses": int(meta.get("misses") or 0),
            "loads": int(meta.get("loads") or 0),
            "errors": int(meta.get("errors") or 0),
            "last_load_ms": meta.get("last_load_ms"),
            "last_error": meta.get("last_error"),
            "last_access_ts": meta.get("last_access_ts"),
            "last_loaded_ts": meta.get("last_loaded_ts"),
        }
    return out


def get_cached_model(path: str) -> Optional[Any]:
    return _MODEL_CACHE.get(_norm_path(path))


def clear_model_cache(path: Optional[str] = None) -> Dict[str, Any]:
    with _LOCK:
        if path:
            key = _norm_path(path)
            removed = key in _MODEL_CACHE
            _MODEL_CACHE.pop(key, None)
            return {"cleared": int(1 if removed else 0), "scope": "single", "path": key}
        count = len(_MODEL_CACHE)
        _MODEL_CACHE.clear()
        return {"cleared": int(count), "scope": "all"}
