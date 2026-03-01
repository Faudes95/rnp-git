import importlib
from importlib import metadata
from typing import Dict, Any, Optional

# name -> import target
DEPENDENCY_IMPORTS = {
    "fastapi": "fastapi",
    "sqlalchemy": "sqlalchemy",
    "pydantic": "pydantic",
    "jinja2": "jinja2",
    "uvicorn": "uvicorn",
    "matplotlib": "matplotlib",
    "numpy": "numpy",
    "pandas": "pandas",
    "scikit-learn": "sklearn",
    "sentence-transformers": "sentence_transformers",
    "redis": "redis",
    "fastapi-cache2": "fastapi_cache",
    "lifelines": "lifelines",
    "prophet": "prophet",
    "folium": "folium",
    "requests": "requests",
    "celery": "celery",
    "python-multipart": "multipart",
}

_STATUS_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _package_version(dist_name: str) -> Optional[str]:
    try:
        return metadata.version(dist_name)
    except Exception:
        return None


def detect_dependencies(force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    global _STATUS_CACHE
    if _STATUS_CACHE is not None and not force_refresh:
        return _STATUS_CACHE

    status: Dict[str, Dict[str, Any]] = {}
    for dist_name, import_name in DEPENDENCY_IMPORTS.items():
        try:
            importlib.import_module(import_name)
            status[dist_name] = {
                "available": True,
                "version": _package_version(dist_name),
                "import": import_name,
                "error": None,
            }
        except Exception as exc:
            status[dist_name] = {
                "available": False,
                "version": None,
                "import": import_name,
                "error": str(exc),
            }
    _STATUS_CACHE = status
    return status


def is_available(dist_name: str) -> bool:
    return bool(detect_dependencies().get(dist_name, {}).get("available", False))

