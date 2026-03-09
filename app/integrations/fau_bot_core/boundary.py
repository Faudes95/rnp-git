from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from fau_bot_core.config import get_config


ROOT = Path(__file__).resolve().parents[3]


def _script_status(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
    }


def build_fau_bot_core_boundary_status(base_status: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = get_config()
    status = dict(base_status or {})
    readonly_scripts = {
        "clinical": _script_status(ROOT / "fau_bot_core" / "sql" / "setup_readonly_role_urologia.sql"),
        "surgical": _script_status(ROOT / "fau_bot_core" / "sql" / "setup_readonly_role_urologia_quirurgico.sql"),
    }
    status["boundary_contract"] = {
        "bridge_mode": "embedded_service",
        "clinical_ro_dsn_configured": bool(cfg.clinical_ro_dsn),
        "surgical_ro_dsn_configured": bool(cfg.surgical_ro_dsn),
        "output_dsn_configured": bool(cfg.output_dsn),
        "read_only_expected": True,
        "output_schema": cfg.output_schema,
        "output_isolation": bool(cfg.output_schema),
        "readonly_role_scripts": readonly_scripts,
        "notes": [
            "fau_bot_core debe leer por DSN read-only y escribir solo en su esquema de salida.",
            "La salida del core queda aislada en output_schema para no contaminar tablas clínicas base.",
        ],
    }
    return status
