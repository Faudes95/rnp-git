from __future__ import annotations

import os
import subprocess
from typing import Any, Dict


def run_optional_alembic_upgrade(
    *,
    enabled: bool,
    alembic_config: str,
    base_dir: str,
) -> Dict[str, Any]:
    """
    Ejecuta migraciones Alembic opcionalmente. No rompe flujo si falla.
    """
    if not enabled:
        return {"enabled": False, "ok": False, "skipped": True}

    config_path = os.path.join(base_dir, alembic_config)
    if not os.path.exists(config_path):
        return {
            "enabled": True,
            "ok": False,
            "skipped": False,
            "error": f"Config Alembic no encontrada: {config_path}",
        }

    cmd = ["alembic", "-c", config_path, "upgrade", "head"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            return {
                "enabled": True,
                "ok": False,
                "skipped": False,
                "error": (proc.stderr or proc.stdout or "Error alembic").strip()[:1200],
            }
        return {"enabled": True, "ok": True, "skipped": False}
    except Exception as exc:
        return {"enabled": True, "ok": False, "skipped": False, "error": str(exc)}
