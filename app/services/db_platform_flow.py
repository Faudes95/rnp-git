from __future__ import annotations

from typing import Any, Dict

from app.infra.db.health import collect_sql_asset_health, engine_mode, mask_url, pgvector_enabled, safe_scalar


def _db_status(label: str, *, url: str, bind: Any) -> Dict[str, Any]:
    mode = engine_mode(url)
    status: Dict[str, Any] = {
        "label": label,
        "mode": mode,
        "url_masked": mask_url(url),
        "ok": False,
        "pgvector_enabled": False,
        "platform_assets": collect_sql_asset_health(database_scope=label, url=url, bind=bind),
    }
    if bind is None:
        return status

    ping = safe_scalar(bind, "SELECT 1")
    status["ok"] = bool(ping == 1)
    if mode == "postgres":
        status["pgvector_enabled"] = pgvector_enabled(bind)
    return status


def get_database_platform_status() -> Dict[str, Any]:
    from app.core.app_context import main_proxy as m

    clinical_url = str(getattr(m, "SQLALCHEMY_DATABASE_URL", "") or "")
    surgical_url = str(getattr(m, "SURGICAL_DATABASE_URL", "") or "")
    target = str(getattr(m, "DATABASE_PLATFORM_TARGET", "postgres") or "postgres").strip().lower()

    clinical = _db_status("clinical", url=clinical_url, bind=getattr(m, "engine", None))
    surgical = _db_status("surgical", url=surgical_url, bind=getattr(m, "surgical_engine", None))

    desired_mode = "postgres" if target == "postgres" else target
    desired_ok = clinical["mode"] == desired_mode and surgical["mode"] == desired_mode
    desired_pgvector = True
    if desired_mode == "postgres":
        desired_pgvector = bool(clinical["pgvector_enabled"])
    clinical_assets_ready = bool(clinical.get("platform_assets", {}).get("ready"))
    surgical_assets_ready = bool(surgical.get("platform_assets", {}).get("ready", True))

    return {
        "target_mode": desired_mode,
        "ready_for_target": bool(desired_ok and desired_pgvector and clinical["ok"] and surgical["ok"]),
        "platform_assets_ready": bool(desired_ok and clinical_assets_ready and surgical_assets_ready),
        "clinical": clinical,
        "surgical": surgical,
        "notes": [
            "ready_for_target=true implica motores en modo objetivo, ping OK y pgvector operativo (si aplica).",
            "platform_assets_ready=true implica assets SQL de la plataforma revisados o ya activos para el modo objetivo.",
        ],
    }
