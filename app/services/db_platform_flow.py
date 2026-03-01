from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text


def _engine_mode(url: str) -> str:
    u = str(url or "").lower()
    if u.startswith("postgres"):
        return "postgres"
    if u.startswith("sqlite"):
        return "sqlite"
    if not u:
        return "unknown"
    return "other"


def _safe_scalar(bind: Any, sql: str) -> Optional[Any]:
    try:
        with bind.connect() as conn:
            return conn.execute(text(sql)).scalar()
    except Exception:
        return None


def _pgvector_enabled(bind: Any) -> bool:
    value = _safe_scalar(
        bind,
        "SELECT 1 FROM pg_extension WHERE extname='vector' LIMIT 1",
    )
    return bool(value)


def _db_status(label: str, *, url: str, bind: Any) -> Dict[str, Any]:
    mode = _engine_mode(url)
    status: Dict[str, Any] = {
        "label": label,
        "mode": mode,
        "url_masked": _mask_url(url),
        "ok": False,
        "pgvector_enabled": False,
    }
    if bind is None:
        return status

    ping = _safe_scalar(bind, "SELECT 1")
    status["ok"] = bool(ping == 1)
    if mode == "postgres":
        status["pgvector_enabled"] = _pgvector_enabled(bind)
    return status


def _mask_url(url: str) -> str:
    txt = str(url or "")
    if "@" not in txt:
        return txt
    prefix, suffix = txt.split("@", 1)
    if "://" in prefix:
        scheme, rest = prefix.split("://", 1)
        if ":" in rest:
            user = rest.split(":", 1)[0]
            return f"{scheme}://{user}:***@{suffix}"
        return f"{scheme}://***@{suffix}"
    return txt


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

    return {
        "target_mode": desired_mode,
        "ready_for_target": bool(desired_ok and desired_pgvector and clinical["ok"] and surgical["ok"]),
        "clinical": clinical,
        "surgical": surgical,
        "notes": [
            "ready_for_target=true implica motores en modo objetivo, ping OK y pgvector operativo (si aplica)."
        ],
    }

