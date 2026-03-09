from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infra.db.sql_assets import SqlAssetSpec, iter_sql_assets_for_scope


def engine_mode(url: str) -> str:
    u = str(url or "").lower()
    if u.startswith("postgres"):
        return "postgres"
    if u.startswith("sqlite"):
        return "sqlite"
    if not u:
        return "unknown"
    return "other"


def mask_url(url: str) -> str:
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


def safe_scalar(bind: Any, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    try:
        with bind.connect() as conn:
            return conn.execute(text(sql), params or {}).scalar()
    except Exception:
        return None


def pgvector_enabled(bind: Any) -> bool:
    value = safe_scalar(bind, "SELECT 1 FROM pg_extension WHERE extname='vector' LIMIT 1")
    return bool(value)


def _relation_exists(bind: Any, relation_name: str) -> bool:
    value = safe_scalar(bind, "SELECT to_regclass(:name)", {"name": relation_name})
    return bool(value)


def _index_exists(bind: Any, index_name: str) -> bool:
    value = safe_scalar(bind, "SELECT to_regclass(:name)", {"name": index_name})
    return bool(value)


def _column_exists(bind: Any, relation_name: str, column_name: str) -> bool:
    value = safe_scalar(
        bind,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
          AND column_name = :column_name
        LIMIT 1
        """,
        {"table_name": relation_name, "column_name": column_name},
    )
    return bool(value)


def _extension_exists(bind: Any, extension_name: str) -> bool:
    value = safe_scalar(
        bind,
        "SELECT 1 FROM pg_extension WHERE extname = :extension_name LIMIT 1",
        {"extension_name": extension_name},
    )
    return bool(value)


def _prerequisites_met(bind: Any, spec: SqlAssetSpec) -> bool:
    for relation_name in spec.required_relations:
        if not _relation_exists(bind, relation_name):
            return False
    for relation_name, column_name in spec.required_columns:
        if not _column_exists(bind, relation_name, column_name):
            return False
    return True


def _verification_state(bind: Any, spec: SqlAssetSpec) -> bool:
    for relation_name in spec.verify_relations:
        if not _relation_exists(bind, relation_name):
            return False
    for relation_name, column_name in spec.verify_columns:
        if not _column_exists(bind, relation_name, column_name):
            return False
    for index_name in spec.verify_indexes:
        if not _index_exists(bind, index_name):
            return False
    for extension_name in spec.verify_extensions:
        if not _extension_exists(bind, extension_name):
            return False
    return True


def collect_sql_asset_health(*, database_scope: str, url: str, bind: Any) -> Dict[str, Any]:
    mode = engine_mode(url)
    items: List[Dict[str, Any]] = []
    if bind is None:
        return {
            "database_scope": database_scope,
            "mode": mode,
            "ready": False,
            "total": 0,
            "healthy": 0,
            "pending": 0,
            "blocked": 0,
            "manual_review_required": 0,
            "items": items,
        }

    for spec in iter_sql_assets_for_scope(database_scope):
        applicable = mode in spec.target_modes
        prerequisites_met = applicable and _prerequisites_met(bind, spec)
        healthy = applicable and prerequisites_met and _verification_state(bind, spec)
        status = "not_applicable"
        if applicable:
            if healthy:
                status = "healthy"
            elif spec.manual_review_required:
                status = "manual_review"
            elif prerequisites_met:
                status = "pending"
            else:
                status = "blocked"

        items.append(
            {
                **spec.to_dict(),
                "applicable": applicable,
                "prerequisites_met": prerequisites_met,
                "healthy": healthy,
                "status": status,
            }
        )

    applicable_items = [item for item in items if item["applicable"]]
    ready = bool(
        all(item["healthy"] or item["status"] == "manual_review" for item in applicable_items)
        and all(item["prerequisites_met"] or item["status"] == "manual_review" for item in applicable_items)
    )
    return {
        "database_scope": database_scope,
        "mode": mode,
        "ready": ready,
        "total": len(applicable_items),
        "healthy": sum(1 for item in applicable_items if item["status"] == "healthy"),
        "pending": sum(1 for item in applicable_items if item["status"] == "pending"),
        "blocked": sum(1 for item in applicable_items if item["status"] == "blocked"),
        "manual_review_required": sum(1 for item in applicable_items if item["status"] == "manual_review"),
        "items": items,
    }
