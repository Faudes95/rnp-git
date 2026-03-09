from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import create_engine

from app.infra.db.health import collect_sql_asset_health, engine_mode, mask_url
from app.infra.db.sql_assets import SqlAssetSpec, get_sql_asset, get_sql_asset_registry, load_sql_asset_sql


def _build_engine(dsn: str) -> Any:
    args = {"check_same_thread": False} if str(dsn or "").startswith("sqlite") else {}
    return create_engine(dsn, connect_args=args)


def _execute_sql_script(bind: Any, sql: str, *, mode: str) -> None:
    with bind.begin() as conn:
        raw_conn = conn.connection
        if mode == "sqlite":
            raw_conn.executescript(sql)
            return
        cursor = raw_conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()


def review_sql_assets(*, dsn: str, database_scope: str = "clinical") -> Dict[str, Any]:
    engine = _build_engine(dsn)
    try:
        review = collect_sql_asset_health(database_scope=database_scope, url=dsn, bind=engine)
    finally:
        engine.dispose()
    review["dsn_masked"] = mask_url(dsn)
    review["registry"] = [spec.to_dict() for spec in get_sql_asset_registry()]
    return review


def apply_sql_assets(
    *,
    dsn: str,
    database_scope: str = "clinical",
    asset_ids: Optional[Iterable[str]] = None,
    allow_manual_review: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    selected_specs: List[SqlAssetSpec]
    if asset_ids:
        selected_specs = [get_sql_asset(asset_id) for asset_id in asset_ids]
    else:
        selected_specs = [spec for spec in get_sql_asset_registry() if spec.database_scope in (database_scope, "both")]

    engine = _build_engine(dsn)
    mode = engine_mode(dsn)
    results: List[Dict[str, Any]] = []
    try:
        pre_review = collect_sql_asset_health(database_scope=database_scope, url=dsn, bind=engine)
        for spec in selected_specs:
            item = {
                "asset_id": spec.asset_id,
                "filename": spec.filename,
                "database_scope": spec.database_scope,
                "mode": mode,
                "applied": False,
                "status": "skipped",
                "reason": "",
                "dry_run": dry_run,
            }
            if spec.database_scope not in (database_scope, "both"):
                item["reason"] = "scope_mismatch"
                results.append(item)
                continue
            if mode not in spec.target_modes:
                item["reason"] = "mode_not_supported"
                results.append(item)
                continue
            if spec.manual_review_required and not allow_manual_review:
                item["status"] = "manual_review"
                item["reason"] = "manual_review_required"
                results.append(item)
                continue

            review_item = next(
                (row for row in pre_review["items"] if row["asset_id"] == spec.asset_id),
                None,
            )
            if review_item and review_item.get("healthy"):
                item["status"] = "already_healthy"
                item["reason"] = "verification_passed"
                results.append(item)
                continue
            if review_item and not review_item.get("prerequisites_met"):
                item["status"] = "blocked"
                item["reason"] = "prerequisites_missing"
                results.append(item)
                continue

            if dry_run:
                item["status"] = "dry_run"
                item["reason"] = "eligible"
                results.append(item)
                continue

            try:
                _execute_sql_script(engine, load_sql_asset_sql(spec.asset_id), mode=mode)
                item["applied"] = True
                item["status"] = "applied"
            except Exception as exc:
                item["status"] = "error"
                item["reason"] = str(exc)
            results.append(item)

        post_review = collect_sql_asset_health(database_scope=database_scope, url=dsn, bind=engine)
    finally:
        engine.dispose()

    return {
        "database_scope": database_scope,
        "mode": mode,
        "results": results,
        "review_after": post_review,
    }
