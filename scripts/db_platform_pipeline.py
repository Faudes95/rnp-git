#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from app.infra.db.pipeline import apply_sql_assets, review_sql_assets


def _emit(payload: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    print(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review/apply PostgreSQL data platform SQL assets.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    review_parser = subparsers.add_parser("review", help="Inspect SQL asset readiness for a DSN.")
    review_parser.add_argument("--dsn", required=True, help="Target DSN.")
    review_parser.add_argument("--database-scope", default="clinical", choices=["clinical", "surgical"])
    review_parser.add_argument("--json", action="store_true")

    apply_parser = subparsers.add_parser("apply", help="Apply SQL assets to a DSN.")
    apply_parser.add_argument("--dsn", required=True, help="Target DSN.")
    apply_parser.add_argument("--database-scope", default="clinical", choices=["clinical", "surgical"])
    apply_parser.add_argument("--asset", action="append", dest="asset_ids", default=[])
    apply_parser.add_argument("--allow-manual-review", action="store_true")
    apply_parser.add_argument("--dry-run", action="store_true")
    apply_parser.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.command == "review":
        payload = review_sql_assets(dsn=args.dsn, database_scope=args.database_scope)
        _emit(payload, as_json=args.json)
        return 0

    payload = apply_sql_assets(
        dsn=args.dsn,
        database_scope=args.database_scope,
        asset_ids=args.asset_ids,
        allow_manual_review=bool(args.allow_manual_review),
        dry_run=bool(args.dry_run),
    )
    _emit(payload, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
