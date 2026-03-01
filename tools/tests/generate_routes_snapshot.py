#!/usr/bin/env python3
"""Genera snapshot de rutas FastAPI para no-regresión de contratos HTTP."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, List


os.environ.setdefault("AUTH_ENABLED", "false")
os.environ.setdefault("ASYNC_EMBEDDINGS", "true")
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app  # noqa: E402


SNAPSHOT_PATH = Path("snapshots/routes_snapshot.json")


def collect_routes() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(list(getattr(route, "methods", []) or []))
        name = getattr(route, "name", "")
        if not path or not methods:
            continue
        rows.append({"path": path, "methods": methods, "name": name})
    rows.sort(key=lambda r: (str(r["path"]), ",".join(r["methods"]), str(r["name"])))
    return rows


def main() -> None:
    routes = collect_routes()
    payload = {"count": len(routes), "routes": routes}
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"snapshot actualizado: {SNAPSHOT_PATH} ({payload['count']} rutas)")


if __name__ == "__main__":
    main()
