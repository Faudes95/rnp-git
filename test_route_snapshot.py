import json
import os
from pathlib import Path

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"

from main import app


SNAPSHOT_PATH = Path("snapshots/routes_snapshot.json")


def collect_routes():
    rows = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(list(getattr(route, "methods", []) or []))
        name = getattr(route, "name", "")
        if not path or not methods:
            continue
        rows.append({"path": path, "methods": methods, "name": name})
    rows.sort(key=lambda r: (r["path"], ",".join(r["methods"]), r["name"]))
    return rows


def test_route_snapshot_exists():
    assert SNAPSHOT_PATH.exists(), "Falta snapshots/routes_snapshot.json"


def test_route_snapshot_matches_current():
    snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    current = {"count": 0, "routes": collect_routes()}
    current["count"] = len(current["routes"])
    assert snapshot == current
