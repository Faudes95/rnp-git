#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app


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


def main():
    output = {
        "count": 0,
        "routes": collect_routes(),
    }
    output["count"] = len(output["routes"])
    out_path = Path("snapshots/routes_snapshot.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"snapshot_saved={out_path} routes={output['count']}")


if __name__ == "__main__":
    main()
