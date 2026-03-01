#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TARGETS = [
    ROOT / "main.py",
    ROOT / "app",
    ROOT / "fau_bot_core",
]
PATTERNS = {
    "datetime.utcnow(": "Use app.core.time_utils.utcnow()",
    "@app.on_event(": "Use lifespan handlers (FastAPI modern startup/shutdown)",
}


def iter_py_files(path: Path):
    if path.is_file() and path.suffix == ".py":
        yield path
        return
    if path.is_dir():
        for p in path.rglob("*.py"):
            yield p


def main() -> int:
    violations = []
    for target in TARGETS:
        for py in iter_py_files(target):
            text = py.read_text(encoding="utf-8", errors="ignore")
            for needle, hint in PATTERNS.items():
                if needle in text:
                    violations.append((py, needle, hint))

    if violations:
        print("Deprecation policy violations found:")
        for py, needle, hint in violations:
            rel = py.relative_to(ROOT)
            print(f"- {rel}: found `{needle}` -> {hint}")
        return 1

    print("OK: no deprecated patterns (datetime.utcnow / @app.on_event) found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

