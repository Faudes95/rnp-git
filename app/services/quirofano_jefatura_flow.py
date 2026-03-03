from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from fastapi import Request
from fastapi.responses import HTMLResponse

from app.core.app_context import main_proxy as m


def _db_path() -> Path:
    # BD dedicada para el módulo de jefatura de quirófano (aislada de quirófano operativo).
    return Path(__file__).resolve().parents[2] / "urologia_jefatura_quirofano.db"


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jefatura_quirofano_access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT,
            route TEXT,
            source_module TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def register_jefatura_access(*, actor: str, route: str, source_module: str = "QUIROFANO_HOME") -> Dict[str, Any]:
    db_file = _db_path()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    created_at = datetime.utcnow().isoformat(timespec="seconds")
    with sqlite3.connect(str(db_file)) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO jefatura_quirofano_access_logs(actor, route, source_module, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (_safe_text(actor) or "system", _safe_text(route), _safe_text(source_module), created_at),
        )
        conn.commit()
        total = int(
            conn.execute("SELECT COUNT(1) FROM jefatura_quirofano_access_logs").fetchone()[0]  # type: ignore[index]
        )
        last_row = conn.execute(
            """
            SELECT actor, created_at
            FROM jefatura_quirofano_access_logs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    return {
        "db_file": str(db_file),
        "total_access": total,
        "last_actor": _safe_text(last_row[0]) if last_row else "N/E",
        "last_at": _safe_text(last_row[1]) if last_row else "N/E",
    }


async def render_jefatura_quirofano_waiting_flow(request: Request) -> HTMLResponse:
    actor = _safe_text(request.headers.get("X-User")) or "ui_shell"
    stats = register_jefatura_access(actor=actor, route="/quirofano/jefatura", source_module="QUIROFANO_HOME")
    return m.render_template(
        "quirofano_jefatura_placeholder.html",
        request=request,
        actor=actor,
        stats=stats,
    )

