from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import create_engine


def build_optional_engine(db_url: str) -> Optional[Any]:
    if not db_url:
        return None
    is_sqlite_url = db_url.startswith("sqlite")
    args = {"check_same_thread": False} if is_sqlite_url else {}
    try:
        return create_engine(db_url, connect_args=args)
    except Exception:
        return None
