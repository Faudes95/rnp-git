from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy.orm import Session


@contextmanager
def clinical_session() -> Generator[Session, None, None]:
    from app.core.app_context import main_proxy as m

    db = m.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def surgical_session() -> Generator[Session, None, None]:
    from app.core.app_context import main_proxy as m

    sdb = m._new_surgical_session(enable_dual_write=True)
    try:
        yield sdb
    finally:
        sdb.close()

