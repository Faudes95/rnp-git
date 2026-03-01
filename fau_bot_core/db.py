from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

from .config import get_config


_CFG = get_config()


def _mk_engine(dsn: str) -> Engine:
    kwargs = {"pool_pre_ping": True, "future": True}
    if dsn.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(dsn, **kwargs)


CLINICAL_RO_ENGINE: Engine = _mk_engine(_CFG.clinical_ro_dsn)
SURGICAL_RO_ENGINE: Engine = _mk_engine(_CFG.surgical_ro_dsn)
OUTPUT_ENGINE: Engine = _mk_engine(_CFG.output_dsn)


def _is_postgres(engine: Engine) -> bool:
    try:
        return str(engine.dialect.name).startswith("postgres")
    except Exception:
        return False


def _is_sqlite(engine: Engine) -> bool:
    try:
        return str(engine.dialect.name).startswith("sqlite")
    except Exception:
        return False


def _attach_output_schema_alias_if_needed(conn: Connection) -> None:
    if not _is_sqlite(OUTPUT_ENGINE):
        return
    schema = (_CFG.output_schema or "").strip()
    db_path = OUTPUT_ENGINE.url.database
    if not schema or not db_path:
        return
    rows = conn.execute(text("PRAGMA database_list")).fetchall()
    attached = {str(r[1]) for r in rows if len(r) >= 2}
    if schema in attached:
        return
    conn.execute(text(f"ATTACH DATABASE :db_path AS {schema}"), {"db_path": db_path})


@contextmanager
def clinical_ro_conn() -> Generator[Connection, None, None]:
    conn = CLINICAL_RO_ENGINE.connect()
    tx = conn.begin()
    try:
        if _is_postgres(CLINICAL_RO_ENGINE):
            conn.execute(text("SET LOCAL TRANSACTION READ ONLY"))
        yield conn
    finally:
        tx.rollback()
        conn.close()


@contextmanager
def surgical_ro_conn() -> Generator[Connection, None, None]:
    conn = SURGICAL_RO_ENGINE.connect()
    tx = conn.begin()
    try:
        if _is_postgres(SURGICAL_RO_ENGINE):
            conn.execute(text("SET LOCAL TRANSACTION READ ONLY"))
        yield conn
    finally:
        tx.rollback()
        conn.close()


@contextmanager
def output_conn() -> Generator[Connection, None, None]:
    conn = OUTPUT_ENGINE.connect()
    tx = conn.begin()
    try:
        _attach_output_schema_alias_if_needed(conn)
        yield conn
        tx.commit()
    except Exception:
        tx.rollback()
        raise
    finally:
        conn.close()
