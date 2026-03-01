from __future__ import annotations

from typing import Any, Callable

from sqlalchemy import inspect, text


def ensure_clinical_sensitive_schema(
    *,
    engine: Any,
    log_suppressed_exception: Callable[[str, Exception], Any],
) -> None:
    """
    Agrega columnas espejo cifradas en consultas de forma aditiva.
    """
    try:
        insp = inspect(engine)
        if "consultas" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("consultas")}
        sql_types = {
            "curp_enc": "TEXT",
            "nss_enc": "TEXT",
            "nombre_enc": "TEXT",
            "telefono_enc": "TEXT",
            "email_enc": "TEXT",
        }
        with engine.begin() as conn:
            for col_name, col_type in sql_types.items():
                if col_name in existing:
                    continue
                conn.execute(text(f"ALTER TABLE consultas ADD COLUMN {col_name} {col_type}"))
    except Exception as exc:
        log_suppressed_exception("ensure_clinical_sensitive_schema_failed", exc)
