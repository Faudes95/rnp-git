from __future__ import annotations

from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    inspect,
    text,
)

from app.core.time_utils import utcnow


INPATIENT_TS_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")


VITALS_TS = Table(
    "vitals_ts",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("recorded_at", DateTime, nullable=False, index=True),
    Column("heart_rate", Integer, nullable=True),
    Column("sbp", Integer, nullable=True),
    Column("dbp", Integer, nullable=True),
    Column("map", Float, nullable=True),
    Column("temperature", Float, nullable=True),
    Column("spo2", Float, nullable=True),
    Column("resp_rate", Integer, nullable=True),
    Column("mental_status_avpu", String(16), nullable=True),
    Column("gcs", Integer, nullable=True),
    Column("o2_device", String(64), nullable=True),
    Column("o2_flow_lpm", Float, nullable=True),
    Column("pain_score_0_10", Integer, nullable=True),
    Column("source", String(120), nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


IO_BLOCKS = Table(
    "io_blocks",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("interval_start", DateTime, nullable=False, index=True),
    Column("interval_end", DateTime, nullable=False, index=True),
    Column("urine_output_ml", Float, nullable=True),
    Column("intake_ml", Float, nullable=True),
    Column("net_balance_ml", Float, nullable=True),
    Column("weight_kg", Float, nullable=True),
    Column("height_cm", Float, nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


DEVICES_TS = Table(
    "devices_ts",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("ts", DateTime, nullable=False, index=True),
    Column("device_family", String(24), nullable=True, index=True),  # DRENAJE / DISPOSITIVO
    Column("device_type", String(80), nullable=True, index=True),
    Column("present", Boolean, nullable=True, index=True),
    Column("side", String(12), nullable=True, index=True),
    Column("size_fr", String(20), nullable=True),
    Column("flow_ml", Float, nullable=True),
    Column("notes", Text, nullable=True),
    Column("source", String(120), nullable=True, index=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


UROLOGY_DEVICES = Table(
    "urology_devices",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("device_type", String(40), nullable=False, index=True),
    Column("present", Boolean, nullable=False, default=True, index=True),
    Column("inserted_at", DateTime, nullable=True, index=True),
    Column("removed_at", DateTime, nullable=True),
    Column("side", String(12), nullable=True),
    Column("location", String(120), nullable=True),
    Column("size_fr", String(20), nullable=True),
    Column("difficulty", String(20), nullable=True),
    Column("irrigation", Boolean, nullable=True),
    Column("planned_removal_at", DateTime, nullable=True),
    Column("planned_change_at", DateTime, nullable=True),
    Column("notes", Text, nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


CLINICAL_EVENT_LOG = Table(
    "clinical_event_log",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("correlation_id", String(64), nullable=True, index=True),
    Column("actor", String(120), nullable=True, index=True),
    Column("module", String(80), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("entity", String(120), nullable=True, index=True),
    Column("entity_id", String(120), nullable=True, index=True),
    Column("source_route", String(255), nullable=True, index=True),
    Column("event_time", DateTime, nullable=True, index=True),
    Column("event_type", String(64), nullable=False, index=True),
    Column("payload_json", JSON_SQL),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


LAB_RESULTS = Table(
    "lab_results",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("collected_at", DateTime, nullable=False, index=True),
    Column("test_name", String(80), nullable=False, index=True),
    Column("value_num", Float, nullable=True),
    Column("value_text", Text, nullable=True),
    Column("unit", String(40), nullable=True),
    Column("source", String(120), nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


ALERT_ACTION_METADATA = Table(
    "alert_action_metadata",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("alert_id", Integer, nullable=False, index=True),
    Column("ack_by", String(120), nullable=True, index=True),
    Column("ack_at", DateTime, nullable=True, index=True),
    Column("resolved_by", String(120), nullable=True, index=True),
    Column("resolved_at", DateTime, nullable=True, index=True),
    Column("resolution_reason", String(64), nullable=True, index=True),
    Column("action_taken_json", JSON_SQL),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
    Column("updated_at", DateTime, nullable=True, index=True),
)


CLINICAL_TAGS = Table(
    "clinical_tags",
    INPATIENT_TS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("tag_type", String(80), nullable=False, index=True),
    Column("tag_value", String(160), nullable=False, index=True),
    Column("laterality", String(16), nullable=True, index=True),
    Column("severity", String(32), nullable=True, index=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
)


def _column_exists(bind: Any, table_name: str, column_name: str) -> bool:
    try:
        cols = {str(c.get("name") or "") for c in inspect(bind).get_columns(table_name)}
        if column_name in cols:
            return True
    except Exception:
        pass
    safe_col = str(column_name or "").strip()
    safe_tbl = str(table_name or "").strip()
    if not safe_col.isidentifier() or not safe_tbl:
        return False
    try:
        with bind.connect() as conn:
            conn.execute(text(f"SELECT {safe_col} FROM {safe_tbl} WHERE 1=0"))
        return True
    except Exception:
        return False


def _ensure_add_column(bind: Any, table_name: str, column_name: str, ddl_type: str) -> None:
    if _column_exists(bind, table_name, column_name):
        return
    try:
        with bind.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"))
    except Exception:
        if not _column_exists(bind, table_name, column_name):
            raise


def ensure_inpatient_time_series_schema(bind_or_session) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    INPATIENT_TS_METADATA.create_all(bind=bind, checkfirst=True)
    # clinical_event_log puede existir de forma legacy sin columnas nuevas.
    for col_name, ddl in [
        ("hospitalizacion_id", "INTEGER"),
        ("event_time", "TIMESTAMP"),
    ]:
        try:
            _ensure_add_column(bind, "clinical_event_log", col_name, ddl)
        except Exception:
            continue
    # inpatient_daily_notes ya existe en el módulo legacy; aquí sólo agregamos
    # columnas aditivas para longitudinalidad sin romper el flujo actual.
    for col_name, ddl in [
        ("problem_list_json", "TEXT"),
        ("plan_by_problem_json", "TEXT"),
        ("devices_snapshot_json", "TEXT"),
        ("io_summary_json", "TEXT"),
        ("symptoms_json", "TEXT"),
        ("events_pending_json", "TEXT"),
        ("free_text", "TEXT"),
        ("is_final", "BOOLEAN"),
        ("version", "INTEGER"),
    ]:
        try:
            _ensure_add_column(bind, "inpatient_daily_notes", col_name, ddl)
        except Exception:
            continue
