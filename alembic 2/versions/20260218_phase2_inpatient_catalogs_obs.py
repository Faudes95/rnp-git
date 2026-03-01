"""phase2 inpatient time-series + observabilidad + fau loop

Revision ID: 20260218_phase2_inpatient_catalogs_obs
Revises: 20260212_add_analytics
Create Date: 2026-02-18
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "20260218_phase2_inpatient_catalogs_obs"
down_revision = "20260212_add_analytics"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _table_exists(name: str) -> bool:
    return _insp().has_table(name)


def _column_exists(table_name: str, column_name: str) -> bool:
    try:
        cols = [c.get("name") for c in _insp().get_columns(table_name)]
        return column_name in cols
    except Exception:
        return False


def _index_exists(table_name: str, index_name: str) -> bool:
    try:
        return any(idx.get("name") == index_name for idx in _insp().get_indexes(table_name))
    except Exception:
        return False


def _add_col_if_missing(table_name: str, column: sa.Column) -> None:
    if not _column_exists(table_name, column.name):
        op.add_column(table_name, column)


def _create_index_if_missing(table_name: str, index_name: str, cols: list[str]) -> None:
    if not _index_exists(table_name, index_name):
        op.create_index(index_name, table_name, cols)


def upgrade():
    if not _table_exists("vitals_ts"):
        op.create_table(
            "vitals_ts",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("recorded_at", sa.DateTime, nullable=False),
            sa.Column("heart_rate", sa.Integer, nullable=True),
            sa.Column("sbp", sa.Integer, nullable=True),
            sa.Column("dbp", sa.Integer, nullable=True),
            sa.Column("map", sa.Float, nullable=True),
            sa.Column("temperature", sa.Float, nullable=True),
            sa.Column("spo2", sa.Float, nullable=True),
            sa.Column("resp_rate", sa.Integer, nullable=True),
            sa.Column("mental_status_avpu", sa.String(16), nullable=True),
            sa.Column("gcs", sa.Integer, nullable=True),
            sa.Column("o2_device", sa.String(64), nullable=True),
            sa.Column("o2_flow_lpm", sa.Float, nullable=True),
            sa.Column("pain_score_0_10", sa.Integer, nullable=True),
            sa.Column("source", sa.String(120), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("vitals_ts", "ix_vitals_ts_consulta_id", ["consulta_id"])
    _create_index_if_missing("vitals_ts", "ix_vitals_ts_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("vitals_ts", "ix_vitals_ts_recorded_at", ["recorded_at"])
    _create_index_if_missing("vitals_ts", "ix_vitals_ts_created_at", ["created_at"])

    if not _table_exists("io_blocks"):
        op.create_table(
            "io_blocks",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("interval_start", sa.DateTime, nullable=False),
            sa.Column("interval_end", sa.DateTime, nullable=False),
            sa.Column("urine_output_ml", sa.Float, nullable=True),
            sa.Column("intake_ml", sa.Float, nullable=True),
            sa.Column("net_balance_ml", sa.Float, nullable=True),
            sa.Column("weight_kg", sa.Float, nullable=True),
            sa.Column("height_cm", sa.Float, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("io_blocks", "ix_io_blocks_consulta_id", ["consulta_id"])
    _create_index_if_missing("io_blocks", "ix_io_blocks_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("io_blocks", "ix_io_blocks_interval_start", ["interval_start"])
    _create_index_if_missing("io_blocks", "ix_io_blocks_interval_end", ["interval_end"])

    if not _table_exists("urology_devices"):
        op.create_table(
            "urology_devices",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("device_type", sa.String(40), nullable=False),
            sa.Column("present", sa.Boolean, nullable=False, server_default=sa.text("1")),
            sa.Column("inserted_at", sa.DateTime, nullable=True),
            sa.Column("removed_at", sa.DateTime, nullable=True),
            sa.Column("side", sa.String(12), nullable=True),
            sa.Column("location", sa.String(120), nullable=True),
            sa.Column("size_fr", sa.String(20), nullable=True),
            sa.Column("difficulty", sa.String(20), nullable=True),
            sa.Column("irrigation", sa.Boolean, nullable=True),
            sa.Column("planned_removal_at", sa.DateTime, nullable=True),
            sa.Column("planned_change_at", sa.DateTime, nullable=True),
            sa.Column("notes", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("urology_devices", "ix_urology_devices_consulta_id", ["consulta_id"])
    _create_index_if_missing("urology_devices", "ix_urology_devices_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("urology_devices", "ix_urology_devices_device_type", ["device_type"])
    _create_index_if_missing("urology_devices", "ix_urology_devices_present", ["present"])

    if not _table_exists("lab_results"):
        op.create_table(
            "lab_results",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("collected_at", sa.DateTime, nullable=False),
            sa.Column("test_name", sa.String(80), nullable=False),
            sa.Column("value_num", sa.Float, nullable=True),
            sa.Column("value_text", sa.Text, nullable=True),
            sa.Column("unit", sa.String(40), nullable=True),
            sa.Column("source", sa.String(120), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("lab_results", "ix_lab_results_consulta_id", ["consulta_id"])
    _create_index_if_missing("lab_results", "ix_lab_results_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("lab_results", "ix_lab_results_collected_at", ["collected_at"])
    _create_index_if_missing("lab_results", "ix_lab_results_test_name", ["test_name"])

    if not _table_exists("alert_action_metadata"):
        op.create_table(
            "alert_action_metadata",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("alert_id", sa.Integer, nullable=False),
            sa.Column("ack_by", sa.String(120), nullable=True),
            sa.Column("ack_at", sa.DateTime, nullable=True),
            sa.Column("resolved_by", sa.String(120), nullable=True),
            sa.Column("resolved_at", sa.DateTime, nullable=True),
            sa.Column("resolution_reason", sa.String(64), nullable=True),
            sa.Column("action_taken_json", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime, nullable=True),
        )
    _create_index_if_missing("alert_action_metadata", "ix_alert_action_metadata_alert_id", ["alert_id"])
    _create_index_if_missing("alert_action_metadata", "ix_alert_action_metadata_resolution_reason", ["resolution_reason"])

    if not _table_exists("clinical_tags"):
        op.create_table(
            "clinical_tags",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("tag_type", sa.String(80), nullable=False),
            sa.Column("tag_value", sa.String(160), nullable=False),
            sa.Column("laterality", sa.String(16), nullable=True),
            sa.Column("severity", sa.String(32), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("clinical_tags", "ix_clinical_tags_consulta_id", ["consulta_id"])
    _create_index_if_missing("clinical_tags", "ix_clinical_tags_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("clinical_tags", "ix_clinical_tags_tag_type", ["tag_type"])
    _create_index_if_missing("clinical_tags", "ix_clinical_tags_tag_value", ["tag_value"])

    if _table_exists("clinical_event_log"):
        _add_col_if_missing("clinical_event_log", sa.Column("hospitalizacion_id", sa.Integer, nullable=True))
        _add_col_if_missing("clinical_event_log", sa.Column("event_time", sa.DateTime, nullable=True))
    else:
        op.create_table(
            "clinical_event_log",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("correlation_id", sa.String(64), nullable=True),
            sa.Column("actor", sa.String(120), nullable=True),
            sa.Column("module", sa.String(80), nullable=False),
            sa.Column("event_type", sa.String(120), nullable=False),
            sa.Column("entity", sa.String(120), nullable=True),
            sa.Column("entity_id", sa.String(120), nullable=True),
            sa.Column("consulta_id", sa.Integer, nullable=True),
            sa.Column("hospitalizacion_id", sa.Integer, nullable=True),
            sa.Column("source_route", sa.String(255), nullable=True),
            sa.Column("event_time", sa.DateTime, nullable=True),
            sa.Column("payload_json", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing("clinical_event_log", "ix_clinical_event_log_consulta_id", ["consulta_id"])
    _create_index_if_missing("clinical_event_log", "ix_clinical_event_log_hospitalizacion_id", ["hospitalizacion_id"])
    _create_index_if_missing("clinical_event_log", "ix_clinical_event_log_event_type", ["event_type"])
    _create_index_if_missing("clinical_event_log", "ix_clinical_event_log_event_time", ["event_time"])

    if not _table_exists("fau_engineering_issues"):
        op.create_table(
            "fau_engineering_issues",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("source", sa.String(80), nullable=True, index=True),
            sa.Column("issue_code", sa.String(120), nullable=True, index=True),
            sa.Column("title", sa.String(255), nullable=True, index=True),
            sa.Column("category", sa.String(80), nullable=True, index=True),
            sa.Column("severity", sa.String(20), nullable=True, index=True),
            sa.Column("priority", sa.String(20), nullable=True, index=True),
            sa.Column("evidence_json", sa.Text, nullable=True),
            sa.Column("first_seen", sa.DateTime, nullable=True, index=True),
            sa.Column("last_seen", sa.DateTime, nullable=True, index=True),
            sa.Column("status", sa.String(40), nullable=True, index=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime, nullable=True),
        )

    if not _table_exists("fau_pr_suggestions"):
        op.create_table(
            "fau_pr_suggestions",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("log_id", sa.Integer, nullable=True),
            sa.Column("titulo_pr", sa.String(240), nullable=True),
            sa.Column("explicacion", sa.Text, nullable=True),
            sa.Column("codigo_sugerido", sa.Text, nullable=True),
            sa.Column("archivo_objetivo", sa.String(255), nullable=True),
            sa.Column("status", sa.String(40), nullable=True),
            sa.Column("creado_en", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("aplicado_en", sa.DateTime, nullable=True),
        )
    _add_col_if_missing("fau_pr_suggestions", sa.Column("category", sa.String(80), nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("priority", sa.String(40), nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("proposal_json", sa.Text, nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("patch_diff", sa.Text, nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("test_report_json", sa.Text, nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("files_affected_json", sa.Text, nullable=True))
    _add_col_if_missing("fau_pr_suggestions", sa.Column("updated_en", sa.DateTime, nullable=True))
    _create_index_if_missing("fau_pr_suggestions", "ix_fau_pr_suggestions_status", ["status"])
    _create_index_if_missing("fau_pr_suggestions", "ix_fau_pr_suggestions_updated_en", ["updated_en"])


def downgrade():
    # Downgrade conservador para no perder trazabilidad clínica.
    pass

