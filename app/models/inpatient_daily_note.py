from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Table, Text

from app.core.time_utils import utcnow
from app.models.hospitalization_episode import HOSPITALIZATION_NOTES_METADATA, JSON_SQL


INPATIENT_DAILY_NOTES = Table(
    "inpatient_daily_notes",
    HOSPITALIZATION_NOTES_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("episode_id", Integer, nullable=False, index=True),
    Column("patient_id", String(20), nullable=False, index=True),  # NSS_10 canónico
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("note_date", Date, nullable=False, index=True),
    Column("note_type", String(40), nullable=False, default="EVOLUCION", index=True),
    Column("service", String(120), nullable=True, index=True),
    Column("location", String(120), nullable=True, index=True),
    Column("shift", String(40), nullable=True, index=True),
    Column("author_user_id", String(120), nullable=True, index=True),
    Column("cie10_codigo", String(20), nullable=True, index=True),
    Column("diagnostico", String(320), nullable=True, index=True),
    Column("vitals_json", JSON_SQL),
    Column("labs_json", JSON_SQL),
    Column("devices_json", JSON_SQL),
    Column("events_json", JSON_SQL),
    Column("payload_json", JSON_SQL),
    # Bloques aditivos para evolución intrahospitalaria estructurada.
    Column("problem_list_json", JSON_SQL),
    Column("plan_by_problem_json", JSON_SQL),
    Column("devices_snapshot_json", JSON_SQL),
    Column("io_summary_json", JSON_SQL),
    Column("symptoms_json", JSON_SQL),
    Column("events_pending_json", JSON_SQL),
    Column("free_text", Text),
    Column("is_final", Boolean, nullable=False, default=False, index=True),
    Column("version", Integer, nullable=False, default=1),
    Column("note_text", Text),
    Column("status", String(24), nullable=False, default="BORRADOR", index=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
    Column("updated_at", DateTime, nullable=False, default=utcnow, index=True),
)
