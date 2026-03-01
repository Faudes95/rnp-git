from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, MetaData, String, Table, Text

from app.core.time_utils import utcnow


HOSPITALIZATION_NOTES_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")


HOSPITALIZATION_EPISODES = Table(
    "hospitalization_episodes",
    HOSPITALIZATION_NOTES_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_id", String(20), nullable=False, index=True),  # NSS_10 canónico
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("service", String(120), nullable=True, index=True),
    Column("location", String(120), nullable=True, index=True),
    Column("shift", String(40), nullable=True, index=True),
    Column("author_user_id", String(120), nullable=True, index=True),
    Column("status", String(24), nullable=False, default="ACTIVO", index=True),
    Column("started_on", Date, nullable=False, index=True),
    Column("ended_on", Date, nullable=True, index=True),
    Column("metrics_json", JSON_SQL),
    Column("source_route", String(240), nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow, index=True),
    Column("updated_at", DateTime, nullable=False, default=utcnow, index=True),
    Column("active", Boolean, nullable=False, default=True, index=True),
)


def ensure_hospitalization_notes_schema(bind_or_session) -> None:
    # Registro tardío de tabla de notas para create_all consistente.
    from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES  # noqa: F401

    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    HOSPITALIZATION_NOTES_METADATA.create_all(bind=bind, checkfirst=True)

