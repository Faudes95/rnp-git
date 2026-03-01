"""Modelos EHR aditivos — Expediente Clínico Integrado.

Tablas nuevas que INDEXAN datos existentes sin modificar tablas legacy.
Se crean con MetaData propio para create_all independiente.

Tablas:
  - ehr_documents         Documentos clínicos indexados
  - ehr_timeline_events   Eventos de timeline (todo es un evento)
  - ehr_tags              Etiquetas clínicas (sin tocar dx legacy)
  - ehr_document_tags     Relación documento↔tag
  - ehr_features_daily    Feature store diario para ML/alertas
  - ehr_problem_list      Lista de problemas auto-generada
  - ehr_alert_lifecycle   Ciclo de vida de alertas clínicas
"""
from __future__ import annotations

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, Integer, MetaData,
    String, Table, Text,
)

from app.core.time_utils import utcnow

EHR_METADATA = MetaData()
_JSON_COL = Text().with_variant(Text(), "sqlite")


# ── Documentos clínicos indexados ──────────────────────────────────────
EHR_DOCUMENTS = Table(
    "ehr_documents",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(40), nullable=False, index=True),     # NSS canónico
    Column("source_table", String(80), nullable=False),                # "consultas", "hospitalizaciones", etc.
    Column("source_id", Integer, nullable=False),                      # PK de la tabla origen
    Column("doc_type", String(60), nullable=False, index=True),        # "nota_consulta", "nota_ingreso", "nota_qx", etc.
    Column("episode_uid", String(80), nullable=True, index=True),      # Agrupador de episodio
    Column("title", String(300), nullable=True),
    Column("author", String(200), nullable=True),
    Column("service", String(120), nullable=True, index=True),         # CONSULTA EXTERNA, LEOCH, etc.
    Column("content_json", _JSON_COL),                                 # Payload completo serializado
    Column("content_text", Text, nullable=True),                       # Texto plano para búsqueda
    Column("version", Integer, nullable=False, default=1),
    Column("signature_user", String(120), nullable=True),
    Column("signature_ts", DateTime, nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow),
    Column("updated_at", DateTime, nullable=False, default=utcnow),
)


# ── Timeline de eventos ───────────────────────────────────────────────
EHR_TIMELINE_EVENTS = Table(
    "ehr_timeline_events",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(40), nullable=False, index=True),
    Column("event_type", String(60), nullable=False, index=True),      # "consulta", "hospitalizacion", "cirugia", "lab", "vital", "alerta", "procedimiento"
    Column("event_ts", DateTime, nullable=False, index=True),          # Timestamp del evento
    Column("episode_uid", String(80), nullable=True, index=True),
    Column("document_id", Integer, nullable=True, index=True),         # FK a ehr_documents.id
    Column("source_table", String(80), nullable=True),
    Column("source_id", Integer, nullable=True),
    Column("title", String(300), nullable=True),
    Column("summary", Text, nullable=True),
    Column("service", String(120), nullable=True, index=True),
    Column("author", String(200), nullable=True),
    Column("severity", String(20), nullable=True),                     # "info", "warning", "critical"
    Column("payload_json", _JSON_COL),                                 # Datos extra
    Column("created_at", DateTime, nullable=False, default=utcnow),
)


# ── Etiquetas clínicas ────────────────────────────────────────────────
EHR_TAGS = Table(
    "ehr_tags",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tag_name", String(120), nullable=False, unique=True),
    Column("tag_category", String(60), nullable=True, index=True),     # "diagnostico", "procedimiento", "sintoma", "comorbilidad"
    Column("cie10_code", String(20), nullable=True),
    Column("snomed_code", String(30), nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow),
)


# ── Relación documento ↔ tag ──────────────────────────────────────────
EHR_DOCUMENT_TAGS = Table(
    "ehr_document_tags",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("document_id", Integer, nullable=False, index=True),        # FK a ehr_documents.id
    Column("tag_id", Integer, nullable=False, index=True),             # FK a ehr_tags.id
    Column("confidence", Float, nullable=True, default=1.0),
    Column("source", String(40), nullable=True, default="auto"),       # "auto", "manual"
    Column("created_at", DateTime, nullable=False, default=utcnow),
)


# ── Feature store diario ──────────────────────────────────────────────
EHR_FEATURES_DAILY = Table(
    "ehr_features_daily",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(40), nullable=False, index=True),
    Column("feature_date", Date, nullable=False, index=True),
    Column("total_consultas", Integer, default=0),
    Column("total_hospitalizaciones", Integer, default=0),
    Column("total_cirugias", Integer, default=0),
    Column("total_labs", Integer, default=0),
    Column("dias_desde_ultima_consulta", Integer, nullable=True),
    Column("dias_desde_ultima_cirugia", Integer, nullable=True),
    Column("comorbilidades_count", Integer, default=0),
    Column("alertas_activas", Integer, default=0),
    Column("completeness_score", Float, default=0.0),
    Column("risk_score", Float, nullable=True),
    Column("features_json", _JSON_COL),                                # Features adicionales en JSON
    Column("created_at", DateTime, nullable=False, default=utcnow),
    Column("updated_at", DateTime, nullable=False, default=utcnow),
)


# ── Problem List ──────────────────────────────────────────────────────
EHR_PROBLEM_LIST = Table(
    "ehr_problem_list",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(40), nullable=False, index=True),
    Column("problem_name", String(300), nullable=False),
    Column("problem_category", String(60), nullable=True, index=True),  # "activo", "resuelto", "cronico"
    Column("cie10_code", String(20), nullable=True),
    Column("onset_date", Date, nullable=True),
    Column("resolution_date", Date, nullable=True),
    Column("source_document_id", Integer, nullable=True),
    Column("notes", Text, nullable=True),
    Column("status", String(20), nullable=False, default="activo", index=True),
    Column("created_at", DateTime, nullable=False, default=utcnow),
    Column("updated_at", DateTime, nullable=False, default=utcnow),
)


# ── Alert Lifecycle ───────────────────────────────────────────────────
EHR_ALERT_LIFECYCLE = Table(
    "ehr_alert_lifecycle",
    EHR_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(40), nullable=False, index=True),
    Column("alert_type", String(80), nullable=False, index=True),       # "inconsistencia", "lab_critico", "interaccion", "seguimiento"
    Column("alert_text", Text, nullable=False),
    Column("severity", String(20), nullable=False, default="warning"),  # "info", "warning", "critical"
    Column("status", String(30), nullable=False, default="generated", index=True),  # generated → acknowledged → resolved → actioned
    Column("source_event_id", Integer, nullable=True),                  # FK a ehr_timeline_events.id
    Column("acknowledged_by", String(120), nullable=True),
    Column("acknowledged_at", DateTime, nullable=True),
    Column("resolved_by", String(120), nullable=True),
    Column("resolved_at", DateTime, nullable=True),
    Column("action_taken", Text, nullable=True),
    Column("outcome", String(120), nullable=True),                      # "mejoria", "sin_cambio", "deterioro", "icu_transfer", "reintervencion", "reingreso", "complicacion", "muerte"
    Column("outcome_notes", Text, nullable=True),
    Column("created_at", DateTime, nullable=False, default=utcnow),
    Column("updated_at", DateTime, nullable=False, default=utcnow),
)


def ensure_ehr_schema(bind_or_session) -> None:
    """Crea todas las tablas EHR si no existen. ADITIVO — nunca elimina."""
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    EHR_METADATA.create_all(bind=bind, checkfirst=True)
