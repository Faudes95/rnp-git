"""
Modelos de Gobernanza Clínica — FASE 2.

ADITIVO: No modifica tablas ni lógica existente.
Crea tablas nuevas para RBAC, Auditoría y Consentimiento Informado.

Tablas:
  - gov_users              Usuarios con roles (RBAC)
  - gov_sessions           Sesiones de usuario
  - gov_access_log         Log de acceso completo (auditoría)
  - gov_consent_forms      Consentimientos informados digitales
  - gov_clinical_alerts    Alertas clínicas cross-module (alergias, interacciones)
"""
from __future__ import annotations

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, Integer, MetaData,
    String, Table, Text, UniqueConstraint,
)
from app.core.time_utils import utcnow

GOV_METADATA = MetaData()


# ── RBAC: Usuarios y Roles ────────────────────────────────────────────
GOV_USERS = Table(
    "gov_users",
    GOV_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(80), nullable=False, unique=True, index=True),
    Column("password_hash", String(255), nullable=False),
    Column("nombre_completo", String(200), nullable=False),
    Column("matricula", String(40), nullable=True, index=True),
    Column("cedula_profesional", String(40), nullable=True),
    Column("rol", String(40), nullable=False, default="residente", index=True),
    # Roles: admin, jefe_servicio, medico_adscrito, residente, enfermeria, capturista, readonly
    Column("servicio", String(80), nullable=True, index=True),
    Column("email", String(120), nullable=True),
    Column("activo", Boolean, nullable=False, default=True, index=True),
    Column("ultimo_login", DateTime, nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
)


GOV_SESSIONS = Table(
    "gov_sessions",
    GOV_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("token_hash", String(128), nullable=False, unique=True, index=True),
    Column("ip_address", String(45), nullable=True),
    Column("user_agent", String(300), nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("expira_en", DateTime, nullable=False),
    Column("activo", Boolean, nullable=False, default=True, index=True),
)


# ── Auditoría de Acceso ───────────────────────────────────────────────
GOV_ACCESS_LOG = Table(
    "gov_access_log",
    GOV_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("timestamp", DateTime, nullable=False, default=utcnow, index=True),
    Column("user_id", Integer, nullable=True, index=True),
    Column("username", String(80), nullable=True, index=True),
    Column("rol", String(40), nullable=True, index=True),
    Column("ip_address", String(45), nullable=True),
    Column("method", String(10), nullable=False),        # GET, POST, PUT, DELETE
    Column("path", String(500), nullable=False, index=True),
    Column("status_code", Integer, nullable=True),
    Column("tabla_afectada", String(120), nullable=True, index=True),
    Column("registro_id", Integer, nullable=True),
    Column("operacion", String(40), nullable=True, index=True),  # CREATE, READ, UPDATE, DELETE
    Column("patient_uid", String(64), nullable=True, index=True),
    Column("nss", String(10), nullable=True, index=True),
    Column("datos_anteriores_json", Text, nullable=True),
    Column("datos_nuevos_json", Text, nullable=True),
    Column("duracion_ms", Integer, nullable=True),
    Column("modulo", String(80), nullable=True, index=True),
)


# ── Consentimiento Informado Digital ──────────────────────────────────
GOV_CONSENT_FORMS = Table(
    "gov_consent_forms",
    GOV_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("tipo_consentimiento", String(80), nullable=False, index=True),
    # Tipos: PROCEDIMIENTO_QX, ANESTESIA, INVESTIGACION, USO_DATOS, HEMOTRANSFUSION, INGRESO_HOSPITALARIO
    Column("procedimiento_descripcion", Text, nullable=True),
    Column("riesgos_descripcion", Text, nullable=True),
    Column("alternativas_descripcion", Text, nullable=True),
    Column("medico_responsable", String(200), nullable=True),
    Column("matricula_medico", String(40), nullable=True),
    Column("paciente_nombre", String(200), nullable=True),
    Column("paciente_o_responsable", String(200), nullable=True),  # Si es menor o incapacitado
    Column("parentesco_responsable", String(80), nullable=True),
    Column("firma_paciente_hash", String(128), nullable=True),     # Hash de firma digital
    Column("firma_medico_hash", String(128), nullable=True),
    Column("firma_testigo1_hash", String(128), nullable=True),
    Column("firma_testigo2_hash", String(128), nullable=True),
    Column("estatus", String(40), nullable=False, default="PENDIENTE", index=True),
    # PENDIENTE, FIRMADO, RECHAZADO, REVOCADO
    Column("fecha_firma", DateTime, nullable=True),
    Column("fecha_revocacion", DateTime, nullable=True),
    Column("motivo_revocacion", Text, nullable=True),
    Column("notas", Text, nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
)


# ── Alertas Clínicas Cross-Module ─────────────────────────────────────
GOV_CLINICAL_ALERTS = Table(
    "gov_clinical_alerts",
    GOV_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    Column("tipo_alerta", String(60), nullable=False, index=True),
    # Tipos: ALERGIA, INTERACCION_MEDICAMENTOSA, RIESGO_QX, RIESGO_ANESTESICO,
    #        ALERTA_LABS, COMORBILIDAD_CRITICA, INFECCION, HEMODERIVADOS
    Column("severidad", String(20), nullable=False, default="MEDIA", index=True),
    # BAJA, MEDIA, ALTA, CRITICA
    Column("titulo", String(300), nullable=False),
    Column("descripcion", Text, nullable=True),
    Column("origen_modulo", String(80), nullable=True, index=True),  # CONSULTA, HOSPITALIZACION, QUIROFANO
    Column("origen_tabla", String(120), nullable=True),
    Column("origen_id", Integer, nullable=True),
    Column("activa", Boolean, nullable=False, default=True, index=True),
    Column("acknowledged_by", String(120), nullable=True),
    Column("acknowledged_at", DateTime, nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
    UniqueConstraint("patient_uid", "tipo_alerta", "titulo", name="uq_gov_alert_patient_type_title"),
)
