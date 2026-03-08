from __future__ import annotations

import hashlib
import os
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.core.app_context import main_proxy as m
from app.core.time_utils import utcnow


DEFAULT_UNIT_CODE = "HES_CMN_LA_RAZA"
DEFAULT_TEMPLATE_NAME = "Plantilla semanal HES CMN La Raza"
DEFAULT_TEMPLATE_LABEL = "Base institucional"
DEFAULT_SHIFT_CUTOFF = time(14, 0)

DAY_OPTIONS = [
    {"value": 0, "code": "LUNES", "label": "Lunes"},
    {"value": 1, "code": "MARTES", "label": "Martes"},
    {"value": 2, "code": "MIERCOLES", "label": "Miércoles"},
    {"value": 3, "code": "JUEVES", "label": "Jueves"},
    {"value": 4, "code": "VIERNES", "label": "Viernes"},
    {"value": 5, "code": "SABADO", "label": "Sábado"},
    {"value": 6, "code": "DOMINGO", "label": "Domingo"},
]

DAY_LABELS = {item["value"]: item["label"] for item in DAY_OPTIONS}
SHIFT_OPTIONS = [
    {"code": "MATUTINO", "label": "Matutino"},
    {"code": "VESPERTINO", "label": "Vespertino"},
]

UI_TERMS = {
    "panel_principal": "Panel principal",
    "dashboard": "Tablero",
    "programacion": "Programación",
    "importaciones": "Importaciones",
    "plantillas": "Plantillas",
    "day_spaces": "Salas/turnos del día",
    "enabled_spaces": "Salas/turnos habilitados",
    "closed_spaces": "Salas/turnos cerrados",
    "contingency_spaces": "Salas/turnos en contingencia",
    "base_distribution": "Distribución base semanal",
    "weekly_spaces": "Espacios base semanales",
    "day_cases": "Cirugías del día",
    "service_assigned": "Servicio asignado",
    "room_shift": "Sala y turno",
    "block_status": {
        "ACTIVO": "Habilitado",
        "BLOQUEADO": "Cerrado",
        "CONTINGENCIA": "Contingencia",
    },
}

BLOCK_STATUS_OPTIONS = ["ACTIVO", "BLOQUEADO", "CONTINGENCIA"]
CASE_STATUS_OPTIONS = ["PROGRAMADA", "EN_CURSO", "REALIZADA", "CANCELADA", "REPROGRAMADA"]
CASE_EVENT_TYPES = [
    "PROGRAMADO",
    "INGRESO_SALA",
    "INICIO_ANESTESIA",
    "INICIO_CIRUGIA",
    "FIN_CIRUGIA",
    "EGRESO",
    "CANCELADO",
]
CASE_INCIDENT_TYPES = [
    "RETRASO",
    "CANCELACION",
    "CAMBIO_SALA",
    "FALTA_PERSONAL",
    "FALTA_INSUMOS",
    "FALTA_CAMA",
    "CONTINGENCIA",
]
STAFF_ROLE_OPTIONS = ["CIRUJANO", "ANESTESIOLOGO", "ENFERMERA", "APOYO"]
IMPORT_STATUS_OPTIONS = ["REVIEW", "CONFIRMED", "UNSUPPORTED"]

SERVICE_LINE_OPTIONS = [
    {"code": "PROCTO", "nombre": "Proctología", "line_type": "CLINICO", "display_order": 10},
    {"code": "CARDIOT", "nombre": "Cirugía Cardíaca", "line_type": "CLINICO", "display_order": 20},
    {"code": "CIR_GRAL", "nombre": "Cirugía General", "line_type": "CLINICO", "display_order": 30},
    {"code": "NEURO", "nombre": "Neurocirugía", "line_type": "CLINICO", "display_order": 40},
    {"code": "URO", "nombre": "Urología", "line_type": "CLINICO", "display_order": 50},
    {"code": "PLASTICA", "nombre": "Cirugía Plástica", "line_type": "CLINICO", "display_order": 60},
    {"code": "ANGIO", "nombre": "Cirugía Vascular", "line_type": "CLINICO", "display_order": 70},
    {"code": "MAXILO", "nombre": "Maxilofacial", "line_type": "CLINICO", "display_order": 80},
    {"code": "TRASPLANTE", "nombre": "Trasplante", "line_type": "CLINICO", "display_order": 90},
    {"code": "JORNADAS", "nombre": "Jornadas", "line_type": "OPERATIVO", "display_order": 100},
    {"code": "QUIROFANO", "nombre": "Quirófano", "line_type": "OPERATIVO", "display_order": 110},
]

BASE_TEMPLATE_MATRIX: List[Dict[str, Any]] = [
    {"day": 0, "shift": "MATUTINO", "room": 1, "service": "PROCTO"},
    {"day": 0, "shift": "MATUTINO", "room": 2, "service": "CARDIOT"},
    {"day": 0, "shift": "MATUTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 0, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 0, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 0, "shift": "MATUTINO", "room": 6, "service": "NEURO"},
    {"day": 0, "shift": "MATUTINO", "room": 7, "service": "URO"},
    {"day": 0, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 0, "shift": "MATUTINO", "room": 9, "service": "JORNADAS"},
    {"day": 0, "shift": "MATUTINO", "room": 10, "service": "ANGIO"},
    {"day": 0, "shift": "MATUTINO", "room": 11, "service": "URO"},
    {"day": 0, "shift": "MATUTINO", "room": 12, "service": "PROCTO"},
    {"day": 0, "shift": "MATUTINO", "room": 13, "service": "CIR_GRAL"},
    {"day": 0, "shift": "MATUTINO", "room": 14, "service": "MAXILO"},
    {"day": 0, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 0, "shift": "VESPERTINO", "room": 2, "service": "ANGIO"},
    {"day": 0, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 0, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 0, "shift": "VESPERTINO", "room": 5, "service": "PLASTICA"},
    {"day": 0, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 0, "shift": "VESPERTINO", "room": 7, "service": "MAXILO"},
    {"day": 0, "shift": "VESPERTINO", "room": 8, "service": "NEURO"},
    {"day": 1, "shift": "MATUTINO", "room": 1, "service": "PROCTO"},
    {"day": 1, "shift": "MATUTINO", "room": 2, "service": "CARDIOT"},
    {"day": 1, "shift": "MATUTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 1, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 1, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 1, "shift": "MATUTINO", "room": 6, "service": "NEURO"},
    {"day": 1, "shift": "MATUTINO", "room": 7, "service": "MAXILO"},
    {"day": 1, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 1, "shift": "MATUTINO", "room": 9, "service": "JORNADAS"},
    {"day": 1, "shift": "MATUTINO", "room": 10, "service": "ANGIO"},
    {"day": 1, "shift": "MATUTINO", "room": 11, "service": "URO"},
    {"day": 1, "shift": "MATUTINO", "room": 12, "service": "PROCTO"},
    {"day": 1, "shift": "MATUTINO", "room": 13, "service": "URO"},
    {"day": 1, "shift": "MATUTINO", "room": 14, "service": "PLASTICA"},
    {"day": 1, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 1, "shift": "VESPERTINO", "room": 2, "service": "ANGIO"},
    {"day": 1, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 1, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 1, "shift": "VESPERTINO", "room": 5, "service": "NEURO"},
    {"day": 1, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 1, "shift": "VESPERTINO", "room": 7, "service": "URO"},
    {"day": 1, "shift": "VESPERTINO", "room": 8, "service": "NEURO"},
    {"day": 2, "shift": "MATUTINO", "room": 1, "service": "TRASPLANTE"},
    {"day": 2, "shift": "MATUTINO", "room": 2, "service": "TRASPLANTE"},
    {"day": 2, "shift": "MATUTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 2, "shift": "MATUTINO", "room": 4, "service": "QUIROFANO"},
    {"day": 2, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 2, "shift": "MATUTINO", "room": 6, "service": "URO"},
    {"day": 2, "shift": "MATUTINO", "room": 7, "service": "PROCTO"},
    {"day": 2, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 2, "shift": "MATUTINO", "room": 9, "service": "JORNADAS"},
    {"day": 2, "shift": "MATUTINO", "room": 10, "service": "ANGIO"},
    {"day": 2, "shift": "MATUTINO", "room": 11, "service": "CIR_GRAL"},
    {"day": 2, "shift": "MATUTINO", "room": 12, "service": "URO"},
    {"day": 2, "shift": "MATUTINO", "room": 13, "service": "URO"},
    {"day": 2, "shift": "MATUTINO", "room": 14, "service": "MAXILO"},
    {"day": 2, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 2, "shift": "VESPERTINO", "room": 2, "service": "PROCTO"},
    {"day": 2, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 2, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 2, "shift": "VESPERTINO", "room": 5, "service": "PLASTICA"},
    {"day": 2, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 2, "shift": "VESPERTINO", "room": 7, "service": "ANGIO"},
    {"day": 2, "shift": "VESPERTINO", "room": 8, "service": "NEURO"},
    {"day": 3, "shift": "MATUTINO", "room": 1, "service": "TRASPLANTE"},
    {"day": 3, "shift": "MATUTINO", "room": 2, "service": "TRASPLANTE"},
    {"day": 3, "shift": "MATUTINO", "room": 3, "service": "MAXILO"},
    {"day": 3, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 3, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 3, "shift": "MATUTINO", "room": 6, "service": "NEURO"},
    {"day": 3, "shift": "MATUTINO", "room": 7, "service": "PROCTO"},
    {"day": 3, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 3, "shift": "MATUTINO", "room": 9, "service": "JORNADAS"},
    {"day": 3, "shift": "MATUTINO", "room": 10, "service": "ANGIO"},
    {"day": 3, "shift": "MATUTINO", "room": 11, "service": "URO"},
    {"day": 3, "shift": "MATUTINO", "room": 12, "service": "URO"},
    {"day": 3, "shift": "MATUTINO", "room": 13, "service": "URO"},
    {"day": 3, "shift": "MATUTINO", "room": 14, "service": "PLASTICA"},
    {"day": 3, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 3, "shift": "VESPERTINO", "room": 2, "service": "CARDIOT"},
    {"day": 3, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 3, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 3, "shift": "VESPERTINO", "room": 5, "service": "PLASTICA"},
    {"day": 3, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 3, "shift": "VESPERTINO", "room": 7, "service": "URO"},
    {"day": 3, "shift": "VESPERTINO", "room": 8, "service": "NEURO"},
    {"day": 4, "shift": "MATUTINO", "room": 1, "service": "PROCTO"},
    {"day": 4, "shift": "MATUTINO", "room": 2, "service": "CIR_GRAL"},
    {"day": 4, "shift": "MATUTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 4, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 4, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 4, "shift": "MATUTINO", "room": 6, "service": "NEURO"},
    {"day": 4, "shift": "MATUTINO", "room": 7, "service": "URO"},
    {"day": 4, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 4, "shift": "MATUTINO", "room": 9, "service": "JORNADAS"},
    {"day": 4, "shift": "MATUTINO", "room": 10, "service": "ANGIO"},
    {"day": 4, "shift": "MATUTINO", "room": 11, "service": "URO"},
    {"day": 4, "shift": "MATUTINO", "room": 12, "service": "PROCTO"},
    {"day": 4, "shift": "MATUTINO", "room": 13, "service": "URO"},
    {"day": 4, "shift": "MATUTINO", "room": 14, "service": "MAXILO"},
    {"day": 4, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 4, "shift": "VESPERTINO", "room": 2, "service": "NEURO"},
    {"day": 4, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 4, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 4, "shift": "VESPERTINO", "room": 5, "service": "PLASTICA"},
    {"day": 4, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 4, "shift": "VESPERTINO", "room": 7, "service": "MAXILO"},
    {"day": 4, "shift": "VESPERTINO", "room": 8, "service": "NEURO"},
    {"day": 5, "shift": "MATUTINO", "room": 1, "service": "PROCTO"},
    {"day": 5, "shift": "MATUTINO", "room": 2, "service": "CARDIOT"},
    {"day": 5, "shift": "MATUTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 5, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 5, "shift": "MATUTINO", "room": 5, "service": "ANGIO"},
    {"day": 5, "shift": "MATUTINO", "room": 6, "service": "URO"},
    {"day": 5, "shift": "MATUTINO", "room": 7, "service": "NEURO"},
    {"day": 5, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 5, "shift": "VESPERTINO", "room": 1, "service": "MAXILO"},
    {"day": 5, "shift": "VESPERTINO", "room": 2, "service": "NEURO"},
    {"day": 5, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 5, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 5, "shift": "VESPERTINO", "room": 5, "service": "PROCTO"},
    {"day": 5, "shift": "VESPERTINO", "room": 6, "service": "URO"},
    {"day": 6, "shift": "MATUTINO", "room": 1, "service": "PROCTO"},
    {"day": 6, "shift": "MATUTINO", "room": 2, "service": "MAXILO"},
    {"day": 6, "shift": "MATUTINO", "room": 3, "service": "ANGIO"},
    {"day": 6, "shift": "MATUTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 6, "shift": "MATUTINO", "room": 5, "service": "NEURO"},
    {"day": 6, "shift": "MATUTINO", "room": 6, "service": "URO"},
    {"day": 6, "shift": "MATUTINO", "room": 7, "service": "NEURO"},
    {"day": 6, "shift": "MATUTINO", "room": 8, "service": "PLASTICA"},
    {"day": 6, "shift": "VESPERTINO", "room": 1, "service": "PROCTO"},
    {"day": 6, "shift": "VESPERTINO", "room": 2, "service": "NEURO"},
    {"day": 6, "shift": "VESPERTINO", "room": 3, "service": "CIR_GRAL"},
    {"day": 6, "shift": "VESPERTINO", "room": 4, "service": "CIR_GRAL"},
    {"day": 6, "shift": "VESPERTINO", "room": 5, "service": "URO"},
    {"day": 6, "shift": "VESPERTINO", "room": 6, "service": "PROCTO"},
]

STATUS_BADGES = {
    "ACTIVO": {"label": "Habilitado", "tone": "green"},
    "BLOQUEADO": {"label": "Cerrado", "tone": "red"},
    "CONTINGENCIA": {"label": "Contingencia", "tone": "amber"},
    "PROGRAMADA": {"label": "Programada", "tone": "blue"},
    "EN_CURSO": {"label": "En curso", "tone": "amber"},
    "REALIZADA": {"label": "Realizada", "tone": "green"},
    "CANCELADA": {"label": "Cancelada", "tone": "red"},
    "REPROGRAMADA": {"label": "Reprogramada", "tone": "slate"},
    "REVIEW": {"label": "En revisión", "tone": "amber"},
    "CONFIRMED": {"label": "Confirmado", "tone": "green"},
    "UNSUPPORTED": {"label": "No soportado", "tone": "red"},
    "ABIERTA": {"label": "Abierta", "tone": "red"},
}

EVENT_ORDER = {
    "PROGRAMADO": 0,
    "INGRESO_SALA": 1,
    "INICIO_ANESTESIA": 2,
    "INICIO_CIRUGIA": 3,
    "FIN_CIRUGIA": 4,
    "EGRESO": 5,
    "CANCELADO": 6,
}


def ui_terms() -> Dict[str, Any]:
    return {
        **UI_TERMS,
        "block_status": dict(UI_TERMS["block_status"]),
    }


def _bind_from(bind_or_session: Any) -> Any:
    bind = getattr(bind_or_session, "bind", None)
    if bind is not None:
        return bind
    return bind_or_session


def _jq_tables() -> List[Any]:
    return [
        m.JefaturaQuirofanoServiceLineDB.__table__,
        m.JefaturaQuirofanoTemplateVersionDB.__table__,
        m.JefaturaQuirofanoTemplateSlotDB.__table__,
        m.JefaturaQuirofanoDailyBlockDB.__table__,
        m.JefaturaQuirofanoCaseProgramacionDB.__table__,
        m.JefaturaQuirofanoCaseStaffDB.__table__,
        m.JefaturaQuirofanoCaseEventDB.__table__,
        m.JefaturaQuirofanoCaseIncidenciaDB.__table__,
        m.JefaturaQuirofanoImportBatchDB.__table__,
        m.JefaturaQuirofanoImportRowDB.__table__,
        m.JefaturaQuirofanoAuditLogDB.__table__,
    ]


def ensure_jefatura_quirofano_schema(bind_or_session: Any) -> None:
    bind = _bind_from(bind_or_session)
    m.SurgicalBase.metadata.create_all(bind=bind, tables=_jq_tables(), checkfirst=True)


def room_code_from_number(room_number: Any) -> str:
    try:
        number = int(room_number)
        return f"Q{number:02d}"
    except Exception:
        return str(room_number or "").strip().upper()


def normalize_room_number(value: Any) -> Optional[int]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    raw = raw.replace("SALA", "").replace("Q", "").strip()
    try:
        number = int(raw)
        return number if number > 0 else None
    except Exception:
        return None


def normalize_room_code(value: Any) -> Optional[str]:
    number = normalize_room_number(value)
    if number is None:
        return None
    return room_code_from_number(number)


def normalize_shift(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    if raw.startswith("MAT"):
        return "MATUTINO"
    if raw.startswith("VES"):
        return "VESPERTINO"
    return raw if raw in {"MATUTINO", "VESPERTINO"} else None


def safe_text(value: Any, *, max_len: int = 255) -> Optional[str]:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return None
    return text[:max_len]


def safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(str(value).strip())
    except Exception:
        return None


def safe_date(value: Any) -> Optional[date]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def safe_time(value: Any) -> Optional[time]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).time()
        except Exception:
            continue
    return None


def format_date(value: Optional[date]) -> str:
    if not isinstance(value, date):
        return ""
    return value.strftime("%d/%m/%Y")


def format_dt(value: Optional[datetime]) -> str:
    if not isinstance(value, datetime):
        return ""
    return value.strftime("%d/%m/%Y %H:%M")


def request_actor(request: Any) -> str:
    for attr in ("username", "user", "authenticated_user"):
        value = getattr(getattr(request, "state", None), attr, None)
        if value:
            return str(value)
    return "JEFATURA_QX"


def day_label(day_value: int) -> str:
    return DAY_LABELS.get(int(day_value or 0), str(day_value))


def shift_for_scheduled_time(value: Any) -> Optional[str]:
    parsed = safe_time(value)
    if parsed is None:
        return None
    return "MATUTINO" if parsed < DEFAULT_SHIFT_CUTOFF else "VESPERTINO"


def status_badge(status: Any) -> Dict[str, str]:
    normalized = str(status or "").strip().upper() or "ACTIVO"
    return STATUS_BADGES.get(normalized, {"label": normalized.title(), "tone": "slate"})


def storage_dir() -> str:
    root = os.path.join(getattr(m, "PATIENT_FILES_DIR", "./patient_files"), "jefatura_quirofano")
    os.makedirs(root, exist_ok=True)
    return os.path.abspath(root)


def import_storage_dir() -> str:
    target = os.path.join(storage_dir(), "imports")
    os.makedirs(target, exist_ok=True)
    return target


def seed_service_lines(session: Session) -> int:
    ensure_jefatura_quirofano_schema(session)
    created = 0
    existing = {
        str(row.code).upper(): row
        for row in session.query(m.JefaturaQuirofanoServiceLineDB).filter(
            m.JefaturaQuirofanoServiceLineDB.unidad_code == DEFAULT_UNIT_CODE
        )
    }
    for item in SERVICE_LINE_OPTIONS:
        code = str(item["code"]).upper()
        row = existing.get(code)
        if row is None:
            row = m.JefaturaQuirofanoServiceLineDB(
                unidad_code=DEFAULT_UNIT_CODE,
                code=code,
                nombre=str(item["nombre"]),
                line_type=str(item["line_type"]),
                activo=True,
                display_order=int(item["display_order"]),
            )
            session.add(row)
            created += 1
        else:
            row.nombre = str(item["nombre"])
            row.line_type = str(item["line_type"])
            row.display_order = int(item["display_order"])
    if created:
        session.flush()
    return created


def seed_default_template(session: Session, *, actor: str = "SYSTEM") -> int:
    ensure_jefatura_quirofano_schema(session)
    active = (
        session.query(m.JefaturaQuirofanoTemplateVersionDB)
        .filter(
            m.JefaturaQuirofanoTemplateVersionDB.unidad_code == DEFAULT_UNIT_CODE,
            m.JefaturaQuirofanoTemplateVersionDB.is_active.is_(True),
        )
        .order_by(m.JefaturaQuirofanoTemplateVersionDB.id.desc())
        .first()
    )
    if active is not None:
        return int(active.id or 0)
    version = m.JefaturaQuirofanoTemplateVersionDB(
        unidad_code=DEFAULT_UNIT_CODE,
        nombre=DEFAULT_TEMPLATE_NAME,
        version_label=DEFAULT_TEMPLATE_LABEL,
        is_active=True,
        created_by=actor,
    )
    session.add(version)
    session.flush()
    for item in BASE_TEMPLATE_MATRIX:
        session.add(
            m.JefaturaQuirofanoTemplateSlotDB(
                template_version_id=int(version.id),
                unidad_code=DEFAULT_UNIT_CODE,
                day_of_week=int(item["day"]),
                turno=str(item["shift"]),
                room_number=int(item["room"]),
                room_code=room_code_from_number(item["room"]),
                service_line_code=str(item["service"]),
                activo=True,
            )
        )
    session.flush()
    log_audit(
        session,
        actor=actor,
        action="seed_template",
        entity_type="template_version",
        entity_id=int(version.id),
        payload={"version_label": version.version_label},
    )
    return int(version.id or 0)


def ensure_jefatura_quirofano_seed(session: Session, *, actor: str = "SYSTEM") -> None:
    ensure_jefatura_quirofano_schema(session)
    seed_service_lines(session)
    seed_default_template(session, actor=actor)
    session.commit()


def active_template_version(session: Session) -> Any:
    ensure_jefatura_quirofano_seed(session)
    return (
        session.query(m.JefaturaQuirofanoTemplateVersionDB)
        .options(joinedload(m.JefaturaQuirofanoTemplateVersionDB.slots))
        .filter(
            m.JefaturaQuirofanoTemplateVersionDB.unidad_code == DEFAULT_UNIT_CODE,
            m.JefaturaQuirofanoTemplateVersionDB.is_active.is_(True),
        )
        .order_by(m.JefaturaQuirofanoTemplateVersionDB.id.desc())
        .first()
    )


def list_service_lines(session: Session) -> List[Any]:
    ensure_jefatura_quirofano_seed(session)
    return (
        session.query(m.JefaturaQuirofanoServiceLineDB)
        .filter(m.JefaturaQuirofanoServiceLineDB.unidad_code == DEFAULT_UNIT_CODE)
        .order_by(m.JefaturaQuirofanoServiceLineDB.display_order.asc(), m.JefaturaQuirofanoServiceLineDB.code.asc())
        .all()
    )


def template_matrix(session: Session) -> List[Dict[str, Any]]:
    version = active_template_version(session)
    slots_by_key: Dict[Tuple[int, str, int], Any] = {}
    if version is not None:
        for slot in getattr(version, "slots", []) or []:
            slots_by_key[(int(slot.day_of_week), str(slot.turno), int(slot.room_number))] = slot
    grid: List[Dict[str, Any]] = []
    for day in DAY_OPTIONS:
        for shift in SHIFT_OPTIONS:
            for room_number in range(1, 15):
                slot = slots_by_key.get((day["value"], shift["code"], room_number))
                grid.append(
                    {
                        "day_of_week": day["value"],
                        "day_label": day["label"],
                        "shift": shift["code"],
                        "shift_label": shift["label"],
                        "room_number": room_number,
                        "room_code": room_code_from_number(room_number),
                        "service_line_code": str(getattr(slot, "service_line_code", "") or ""),
                        "activo": bool(getattr(slot, "activo", False)) if slot is not None else False,
                    }
                )
    return grid


def template_slot_for_date(session: Session, target_date: date, *, room_number: int, turno: str) -> Optional[Any]:
    version = active_template_version(session)
    if version is None:
        return None
    day_of_week = int(target_date.weekday())
    resolved_turno = str(turno or "").upper()
    resolved_room = int(room_number)
    for slot in getattr(version, "slots", []) or []:
        if not bool(getattr(slot, "activo", True)):
            continue
        if int(getattr(slot, "day_of_week", -1)) != day_of_week:
            continue
        if str(getattr(slot, "turno", "")).upper() != resolved_turno:
            continue
        if int(getattr(slot, "room_number", 0) or 0) != resolved_room:
            continue
        return slot
    return None


def clone_template_version(
    session: Session,
    *,
    actor: str,
    version_label: str,
    slot_specs: Iterable[Dict[str, Any]],
) -> Any:
    ensure_jefatura_quirofano_seed(session, actor=actor)
    (
        session.query(m.JefaturaQuirofanoTemplateVersionDB)
        .filter(
            m.JefaturaQuirofanoTemplateVersionDB.unidad_code == DEFAULT_UNIT_CODE,
            m.JefaturaQuirofanoTemplateVersionDB.is_active.is_(True),
        )
        .update({"is_active": False}, synchronize_session=False)
    )
    version = m.JefaturaQuirofanoTemplateVersionDB(
        unidad_code=DEFAULT_UNIT_CODE,
        nombre=DEFAULT_TEMPLATE_NAME,
        version_label=version_label,
        is_active=True,
        created_by=actor,
    )
    session.add(version)
    session.flush()
    for spec in slot_specs:
        line_code = safe_text(spec.get("service_line_code"), max_len=60)
        if not line_code:
            continue
        session.add(
            m.JefaturaQuirofanoTemplateSlotDB(
                template_version_id=int(version.id),
                unidad_code=DEFAULT_UNIT_CODE,
                day_of_week=int(spec["day_of_week"]),
                turno=str(spec["turno"]),
                room_number=int(spec["room_number"]),
                room_code=room_code_from_number(spec["room_number"]),
                service_line_code=str(line_code).upper(),
                activo=bool(spec.get("activo", True)),
            )
        )
    session.flush()
    log_audit(
        session,
        actor=actor,
        action="create_template_version",
        entity_type="template_version",
        entity_id=int(version.id),
        payload={"version_label": version.version_label},
    )
    return version


def log_audit(
    session: Session,
    *,
    actor: str,
    action: str,
    entity_type: str,
    entity_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    session.add(
        m.JefaturaQuirofanoAuditLogDB(
            unidad_code=DEFAULT_UNIT_CODE,
            actor=str(actor or "SYSTEM"),
            action=str(action),
            entity_type=str(entity_type),
            entity_id=entity_id,
            payload=payload or {},
        )
    )


def ensure_daily_blocks_for_date(session: Session, target_date: date, *, actor: str = "SYSTEM") -> List[Any]:
    ensure_jefatura_quirofano_seed(session, actor=actor)
    version = active_template_version(session)
    existing = (
        session.query(m.JefaturaQuirofanoDailyBlockDB)
        .filter(
            m.JefaturaQuirofanoDailyBlockDB.unidad_code == DEFAULT_UNIT_CODE,
            m.JefaturaQuirofanoDailyBlockDB.fecha == target_date,
        )
        .all()
    )
    existing_map = {(int(row.room_number), str(row.turno)): row for row in existing}
    template_slots = [
        slot for slot in (getattr(version, "slots", []) or [])
        if int(getattr(slot, "day_of_week", -1)) == int(target_date.weekday()) and bool(getattr(slot, "activo", True))
    ]
    created = False
    for slot in template_slots:
        key = (int(slot.room_number), str(slot.turno))
        if key in existing_map:
            continue
        session.add(
            m.JefaturaQuirofanoDailyBlockDB(
                unidad_code=DEFAULT_UNIT_CODE,
                fecha=target_date,
                turno=str(slot.turno),
                room_number=int(slot.room_number),
                room_code=room_code_from_number(slot.room_number),
                service_line_code=str(slot.service_line_code),
                template_version_id=getattr(version, "id", None),
                block_status="ACTIVO",
                confirmed_by=actor,
                confirmed_at=utcnow(),
            )
        )
        created = True
    if created:
        session.flush()
        log_audit(
            session,
            actor=actor,
            action="ensure_daily_blocks",
            entity_type="daily_block",
            payload={"fecha": target_date.isoformat()},
        )
        session.commit()
    return (
        session.query(m.JefaturaQuirofanoDailyBlockDB)
        .options(joinedload(m.JefaturaQuirofanoDailyBlockDB.cases))
        .filter(
            m.JefaturaQuirofanoDailyBlockDB.unidad_code == DEFAULT_UNIT_CODE,
            m.JefaturaQuirofanoDailyBlockDB.fecha == target_date,
        )
        .order_by(m.JefaturaQuirofanoDailyBlockDB.turno.asc(), m.JefaturaQuirofanoDailyBlockDB.room_number.asc())
        .all()
    )


def daily_blocks_for_date(session: Session, target_date: date, *, actor: str = "SYSTEM") -> List[Any]:
    return ensure_daily_blocks_for_date(session, target_date, actor=actor)


def scheduled_window(target_date: date, scheduled_time: Any, duration_min: Any) -> Tuple[Optional[datetime], Optional[datetime]]:
    parsed_time = safe_time(scheduled_time)
    if parsed_time is None:
        return None, None
    start = datetime.combine(target_date, parsed_time)
    duration = safe_int(duration_min) or 60
    if duration <= 0:
        duration = 60
    return start, start + timedelta(minutes=duration)


def primary_staff_map(case_row: Any) -> Dict[str, str]:
    return {
        "CIRUJANO": str(getattr(case_row, "cirujano", "") or "").strip().upper(),
        "ANESTESIOLOGO": str(getattr(case_row, "anestesiologo", "") or "").strip().upper(),
        "ENFERMERA": str(getattr(case_row, "enfermera_especialista", "") or "").strip().upper(),
    }


def _load_extra_staff(session: Session, case_ids: Iterable[int]) -> Dict[int, List[Tuple[str, str]]]:
    rows = (
        session.query(m.JefaturaQuirofanoCaseStaffDB)
        .filter(m.JefaturaQuirofanoCaseStaffDB.case_id.in_(list(case_ids) or [-1]))
        .all()
    )
    mapping: Dict[int, List[Tuple[str, str]]] = defaultdict(list)
    for row in rows:
        mapping[int(row.case_id)].append(
            (str(row.staff_role or "").strip().upper(), str(row.staff_name or "").strip().upper())
        )
    return mapping


def overlaps(window_a: Tuple[Optional[datetime], Optional[datetime]], window_b: Tuple[Optional[datetime], Optional[datetime]]) -> bool:
    start_a, end_a = window_a
    start_b, end_b = window_b
    if None in (start_a, end_a, start_b, end_b):
        return False
    return bool(start_a < end_b and start_b < end_a)


def validate_case_conflicts(
    session: Session,
    *,
    daily_block_id: int,
    scheduled_time: Any,
    duration_min: Any,
    cirujano: Any,
    anestesiologo: Any,
    enfermera_especialista: Any,
    exclude_case_id: Optional[int] = None,
) -> List[str]:
    block = session.get(m.JefaturaQuirofanoDailyBlockDB, int(daily_block_id or 0))
    if block is None:
        return ["No existe el bloque diario seleccionado."]
    target_date = getattr(block, "fecha", None)
    if not isinstance(target_date, date):
        return ["El bloque diario no tiene fecha válida."]
    current_window = scheduled_window(target_date, scheduled_time, duration_min)
    if current_window[0] is None:
        return ["Debes capturar una hora programada válida."]
    q = (
        session.query(m.JefaturaQuirofanoCaseProgramacionDB)
        .join(
            m.JefaturaQuirofanoDailyBlockDB,
            m.JefaturaQuirofanoDailyBlockDB.id == m.JefaturaQuirofanoCaseProgramacionDB.daily_block_id,
        )
        .filter(
            m.JefaturaQuirofanoDailyBlockDB.fecha == target_date,
            m.JefaturaQuirofanoCaseProgramacionDB.status != "CANCELADA",
        )
    )
    if exclude_case_id:
        q = q.filter(m.JefaturaQuirofanoCaseProgramacionDB.id != int(exclude_case_id))
    others = q.all()
    errors: List[str] = []
    extra_staff = _load_extra_staff(session, [int(row.id) for row in others])
    requested_staff = {
        "CIRUJANO": str(cirujano or "").strip().upper(),
        "ANESTESIOLOGO": str(anestesiologo or "").strip().upper(),
        "ENFERMERA": str(enfermera_especialista or "").strip().upper(),
    }
    for row in others:
        row_block = getattr(row, "daily_block", None)
        if row_block is None:
            row_block = session.get(m.JefaturaQuirofanoDailyBlockDB, int(row.daily_block_id))
        row_window = scheduled_window(target_date, getattr(row, "scheduled_time", None), getattr(row, "duracion_estimada_min", None))
        if not overlaps(current_window, row_window):
            continue
        if int(getattr(row, "daily_block_id", 0) or 0) == int(daily_block_id):
            errors.append(
                f"Traslape en la misma sala con el caso #{row.id} ({getattr(row, 'patient_name', 'SIN_NOMBRE')}) a las {getattr(row, 'scheduled_time', '')}."
            )
        other_staff = primary_staff_map(row)
        for role, requested_name in requested_staff.items():
            if not requested_name:
                continue
            if requested_name and requested_name == other_staff.get(role):
                errors.append(
                    f"Conflicto de personal: {role.title()} '{requested_name}' ya está asignado al caso #{row.id} en horario traslapado."
                )
            for extra_role, extra_name in extra_staff.get(int(row.id), []):
                if requested_name and requested_name == extra_name and role == extra_role:
                    errors.append(
                        f"Conflicto de personal: {role.title()} '{requested_name}' ya aparece como apoyo del caso #{row.id} en horario traslapado."
                    )
    return list(dict.fromkeys(errors))


def validate_case_event_sequence(
    session: Session,
    *,
    case_id: int,
    event_type: str,
    event_at: datetime,
) -> List[str]:
    event_key = str(event_type or "").strip().upper()
    current_order = EVENT_ORDER.get(event_key)
    if current_order is None:
        return ["Tipo de evento no reconocido."]
    rows = (
        session.query(m.JefaturaQuirofanoCaseEventDB)
        .filter(m.JefaturaQuirofanoCaseEventDB.case_id == int(case_id))
        .order_by(m.JefaturaQuirofanoCaseEventDB.event_at.asc(), m.JefaturaQuirofanoCaseEventDB.id.asc())
        .all()
    )
    errors: List[str] = []
    for row in rows:
        row_order = EVENT_ORDER.get(str(row.event_type or "").upper(), -1)
        if row_order <= current_order and row.event_at > event_at:
            errors.append("La hora del evento es inconsistente con un evento previo ya capturado.")
        if row_order >= current_order and row.event_at < event_at and row_order != current_order:
            errors.append("La hora del evento rebasa un evento posterior ya registrado.")
    return list(dict.fromkeys(errors))


def service_line_guess(diagnostico: Any, operacion: Any) -> Optional[str]:
    text = f"{diagnostico or ''} {operacion or ''}".upper()
    mapping = [
        ("PROCTO", ["HEMORROID", "FISTULA ANAL", "FISURA ANAL", "ANO", "ANORRECTAL"]),
        ("URO", ["PROSTAT", "URETER", "VEJIGA", "LITO", "URO", "NEFRO", "JJ", "HIDRONEFROSIS"]),
        ("NEURO", ["ENCEFAL", "CEREBR", "MENING", "HIPOFISIS", "NEURO"]),
        ("PLASTICA", ["MAMA", "PLASTIC", "INJERTO", "PROTESIS", "RECONSTRUCCION"]),
        ("MAXILO", ["FACIAL", "MAXIL", "MANDIB", "HUESO FACIAL"]),
        ("ANGIO", ["VENA", "ARTER", "VASCULAR", "PIE", "DIALISIS", "AMPUTACION"]),
        ("CARDIOT", ["VALVULA", "AORTICA", "CARDI", "TRASPLANTE CARDIACO"]),
        ("CIR_GRAL", ["COLECIST", "PANCREAS", "SUPRARRENAL", "TIROID", "GASTRO", "INTESTINO"]),
        ("TRASPLANTE", ["TRASPLANTE"]),
    ]
    for code, keywords in mapping:
        if any(keyword in text for keyword in keywords):
            return code
    return None


def parse_pdf_date_from_text(text: str) -> Optional[date]:
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", text or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(0), "%d-%m-%Y").date()
    except Exception:
        return None


def digest_file(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def sorted_import_rows(rows: Iterable[Any]) -> List[Any]:
    def _key(row: Any) -> Tuple[int, int, str]:
        room = normalize_room_number(getattr(row, "room_code", None) or "")
        return (
            room or 999,
            0 if str(getattr(row, "turno", "")).upper() == "MATUTINO" else 1,
            str(getattr(row, "hora_programada", "") or ""),
        )
    return sorted(list(rows), key=_key)


def duration_map_for_import_rows(rows: Iterable[Any]) -> Dict[int, int]:
    grouped: Dict[Tuple[str, str], List[Any]] = defaultdict(list)
    for row in sorted_import_rows(rows):
        grouped[(str(getattr(row, "room_code", "") or ""), str(getattr(row, "turno", "") or ""))].append(row)
    durations: Dict[int, int] = {}
    for items in grouped.values():
        for index, row in enumerate(items):
            current_time = safe_time(getattr(row, "hora_programada", None))
            next_time = safe_time(getattr(items[index + 1], "hora_programada", None)) if index + 1 < len(items) else None
            duration = 60
            if current_time is not None and next_time is not None:
                start_dt = datetime.combine(date.today(), current_time)
                next_dt = datetime.combine(date.today(), next_time)
                diff = int((next_dt - start_dt).total_seconds() // 60)
                if diff > 0:
                    duration = max(20, min(diff, 240))
            durations[int(getattr(row, "id", 0) or 0)] = duration
    return durations


def serialize_service_line(row: Any) -> Dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "code": str(getattr(row, "code", "") or ""),
        "nombre": str(getattr(row, "nombre", "") or ""),
        "line_type": str(getattr(row, "line_type", "") or ""),
        "activo": bool(getattr(row, "activo", False)),
        "display_order": int(getattr(row, "display_order", 0) or 0),
    }


def serialize_daily_block(row: Any) -> Dict[str, Any]:
    badge = status_badge(getattr(row, "block_status", "ACTIVO"))
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "fecha": format_date(getattr(row, "fecha", None)),
        "turno": str(getattr(row, "turno", "") or ""),
        "room_number": int(getattr(row, "room_number", 0) or 0),
        "room_code": str(getattr(row, "room_code", "") or ""),
        "service_line_code": str(getattr(row, "service_line_code", "") or ""),
        "block_status": str(getattr(row, "block_status", "") or ""),
        "block_badge": badge,
        "notes": str(getattr(row, "notes", "") or ""),
        "cases_count": len(getattr(row, "cases", []) or []),
    }


def serialize_case(row: Any, *, include_relations: bool = False) -> Dict[str, Any]:
    block = getattr(row, "daily_block", None)
    if block is None and getattr(row, "daily_block_id", None):
        block = None
    payload = {
        "id": int(getattr(row, "id", 0) or 0),
        "daily_block_id": int(getattr(row, "daily_block_id", 0) or 0),
        "status": str(getattr(row, "status", "") or ""),
        "status_badge": status_badge(getattr(row, "status", "PROGRAMADA")),
        "scheduled_time": str(getattr(row, "scheduled_time", "") or ""),
        "duracion_estimada_min": int(getattr(row, "duracion_estimada_min", 0) or 0),
        "cama": str(getattr(row, "cama", "") or ""),
        "patient_name": str(getattr(row, "patient_name", "") or ""),
        "nss": str(getattr(row, "nss", "") or ""),
        "agregado_medico": str(getattr(row, "agregado_medico", "") or ""),
        "edad": int(getattr(row, "edad", 0) or 0) if getattr(row, "edad", None) is not None else None,
        "diagnostico_preoperatorio": str(getattr(row, "diagnostico_preoperatorio", "") or ""),
        "operacion_proyectada": str(getattr(row, "operacion_proyectada", "") or ""),
        "cirujano": str(getattr(row, "cirujano", "") or ""),
        "anestesiologo": str(getattr(row, "anestesiologo", "") or ""),
        "enfermera_especialista": str(getattr(row, "enfermera_especialista", "") or ""),
        "tipo_anestesia": str(getattr(row, "tipo_anestesia", "") or ""),
        "notes": str(getattr(row, "notes", "") or ""),
        "source_type": str(getattr(row, "source_type", "") or ""),
        "turno": str(getattr(block, "turno", "") or ""),
        "room_code": str(getattr(block, "room_code", "") or ""),
        "room_number": int(getattr(block, "room_number", 0) or 0) if block is not None else None,
        "service_line_code": str(getattr(block, "service_line_code", "") or ""),
    }
    if include_relations:
        payload["staff_assignments"] = [
            {
                "id": int(getattr(item, "id", 0) or 0),
                "staff_name": str(getattr(item, "staff_name", "") or ""),
                "staff_role": str(getattr(item, "staff_role", "") or ""),
                "notes": str(getattr(item, "notes", "") or ""),
            }
            for item in (getattr(row, "staff_assignments", []) or [])
        ]
        payload["events"] = [
            {
                "id": int(getattr(item, "id", 0) or 0),
                "event_type": str(getattr(item, "event_type", "") or ""),
                "event_at": format_dt(getattr(item, "event_at", None)),
                "notes": str(getattr(item, "notes", "") or ""),
            }
            for item in sorted(getattr(row, "events", []) or [], key=lambda item: getattr(item, "event_at", datetime.min))
        ]
        payload["incidencias"] = [
            {
                "id": int(getattr(item, "id", 0) or 0),
                "incidence_type": str(getattr(item, "incidence_type", "") or ""),
                "status": str(getattr(item, "status", "") or ""),
                "status_badge": status_badge(getattr(item, "status", "ABIERTA")),
                "event_at": format_dt(getattr(item, "event_at", None)),
                "description": str(getattr(item, "description", "") or ""),
            }
            for item in sorted(getattr(row, "incidencias", []) or [], key=lambda item: getattr(item, "event_at", datetime.min), reverse=True)
        ]
    return payload


def serialize_import_row(row: Any) -> Dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "page_number": int(getattr(row, "page_number", 0) or 0),
        "row_number": int(getattr(row, "row_number", 0) or 0),
        "review_status": str(getattr(row, "review_status", "") or ""),
        "room_code": str(getattr(row, "room_code", "") or ""),
        "turno": str(getattr(row, "turno", "") or ""),
        "hora_programada": str(getattr(row, "hora_programada", "") or ""),
        "cama": str(getattr(row, "cama", "") or ""),
        "paciente_nombre": str(getattr(row, "paciente_nombre", "") or ""),
        "nss": str(getattr(row, "nss", "") or ""),
        "agregado_medico": str(getattr(row, "agregado_medico", "") or ""),
        "edad": int(getattr(row, "edad", 0) or 0) if getattr(row, "edad", None) is not None else None,
        "diagnostico_preoperatorio": str(getattr(row, "diagnostico_preoperatorio", "") or ""),
        "operacion_proyectada": str(getattr(row, "operacion_proyectada", "") or ""),
        "cirujano": str(getattr(row, "cirujano", "") or ""),
        "anestesiologo": str(getattr(row, "anestesiologo", "") or ""),
        "tipo_anestesia": str(getattr(row, "tipo_anestesia", "") or ""),
        "enfermera_especialista": str(getattr(row, "enfermera_especialista", "") or ""),
        "specialty_guess": str(getattr(row, "specialty_guess", "") or ""),
        "discrepancy_flag": bool(getattr(row, "discrepancy_flag", False)),
        "discrepancy_json": getattr(row, "discrepancy_json", None) or {},
    }


def build_day_overview(session: Session, target_date: date, *, actor: str = "SYSTEM") -> Dict[str, Any]:
    blocks = daily_blocks_for_date(session, target_date, actor=actor)
    block_ids = [int(row.id) for row in blocks]
    cases = (
        session.query(m.JefaturaQuirofanoCaseProgramacionDB)
        .options(
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.daily_block),
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.events),
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.incidencias),
        )
        .filter(m.JefaturaQuirofanoCaseProgramacionDB.daily_block_id.in_(block_ids or [-1]))
        .order_by(m.JefaturaQuirofanoCaseProgramacionDB.scheduled_time.asc(), m.JefaturaQuirofanoCaseProgramacionDB.id.asc())
        .all()
    )
    grouped_blocks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    grouped_cases: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for block in blocks:
        grouped_blocks[str(block.turno)].append(serialize_daily_block(block))
    for case in cases:
        grouped_cases[str(getattr(getattr(case, "daily_block", None), "turno", "") or "")].append(serialize_case(case))
    performed = sum(1 for row in cases if str(row.status).upper() == "REALIZADA")
    programmed = len(cases)
    cancellations = sum(1 for row in cases if str(row.status).upper() == "CANCELADA")
    starts = []
    turnovers = []
    conflicts = 0
    conflict_case_ids: List[int] = []
    cases_by_block: Dict[int, List[Any]] = defaultdict(list)
    for case in cases:
        cases_by_block[int(case.daily_block_id)].append(case)
        first_event = next((evt for evt in sorted(case.events, key=lambda item: item.event_at) if str(evt.event_type).upper() == "INICIO_CIRUGIA"), None)
        if first_event and safe_time(case.scheduled_time):
            scheduled_dt = datetime.combine(target_date, safe_time(case.scheduled_time))
            starts.append(int((first_event.event_at - scheduled_dt).total_seconds() // 60))
    for block_cases in cases_by_block.values():
        ordered = sorted(block_cases, key=lambda item: safe_time(item.scheduled_time) or time(0, 0))
        for prev_case, next_case in zip(ordered, ordered[1:]):
            prev_finish = next((evt for evt in sorted(prev_case.events, key=lambda item: item.event_at) if str(evt.event_type).upper() == "FIN_CIRUGIA"), None)
            next_start = next((evt for evt in sorted(next_case.events, key=lambda item: item.event_at) if str(evt.event_type).upper() == "INICIO_CIRUGIA"), None)
            if prev_finish and next_start and next_start.event_at >= prev_finish.event_at:
                turnovers.append(int((next_start.event_at - prev_finish.event_at).total_seconds() // 60))
    for case in cases:
        errs = validate_case_conflicts(
            session,
            daily_block_id=int(case.daily_block_id),
            scheduled_time=case.scheduled_time,
            duration_min=case.duracion_estimada_min,
            cirujano=case.cirujano,
            anestesiologo=case.anestesiologo,
            enfermera_especialista=case.enfermera_especialista,
            exclude_case_id=int(case.id),
        )
        if errs:
            conflicts += 1
            conflict_case_ids.append(int(getattr(case, "id", 0) or 0))
    shift_code_list = [str(item["code"]) for item in SHIFT_OPTIONS]
    shift_block_counts = {
        code: sum(1 for row in blocks if str(getattr(row, "turno", "") or "").upper() == code)
        for code in shift_code_list
    }
    shift_case_counts = {
        code: sum(
            1
            for row in cases
            if str(getattr(getattr(row, "daily_block", None), "turno", "") or "").upper() == code
        )
        for code in shift_code_list
    }
    block_status_counts = {
        code: sum(1 for row in blocks if str(getattr(row, "block_status", "") or "").upper() == code)
        for code in BLOCK_STATUS_OPTIONS
    }
    return {
        "target_date": target_date,
        "target_date_label": format_date(target_date),
        "blocks": blocks,
        "cases": cases,
        "grouped_blocks": grouped_blocks,
        "grouped_cases": grouped_cases,
        "conflict_case_ids": conflict_case_ids,
        "kpis": {
            "blocks_total": len(blocks),
            "programmed_cases": programmed,
            "performed_cases": performed,
            "cancelled_cases": cancellations,
            "occupancy_pct": round((programmed / len(blocks) * 100.0), 1) if blocks else 0.0,
            "avg_start_delay_min": round(sum(starts) / len(starts), 1) if starts else None,
            "avg_turnover_min": round(sum(turnovers) / len(turnovers), 1) if turnovers else None,
            "conflict_cases": conflicts,
            "active_blocks": block_status_counts.get("ACTIVO", 0),
            "blocked_blocks": block_status_counts.get("BLOQUEADO", 0),
            "contingency_blocks": block_status_counts.get("CONTINGENCIA", 0),
            "matutino_blocks": shift_block_counts.get("MATUTINO", 0),
            "vespertino_blocks": shift_block_counts.get("VESPERTINO", 0),
            "matutino_cases": shift_case_counts.get("MATUTINO", 0),
            "vespertino_cases": shift_case_counts.get("VESPERTINO", 0),
        },
    }


def recent_import_batches(session: Session, *, limit: int = 10) -> List[Any]:
    ensure_jefatura_quirofano_seed(session)
    return (
        session.query(m.JefaturaQuirofanoImportBatchDB)
        .order_by(m.JefaturaQuirofanoImportBatchDB.id.desc())
        .limit(max(1, min(int(limit or 10), 50)))
        .all()
    )


def publication_rows_for_date(session: Session, target_date: date, *, actor: str = "SYSTEM") -> List[Dict[str, Any]]:
    overview = build_day_overview(session, target_date, actor=actor)
    rows: List[Dict[str, Any]] = []
    for case in overview["cases"]:
        block = getattr(case, "daily_block", None)
        rows.append(
            {
                "turno": str(getattr(block, "turno", "") or ""),
                "room_code": str(getattr(block, "room_code", "") or ""),
                "service_line_code": str(getattr(block, "service_line_code", "") or ""),
                "hora": str(getattr(case, "scheduled_time", "") or ""),
                "paciente": str(getattr(case, "patient_name", "") or ""),
                "operacion": str(getattr(case, "operacion_proyectada", "") or ""),
                "cirujano": str(getattr(case, "cirujano", "") or ""),
                "estatus": status_badge(getattr(case, "status", "PROGRAMADA")),
            }
        )
    return sorted(rows, key=lambda item: (item["turno"], item["room_code"], item["hora"]))
