from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.services.resident_profiles_flow import load_resident_catalog, resident_lookup


CENTRAL_MODULE: Dict[str, Any] = {
    "slug": "central",
    "nombre": "Central",
    "icono": "🎛️",
    "descripcion": "Comando académico para gestionar evaluaciones, casos asociados, incidencias, insumos y seguimiento longitudinal de residentes.",
    "color": "azul",
}


CENTRAL_SUBMODULES: List[Dict[str, str]] = [
    {
        "slug": "examenes",
        "nombre": "Exámenes",
        "icono": "🧠",
        "descripcion": "Diseño y asignación de evaluaciones semestrales de opción múltiple.",
        "href": "/jefatura-urologia/central/examenes",
        "color": "azul",
    },
    {
        "slug": "casos",
        "nombre": "Casos Asociados",
        "icono": "🗂️",
        "descripcion": "Asignación de pacientes y seguimiento docente por residente.",
        "href": "/jefatura-urologia/central/casos",
        "color": "naranja",
    },
    {
        "slug": "incidencias",
        "nombre": "Incidencias",
        "icono": "🚨",
        "descripcion": "Registro de incidencias académicas y operativas con severidad y estado.",
        "href": "/jefatura-urologia/central/incidencias",
        "color": "rojo",
    },
    {
        "slug": "insumos",
        "nombre": "Insumos",
        "icono": "🧰",
        "descripcion": "Lectura centralizada de insumos solicitados, alertas de intermed y presión de demanda quirúrgica.",
        "href": "/jefatura-urologia/central/insumos",
        "color": "rojo",
    },
]


CASE_PRIORITY_OPTIONS = ["ALTA", "MEDIA", "BAJA"]
CASE_STATUS_OPTIONS = ["PENDIENTE", "EN_SEGUIMIENTO", "RESUELTO", "CERRADO"]
INCIDENCE_SEVERITY_OPTIONS = ["LEVE", "MODERADA", "ALTA", "CRITICA"]
INCIDENCE_STATUS_OPTIONS = ["ABIERTA", "EN_REVISION", "RESUELTA", "CERRADA"]


def normalize_text(value: Any, *, max_len: int) -> Optional[str]:
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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def request_actor(request: Any) -> str:
    for attr in ("username", "user", "authenticated_user"):
        value = getattr(getattr(request, "state", None), attr, None)
        if value:
            return str(value)
    return "JEFATURA"


def resident_display(code: str) -> Dict[str, str]:
    lookup = resident_lookup()
    resident = lookup.get(str(code or "").upper()) or {}
    return {
        "code": str(code or "").strip().upper(),
        "name": str(resident.get("name") or str(code or "").strip().upper()),
        "grade": str(resident.get("grade") or ""),
    }


def resident_selection_groups() -> List[Dict[str, Any]]:
    grouped: List[Dict[str, Any]] = []
    catalog = load_resident_catalog()
    for grade, residents in catalog.items():
        grouped.append(
            {
                "grade": grade,
                "label": grade,
                "residents": residents,
            }
        )
    return grouped


def resolve_assignment_targets(mode: str, resident_code: Optional[str], resident_grade: Optional[str]) -> List[Dict[str, str]]:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "grade":
        grade = str(resident_grade or "").strip().upper()
        if not grade:
            return []
        return [
            {
                "code": str(item["code"]),
                "name": str(item["name"]),
                "grade": str(item["grade"]),
            }
            for item in load_resident_catalog().get(grade, [])
        ]
    code = str(resident_code or "").strip().upper()
    if not code:
        return []
    return [resident_display(code)]


def effective_exam_status(assignment: Any, submitted_at: Optional[datetime] = None, ref_date: Optional[date] = None) -> str:
    today = ref_date or date.today()
    if submitted_at or str(getattr(assignment, "estado", "")).upper() == "CONTESTADA":
        return "CONTESTADA"
    start_on = getattr(assignment, "disponible_desde", None)
    end_on = getattr(assignment, "cierra_en", None)
    if isinstance(end_on, date) and today > end_on:
        return "VENCIDA"
    if isinstance(start_on, date) and today < start_on:
        return "PENDIENTE"
    return "DISPONIBLE"


def status_badge(status: str) -> Dict[str, str]:
    normalized = str(status or "PENDIENTE").upper()
    mapping = {
        "PENDIENTE": {"label": "Pendiente", "tone": "slate"},
        "DISPONIBLE": {"label": "Disponible", "tone": "blue"},
        "CONTESTADA": {"label": "Contestada", "tone": "green"},
        "VENCIDA": {"label": "Vencida", "tone": "red"},
        "ABIERTA": {"label": "Abierta", "tone": "red"},
        "EN_REVISION": {"label": "En revisión", "tone": "amber"},
        "RESUELTA": {"label": "Resuelta", "tone": "green"},
        "CERRADA": {"label": "Cerrada", "tone": "slate"},
        "PENDIENTE_CASO": {"label": "Pendiente", "tone": "amber"},
        "EN_SEGUIMIENTO": {"label": "En seguimiento", "tone": "blue"},
        "RESUELTO": {"label": "Resuelto", "tone": "green"},
    }
    return mapping.get(normalized, {"label": normalized.title(), "tone": "slate"})

