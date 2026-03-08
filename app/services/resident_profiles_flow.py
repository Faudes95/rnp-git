from __future__ import annotations

import json
import logging
import mimetypes
import os
import re
import secrets
import unicodedata
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi.responses import FileResponse, Response
from sqlalchemy import func, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.core.patient_file_utils import detect_mime, extract_extension, safe_filename

logger = logging.getLogger(__name__)

GRADES: List[str] = ["R5", "R4", "R3", "R2"]
APPROACH_OPTIONS: List[str] = ["ABIERTO", "LAPAROSCOPICO", "ENDOSCOPICO", "ROBOTICO"]
ROLE_OPTIONS: List[str] = ["NA", "1ER_AYUDANTE", "2DO_AYUDANTE", "3ER_AYUDANTE", "VISOR"]
PARTICIPATION_OPTIONS: List[str] = ["NA", "OBSERVO", "ASISTIO", "PARCIAL", "MAYORIA"]
METAS_CURVAS_APRENDIZAJE: Dict[str, int] = {
    "RTUP": 50,
    "NEFROLITOTOMIA": 30,
    "CISTECTOMIA PARCIAL": 20,
    "URETEROSCOPIA": 40,
}
ROLE_WEIGHT_MAP: Dict[str, int] = {
    "NA": 0,
    "VISOR": 15,
    "3ER_AYUDANTE": 40,
    "2DO_AYUDANTE": 70,
    "1ER_AYUDANTE": 100,
}
PARTICIPATION_WEIGHT_MAP: Dict[str, int] = {
    "NA": 0,
    "OBSERVO": 15,
    "ASISTIO": 40,
    "PARCIAL": 70,
    "MAYORIA": 100,
}
MONTHS_CYCLE: List[str] = ["MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC", "ENE", "FEB"]
MONTH_TO_NUM: Dict[str, int] = {"MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12, "ENE": 1, "FEB": 2}
NUM_TO_MONTH: Dict[int, str] = {value: key for key, value in MONTH_TO_NUM.items()}
MONTH_LABELS_ES: Dict[str, str] = {
    "MAR": "Marzo",
    "ABR": "Abril",
    "MAY": "Mayo",
    "JUN": "Junio",
    "JUL": "Julio",
    "AGO": "Agosto",
    "SEP": "Septiembre",
    "OCT": "Octubre",
    "NOV": "Noviembre",
    "DIC": "Diciembre",
    "ENE": "Enero",
    "FEB": "Febrero",
}
MONTH_SHORT_ES: Dict[int, str] = {
    1: "Ene",
    2: "Feb",
    3: "Mar",
    4: "Abr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dic",
}
PROFILE_SEXO_OPTIONS: List[str] = ["", "FEMENINO", "MASCULINO", "NO ESPECIFICADO"]


def _programa_operativo_js_path() -> Path:
    return Path(__file__).resolve().parents[1] / "static" / "js" / "jefatura_programa_operativo.js"


def _slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or ""))
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_text = re.sub(r"[^A-Z0-9]+", "_", ascii_text.upper()).strip("_")
    return ascii_text or "NA"


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return None


def _safe_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _safe_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _normalize_text(value: Any, *, max_len: int) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    return text[:max_len]


def _normalize_sexo(value: Any) -> Optional[str]:
    text = _normalize_text(value, max_len=40)
    if not text:
        return None
    upper = text.upper()
    if upper in {"MASCULINO", "FEMENINO", "NO ESPECIFICADO"}:
        return upper
    return text


def _normalize_procedure_key(name: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(name or "").upper())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip()


def _resident_code_filter(model_field: Any, resident_code: str) -> Any:
    return func.upper(model_field) == str(resident_code or "").strip().upper()


def _safe_avg(value: Any, digits: int = 1) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _safe_pct(numerator: float, denominator: float, digits: int = 1) -> Optional[float]:
    if denominator in (None, 0):
        return None
    try:
        return round((float(numerator) / float(denominator)) * 100.0, digits)
    except Exception:
        return None


def _default_grade_from_code(code: str) -> str:
    match = re.match(r"^(R[2-5])U[_-]?", str(code or "").upper())
    return match.group(1) if match else "NA"


def _resident_analytics_schema_ready(db: Session) -> bool:
    try:
        inspector = inspect(db.bind)
        tables = set(inspector.get_table_names())
    except SQLAlchemyError:
        logger.exception("No fue posible inspeccionar el esquema analitico de residentes")
        return False
    return {"surgery_event_index", "surgery_event_participants"}.issubset(tables)


@lru_cache(maxsize=1)
def load_schedule_rows() -> List[Dict[str, Any]]:
    js_path = _programa_operativo_js_path()
    if not js_path.exists():
        return []
    raw = js_path.read_text(encoding="utf-8")
    match = re.search(r"let\s+SCHEDULE_ROWS\s*=\s*(\[[\s\S]*?\]);", raw)
    if not match:
        return []
    try:
        rows = json.loads(match.group(1))
    except Exception:
        logger.exception("No fue posible parsear SCHEDULE_ROWS para residentes")
        return []
    return rows if isinstance(rows, list) else []


@lru_cache(maxsize=1)
def load_resident_catalog() -> Dict[str, List[Dict[str, str]]]:
    catalog: Dict[str, List[Dict[str, str]]] = {grade: [] for grade in GRADES}
    seen: set[tuple[str, str]] = set()
    for row in load_schedule_rows():
        grade = str(row.get("Gdo") or "").strip().upper()
        name = str(row.get("Residente") or "").strip()
        if grade not in catalog or not name:
            continue
        code = f"{grade}U_{_slugify(name)}"
        key = (grade, code)
        if key in seen:
            continue
        seen.add(key)
        catalog[grade].append(
            {
                "grade": grade,
                "code": code,
                "name": name,
                "label": name,
            }
        )
    for grade in GRADES:
        catalog[grade].sort(key=lambda item: item["name"])
    return catalog


@lru_cache(maxsize=1)
def resident_lookup() -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for members in load_resident_catalog().values():
        for member in members:
            lookup[member["code"]] = member
            lookup[member["code"].upper()] = member
            lookup[member["name"].upper()] = member
    return lookup


@lru_cache(maxsize=1)
def resident_schedule_lookup() -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in load_schedule_rows():
        grade = str(row.get("Gdo") or "").strip().upper()
        name = str(row.get("Residente") or "").strip()
        if not grade or not name:
            continue
        code = f"{grade}U_{_slugify(name)}"
        lookup[code] = row
        lookup[code.upper()] = row
        lookup[name.upper()] = row
    return lookup


@lru_cache(maxsize=1)
def resident_cards() -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    for grade in GRADES:
        for member in load_resident_catalog().get(grade, []):
            cards.append(
                {
                    "grade": grade,
                    "code": member["code"],
                    "name": member["name"],
                    "icon": "🩺",
                }
            )
    return cards


def parse_resident_team(form_dict: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    lookup = resident_lookup()
    team: Dict[str, Dict[str, str]] = {}
    for grade in GRADES:
        residente_raw = str(form_dict.get(f"resident_team[{grade}][residente]") or "NA").strip()
        rol = str(form_dict.get(f"resident_team[{grade}][rol]") or "NA").strip().upper() or "NA"
        participacion = str(form_dict.get(f"resident_team[{grade}][participacion]") or "NA").strip().upper() or "NA"
        if residente_raw.upper() == "NA":
            team[grade] = {
                "residente": "NA",
                "residente_nombre": "",
                "rol": "NA",
                "participacion": "NA",
            }
            continue

        resolved = lookup.get(residente_raw) or lookup.get(residente_raw.upper())
        resident_code = resolved["code"] if resolved else f"{grade}U_{_slugify(residente_raw)}"
        resident_name = resolved["name"] if resolved else residente_raw
        team[grade] = {
            "residente": resident_code,
            "residente_nombre": resident_name,
            "rol": rol,
            "participacion": participacion,
        }
    return team


def _normalize_assignment(value: Any) -> str:
    raw = str(value or "").strip()
    return re.sub(r"\s*\[V:.*?\]\s*", "", raw, flags=re.IGNORECASE).strip()


def _parse_vacation_from_assignment(value: Any) -> Dict[str, Optional[int]]:
    raw = str(value or "").strip()
    match = re.search(r"\[\s*V\s*:\s*(\d{1,2})\s*-\s*(\d{1,2})\s*\]", raw, flags=re.IGNORECASE)
    if not match:
        return {"start_day": None, "end_day": None, "days": 0}
    start_day = _safe_int(match.group(1))
    end_day = _safe_int(match.group(2))
    if start_day is None or end_day is None or end_day < start_day:
        return {"start_day": None, "end_day": None, "days": 0}
    return {
        "start_day": start_day,
        "end_day": end_day,
        "days": max(end_day - start_day + 1, 0),
    }


def _current_cycle_start_year(ref_date: date) -> int:
    return ref_date.year if ref_date.month >= 3 else (ref_date.year - 1)


def _year_for_month_code(month_code: str, cycle_start_year: int) -> int:
    return cycle_start_year + 1 if month_code in {"ENE", "FEB"} else cycle_start_year


def _month_code_from_date(ref_date: date) -> str:
    return NUM_TO_MONTH.get(ref_date.month, "MAR")


def _next_month_code(month_code: str) -> Optional[str]:
    try:
        index = MONTHS_CYCLE.index(month_code)
    except ValueError:
        return None
    if index == len(MONTHS_CYCLE) - 1:
        return None
    return MONTHS_CYCLE[index + 1]


def _build_vacation_period(month_code: str, assignment: Any, cycle_start_year: int) -> Optional[Dict[str, Any]]:
    vacation = _parse_vacation_from_assignment(assignment)
    start_day = vacation.get("start_day")
    end_day = vacation.get("end_day")
    if start_day is None or end_day is None:
        return None
    year = _year_for_month_code(month_code, cycle_start_year)
    month = MONTH_TO_NUM.get(month_code)
    if month is None:
        return None
    try:
        start_date = date(year, month, int(start_day))
        end_date = date(year, month, int(end_day))
    except ValueError:
        return None
    return {
        "month_code": month_code,
        "start": start_date,
        "end": end_date,
        "days": int(vacation.get("days") or 0),
        "label": f"{_format_date_short_es(start_date)} - {_format_date_short_es(end_date)}",
    }


def _format_date_short_es(value: date) -> str:
    return f"{value.day:02d} {MONTH_SHORT_ES.get(value.month, '')} {value.year}"


def _format_date_iso(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value else None


def _initials_for_name(name: str) -> str:
    tokens = [token for token in re.split(r"\s+", str(name or "").strip()) if token]
    if not tokens:
        return "R"
    letters = "".join(token[0] for token in tokens[:2])
    return letters.upper() or "R"


def _build_placeholder_svg(name: str) -> str:
    initials = _initials_for_name(name)
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="640" viewBox="0 0 640 640" fill="none">'
        '<defs><linearGradient id="g" x1="48" y1="52" x2="590" y2="592" gradientUnits="userSpaceOnUse">'
        '<stop stop-color="#1B4D8C"/><stop offset="1" stop-color="#0E223F"/></linearGradient></defs>'
        '<rect width="640" height="640" rx="120" fill="#EEF3F8"/>'
        '<rect x="26" y="26" width="588" height="588" rx="104" fill="url(#g)"/>'
        '<circle cx="320" cy="250" r="104" fill="#E7F0FA" fill-opacity="0.22"/>'
        f'<text x="320" y="380" text-anchor="middle" font-family="Montserrat, Arial, sans-serif" font-size="176" font-weight="700" fill="#F8FBFF">{initials}</text>'
        '<text x="320" y="468" text-anchor="middle" font-family="Montserrat, Arial, sans-serif" font-size="34" font-weight="600" fill="#CFE0F5">Programa de Residentes</text>'
        "</svg>"
    )


def _resident_photo_version(photo_updated_at: Any) -> str:
    if isinstance(photo_updated_at, datetime):
        return str(int(photo_updated_at.timestamp()))
    return "0"


def _resident_photo_url(code: str, photo_updated_at: Any) -> str:
    version = _resident_photo_version(photo_updated_at)
    return f"/jefatura-urologia/programa-academico/residentes/{code}/foto?v={version}"


def _resident_profile_photos_dir() -> str:
    return os.path.abspath(getattr(m, "RESIDENT_PROFILE_PHOTOS_DIR", os.path.join(getattr(m, "PATIENT_FILES_DIR", "./patient_files"), "resident_profiles")))


def ensure_resident_profile_photos_dir() -> None:
    os.makedirs(_resident_profile_photos_dir(), exist_ok=True)


def _get_resident_profile_row(db: Session, resident_code: str) -> Any:
    code = str(resident_code or "").strip().upper()
    if not code:
        return None
    try:
        return db.query(m.ResidentProfileDB).filter(func.upper(m.ResidentProfileDB.resident_code) == code).first()
    except SQLAlchemyError:
        logger.exception("No fue posible leer resident_profiles")
        return None


def _top_bucket(rows: List[Any], key_field: str) -> Optional[tuple[str, int]]:
    if not rows:
        return None
    ordered = sorted(
        ((str(getattr(row, key_field) or "N/E"), int(getattr(row, "cantidad") or 0)) for row in rows),
        key=lambda item: (-item[1], item[0]),
    )
    return ordered[0] if ordered else None


def _format_distribution(rows: List[Any], key_field: str) -> List[Dict[str, Any]]:
    total = sum(int(getattr(row, "cantidad") or 0) for row in rows)
    formatted: List[Dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: (-int(getattr(item, "cantidad") or 0), str(getattr(item, key_field) or ""))):
        cantidad = int(getattr(row, "cantidad") or 0)
        label = str(getattr(row, key_field) or "N/E")
        formatted.append(
            {
                "label": label,
                "cantidad": cantidad,
                "porcentaje": round((cantidad / total) * 100.0, 1) if total else 0.0,
            }
        )
    return formatted


def _build_weighted_summary(rows: List[Any], key_field: str, weights: Dict[str, int], title: str) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    total_eventos = 0
    puntos_acumulados = 0
    for row in sorted(rows, key=lambda item: (-int(getattr(item, "cantidad") or 0), str(getattr(item, key_field) or ""))):
        label = str(getattr(row, key_field) or "NA")
        cantidad = int(getattr(row, "cantidad") or 0)
        peso = int(weights.get(label, 0))
        puntos = cantidad * peso
        total_eventos += cantidad
        puntos_acumulados += puntos
        items.append(
            {
                "label": label,
                "cantidad": cantidad,
                "peso": peso,
                "puntos": puntos,
                "porcentaje": 0.0,
            }
        )
    for item in items:
        item["porcentaje"] = round((item["cantidad"] / total_eventos) * 100.0, 1) if total_eventos else 0.0
    promedio = round((puntos_acumulados / total_eventos), 1) if total_eventos else None
    return {
        "title": title,
        "promedio": promedio,
        "puntos_acumulados": puntos_acumulados,
        "total_eventos": total_eventos,
        "maximo_teorico": total_eventos * 100,
        "items": items,
        "escala": [{"label": label, "peso": weight} for label, weight in weights.items()],
    }


def _build_operational_profile(resident_code: str, ref_date: Optional[date] = None) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    today = ref_date or date.today()
    resident = resident_lookup().get(code) or {}
    row = resident_schedule_lookup().get(code) or resident_schedule_lookup().get(str(resident.get("name") or "").upper()) or {}

    grade = str(row.get("Gdo") or resident.get("grade") or _default_grade_from_code(code) or "NA")
    current_month_code = _month_code_from_date(today)
    current_cycle_year = _current_cycle_start_year(today)
    current_year = _year_for_month_code(current_month_code, current_cycle_year)
    next_month = _next_month_code(current_month_code)

    current_assignment = row.get(current_month_code) if row else None
    current_rotation = _normalize_assignment(current_assignment) or "Sin dato en programa operativo"
    next_rotation = "Pendiente siguiente ciclo"
    next_rotation_month_label = "Sin dato"
    if next_month and row:
        next_rotation = _normalize_assignment(row.get(next_month)) or "Sin dato en programa operativo"
        next_rotation_month_label = f"{MONTH_LABELS_ES.get(next_month, next_month)} {_year_for_month_code(next_month, current_cycle_year)}"

    current_vacation: Optional[Dict[str, Any]] = None
    next_vacation: Optional[Dict[str, Any]] = None
    total_vacation_days = 0

    if row:
        for month_code in MONTHS_CYCLE:
            assignment = row.get(month_code)
            period = _build_vacation_period(month_code, assignment, current_cycle_year)
            if not period:
                continue
            total_vacation_days += int(period["days"] or 0)
            if period["start"] <= today <= period["end"]:
                current_vacation = period
                continue
            if period["start"] > today and next_vacation is None:
                next_vacation = period

    if current_vacation:
        vacation_status = f"En vacaciones hasta {_format_date_short_es(current_vacation['end'])}"
        vacation_status_tone = "warn"
    elif next_vacation:
        vacation_status = f"Proximas desde {_format_date_short_es(next_vacation['start'])}"
        vacation_status_tone = "info"
    else:
        vacation_status = "Sin vacaciones proximas registradas"
        vacation_status_tone = "neutral"

    return {
        "grado_actual": grade,
        "rotacion_actual": current_rotation,
        "rotacion_actual_label": f"{MONTH_LABELS_ES.get(current_month_code, current_month_code)} {current_year}",
        "proxima_rotacion": next_rotation,
        "proxima_rotacion_label": next_rotation_month_label,
        "estatus_vacacional": vacation_status,
        "estatus_vacacional_tone": vacation_status_tone,
        "vacaciones_actuales": current_vacation,
        "vacaciones_proximas": next_vacation,
        "vacaciones_actuales_label": current_vacation["label"] if current_vacation else "No activas",
        "vacaciones_proximas_label": next_vacation["label"] if next_vacation else "Sin periodo proximo",
        "dias_vacacionales_ciclo": total_vacation_days,
        "ciclo_label": f"MAR {current_cycle_year} - FEB {current_cycle_year + 1}",
        "fuente_label": "Programa Operativo",
    }


def _build_identity_bundle(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    schedule_resident = resident_lookup().get(code) or {}
    operational = _build_operational_profile(code)
    profile_row = _get_resident_profile_row(db, code)

    identity_row = None
    if _resident_analytics_schema_ready(db):
        identity_row = (
            db.query(
                func.max(m.SurgeryEventParticipant.resident_name).label("resident_name"),
                func.max(m.SurgeryEventParticipant.grade).label("grade"),
            )
            .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
            .first()
        )

    display_name = str(
        getattr(profile_row, "nombre", None)
        or schedule_resident.get("name")
        or getattr(identity_row, "resident_name", None)
        or code
        or "Residente sin identificar"
    )
    grade = str(
        operational.get("grado_actual")
        or schedule_resident.get("grade")
        or getattr(identity_row, "grade", None)
        or _default_grade_from_code(code)
    )
    return {
        "resident": {
            "code": code or "NA",
            "name": display_name,
            "grade": grade,
            "display_name": display_name,
            "initials": _initials_for_name(display_name),
            "photo_url": _resident_photo_url(code or "NA", getattr(profile_row, "photo_updated_at", None)),
            "photo_version": _resident_photo_version(getattr(profile_row, "photo_updated_at", None)),
            "photo_updated_at": getattr(profile_row, "photo_updated_at", None),
        },
        "personal": {
            "nombre": display_name,
            "sexo": getattr(profile_row, "sexo", None) or "Sin capturar",
            "universidad": getattr(profile_row, "universidad", None) or "Sin capturar",
            "sede_r1": getattr(profile_row, "sede_r1", None) or "Sin capturar",
            "inscripcion_unam": getattr(profile_row, "inscripcion_unam", None) or "Sin capturar",
        },
        "profile_row": profile_row,
        "operativo": operational,
    }


def _ensure_editor_rows(items: List[Dict[str, Any]], blank_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return items if items else [dict(blank_row)]


def _serialize_publications(rows: List[Any]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        serialized.append(
            {
                "titulo": str(getattr(row, "titulo", "") or ""),
                "revista_o_medio": str(getattr(row, "revista_o_medio", "") or ""),
                "anio": int(getattr(row, "anio", 0) or 0) if getattr(row, "anio", None) not in (None, "") else None,
                "estatus": str(getattr(row, "estatus", "") or ""),
                "notas": str(getattr(row, "notas", "") or ""),
            }
        )
    return serialized


def _serialize_congresses(rows: List[Any]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        congress_date = getattr(row, "fecha", None)
        serialized.append(
            {
                "nombre_evento": str(getattr(row, "nombre_evento", "") or ""),
                "sede": str(getattr(row, "sede", "") or ""),
                "fecha": congress_date.isoformat() if isinstance(congress_date, date) else "",
                "fecha_label": _format_date_short_es(congress_date) if isinstance(congress_date, date) else "Sin fecha",
                "rol": str(getattr(row, "rol", "") or ""),
                "notas": str(getattr(row, "notas", "") or ""),
            }
        )
    return serialized


def _serialize_awards(rows: List[Any]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        award_date = getattr(row, "fecha", None)
        serialized.append(
            {
                "nombre": str(getattr(row, "nombre", "") or ""),
                "institucion": str(getattr(row, "institucion", "") or ""),
                "fecha": award_date.isoformat() if isinstance(award_date, date) else "",
                "fecha_label": _format_date_short_es(award_date) if isinstance(award_date, date) else "Sin fecha",
                "descripcion": str(getattr(row, "descripcion", "") or ""),
            }
        )
    return serialized


def _effective_exam_status(assignment: Any, submitted_at: Optional[datetime] = None, ref_date: Optional[date] = None) -> str:
    today = ref_date or date.today()
    if submitted_at or str(getattr(assignment, "estado", "")).upper() == "CONTESTADA":
        return "CONTESTADA"
    available_from = getattr(assignment, "disponible_desde", None)
    due_on = getattr(assignment, "cierra_en", None)
    if isinstance(due_on, date) and today > due_on:
        return "VENCIDA"
    if isinstance(available_from, date) and today < available_from:
        return "PENDIENTE"
    return "DISPONIBLE"


def _exam_status_badge(status: str) -> Dict[str, str]:
    mapping = {
        "PENDIENTE": {"label": "Pendiente", "tone": "neutral"},
        "DISPONIBLE": {"label": "Disponible", "tone": "info"},
        "CONTESTADA": {"label": "Contestada", "tone": "ok"},
        "VENCIDA": {"label": "Vencida", "tone": "error"},
    }
    return mapping.get(str(status or "").upper(), {"label": str(status or "N/E").title(), "tone": "neutral"})


def _serialize_exam_assignments(rows: List[Any], resident_code: str) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    code = str(resident_code or "").strip().upper()
    for row in rows:
        attempt = next(
            (
                item
                for item in sorted(getattr(row, "attempts", []) or [], key=lambda item: item.id or 0, reverse=True)
                if getattr(item, "submitted_at", None)
            ),
            None,
        )
        status = _effective_exam_status(row, getattr(attempt, "submitted_at", None))
        badge = _exam_status_badge(status)
        serialized.append(
            {
                "id": int(getattr(row, "id", 0) or 0),
                "title": str(getattr(getattr(row, "exam", None), "title", None) or "Examen semestral"),
                "description": str(getattr(getattr(row, "exam", None), "description", None) or ""),
                "periodo_label": str(getattr(row, "periodo_label", None) or getattr(getattr(row, "exam", None), "period_label", None) or "Sin periodo"),
                "status": status,
                "status_label": badge["label"],
                "status_tone": badge["tone"],
                "available_from": getattr(row, "disponible_desde", None),
                "available_from_label": _format_date_short_es(getattr(row, "disponible_desde", None)) if isinstance(getattr(row, "disponible_desde", None), date) else "Sin fecha",
                "due_on": getattr(row, "cierra_en", None),
                "due_on_label": _format_date_short_es(getattr(row, "cierra_en", None)) if isinstance(getattr(row, "cierra_en", None), date) else "Sin fecha",
                "score_pct": round(float(getattr(attempt, "score_pct", 0) or 0), 1) if attempt and getattr(attempt, "score_pct", None) is not None else None,
                "correct_answers": int(getattr(attempt, "correct_answers", 0) or 0) if attempt else None,
                "total_questions": int(getattr(attempt, "total_questions", 0) or 0) if attempt else None,
                "submitted_at": getattr(attempt, "submitted_at", None),
                "submitted_at_label": _format_date_short_es(getattr(attempt, "submitted_at", None).date()) if attempt and getattr(attempt, "submitted_at", None) else "Pendiente",
                "take_href": f"/jefatura-urologia/programa-academico/residentes/{code}/examenes/{int(getattr(row, 'id', 0) or 0)}",
                "can_take": status == "DISPONIBLE" and attempt is None,
            }
        )
    return serialized


def _case_status_badge(status: str) -> Dict[str, str]:
    mapping = {
        "PENDIENTE": {"label": "Pendiente", "tone": "warn"},
        "EN_SEGUIMIENTO": {"label": "En seguimiento", "tone": "info"},
        "RESUELTO": {"label": "Resuelto", "tone": "ok"},
        "CERRADO": {"label": "Cerrado", "tone": "neutral"},
    }
    return mapping.get(str(status or "").upper(), {"label": str(status or "N/E").title(), "tone": "neutral"})


def _serialize_case_assignments(rows: List[Any]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        badge = _case_status_badge(str(getattr(row, "estado", "") or "PENDIENTE"))
        due_on = getattr(row, "fecha_limite", None)
        serialized.append(
            {
                "id": int(getattr(row, "id", 0) or 0),
                "patient_snapshot": str(getattr(row, "patient_snapshot", "") or "Paciente sin snapshot"),
                "objetivo": str(getattr(row, "objetivo", "") or ""),
                "prioridad": str(getattr(row, "prioridad", "") or "MEDIA"),
                "consulta_id": getattr(row, "consulta_id", None),
                "estado": str(getattr(row, "estado", "") or "PENDIENTE"),
                "status_label": badge["label"],
                "status_tone": badge["tone"],
                "fecha_limite": due_on,
                "fecha_limite_label": _format_date_short_es(due_on) if isinstance(due_on, date) else "Sin fecha",
                "notas": str(getattr(row, "notas", "") or ""),
            }
        )
    return serialized


def _incidence_status_badge(status: str) -> Dict[str, str]:
    mapping = {
        "ABIERTA": {"label": "Abierta", "tone": "error"},
        "EN_REVISION": {"label": "En revisión", "tone": "warn"},
        "RESUELTA": {"label": "Resuelta", "tone": "ok"},
        "CERRADA": {"label": "Cerrada", "tone": "neutral"},
    }
    return mapping.get(str(status or "").upper(), {"label": str(status or "N/E").title(), "tone": "neutral"})


def _serialize_incidences(rows: List[Any]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        event_date = getattr(row, "fecha_evento", None)
        badge = _incidence_status_badge(str(getattr(row, "estado", "") or "ABIERTA"))
        serialized.append(
            {
                "id": int(getattr(row, "id", 0) or 0),
                "tipo": str(getattr(row, "tipo", "") or ""),
                "severidad": str(getattr(row, "severidad", "") or "MODERADA"),
                "estado": str(getattr(row, "estado", "") or "ABIERTA"),
                "status_label": badge["label"],
                "status_tone": badge["tone"],
                "fecha_evento": event_date,
                "fecha_evento_label": _format_date_short_es(event_date) if isinstance(event_date, date) else "Sin fecha",
                "descripcion": str(getattr(row, "descripcion", "") or ""),
                "resolucion": str(getattr(row, "resolucion", "") or ""),
                "consulta_id": getattr(row, "consulta_id", None),
            }
        )
    return serialized


def _build_longitudinal_summary(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    empty = {
        "activity_30_days": 0,
        "activity_90_days": 0,
        "procedures_90_days": 0,
        "last_indexed_surgery": None,
    }
    if not code or not _resident_analytics_schema_ready(db):
        return empty

    today = date.today()
    cutoff_30 = today - timedelta(days=30)
    cutoff_90 = today - timedelta(days=90)
    joined = db.query(m.SurgeryEventIndex).join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
    activity_30 = (
        joined.filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .filter(m.SurgeryEventIndex.event_date.isnot(None), m.SurgeryEventIndex.event_date >= cutoff_30)
        .with_entities(func.count(func.distinct(m.SurgeryEventIndex.id)))
        .scalar()
        or 0
    )
    activity_90 = (
        joined.filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .filter(m.SurgeryEventIndex.event_date.isnot(None), m.SurgeryEventIndex.event_date >= cutoff_90)
        .with_entities(func.count(func.distinct(m.SurgeryEventIndex.id)))
        .scalar()
        or 0
    )
    procedures_90 = (
        joined.filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .filter(m.SurgeryEventIndex.event_date.isnot(None), m.SurgeryEventIndex.event_date >= cutoff_90)
        .with_entities(func.count(func.distinct(m.SurgeryEventIndex.procedure_name)))
        .scalar()
        or 0
    )
    last_row = (
        db.query(
            m.SurgeryEventIndex.event_date.label("event_date"),
            m.SurgeryEventIndex.patient_name.label("patient_name"),
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            m.SurgeryEventIndex.approach.label("approach"),
            m.SurgeryEventIndex.blood_loss_ml.label("blood_loss_ml"),
            m.SurgeryEventParticipant.role.label("role"),
            m.SurgeryEventParticipant.participacion_tecnica.label("participacion_tecnica"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .order_by(m.SurgeryEventIndex.event_date.is_(None), m.SurgeryEventIndex.event_date.desc(), m.SurgeryEventIndex.id.desc())
        .first()
    )
    last_surgery = None
    if last_row is not None:
        event_date = getattr(last_row, "event_date", None)
        last_surgery = {
            "date": event_date,
            "date_label": _format_date_short_es(event_date) if isinstance(event_date, date) else "Sin fecha",
            "patient_name": str(getattr(last_row, "patient_name", None) or "Paciente sin nombre"),
            "procedure_name": str(getattr(last_row, "procedure_name", None) or "Procedimiento no especificado"),
            "approach": str(getattr(last_row, "approach", None) or "N/E"),
            "role": str(getattr(last_row, "role", None) or "N/E"),
            "participacion": str(getattr(last_row, "participacion_tecnica", None) or "N/E"),
            "blood_loss_ml": getattr(last_row, "blood_loss_ml", None),
        }
    return {
        "activity_30_days": int(activity_30),
        "activity_90_days": int(activity_90),
        "procedures_90_days": int(procedures_90),
        "last_indexed_surgery": last_surgery,
    }


def _build_academic_profile_data(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    publication_rows = (
        db.query(m.ResidentPublicationDB)
        .filter(_resident_code_filter(m.ResidentPublicationDB.resident_code, code))
        .order_by(m.ResidentPublicationDB.anio.is_(None), m.ResidentPublicationDB.anio.desc(), m.ResidentPublicationDB.created_at.desc(), m.ResidentPublicationDB.id.desc())
        .all()
    )
    congress_rows = (
        db.query(m.ResidentCongressDB)
        .filter(_resident_code_filter(m.ResidentCongressDB.resident_code, code))
        .order_by(m.ResidentCongressDB.fecha.is_(None), m.ResidentCongressDB.fecha.desc(), m.ResidentCongressDB.created_at.desc(), m.ResidentCongressDB.id.desc())
        .all()
    )
    award_rows = (
        db.query(m.ResidentAwardDB)
        .filter(_resident_code_filter(m.ResidentAwardDB.resident_code, code))
        .order_by(m.ResidentAwardDB.fecha.is_(None), m.ResidentAwardDB.fecha.desc(), m.ResidentAwardDB.created_at.desc(), m.ResidentAwardDB.id.desc())
        .all()
    )
    exam_rows = (
        db.query(m.ResidentExamAssignmentDB)
        .filter(_resident_code_filter(m.ResidentExamAssignmentDB.resident_code, code))
        .order_by(m.ResidentExamAssignmentDB.assigned_at.desc(), m.ResidentExamAssignmentDB.id.desc())
        .all()
    )
    case_rows = (
        db.query(m.ResidentCaseAssignmentDB)
        .filter(_resident_code_filter(m.ResidentCaseAssignmentDB.resident_code, code))
        .order_by(m.ResidentCaseAssignmentDB.created_at.desc(), m.ResidentCaseAssignmentDB.id.desc())
        .all()
    )
    incidence_rows = (
        db.query(m.ResidentIncidenceDB)
        .filter(_resident_code_filter(m.ResidentIncidenceDB.resident_code, code))
        .order_by(m.ResidentIncidenceDB.fecha_evento.desc(), m.ResidentIncidenceDB.id.desc())
        .all()
    )

    publications = _serialize_publications(publication_rows)
    congresses = _serialize_congresses(congress_rows)
    awards = _serialize_awards(award_rows)
    evaluations = _serialize_exam_assignments(exam_rows, code)
    cases = _serialize_case_assignments(case_rows)
    incidences = _serialize_incidences(incidence_rows)

    return {
        "academico_no_clinico": {
            "publicaciones": publications,
            "congresos": congresses,
            "premios": awards,
        },
        "evaluaciones_semestrales": evaluations,
        "casos_asociados": cases,
        "incidencias": incidences,
        "editor": {
            "publicaciones": _ensure_editor_rows(publications, {"titulo": "", "revista_o_medio": "", "anio": "", "estatus": "", "notas": ""}),
            "congresos": _ensure_editor_rows(congresses, {"nombre_evento": "", "sede": "", "fecha": "", "rol": "", "notas": ""}),
            "premios": _ensure_editor_rows(awards, {"nombre": "", "institucion": "", "fecha": "", "descripcion": ""}),
        },
        "counts": {
            "publicaciones": len(publications),
            "congresos": len(congresses),
            "premios": len(awards),
            "evaluaciones": len(evaluations),
            "casos": len(cases),
            "incidencias": len(incidences),
        },
    }


def _empty_profile_viewmodel(db: Session, resident_code: str) -> Dict[str, Any]:
    identity = _build_identity_bundle(db, resident_code)
    return {
        "resident": identity["resident"],
        "personal": identity["personal"],
        "operativo": identity["operativo"],
        "has_activity": False,
        "empty_state": "Sin eventos indexados",
        "kpis": {
            "total_cirugias": 0,
            "procedimientos_distintos": 0,
            "abordajes_distintos": 0,
            "sangrado_promedio_global_residente": None,
            "rol_mas_frecuente": "N/E",
            "participacion_mas_frecuente": "N/E",
            "abordaje_mas_frecuente": "N/E",
            "ultima_actividad": None,
            "ultima_actividad_label": "Sin actividad indexada",
        },
        "distribucion_abordaje": [],
        "distribucion_roles": [],
        "distribucion_participacion": [],
        "ponderacion": {
            "rol": _build_weighted_summary([], "role", ROLE_WEIGHT_MAP, "Ponderacion por rol"),
            "participacion": _build_weighted_summary([], "participacion_tecnica", PARTICIPATION_WEIGHT_MAP, "Ponderacion por participacion"),
        },
        "curva_aprendizaje": [],
        "sangrado_por_procedimiento": [],
        "charts": {
            "abordajes": {"labels": [], "values": []},
            "roles": {"labels": [], "values": []},
            "participacion": {"labels": [], "values": []},
            "sangrado": {"labels": [], "resident": [], "service": []},
        },
        "longitudinal": {
            "activity_30_days": 0,
            "activity_90_days": 0,
            "procedures_90_days": 0,
            "last_indexed_surgery": None,
        },
        "academico_no_clinico": {
            "publicaciones": [],
            "congresos": [],
            "premios": [],
        },
        "evaluaciones_semestrales": [],
        "casos_asociados": [],
        "incidencias": [],
        "editor": {
            "publicaciones": [{"titulo": "", "revista_o_medio": "", "anio": "", "estatus": "", "notas": ""}],
            "congresos": [{"nombre_evento": "", "sede": "", "fecha": "", "rol": "", "notas": ""}],
            "premios": [{"nombre": "", "institucion": "", "fecha": "", "descripcion": ""}],
        },
        "counts": {
            "publicaciones": 0,
            "congresos": 0,
            "premios": 0,
            "evaluaciones": 0,
            "casos": 0,
            "incidencias": 0,
        },
        "meta": {
            "procedimientos_con_meta": 0,
            "procedimientos_activos": 0,
        },
        "form_options": {
            "sexo": PROFILE_SEXO_OPTIONS,
        },
    }


def build_resident_card_summaries(db: Session) -> Dict[str, Dict[str, Any]]:
    if not _resident_analytics_schema_ready(db):
        return {}

    base_rows = (
        db.query(
            m.SurgeryEventParticipant.resident_code.label("resident_code"),
            func.max(m.SurgeryEventParticipant.resident_name).label("resident_name"),
            func.count(func.distinct(m.SurgeryEventIndex.id)).label("total_cirugias"),
            func.count(func.distinct(m.SurgeryEventIndex.procedure_name)).label("procedimientos"),
            func.max(m.SurgeryEventIndex.event_date).label("ultima_fecha"),
        )
        .join(m.SurgeryEventIndex, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .group_by(m.SurgeryEventParticipant.resident_code)
        .all()
    )

    approach_rows = (
        db.query(
            m.SurgeryEventParticipant.resident_code.label("resident_code"),
            m.SurgeryEventIndex.approach.label("approach"),
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventIndex, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .group_by(m.SurgeryEventParticipant.resident_code, m.SurgeryEventIndex.approach)
        .all()
    )
    approach_map: Dict[str, tuple[str, int]] = {}
    for row in approach_rows:
        resident_code = str(row.resident_code or "").strip()
        label = str(row.approach or "N/E")
        cantidad = int(row.cantidad or 0)
        current = approach_map.get(resident_code)
        if current is None or cantidad > current[1] or (cantidad == current[1] and label < current[0]):
            approach_map[resident_code] = (label, cantidad)

    summaries: Dict[str, Dict[str, Any]] = {}
    for row in base_rows:
        resident_code = str(row.resident_code or "").strip()
        summaries[resident_code] = {
            "resident_name": str(row.resident_name or ""),
            "total_cirugias": int(row.total_cirugias or 0),
            "procedimientos": int(row.procedimientos or 0),
            "ultima_fecha": row.ultima_fecha.isoformat() if getattr(row, "ultima_fecha", None) else None,
            "abordaje_dominante": (approach_map.get(resident_code) or ("N/E", 0))[0],
        }
    return summaries


def index_postqx_feedback(db: Session, feedback_row: Any, *, payload_override: Optional[Dict[str, Any]] = None) -> Optional[int]:
    if not _resident_analytics_schema_ready(db):
        logger.warning("Indexacion postqx omitida: tablas analiticas de residentes no disponibles")
        return None

    payload: Dict[str, Any]
    if payload_override is not None:
        payload = dict(payload_override)
    else:
        raw_payload = getattr(feedback_row, "payload", {}) or {}
        if isinstance(raw_payload, str):
            payload = json.loads(raw_payload)
        else:
            payload = dict(raw_payload)

    surgical_programacion_id = _safe_int(payload.get("surgical_programacion_id"))
    if not surgical_programacion_id:
        logger.warning("Indexacion postqx omitida: surgical_programacion_id ausente")
        return None

    event = db.query(m.SurgeryEventIndex).filter(m.SurgeryEventIndex.surgical_programacion_id == surgical_programacion_id).first()
    if event is None:
        event = m.SurgeryEventIndex(surgical_programacion_id=surgical_programacion_id)
        db.add(event)
        db.flush()

    event.feedback_id = getattr(feedback_row, "id", None)
    event.postquirurgica_id = _safe_int(payload.get("postquirurgica_id"))
    event.event_date = _safe_date(payload.get("fecha_realizacion"))
    event.patient_name = _safe_text(payload.get("paciente_nombre"))
    event.patient_nss = _safe_text(payload.get("paciente_nss"))
    event.patient_age = _safe_int(payload.get("paciente_edad"))
    event.patient_sex = _safe_text(payload.get("paciente_sexo"))
    event.dx = _safe_text(payload.get("diagnostico_postop") or payload.get("diagnostico") or payload.get("patologia"))
    event.procedure_name = _safe_text(payload.get("procedimiento_realizado") or payload.get("procedimiento") or payload.get("procedimiento_programado"))
    event.approach = _safe_text(payload.get("tipo_abordaje"))
    event.blood_loss_ml = _safe_int(payload.get("sangrado_ml"))
    event.blood_loss_allowed_ml = _safe_int(payload.get("sangrado_permisible_ml"))

    db.flush()
    db.query(m.SurgeryEventParticipant).filter(m.SurgeryEventParticipant.event_id == event.id).delete(synchronize_session=False)

    resident_team = payload.get("resident_team") or {}
    for grade in GRADES:
        item = resident_team.get(grade) or {}
        resident_code = str(item.get("residente") or "NA").strip()
        if resident_code.upper() == "NA":
            continue
        db.add(
            m.SurgeryEventParticipant(
                event_id=event.id,
                grade=grade,
                resident_code=resident_code,
                resident_name=_safe_text(item.get("residente_nombre")),
                role=_safe_text(item.get("rol")) or "NA",
                participacion_tecnica=_safe_text(item.get("participacion")) or "NA",
            )
        )

    db.commit()
    return event.id


def obtener_datos_perfil_residente(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip()
    if not code or not _resident_analytics_schema_ready(db):
        return {
            "kpis": None,
            "distribucion_abordaje": [],
            "demografia": [],
            "curva_aprendizaje": [],
        }

    kpis = (
        db.query(
            func.count(func.distinct(m.SurgeryEventIndex.id)).label("total_cirugias"),
            func.avg(m.SurgeryEventIndex.blood_loss_ml).label("sangrado_promedio"),
            func.max(m.SurgeryEventIndex.event_date).label("ultima_actividad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .first()
    )

    dist_abordaje = (
        db.query(
            m.SurgeryEventIndex.approach,
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.approach)
        .all()
    )

    demografia = (
        db.query(
            m.SurgeryEventIndex.patient_sex,
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.patient_sex)
        .all()
    )

    procedimientos_realizados = (
        db.query(
            m.SurgeryEventIndex.procedure_name,
            func.count(m.SurgeryEventIndex.id).label("realizados"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.procedure_name)
        .all()
    )

    curva_aprendizaje: List[Dict[str, Any]] = []
    for proc in procedimientos_realizados:
        nombre_proc = str(proc.procedure_name or "SIN_NOMBRE")
        realizados = int(proc.realizados or 0)
        meta_objetivo = METAS_CURVAS_APRENDIZAJE.get(_normalize_procedure_key(nombre_proc), 10)
        progreso = min(int((realizados / meta_objetivo) * 100), 100) if meta_objetivo else 0
        curva_aprendizaje.append(
            {
                "procedimiento": nombre_proc,
                "meta": meta_objetivo,
                "realizados": realizados,
                "progreso": progreso,
            }
        )

    return {
        "kpis": kpis,
        "distribucion_abordaje": dist_abordaje,
        "demografia": demografia,
        "curva_aprendizaje": curva_aprendizaje,
    }


def obtener_metricas_sangrado_residente(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip()
    if not code or not _resident_analytics_schema_ready(db):
        return {
            "sangrado_promedio_global_residente": None,
            "por_procedimiento": [],
        }

    resident_global = (
        db.query(func.avg(m.SurgeryEventIndex.blood_loss_ml).label("promedio"))
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .filter(m.SurgeryEventIndex.blood_loss_ml.isnot(None))
        .first()
    )

    resident_case_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            func.count(func.distinct(m.SurgeryEventIndex.id)).label("casos_participados"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.procedure_name)
        .all()
    )
    resident_cases = {str(row.procedure_name or "SIN_NOMBRE"): int(row.casos_participados or 0) for row in resident_case_rows}

    resident_bleeding_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            func.avg(m.SurgeryEventIndex.blood_loss_ml).label("promedio_residente"),
            func.count(func.distinct(m.SurgeryEventIndex.id)).label("casos_con_sangrado_residente"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .filter(m.SurgeryEventIndex.blood_loss_ml.isnot(None))
        .group_by(m.SurgeryEventIndex.procedure_name)
        .all()
    )

    service_bleeding_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            func.avg(m.SurgeryEventIndex.blood_loss_ml).label("promedio_servicio"),
            func.count(func.distinct(m.SurgeryEventIndex.id)).label("casos_servicio"),
        )
        .filter(m.SurgeryEventIndex.blood_loss_ml.isnot(None))
        .group_by(m.SurgeryEventIndex.procedure_name)
        .all()
    )
    service_map = {
        str(row.procedure_name or "SIN_NOMBRE"): {
            "promedio_servicio": _safe_avg(row.promedio_servicio),
            "casos_servicio": int(row.casos_servicio or 0),
        }
        for row in service_bleeding_rows
    }

    approach_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            m.SurgeryEventIndex.approach.label("approach"),
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.procedure_name, m.SurgeryEventIndex.approach)
        .all()
    )
    role_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            m.SurgeryEventParticipant.role.label("role"),
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.procedure_name, m.SurgeryEventParticipant.role)
        .all()
    )
    participation_rows = (
        db.query(
            m.SurgeryEventIndex.procedure_name.label("procedure_name"),
            m.SurgeryEventParticipant.participacion_tecnica.label("participacion_tecnica"),
            func.count(m.SurgeryEventIndex.id).label("cantidad"),
        )
        .join(m.SurgeryEventParticipant, m.SurgeryEventParticipant.event_id == m.SurgeryEventIndex.id)
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventIndex.procedure_name, m.SurgeryEventParticipant.participacion_tecnica)
        .all()
    )

    dominant_approach: Dict[str, tuple[str, int]] = {}
    dominant_role: Dict[str, tuple[str, int]] = {}
    dominant_participation: Dict[str, tuple[str, int]] = {}
    for row in approach_rows:
        proc = str(row.procedure_name or "SIN_NOMBRE")
        item = (str(row.approach or "N/E"), int(row.cantidad or 0))
        current = dominant_approach.get(proc)
        if current is None or item[1] > current[1] or (item[1] == current[1] and item[0] < current[0]):
            dominant_approach[proc] = item
    for row in role_rows:
        proc = str(row.procedure_name or "SIN_NOMBRE")
        item = (str(row.role or "N/E"), int(row.cantidad or 0))
        current = dominant_role.get(proc)
        if current is None or item[1] > current[1] or (item[1] == current[1] and item[0] < current[0]):
            dominant_role[proc] = item
    for row in participation_rows:
        proc = str(row.procedure_name or "SIN_NOMBRE")
        item = (str(row.participacion_tecnica or "N/E"), int(row.cantidad or 0))
        current = dominant_participation.get(proc)
        if current is None or item[1] > current[1] or (item[1] == current[1] and item[0] < current[0]):
            dominant_participation[proc] = item

    metrics: List[Dict[str, Any]] = []
    resident_bleeding_map = {str(row.procedure_name or "SIN_NOMBRE"): row for row in resident_bleeding_rows}
    all_procedures = sorted(set(resident_cases) | set(service_map) | set(resident_bleeding_map))

    for procedure_name in all_procedures:
        resident_row = resident_bleeding_map.get(procedure_name)
        resident_avg = _safe_avg(getattr(resident_row, "promedio_residente", None)) if resident_row is not None else None
        resident_blood_cases = int(getattr(resident_row, "casos_con_sangrado_residente", 0) or 0) if resident_row is not None else 0
        total_cases = int(resident_cases.get(procedure_name, 0) or 0)
        service_info = service_map.get(procedure_name, {})
        service_avg = service_info.get("promedio_servicio")
        service_cases = int(service_info.get("casos_servicio") or 0)
        desviacion_ml = round((resident_avg or 0) - (service_avg or 0), 1) if resident_avg is not None and service_avg is not None else None
        desviacion_pct = round(((resident_avg - service_avg) / service_avg) * 100.0, 1) if resident_avg is not None and service_avg not in (None, 0) else None
        muestra_suficiente = resident_blood_cases >= 3 and service_cases >= 3
        if not muestra_suficiente:
            semaforo = "gris"
        elif resident_avg is not None and service_avg is not None and resident_avg <= (service_avg * 1.10):
            semaforo = "azul"
        else:
            semaforo = "rojo"
        metrics.append(
            {
                "procedimiento": procedure_name,
                "casos_participados": total_cases,
                "casos_con_sangrado_residente": resident_blood_cases,
                "casos_con_sangrado_servicio": service_cases,
                "sangrado_promedio_residente": resident_avg,
                "sangrado_promedio_servicio": service_avg,
                "desviacion_ml": desviacion_ml,
                "desviacion_pct": desviacion_pct,
                "abordaje_mas_frecuente": (dominant_approach.get(procedure_name) or ("N/E", 0))[0],
                "rol_mas_frecuente": (dominant_role.get(procedure_name) or ("N/E", 0))[0],
                "participacion_mas_frecuente": (dominant_participation.get(procedure_name) or ("N/E", 0))[0],
                "muestra_suficiente": muestra_suficiente,
                "semaforo": semaforo,
            }
        )

    metrics.sort(key=lambda item: (-int(item["casos_participados"] or 0), str(item["procedimiento"])))
    return {
        "sangrado_promedio_global_residente": _safe_avg(getattr(resident_global, "promedio", None)),
        "por_procedimiento": metrics,
    }


def build_resident_profile_viewmodel(db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    viewmodel = _empty_profile_viewmodel(db, code)
    if not code:
        return viewmodel

    academic_data = _build_academic_profile_data(db, code)
    viewmodel["academico_no_clinico"] = academic_data["academico_no_clinico"]
    viewmodel["evaluaciones_semestrales"] = academic_data["evaluaciones_semestrales"]
    viewmodel["casos_asociados"] = academic_data["casos_asociados"]
    viewmodel["incidencias"] = academic_data["incidencias"]
    viewmodel["editor"] = academic_data["editor"]
    viewmodel["counts"] = academic_data["counts"]
    viewmodel["longitudinal"] = _build_longitudinal_summary(db, code)

    analytics_ready = _resident_analytics_schema_ready(db)
    if not analytics_ready:
        return viewmodel

    base_data = obtener_datos_perfil_residente(db, code)
    bleed_data = obtener_metricas_sangrado_residente(db, code)

    kpi_row = base_data.get("kpis")
    approach_dist = _format_distribution(base_data.get("distribucion_abordaje") or [], "approach")
    role_rows = (
        db.query(
            m.SurgeryEventParticipant.role.label("role"),
            func.count(m.SurgeryEventParticipant.id).label("cantidad"),
        )
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventParticipant.role)
        .all()
    )
    participation_rows = (
        db.query(
            m.SurgeryEventParticipant.participacion_tecnica.label("participacion_tecnica"),
            func.count(m.SurgeryEventParticipant.id).label("cantidad"),
        )
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .group_by(m.SurgeryEventParticipant.participacion_tecnica)
        .all()
    )

    role_dist = _format_distribution(role_rows, "role")
    participation_dist = _format_distribution(participation_rows, "participacion_tecnica")
    role_weighted = _build_weighted_summary(role_rows, "role", ROLE_WEIGHT_MAP, "Ponderacion por rol")
    participation_weighted = _build_weighted_summary(participation_rows, "participacion_tecnica", PARTICIPATION_WEIGHT_MAP, "Ponderacion por participacion")
    dominant_approach = approach_dist[0]["label"] if approach_dist else "N/E"
    dominant_role = role_dist[0]["label"] if role_dist else "N/E"
    dominant_participation = participation_dist[0]["label"] if participation_dist else "N/E"

    total_cirugias = int(getattr(kpi_row, "total_cirugias", 0) or 0) if kpi_row else 0
    procedimientos_distintos = len(base_data.get("curva_aprendizaje") or [])
    abordajes_distintos = len(approach_dist)
    ultima_actividad = getattr(kpi_row, "ultima_actividad", None) if kpi_row else None

    curva_aprendizaje: List[Dict[str, Any]] = []
    for item in sorted(base_data.get("curva_aprendizaje") or [], key=lambda row: (-int(row.get("realizados") or 0), str(row.get("procedimiento") or ""))):
        meta = int(item.get("meta") or 0)
        realizados = int(item.get("realizados") or 0)
        curva_aprendizaje.append(
            {
                **item,
                "faltantes": max(meta - realizados, 0),
                "progreso": min(int(item.get("progreso") or 0), 100),
            }
        )

    has_activity = total_cirugias > 0
    viewmodel["has_activity"] = has_activity
    viewmodel["empty_state"] = "Sin eventos indexados" if not has_activity else ""
    viewmodel["kpis"] = {
        "total_cirugias": total_cirugias,
        "procedimientos_distintos": procedimientos_distintos,
        "abordajes_distintos": abordajes_distintos,
        "sangrado_promedio_global_residente": bleed_data.get("sangrado_promedio_global_residente"),
        "rol_mas_frecuente": dominant_role,
        "participacion_mas_frecuente": dominant_participation,
        "abordaje_mas_frecuente": dominant_approach,
        "ultima_actividad": _format_date_iso(ultima_actividad),
        "ultima_actividad_label": _format_date_short_es(ultima_actividad) if isinstance(ultima_actividad, date) else "Sin actividad indexada",
    }
    viewmodel["distribucion_abordaje"] = approach_dist
    viewmodel["distribucion_roles"] = role_dist
    viewmodel["distribucion_participacion"] = participation_dist
    viewmodel["ponderacion"] = {
        "rol": role_weighted,
        "participacion": participation_weighted,
    }
    viewmodel["curva_aprendizaje"] = curva_aprendizaje
    viewmodel["sangrado_por_procedimiento"] = bleed_data.get("por_procedimiento") or []
    viewmodel["charts"] = {
        "abordajes": {
            "labels": [row["label"] for row in approach_dist],
            "values": [row["cantidad"] for row in approach_dist],
        },
        "roles": {
            "labels": [row["label"] for row in role_dist],
            "values": [row["cantidad"] for row in role_dist],
        },
        "participacion": {
            "labels": [row["label"] for row in participation_dist],
            "values": [row["cantidad"] for row in participation_dist],
        },
        "sangrado": {
            "labels": [row["procedimiento"] for row in viewmodel["sangrado_por_procedimiento"]],
            "resident": [row["sangrado_promedio_residente"] for row in viewmodel["sangrado_por_procedimiento"]],
            "service": [row["sangrado_promedio_servicio"] for row in viewmodel["sangrado_por_procedimiento"]],
        },
    }
    viewmodel["meta"] = {
        "procedimientos_con_meta": sum(1 for row in curva_aprendizaje if int(row.get("meta") or 0) > 0),
        "procedimientos_activos": len(curva_aprendizaje),
    }
    return viewmodel


def _parse_publication_form_rows(form: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    titles = form.getlist("publications_title[]")
    media = form.getlist("publications_media[]")
    years = form.getlist("publications_year[]")
    statuses = form.getlist("publications_status[]")
    notes = form.getlist("publications_notes[]")
    row_count = max(len(titles), len(media), len(years), len(statuses), len(notes), 1)
    rows: List[Dict[str, Any]] = []
    for index in range(row_count):
        title = _normalize_text(titles[index] if index < len(titles) else "", max_len=240)
        medium = _normalize_text(media[index] if index < len(media) else "", max_len=180)
        year_raw = years[index] if index < len(years) else ""
        status = _normalize_text(statuses[index] if index < len(statuses) else "", max_len=80)
        note = _normalize_text(notes[index] if index < len(notes) else "", max_len=2000)
        if not any([title, medium, str(year_raw or "").strip(), status, note]):
            continue
        year = _safe_int(year_raw)
        if year_raw not in (None, "") and year is None:
            return [], f"La publicación {index + 1} tiene un año inválido."
        if not title:
            return [], f"La publicación {index + 1} requiere título."
        rows.append(
            {
                "titulo": title,
                "revista_o_medio": medium,
                "anio": year,
                "estatus": status,
                "notas": note,
            }
        )
    return rows, None


def _parse_congress_form_rows(form: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    events = form.getlist("congresses_event[]")
    locations = form.getlist("congresses_location[]")
    dates = form.getlist("congresses_date[]")
    roles = form.getlist("congresses_role[]")
    notes = form.getlist("congresses_notes[]")
    row_count = max(len(events), len(locations), len(dates), len(roles), len(notes), 1)
    rows: List[Dict[str, Any]] = []
    for index in range(row_count):
        event = _normalize_text(events[index] if index < len(events) else "", max_len=220)
        location = _normalize_text(locations[index] if index < len(locations) else "", max_len=180)
        congress_date_raw = dates[index] if index < len(dates) else ""
        congress_date = _safe_date(congress_date_raw)
        role = _normalize_text(roles[index] if index < len(roles) else "", max_len=120)
        note = _normalize_text(notes[index] if index < len(notes) else "", max_len=2000)
        if not any([event, location, str(congress_date_raw or "").strip(), role, note]):
            continue
        if congress_date_raw not in (None, "") and congress_date is None:
            return [], f"El congreso {index + 1} tiene una fecha inválida."
        if not event:
            return [], f"El congreso {index + 1} requiere nombre del evento."
        rows.append(
            {
                "nombre_evento": event,
                "sede": location,
                "fecha": congress_date,
                "rol": role,
                "notas": note,
            }
        )
    return rows, None


def _parse_award_form_rows(form: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    names = form.getlist("awards_name[]")
    institutions = form.getlist("awards_institution[]")
    dates = form.getlist("awards_date[]")
    descriptions = form.getlist("awards_description[]")
    row_count = max(len(names), len(institutions), len(dates), len(descriptions), 1)
    rows: List[Dict[str, Any]] = []
    for index in range(row_count):
        name = _normalize_text(names[index] if index < len(names) else "", max_len=220)
        institution = _normalize_text(institutions[index] if index < len(institutions) else "", max_len=180)
        award_date_raw = dates[index] if index < len(dates) else ""
        award_date = _safe_date(award_date_raw)
        description = _normalize_text(descriptions[index] if index < len(descriptions) else "", max_len=2000)
        if not any([name, institution, str(award_date_raw or "").strip(), description]):
            continue
        if award_date_raw not in (None, "") and award_date is None:
            return [], f"El premio {index + 1} tiene una fecha inválida."
        if not name:
            return [], f"El premio {index + 1} requiere nombre."
        rows.append(
            {
                "nombre": name,
                "institucion": institution,
                "fecha": award_date,
                "descripcion": description,
            }
        )
    return rows, None


async def update_resident_profile_from_request(request: Any, db: Session, resident_code: str) -> Dict[str, Any]:
    code = str(resident_code or "").strip().upper()
    if not code:
        return {"ok": False, "error": "Codigo de residente invalido."}

    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)

    nombre = _normalize_text(form.get("nombre"), max_len=200)
    sexo = _normalize_sexo(form.get("sexo"))
    universidad = _normalize_text(form.get("universidad"), max_len=180)
    sede_r1 = _normalize_text(form.get("sede_r1"), max_len=180)
    inscripcion_unam = _normalize_text(form.get("inscripcion_unam"), max_len=120)
    publications, publications_error = _parse_publication_form_rows(form)
    if publications_error:
        return {"ok": False, "error": publications_error}
    congresses, congresses_error = _parse_congress_form_rows(form)
    if congresses_error:
        return {"ok": False, "error": congresses_error}
    awards, awards_error = _parse_award_form_rows(form)
    if awards_error:
        return {"ok": False, "error": awards_error}
    photo_upload = form.get("photo")

    photo_bytes: Optional[bytes] = None
    photo_filename: Optional[str] = None
    photo_ext: Optional[str] = None
    photo_mime: Optional[str] = None

    if getattr(photo_upload, "filename", ""):
        photo_filename = safe_filename(str(photo_upload.filename or "foto"))
        photo_ext = extract_extension(photo_filename)
        allowed_exts = set(getattr(m, "ALLOWED_RESIDENT_PROFILE_PHOTO_EXTENSIONS", {".png", ".jpg", ".jpeg", ".webp"}))
        if photo_ext not in allowed_exts:
            try:
                await photo_upload.close()
            except Exception:
                pass
            return {"ok": False, "error": "La foto debe ser JPG, PNG o WEBP."}
        photo_bytes = await photo_upload.read()
        try:
            await photo_upload.close()
        except Exception:
            pass
        if not photo_bytes:
            return {"ok": False, "error": "La foto seleccionada esta vacia."}
        max_size_mb = int(getattr(m, "MAX_RESIDENT_PROFILE_PHOTO_SIZE_MB", 5) or 5)
        max_size_bytes = max_size_mb * 1024 * 1024
        if len(photo_bytes) > max_size_bytes:
            return {"ok": False, "error": f"La foto excede el limite de {max_size_mb} MB."}
        photo_mime = detect_mime(photo_upload, photo_ext)
        if not str(photo_mime or "").lower().startswith("image/"):
            return {"ok": False, "error": "El archivo seleccionado no es una imagen valida."}

    profile_row = _get_resident_profile_row(db, code)
    if profile_row is None and any([nombre, sexo, universidad, sede_r1, inscripcion_unam, photo_bytes, publications, congresses, awards]):
        profile_row = m.ResidentProfileDB(resident_code=code)
        db.add(profile_row)
        db.flush()

    if profile_row is None:
        return {"ok": True}

    old_photo_path = str(getattr(profile_row, "photo_storage_path", "") or "")
    new_photo_path: Optional[str] = None

    try:
        profile_row.nombre = nombre
        profile_row.sexo = sexo
        profile_row.universidad = universidad
        profile_row.sede_r1 = sede_r1
        profile_row.inscripcion_unam = inscripcion_unam

        db.query(m.ResidentPublicationDB).filter(_resident_code_filter(m.ResidentPublicationDB.resident_code, code)).delete(synchronize_session=False)
        db.query(m.ResidentCongressDB).filter(_resident_code_filter(m.ResidentCongressDB.resident_code, code)).delete(synchronize_session=False)
        db.query(m.ResidentAwardDB).filter(_resident_code_filter(m.ResidentAwardDB.resident_code, code)).delete(synchronize_session=False)

        for publication in publications:
            db.add(
                m.ResidentPublicationDB(
                    resident_code=code,
                    titulo=publication["titulo"],
                    revista_o_medio=publication.get("revista_o_medio"),
                    anio=publication.get("anio"),
                    estatus=publication.get("estatus"),
                    notas=publication.get("notas"),
                )
            )
        for congress in congresses:
            db.add(
                m.ResidentCongressDB(
                    resident_code=code,
                    nombre_evento=congress["nombre_evento"],
                    sede=congress.get("sede"),
                    fecha=congress.get("fecha"),
                    rol=congress.get("rol"),
                    notas=congress.get("notas"),
                )
            )
        for award in awards:
            db.add(
                m.ResidentAwardDB(
                    resident_code=code,
                    nombre=award["nombre"],
                    institucion=award.get("institucion"),
                    fecha=award.get("fecha"),
                    descripcion=award.get("descripcion"),
                )
            )

        if photo_bytes is not None and photo_ext is not None and photo_filename is not None:
            ensure_resident_profile_photos_dir()
            stored_name = f"{_slugify(code)}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(6)}{photo_ext}"
            new_photo_path = os.path.join(_resident_profile_photos_dir(), stored_name)
            with open(new_photo_path, "wb") as file_obj:
                file_obj.write(photo_bytes)
            profile_row.photo_storage_path = new_photo_path
            profile_row.photo_original_name = photo_filename
            profile_row.photo_mime_type = photo_mime or mimetypes.guess_type(photo_filename)[0] or "image/jpeg"
            profile_row.photo_updated_at = datetime.utcnow()

        db.commit()
    except Exception:
        db.rollback()
        if new_photo_path and os.path.isfile(new_photo_path):
            try:
                os.remove(new_photo_path)
            except OSError:
                pass
        logger.exception("No fue posible actualizar el perfil del residente %s", code)
        return {"ok": False, "error": "No fue posible guardar el perfil del residente."}

    if new_photo_path and old_photo_path and old_photo_path != new_photo_path:
        try:
            if os.path.isfile(old_photo_path):
                os.remove(old_photo_path)
        except OSError:
            logger.warning("No fue posible borrar la foto previa del residente %s", code)

    return {"ok": True}


def resident_profile_photo_response(db: Session, resident_code: str) -> Response:
    code = str(resident_code or "").strip().upper()
    profile_row = _get_resident_profile_row(db, code)
    if profile_row is not None:
        storage_path = str(getattr(profile_row, "photo_storage_path", "") or "")
        if storage_path and os.path.isfile(storage_path):
            media_type = getattr(profile_row, "photo_mime_type", None) or mimetypes.guess_type(storage_path)[0] or "image/jpeg"
            filename = getattr(profile_row, "photo_original_name", None) or f"{code}.jpg"
            return FileResponse(path=storage_path, media_type=media_type, filename=filename)

    display_name = build_resident_profile_viewmodel(db, code)["resident"]["display_name"]
    return Response(content=_build_placeholder_svg(display_name), media_type="image/svg+xml")
