from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import func, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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


def _programa_operativo_js_path() -> Path:
    return Path(__file__).resolve().parents[1] / "static" / "js" / "jefatura_programa_operativo.js"


def _slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or ""))
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_text = re.sub(r"[^A-Z0-9]+", "_", ascii_text.upper()).strip("_")
    return ascii_text or "NA"


@lru_cache(maxsize=1)
def load_resident_catalog() -> Dict[str, List[Dict[str, str]]]:
    catalog: Dict[str, List[Dict[str, str]]] = {grade: [] for grade in GRADES}
    js_path = _programa_operativo_js_path()
    if not js_path.exists():
        return catalog

    raw = js_path.read_text(encoding="utf-8")
    match = re.search(r"let\s+SCHEDULE_ROWS\s*=\s*(\[[\s\S]*?\]);", raw)
    if not match:
        return catalog

    try:
        rows = json.loads(match.group(1))
    except Exception:
        logger.exception("No fue posible parsear SCHEDULE_ROWS para catálogo de residentes")
        return catalog

    seen: set[tuple[str, str]] = set()
    for row in rows:
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
        logger.exception("No fue posible inspeccionar el esquema analítico de residentes")
        return False
    return {"surgery_event_index", "surgery_event_participants"}.issubset(tables)


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


def _empty_profile_viewmodel(resident_code: str) -> Dict[str, Any]:
    lookup = resident_lookup()
    resident = lookup.get(str(resident_code or "").strip()) or lookup.get(str(resident_code or "").strip().upper()) or {}
    code = str(resident.get("code") or resident_code or "").strip()
    grade = str(resident.get("grade") or _default_grade_from_code(code))
    name = str(resident.get("name") or code or "Residente sin identificar")
    return {
        "resident": {
            "code": code or "NA",
            "name": name,
            "grade": grade,
            "display_name": name,
        },
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
        },
        "distribucion_abordaje": [],
        "distribucion_roles": [],
        "distribucion_participacion": [],
        "curva_aprendizaje": [],
        "sangrado_por_procedimiento": [],
        "charts": {
            "abordajes": {"labels": [], "values": []},
            "roles": {"labels": [], "values": []},
            "participacion": {"labels": [], "values": []},
            "sangrado": {"labels": [], "resident": [], "service": []},
        },
        "meta": {
            "procedimientos_con_meta": 0,
            "procedimientos_activos": 0,
        },
    }


def build_resident_card_summaries(db: Session) -> Dict[str, Dict[str, Any]]:
    from app.core.app_context import main_proxy as m

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
    from app.core.app_context import main_proxy as m

    if not _resident_analytics_schema_ready(db):
        logger.warning("Indexación postqx omitida: tablas analíticas de residentes no disponibles")
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
        logger.warning("Indexación postqx omitida: surgical_programacion_id ausente")
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
    from app.core.app_context import main_proxy as m

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
    from app.core.app_context import main_proxy as m

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
        desviacion_pct = (
            round(((resident_avg - service_avg) / service_avg) * 100.0, 1)
            if resident_avg is not None and service_avg not in (None, 0)
            else None
        )
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
    from app.core.app_context import main_proxy as m

    code = str(resident_code or "").strip()
    if not code or not _resident_analytics_schema_ready(db):
        return _empty_profile_viewmodel(code)

    base_data = obtener_datos_perfil_residente(db, code)
    bleed_data = obtener_metricas_sangrado_residente(db, code)
    lookup = resident_lookup()
    resident = lookup.get(code) or lookup.get(code.upper()) or {}

    identity_row = (
        db.query(
            func.max(m.SurgeryEventParticipant.resident_name).label("resident_name"),
            func.max(m.SurgeryEventParticipant.grade).label("grade"),
        )
        .filter(_resident_code_filter(m.SurgeryEventParticipant.resident_code, code))
        .first()
    )
    display_name = str(resident.get("name") or getattr(identity_row, "resident_name", None) or code or "Residente sin identificar")
    grade = str(resident.get("grade") or getattr(identity_row, "grade", None) or _default_grade_from_code(code))

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
    dominant_approach = (approach_dist[0]["label"] if approach_dist else "N/E")
    dominant_role = (role_dist[0]["label"] if role_dist else "N/E")
    dominant_participation = (participation_dist[0]["label"] if participation_dist else "N/E")

    total_cirugias = int(getattr(kpi_row, "total_cirugias", 0) or 0) if kpi_row else 0
    procedimientos_distintos = len(base_data.get("curva_aprendizaje") or [])
    abordajes_distintos = len(approach_dist)
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
    viewmodel = _empty_profile_viewmodel(code)
    viewmodel["resident"] = {
        "code": code,
        "name": display_name,
        "grade": grade,
        "display_name": display_name,
    }
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
    }
    viewmodel["distribucion_abordaje"] = approach_dist
    viewmodel["distribucion_roles"] = role_dist
    viewmodel["distribucion_participacion"] = participation_dist
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
