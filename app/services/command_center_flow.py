"""
Patient Command Center Flow — Vista de Mando del Paciente.

ADITIVO: No modifica ninguna lógica existente.
Inspirado en Epic Synopsis + SMART on FHIR.
Integra datos de consulta, hospitalización, quirófano, labs, vitales,
dispositivos, alertas, timeline y scores en una sola vista ejecutiva.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from sqlalchemy import and_, desc, text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _safe(val: Any, default: str = "") -> str:
    return str(val).strip() if val else default

def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def _safe_int(val: Any) -> Optional[int]:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

def _load_json(val: Any, default=None):
    if default is None:
        default = []
    if not val:
        return default
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return default

def _serialize(obj):
    """Convierte datetime/date a ISO string recursivamente."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(i) for i in obj]
    return obj


# ---------------------------------------------------------------------------
# Data aggregation helpers
# ---------------------------------------------------------------------------

def _patient_demographics(consulta: Dict) -> Dict[str, Any]:
    """Resumen demográfico del paciente."""
    nombre = _safe(consulta.get("nombre_completo") or
                   f"{_safe(consulta.get('nombre'))} {_safe(consulta.get('apellido_paterno'))} {_safe(consulta.get('apellido_materno'))}".strip())
    return {
        "nombre": nombre,
        "nss": _safe(consulta.get("nss")),
        "edad": _safe_int(consulta.get("edad")),
        "sexo": _safe(consulta.get("sexo")),
        "tipo_sangre": _safe(consulta.get("tipo_sangre")),
        "curp": _safe(consulta.get("curp")),
        "hgz": _safe(consulta.get("hgz_procedencia") or consulta.get("hgz")),
        "telefono": _safe(consulta.get("telefono")),
        "alergias": _safe(consulta.get("alergeno", "Negadas")),
    }


def _diagnosis_summary(consulta: Dict) -> Dict[str, Any]:
    """Diagnóstico principal y secundarios."""
    dx_principal = _safe(consulta.get("diagnostico_nosologico") or
                         consulta.get("diagnostico_cie_principal") or
                         consulta.get("diagnostico"))
    dx_code = _safe(consulta.get("cie11_code") or consulta.get("cie10_principal"))
    secundarios = _load_json(consulta.get("diagnosticos_secundarios_json"), [])
    return {
        "principal": dx_principal,
        "cie_code": dx_code,
        "secundarios": secundarios if isinstance(secundarios, list) else [],
    }


def _treatment_plan(consulta: Dict) -> Dict[str, Any]:
    """Plan de tratamiento actual."""
    return {
        "plan": _safe(consulta.get("plan_tratamiento") or consulta.get("plan")),
        "indicaciones": _safe(consulta.get("indicaciones_medicas")),
        "medicamentos": _load_json(consulta.get("medicamentos_json"), []),
        "proxima_cita": _safe(consulta.get("proxima_cita")),
    }


def _fetch_vitals_latest(db: Session, consulta_id: int) -> Dict[str, Any]:
    """Últimos signos vitales registrados."""
    try:
        from app.models.inpatient_ai_models import VITALS_TS
        row = db.execute(
            sa_text(
                "SELECT * FROM vitals_time_series "
                "WHERE consulta_id = :cid "
                "ORDER BY recorded_at DESC LIMIT 1"
            ),
            {"cid": consulta_id},
        ).mappings().first()
        if row:
            d = dict(row)
            return _serialize(d)
    except Exception:
        pass
    return {}


def _fetch_vitals_trend(db: Session, consulta_id: int, days: int = 7) -> List[Dict]:
    """Tendencia de vitales para sparklines."""
    try:
        cutoff = datetime.now() - timedelta(days=days)
        rows = db.execute(
            sa_text(
                "SELECT recorded_at, sbp, dbp, heart_rate, temperature, spo2 "
                "FROM vitals_time_series "
                "WHERE consulta_id = :cid AND recorded_at >= :cut "
                "ORDER BY recorded_at ASC"
            ),
            {"cid": consulta_id, "cut": cutoff},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _fetch_labs_latest(db: Session, consulta_id: int) -> List[Dict]:
    """Últimos labs con alertas."""
    try:
        rows = db.execute(
            sa_text(
                "SELECT test_name, value_num, unit, reference_range, flag, collected_at "
                "FROM lab_results "
                "WHERE consulta_id = :cid "
                "ORDER BY collected_at DESC LIMIT 20"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _fetch_active_devices(db: Session, consulta_id: int) -> List[Dict]:
    """Dispositivos urológicos activos."""
    try:
        rows = db.execute(
            sa_text(
                "SELECT device_type, description, placed_at, expected_removal "
                "FROM urology_devices "
                "WHERE consulta_id = :cid AND removed_at IS NULL "
                "ORDER BY placed_at DESC"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _fetch_active_alerts(db: Session, consulta_id: int) -> List[Dict]:
    """Alertas activas del paciente."""
    try:
        rows = db.execute(
            sa_text(
                "SELECT severity, alert_type, message, created_at "
                "FROM hospitalizacion_alertas "
                "WHERE consulta_id = :cid AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 10"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _fetch_hospitalization(db: Session, consulta_id: int, nss: str) -> Optional[Dict]:
    """Hospitalización activa."""
    try:
        row = db.execute(
            sa_text(
                "SELECT * FROM hospitalizaciones "
                "WHERE (consulta_id = :cid OR nss = :nss) AND estatus = 'ACTIVO' "
                "ORDER BY fecha_ingreso DESC LIMIT 1"
            ),
            {"cid": consulta_id, "nss": nss},
        ).mappings().first()
        if row:
            d = _serialize(dict(row))
            # Calculate days
            fi = d.get("fecha_ingreso")
            if fi:
                try:
                    fi_date = datetime.fromisoformat(str(fi)).date() if isinstance(fi, str) else fi
                    d["dias_hospitalizacion"] = (date.today() - fi_date).days
                except Exception:
                    d["dias_hospitalizacion"] = None
            return d
    except Exception:
        pass
    return None


def _fetch_surgical_history(db: Session, consulta_id: int) -> List[Dict]:
    """Historial quirúrgico."""
    try:
        rows = db.execute(
            sa_text(
                "SELECT procedimiento_programado, fecha_programacion, estatus, "
                "sangrado_estimado, tiempo_quirurgico_min, cirujano_principal "
                "FROM surgical_programaciones "
                "WHERE consulta_id = :cid "
                "ORDER BY fecha_programacion DESC LIMIT 10"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _fetch_daily_notes(db: Session, consulta_id: int) -> List[Dict]:
    """Últimas notas diarias."""
    try:
        rows = db.execute(
            sa_text(
                "SELECT note_date, subjective, objective, assessment, plan, author "
                "FROM inpatient_daily_notes "
                "WHERE consulta_id = :cid "
                "ORDER BY note_date DESC LIMIT 5"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        return [_serialize(dict(r)) for r in rows]
    except Exception:
        return []


def _build_clinical_timeline(db: Session, consulta_id: int, nss: str) -> List[Dict]:
    """Timeline clínico unificado - eventos de todas las fuentes."""
    events = []

    # Consultas
    try:
        rows = db.execute(
            sa_text(
                "SELECT id, fecha_consulta, diagnostico_nosologico, tipo_consulta "
                "FROM consultas WHERE nss = :nss ORDER BY fecha_consulta DESC LIMIT 10"
            ),
            {"nss": nss},
        ).mappings().all()
        for r in rows:
            d = dict(r)
            events.append({
                "date": str(d.get("fecha_consulta", "")),
                "type": "consulta",
                "icon": "📋",
                "title": f"Consulta {_safe(d.get('tipo_consulta', ''))}",
                "detail": _safe(d.get("diagnostico_nosologico")),
            })
    except Exception:
        pass

    # Cirugías
    try:
        rows = db.execute(
            sa_text(
                "SELECT fecha_programacion, procedimiento_programado, estatus "
                "FROM surgical_programaciones WHERE consulta_id = :cid "
                "ORDER BY fecha_programacion DESC LIMIT 10"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        for r in rows:
            d = dict(r)
            events.append({
                "date": str(d.get("fecha_programacion", "")),
                "type": "cirugia",
                "icon": "🔪",
                "title": f"Cirugía: {_safe(d.get('procedimiento_programado'))}",
                "detail": f"Estatus: {_safe(d.get('estatus'))}",
            })
    except Exception:
        pass

    # Hospitalizaciones
    try:
        rows = db.execute(
            sa_text(
                "SELECT fecha_ingreso, diagnostico_ingreso, estatus, motivo_alta "
                "FROM hospitalizaciones WHERE nss = :nss "
                "ORDER BY fecha_ingreso DESC LIMIT 10"
            ),
            {"nss": nss},
        ).mappings().all()
        for r in rows:
            d = dict(r)
            events.append({
                "date": str(d.get("fecha_ingreso", "")),
                "type": "hospitalizacion",
                "icon": "🏥",
                "title": f"Ingreso hospitalario",
                "detail": _safe(d.get("diagnostico_ingreso")),
            })
    except Exception:
        pass

    # Sort by date descending
    events.sort(key=lambda x: x.get("date", ""), reverse=True)
    return events[:20]


def _compute_risk_profile(consulta: Dict, hosp: Optional[Dict], labs: List[Dict]) -> Dict[str, Any]:
    """Calcula perfil de riesgo del paciente."""
    risks = []
    score = 0

    edad = _safe_int(consulta.get("edad"))
    if edad and edad >= 70:
        risks.append({"label": "Edad ≥70 años", "level": "alto"})
        score += 2
    elif edad and edad >= 60:
        risks.append({"label": "Edad 60-69 años", "level": "moderado"})
        score += 1

    # Comorbidities
    app = _load_json(consulta.get("app_patologias_json"), [])
    dm = any("diabet" in str(p).lower() for p in app) if app else False
    hta = any("hipertens" in str(p).lower() for p in app) if app else False
    if dm:
        risks.append({"label": "Diabetes Mellitus", "level": "moderado"})
        score += 1
    if hta:
        risks.append({"label": "Hipertensión Arterial", "level": "moderado"})
        score += 1

    # Lab risks
    for lab in labs:
        name = _safe(lab.get("test_name", "")).lower()
        val = _safe_float(lab.get("value_num"))
        if val is None:
            continue
        if "creat" in name and val > 1.5:
            risks.append({"label": f"Creatinina elevada: {val}", "level": "alto"})
            score += 2
        elif "hemoglobin" in name or name == "hb":
            if val < 8:
                risks.append({"label": f"Anemia severa Hb: {val}", "level": "critico"})
                score += 3
            elif val < 10:
                risks.append({"label": f"Anemia Hb: {val}", "level": "alto"})
                score += 2

    # Hospitalization days
    if hosp and hosp.get("dias_hospitalizacion") and hosp["dias_hospitalizacion"] > 7:
        risks.append({"label": f"Estancia prolongada: {hosp['dias_hospitalizacion']}d", "level": "moderado"})
        score += 1

    level = "bajo"
    if score >= 5:
        level = "critico"
    elif score >= 3:
        level = "alto"
    elif score >= 1:
        level = "moderado"

    return {"risks": risks, "score": score, "level": level}


def _compute_completeness(consulta: Dict) -> Dict[str, Any]:
    """Calcula completitud del expediente."""
    fields_required = [
        "nombre", "nss", "edad", "sexo", "diagnostico_nosologico",
        "exploracion_fisica", "plan_tratamiento",
    ]
    fields_optional = [
        "tipo_sangre", "curp", "app_patologias_json",
        "cie11_code", "indicaciones_medicas",
    ]
    filled_req = sum(1 for f in fields_required if _safe(consulta.get(f)))
    filled_opt = sum(1 for f in fields_optional if _safe(consulta.get(f)))
    total = len(fields_required) + len(fields_optional)
    filled = filled_req + filled_opt
    pct = round(filled / total * 100) if total else 0

    missing = [f for f in fields_required if not _safe(consulta.get(f))]
    return {"pct": pct, "filled": filled, "total": total, "missing_critical": missing}


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

async def command_center_flow(
    request: Request,
    db: Session,
    consulta_id: int,
) -> HTMLResponse:
    """Renderiza el Patient Command Center."""
    from app.core.app_context import main_proxy as m

    # Load consulta
    try:
        row = db.execute(
            sa_text("SELECT * FROM consultas WHERE id = :cid"),
            {"cid": consulta_id},
        ).mappings().first()
    except Exception:
        row = None

    if not row:
        return m.render_template("command_center.html", request,
                                 error="Consulta no encontrada", consulta_id=consulta_id)

    consulta = _serialize(dict(row))
    nss = _safe(consulta.get("nss"))

    # Gather all data in sequence (could be parallelized in future)
    demographics = _patient_demographics(consulta)
    diagnosis = _diagnosis_summary(consulta)
    treatment = _treatment_plan(consulta)
    vitals_latest = _fetch_vitals_latest(db, consulta_id)
    vitals_trend = _fetch_vitals_trend(db, consulta_id)
    labs = _fetch_labs_latest(db, consulta_id)
    devices = _fetch_active_devices(db, consulta_id)
    alerts = _fetch_active_alerts(db, consulta_id)
    hosp = _fetch_hospitalization(db, consulta_id, nss)
    surgeries = _fetch_surgical_history(db, consulta_id)
    notes = _fetch_daily_notes(db, consulta_id)
    timeline = _build_clinical_timeline(db, consulta_id, nss)
    risk_profile = _compute_risk_profile(consulta, hosp, labs)
    completeness = _compute_completeness(consulta)

    # Enriched data
    enriched = {}
    try:
        from app.services.expediente_plus_flow import EXPEDIENTE_ENRIQUECIDO
        from sqlalchemy import select
        e_row = db.execute(
            select(EXPEDIENTE_ENRIQUECIDO)
            .where(EXPEDIENTE_ENRIQUECIDO.c.consulta_id == consulta_id)
        ).mappings().first()
        if e_row:
            enriched = _serialize(dict(e_row))
    except Exception:
        pass

    return m.render_template(
        "command_center.html",
        request,
        consulta=consulta,
        consulta_id=consulta_id,
        demographics=demographics,
        diagnosis=diagnosis,
        treatment=treatment,
        vitals_latest=vitals_latest,
        vitals_trend=vitals_trend,
        labs=labs,
        devices=devices,
        alerts=alerts,
        hosp=hosp,
        surgeries=surgeries,
        notes=notes,
        timeline=timeline,
        risk_profile=risk_profile,
        completeness=completeness,
        enriched=enriched,
    )
