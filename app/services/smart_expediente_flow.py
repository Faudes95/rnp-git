"""
Smart Expediente Flow — Expediente Inteligente por Patología.

ADITIVO: No modifica ninguna lógica existente.
Proporciona vista de expediente orientada a la patología con:
- Tarjeta de estadificación
- Curva de APE/marcadores con velocidad y tiempo de duplicación
- Scores validados (IPSS, IIEF-5, ICIQ-SF) con barras
- Timeline clínico cronológico
- Resultados postquirúrgicos estructurados
- eGFR calculado (CKD-EPI)
- Recomendaciones IA
"""
from __future__ import annotations

import json
import logging
import math
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import and_, desc, func, select, text as sa_text
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _load_json(val: Any, default: Any = None):
    if default is None:
        default = {}
    if val is None:
        return default
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Pathology profile classification
# ---------------------------------------------------------------------------

PATHOLOGY_PROFILES = {
    "CANCER DE PROSTATA": "ONCO_PROSTATA",
    "CANCER DE RINON": "ONCO_RINON",
    "CANCER DE VEJIGA": "ONCO_VEJIGA",
    "CANCER DE TESTICULO": "ONCO_TESTICULO",
    "CANCER DE PENE": "ONCO_PENE",
    "UTUC": "ONCO_UTUC",
    "TUMOR SUPRARRENAL": "ONCO_SUPRARRENAL",
    "TUMOR DE COMPORTAMIENTO INCIERTO": "ONCO_INCIERTO",
    "LITIASIS": "LITIASIS",
    "HPB": "HPB_STUI",
    "HIPERPLASIA": "HPB_STUI",
    "INFECCION": "INFECCION",
    "INCONTINENCIA": "FUNCIONAL",
    "FISTULA": "FUNCIONAL",
    "PRIAPISMO": "FUNCIONAL",
    "DISFUNCION ERECTIL": "FUNCIONAL",
    "TRASPLANTE": "TRASPLANTE",
}


def _determine_pathology_profile(consulta: Dict) -> str:
    """Clasifica el perfil patológico principal del paciente."""
    dx = _safe(consulta.get("diagnostico", "")).upper()
    for key, profile in PATHOLOGY_PROFILES.items():
        if key in dx:
            return profile

    # Check if prostate fields populated
    if consulta.get("pros_tnm") or consulta.get("pros_gleason"):
        return "ONCO_PROSTATA"
    if consulta.get("rinon_tnm"):
        return "ONCO_RINON"
    if consulta.get("vejiga_tnm"):
        return "ONCO_VEJIGA"
    if consulta.get("testiculo_tnm"):
        return "ONCO_TESTICULO"
    if consulta.get("lit_tamano") or consulta.get("lit_localizacion"):
        return "LITIASIS"
    if consulta.get("hpb_ipss") or consulta.get("hpb_tamano_prostata"):
        return "HPB_STUI"

    return "GENERAL"


PROFILE_LABELS = {
    "ONCO_PROSTATA": "Oncológico - Próstata",
    "ONCO_RINON": "Oncológico - Riñón",
    "ONCO_VEJIGA": "Oncológico - Vejiga",
    "ONCO_TESTICULO": "Oncológico - Testículo",
    "ONCO_PENE": "Oncológico - Pene",
    "ONCO_UTUC": "Oncológico - UTUC",
    "ONCO_SUPRARRENAL": "Oncológico - Suprarrenal",
    "ONCO_INCIERTO": "Tumor de Comportamiento Incierto",
    "LITIASIS": "Litiasis Urinaria",
    "HPB_STUI": "HPB / STUI",
    "INFECCION": "Infección Urinaria",
    "FUNCIONAL": "Urología Funcional",
    "TRASPLANTE": "Trasplante Renal",
    "GENERAL": "General",
}


# ---------------------------------------------------------------------------
# PSA timeline parsing and kinetics
# ---------------------------------------------------------------------------

def _parse_psa_timeline(historial_ape: Any) -> Dict[str, Any]:
    """
    Parsea el historial de APE y calcula velocidad y tiempo de duplicación.
    Acepta formatos: JSON array, texto con fechas, o valores separados.
    """
    points: List[Dict[str, Any]] = []

    if not historial_ape:
        return {"points": [], "velocity": None, "doubling_time": None}

    # Try JSON parse first
    data = _load_json(historial_ape, None)
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                d = item.get("fecha") or item.get("date")
                v = _safe_float(item.get("valor") or item.get("value") or item.get("ape"))
                if d and v is not None:
                    try:
                        if isinstance(d, str):
                            d = datetime.strptime(d[:10], "%Y-%m-%d").date()
                        points.append({"date": d, "value": v})
                    except Exception:
                        pass

    # Try text parsing as fallback
    if not points and isinstance(historial_ape, str):
        # Pattern: date: value or date - value
        pattern = r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})\s*[:\-=]\s*([\d.]+)"
        for m in re.finditer(pattern, historial_ape):
            try:
                d = datetime.strptime(m.group(1).replace("/", "-"), "%Y-%m-%d").date()
                v = float(m.group(2))
                points.append({"date": d, "value": v})
            except Exception:
                pass

    points.sort(key=lambda x: x["date"])

    # Compute PSA velocity (ng/ml/year) using least squares on last 3+ values
    velocity = None
    doubling_time = None

    if len(points) >= 2:
        first = points[0]
        last = points[-1]
        years = (last["date"] - first["date"]).days / 365.25
        if years > 0:
            velocity = round((last["value"] - first["value"]) / years, 3)

        # PSA doubling time: ln(2) / slope of ln(PSA) vs time
        if len(points) >= 2 and all(p["value"] > 0 for p in points):
            try:
                import math
                n = len(points)
                t0 = points[0]["date"]
                ts = [(p["date"] - t0).days / 30.44 for p in points]  # months
                lnv = [math.log(p["value"]) for p in points]

                # Simple linear regression
                mean_t = sum(ts) / n
                mean_ln = sum(lnv) / n
                num = sum((ts[i] - mean_t) * (lnv[i] - mean_ln) for i in range(n))
                den = sum((ts[i] - mean_t) ** 2 for i in range(n))
                if den > 0:
                    slope = num / den
                    if slope > 0:
                        doubling_time = round(math.log(2) / slope, 1)  # months
            except Exception:
                pass

    # Serialize dates for JSON/template
    for p in points:
        if isinstance(p["date"], date):
            p["date_str"] = p["date"].strftime("%b %Y")
            p["date_iso"] = p["date"].isoformat()

    return {
        "points": points,
        "velocity": velocity,
        "doubling_time": doubling_time,
    }


# ---------------------------------------------------------------------------
# Staging card builder
# ---------------------------------------------------------------------------

def _build_staging_card(consulta: Dict, profile: str) -> Dict[str, Any]:
    """Construye la tarjeta de estadificación según el perfil patológico."""
    card: Dict[str, Any] = {"profile": profile, "fields": []}

    if profile == "ONCO_PROSTATA":
        gleason = _safe(consulta.get("pros_gleason"))
        # Calculate ISUP group from Gleason
        isup = ""
        if gleason:
            parts = gleason.replace(" ", "").split("+")
            if len(parts) == 2:
                try:
                    g1, g2 = int(parts[0]), int(parts[1])
                    total = g1 + g2
                    if total <= 6:
                        isup = "1"
                    elif total == 7 and g1 == 3:
                        isup = "2"
                    elif total == 7 and g1 == 4:
                        isup = "3"
                    elif total == 8:
                        isup = "4"
                    elif total >= 9:
                        isup = "5"
                except Exception:
                    pass

        # D'Amico risk
        riesgo = _safe(consulta.get("pros_riesgo"))
        tnm = _safe(consulta.get("pros_tnm"))
        ape_act = _safe(consulta.get("pros_ape_act"))

        card["fields"] = [
            {"label": "TNM", "value": tnm or "N/E"},
            {"label": "Gleason", "value": gleason or "N/E"},
            {"label": "Grupo ISUP", "value": isup or "N/E"},
            {"label": "Riesgo D'Amico", "value": riesgo or "N/E"},
            {"label": "ECOG", "value": _safe(consulta.get("pros_ecog")) or "N/E"},
            {"label": "APE actual", "value": f"{ape_act} ng/ml" if ape_act else "N/E"},
            {"label": "PI-RADS", "value": _safe(consulta.get("pros_rmn")) or "N/E"},
            {"label": "Briganti", "value": f"{_safe(consulta.get('pros_briganti'))}%" if consulta.get("pros_briganti") else "N/E"},
        ]

    elif profile == "ONCO_RINON":
        card["fields"] = [
            {"label": "TNM", "value": _safe(consulta.get("rinon_tnm")) or "N/E"},
            {"label": "Etapa", "value": _safe(consulta.get("rinon_etapa")) or "N/E"},
            {"label": "ECOG", "value": _safe(consulta.get("rinon_ecog")) or "N/E"},
            {"label": "Charlson", "value": _safe(consulta.get("rinon_charlson")) or "N/E"},
            {"label": "Nefrectomía", "value": _safe(consulta.get("rinon_nefrectomia")) or "N/E"},
            {"label": "RHP", "value": _safe(consulta.get("rinon_rhp")) or "N/E"},
            {"label": "Tx Sistémico", "value": _safe(consulta.get("rinon_sistemico")) or "N/E"},
        ]

    elif profile == "ONCO_VEJIGA":
        card["fields"] = [
            {"label": "TNM", "value": _safe(consulta.get("vejiga_tnm")) or "N/E"},
            {"label": "ECOG", "value": _safe(consulta.get("vejiga_ecog")) or "N/E"},
            {"label": "Hematuria", "value": _safe(consulta.get("vejiga_hematuria_tipo")) or "N/E"},
            {"label": "Cistoscopias previas", "value": _safe(consulta.get("vejiga_cistoscopias_previas")) or "N/E"},
            {"label": "QT intravesical", "value": _safe(consulta.get("vejiga_quimio_intravesical")) or "N/E"},
            {"label": "Tx Sistémico", "value": _safe(consulta.get("vejiga_sistemico")) or "N/E"},
        ]

    elif profile == "ONCO_TESTICULO":
        card["fields"] = [
            {"label": "TNM", "value": _safe(consulta.get("testiculo_tnm")) or "N/E"},
            {"label": "ECOG", "value": _safe(consulta.get("testiculo_ecog", consulta.get("pros_ecog"))) or "N/E"},
            {"label": "Orquiectomía", "value": _safe(consulta.get("testiculo_orquiectomia_fecha")) or "N/E"},
            {"label": "AFP pre", "value": _safe(consulta.get("testiculo_marcadores_pre")) or "N/E"},
            {"label": "AFP post", "value": _safe(consulta.get("testiculo_marcadores_post")) or "N/E"},
            {"label": "RHP", "value": _safe(consulta.get("testiculo_rhp", "")) or "N/E"},
        ]

    elif profile == "LITIASIS":
        card["fields"] = [
            {"label": "Tamaño", "value": f"{_safe(consulta.get('lit_tamano'))} mm" if consulta.get("lit_tamano") else "N/E"},
            {"label": "Localización", "value": _safe(consulta.get("lit_localizacion")) or "N/E"},
            {"label": "Densidad UH", "value": _safe(consulta.get("lit_densidad_uh")) or "N/E"},
            {"label": "Guy's Score", "value": _safe(consulta.get("lit_guys_score")) or "N/E"},
            {"label": "CROES", "value": _safe(consulta.get("lit_croes_score")) or "N/E"},
            {"label": "Estatus post-op", "value": _safe(consulta.get("lit_estatus_postop")) or "N/E"},
            {"label": "Unidad metabólica", "value": _safe(consulta.get("lit_unidad_metabolica")) or "N/E"},
        ]

    elif profile == "HPB_STUI":
        card["fields"] = [
            {"label": "Tamaño próstata", "value": f"{_safe(consulta.get('hpb_tamano_prostata'))} g" if consulta.get("hpb_tamano_prostata") else "N/E"},
            {"label": "APE", "value": _safe(consulta.get("hpb_ape")) or "N/E"},
            {"label": "IPSS", "value": _safe(consulta.get("hpb_ipss")) or "N/E"},
            {"label": "Tamsulosina", "value": _safe(consulta.get("hpb_tamsulosina")) or "N/E"},
            {"label": "Finasteride", "value": _safe(consulta.get("hpb_finasteride")) or "N/E"},
        ]

    return card


# ---------------------------------------------------------------------------
# eGFR CKD-EPI 2021
# ---------------------------------------------------------------------------

def _compute_egfr(creatinine: Optional[float], age: Optional[int], sex: Optional[str]) -> Optional[float]:
    """Calcula eGFR usando CKD-EPI 2021 (race-free)."""
    if creatinine is None or age is None or creatinine <= 0 or age <= 0:
        return None
    try:
        is_female = (sex or "").upper().startswith("F")
        if is_female:
            if creatinine <= 0.7:
                egfr = 142 * (creatinine / 0.7) ** (-0.241) * (0.9938 ** age) * 1.012
            else:
                egfr = 142 * (creatinine / 0.7) ** (-1.200) * (0.9938 ** age) * 1.012
        else:
            if creatinine <= 0.9:
                egfr = 142 * (creatinine / 0.9) ** (-0.302) * (0.9938 ** age)
            else:
                egfr = 142 * (creatinine / 0.9) ** (-1.200) * (0.9938 ** age)
        return round(egfr, 1)
    except Exception:
        return None


def _ckd_stage(egfr: Optional[float]) -> str:
    """Estadio CKD según KDIGO."""
    if egfr is None:
        return "N/E"
    if egfr >= 90:
        return "G1"
    if egfr >= 60:
        return "G2"
    if egfr >= 45:
        return "G3a"
    if egfr >= 30:
        return "G3b"
    if egfr >= 15:
        return "G4"
    return "G5"


# ---------------------------------------------------------------------------
# QoL scores
# ---------------------------------------------------------------------------

def _build_qol_scores(enriched: Optional[Dict]) -> List[Dict[str, Any]]:
    """Construye datos de scores QoL con barras de progreso."""
    scores = []
    if not enriched:
        return scores

    ipss = _safe_int(enriched.get("qol_ipss_score"))
    if ipss is not None:
        pct = min(100, round(ipss / 35 * 100))
        if ipss <= 7:
            label = "LEVE"
        elif ipss <= 19:
            label = "MODERADO"
        else:
            label = "SEVERO"
        scores.append({
            "name": "IPSS", "value": ipss, "max": 35,
            "pct": pct, "label": label,
            "color": "#2f8f63" if label == "LEVE" else "#d4a017" if label == "MODERADO" else "#c0392b",
        })

    iief = _safe_int(enriched.get("qol_iief5_score"))
    if iief is not None:
        pct = min(100, round(iief / 25 * 100))
        if iief >= 22:
            label = "SIN DISFUNCION"
        elif iief >= 17:
            label = "LEVE"
        elif iief >= 12:
            label = "MODERADO"
        else:
            label = "SEVERO"
        scores.append({
            "name": "IIEF-5", "value": iief, "max": 25,
            "pct": pct, "label": label,
            "color": "#2f8f63" if label == "SIN DISFUNCION" else "#3498db" if label == "LEVE" else "#d4a017" if label == "MODERADO" else "#c0392b",
        })

    iciq = _safe_int(enriched.get("qol_iciqsf_score"))
    if iciq is not None:
        pct = min(100, round(iciq / 21 * 100))
        if iciq <= 5:
            label = "LEVE"
        elif iciq <= 12:
            label = "MODERADO"
        else:
            label = "SEVERO"
        scores.append({
            "name": "ICIQ-SF", "value": iciq, "max": 21,
            "pct": pct, "label": label,
            "color": "#2f8f63" if label == "LEVE" else "#d4a017" if label == "MODERADO" else "#c0392b",
        })

    return scores


# ---------------------------------------------------------------------------
# Clinical timeline builder
# ---------------------------------------------------------------------------

def _build_clinical_timeline(db: Session, consulta_id: int, nss: str) -> List[Dict[str, Any]]:
    """Construye timeline clínico cronológico."""
    events: List[Dict[str, Any]] = []

    # Consultation events
    try:
        rows = db.execute(
            sa_text(
                "SELECT id, fecha_registro, diagnostico, procedimiento_programado "
                "FROM consultas WHERE nss = :nss ORDER BY fecha_registro ASC"
            ),
            {"nss": nss},
        ).mappings().all()
        for r in rows:
            d = r.get("fecha_registro")
            if d:
                events.append({
                    "date": d if isinstance(d, date) else str(d)[:10],
                    "type": "CONSULTA",
                    "icon": "stethoscope",
                    "title": "Consulta externa",
                    "detail": _safe(r.get("diagnostico", ""))[:80],
                })
    except Exception:
        pass

    # Hospitalization events
    try:
        rows = db.execute(
            sa_text(
                "SELECT id, fecha_ingreso, fecha_egreso, diagnostico, cama "
                "FROM hospitalizaciones WHERE nss = :nss ORDER BY fecha_ingreso ASC"
            ),
            {"nss": nss},
        ).mappings().all()
        for r in rows:
            d = r.get("fecha_ingreso")
            if d:
                events.append({
                    "date": d if isinstance(d, date) else str(d)[:10],
                    "type": "INGRESO",
                    "icon": "hospital",
                    "title": f"Ingreso hospitalario (Cama {_safe(r.get('cama'))})",
                    "detail": _safe(r.get("diagnostico", ""))[:80],
                })
            d_e = r.get("fecha_egreso")
            if d_e:
                events.append({
                    "date": d_e if isinstance(d_e, date) else str(d_e)[:10],
                    "type": "EGRESO",
                    "icon": "door-open",
                    "title": "Egreso hospitalario",
                    "detail": "",
                })
    except Exception:
        pass

    # Surgical events
    try:
        rows = db.execute(
            sa_text(
                "SELECT id, fecha_programacion, procedimiento, sangrado_ml, tiempo_quirurgico_min "
                "FROM surgical_programaciones WHERE consulta_id = :cid "
                "ORDER BY fecha_programacion ASC"
            ),
            {"cid": consulta_id},
        ).mappings().all()
        for r in rows:
            d = r.get("fecha_programacion")
            detail_parts = []
            if r.get("sangrado_ml"):
                detail_parts.append(f"Sangrado: {r['sangrado_ml']}ml")
            if r.get("tiempo_quirurgico_min"):
                detail_parts.append(f"TQx: {r['tiempo_quirurgico_min']} min")
            if d:
                events.append({
                    "date": d if isinstance(d, date) else str(d)[:10],
                    "type": "CIRUGIA",
                    "icon": "scalpel",
                    "title": _safe(r.get("procedimiento", "Procedimiento quirúrgico"))[:60],
                    "detail": " | ".join(detail_parts),
                })
    except Exception:
        pass

    # Sort by date
    def _sort_key(ev):
        d = ev.get("date", "")
        if isinstance(d, date):
            return d.isoformat()
        return str(d)[:10]

    events.sort(key=_sort_key)
    return events


# ---------------------------------------------------------------------------
# Vitals & Labs trends
# ---------------------------------------------------------------------------

def _fetch_vitals_trend(db: Session, consulta_id: int, days: int = 5) -> List[Dict]:
    """Últimos N días de vitales."""
    try:
        from app.models.inpatient_ai_models import VITALS_TS
        cutoff = datetime.now() - timedelta(days=days)
        rows = db.execute(
            select(VITALS_TS)
            .where(
                and_(
                    VITALS_TS.c.consulta_id == consulta_id,
                    VITALS_TS.c.recorded_at >= cutoff,
                )
            )
            .order_by(VITALS_TS.c.recorded_at.asc())
        ).mappings().all()
        return [_serialize_row(dict(r)) for r in rows]
    except Exception:
        return []


def _serialize_row(row: Dict) -> Dict:
    """Serializa datetime/date para que sean JSON-safe."""
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, date):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _fetch_labs_trend(db: Session, consulta_id: int, days: int = 5) -> List[Dict]:
    """Últimos N días de laboratorios."""
    try:
        from app.models.inpatient_ai_models import LAB_RESULTS
        cutoff = datetime.now() - timedelta(days=days)
        rows = db.execute(
            select(LAB_RESULTS)
            .where(
                and_(
                    LAB_RESULTS.c.consulta_id == consulta_id,
                    LAB_RESULTS.c.collected_at >= cutoff,
                )
            )
            .order_by(LAB_RESULTS.c.collected_at.asc())
        ).mappings().all()
        return [_serialize_row(dict(r)) for r in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Surgical outcomes
# ---------------------------------------------------------------------------

def _fetch_surgical_outcomes(db: Session, consulta_id: int) -> Dict[str, Any]:
    """Obtiene resultados quirúrgicos postoperatorios."""
    try:
        row = db.execute(
            sa_text(
                "SELECT procedimiento, margen_quirurgico, nervios_preservados, "
                "linfadenectomia, ganglios_positivos, ganglios_totales, "
                "clavien_dindo, sangrado_ml, tiempo_quirurgico_min, "
                "transfusion, reingreso_30d, reintervencion_30d, mortalidad_30d, "
                "reingreso_90d, reintervencion_90d, mortalidad_90d, "
                "stone_free, composicion_lito, extension_extrapros "
                "FROM surgical_programaciones "
                "WHERE consulta_id = :cid AND estatus IN ('REALIZADA','COMPLETADA') "
                "ORDER BY fecha_programacion DESC LIMIT 1"
            ),
            {"cid": consulta_id},
        ).mappings().first()
        if row:
            return dict(row)
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Comorbidities
# ---------------------------------------------------------------------------

def _build_comorbidities(consulta: Dict) -> List[Dict[str, Any]]:
    """Construye lista de comorbilidades del paciente."""
    comorbidities = []

    # Parse APP (antecedentes personales patológicos)
    app_json = _load_json(consulta.get("app_patologias_json"), [])
    if isinstance(app_json, list):
        for item in app_json:
            if isinstance(item, dict):
                comorbidities.append({
                    "name": _safe(item.get("patologia", item.get("nombre"))),
                    "time": _safe(item.get("tiempo_evolucion", "")),
                    "treatment": _safe(item.get("tratamiento_actual", "")),
                })

    # Toxicomanias
    tab_status = _safe(consulta.get("tabaquismo_status", ""))
    if tab_status and tab_status.upper() not in ("NEGADO", "NO", ""):
        it = _safe(consulta.get("indice_tabaquico", ""))
        comorbidities.append({
            "name": f"Tabaquismo ({tab_status})",
            "time": f"IT: {it}" if it else "",
            "treatment": "",
        })

    # Allergies
    alergeno = _safe(consulta.get("alergeno", ""))
    if alergeno and alergeno.upper() not in ("NEGADAS", "NO", ""):
        comorbidities.append({
            "name": f"Alergia: {alergeno}",
            "time": "",
            "treatment": _safe(consulta.get("alergia_reaccion", "")),
        })

    return comorbidities


# ---------------------------------------------------------------------------
# Risk scores from preanesthetic
# ---------------------------------------------------------------------------

def _fetch_risk_scores(db: Session, consulta_id: int) -> Dict[str, Any]:
    """Obtiene scores de riesgo preanestésico."""
    try:
        row = db.execute(
            sa_text(
                "SELECT asa, goldman, detsky, lee, caprini "
                "FROM hospital_ingresos_preop "
                "WHERE consulta_id = :cid "
                "ORDER BY id DESC LIMIT 1"
            ),
            {"cid": consulta_id},
        ).mappings().first()
        if row:
            return dict(row)
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Active alerts
# ---------------------------------------------------------------------------

def _fetch_active_alerts(db: Session, consulta_id: int) -> List[Dict]:
    """Obtiene alertas activas del paciente."""
    alerts = []
    try:
        from app.services.fau_hospitalizacion_agent import HOSPITALIZACION_ALERTAS
        rows = db.execute(
            select(HOSPITALIZACION_ALERTAS)
            .where(
                and_(
                    HOSPITALIZACION_ALERTAS.c.consulta_id == consulta_id,
                    HOSPITALIZACION_ALERTAS.c.resolved == False,
                )
            )
            .order_by(desc(HOSPITALIZACION_ALERTAS.c.created_at))
            .limit(10)
        ).mappings().all()
        alerts = [_serialize_row(dict(r)) for r in rows]
    except Exception:
        pass
    return alerts


# ---------------------------------------------------------------------------
# Hospitalization status
# ---------------------------------------------------------------------------

def _fetch_active_hospitalization(db: Session, consulta_id: int, nss: str) -> Optional[Dict]:
    """Obtiene hospitalización activa."""
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
            return dict(row)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

async def smart_expediente_flow(
    request: Request,
    db: Session,
    consulta_id: int,
) -> HTMLResponse:
    """Renderiza el expediente inteligente por patología."""
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
        return m.render_template("smart_expediente.html", request, error="Consulta no encontrada")

    consulta = dict(row)
    nss = _safe(consulta.get("nss"))

    # Determine pathology profile
    profile = _determine_pathology_profile(consulta)
    profile_label = PROFILE_LABELS.get(profile, "General")

    # Staging card
    staging = _build_staging_card(consulta, profile)

    # PSA timeline (for prostate profiles)
    psa_data = {"points": [], "velocity": None, "doubling_time": None}
    if profile in ("ONCO_PROSTATA", "ONCO_INCIERTO", "HPB_STUI"):
        psa_data = _parse_psa_timeline(
            consulta.get("pros_historial_ape") or consulta.get("hpb_ape")
        )
        # Add current APE if available
        ape_act = _safe_float(consulta.get("pros_ape_act"))
        if ape_act is not None and not any(
            p.get("value") == ape_act for p in psa_data["points"]
        ):
            psa_data["points"].append({
                "date": date.today(),
                "date_str": date.today().strftime("%b %Y"),
                "date_iso": date.today().isoformat(),
                "value": ape_act,
            })

    # QoL scores from enriched record
    enriched = {}
    try:
        from app.services.expediente_plus_flow import EXPEDIENTE_ENRIQUECIDO
        e_row = db.execute(
            select(EXPEDIENTE_ENRIQUECIDO)
            .where(EXPEDIENTE_ENRIQUECIDO.c.consulta_id == consulta_id)
        ).mappings().first()
        if e_row:
            enriched = dict(e_row)
    except Exception:
        pass

    qol_scores = _build_qol_scores(enriched)

    # Comorbidities
    comorbidities = _build_comorbidities(consulta)

    # Risk scores
    risk_scores = _fetch_risk_scores(db, consulta_id)

    # Surgical outcomes
    surgical_outcomes = _fetch_surgical_outcomes(db, consulta_id)

    # Clinical timeline
    timeline = _build_clinical_timeline(db, consulta_id, nss)

    # Active hospitalization
    active_hosp = _fetch_active_hospitalization(db, consulta_id, nss)

    # Vitals & labs trends
    vitals_trend = _fetch_vitals_trend(db, consulta_id)
    labs_trend = _fetch_labs_trend(db, consulta_id)

    # eGFR calculation from latest creatinine
    egfr = None
    ckd = "N/E"
    latest_cr = None
    for lab in reversed(labs_trend):
        name = _safe(lab.get("test_name", "")).lower()
        if "creat" in name and lab.get("value_num"):
            latest_cr = lab["value_num"]
            break
    if latest_cr:
        egfr = _compute_egfr(latest_cr, _safe_int(consulta.get("edad")), _safe(consulta.get("sexo")))
        ckd = _ckd_stage(egfr)

    # Active alerts
    alerts = _fetch_active_alerts(db, consulta_id)

    # Devices
    devices = []
    try:
        from app.models.inpatient_ai_models import UROLOGY_DEVICES
        dev_rows = db.execute(
            select(UROLOGY_DEVICES)
            .where(
                and_(
                    UROLOGY_DEVICES.c.consulta_id == consulta_id,
                    UROLOGY_DEVICES.c.removed_at.is_(None),
                )
            )
        ).mappings().all()
        devices = [dict(d) for d in dev_rows]
    except Exception:
        pass

    return m.render_template(
        "smart_expediente.html",
        request,
        consulta=consulta,
        consulta_id=consulta_id,
        nss=nss,
        profile=profile,
        profile_label=profile_label,
        staging=staging,
        psa_data=psa_data,
        qol_scores=qol_scores,
        comorbidities=comorbidities,
        risk_scores=risk_scores,
        surgical_outcomes=surgical_outcomes,
        timeline=timeline,
        active_hosp=active_hosp,
        vitals_trend=vitals_trend,
        labs_trend=labs_trend,
        egfr=egfr,
        ckd_stage=ckd,
        latest_cr=latest_cr,
        alerts=alerts,
        devices=devices,
        enriched=enriched,
    )
