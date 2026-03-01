from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from app.core.time_utils import utcnow

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    desc,
    insert,
    or_,
    select,
)
from sqlalchemy.orm import Session


HOSP_AGENT_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")

HOSPITALIZACION_PREDICCIONES = Table(
    "hospitalizacion_predicciones",
    HOSP_AGENT_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("consulta_id", Integer, index=True, nullable=False),
    Column("hospitalizacion_id", Integer, index=True),
    Column("nss", String(20), index=True),
    Column("paciente", String(220), index=True),
    Column("prediction_date", Date, index=True, nullable=False),
    Column("prediction_type", String(80), index=True, nullable=False),
    Column("prediction_value", Float, index=True),
    Column("ci_low", Float),
    Column("ci_high", Float),
    Column("risk_level", String(20), index=True),
    Column("model_version", String(80), index=True),
    Column("factors_json", JSON_SQL),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)

HOSPITALIZACION_ALERTAS = Table(
    "hospitalizacion_alertas",
    HOSP_AGENT_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("consulta_id", Integer, index=True, nullable=False),
    Column("hospitalizacion_id", Integer, index=True),
    Column("nss", String(20), index=True),
    Column("paciente", String(220), index=True),
    Column("alert_ts", DateTime, nullable=False, index=True),
    Column("alert_type", String(80), index=True),
    Column("severity", String(20), index=True),
    Column("message", Text),
    Column("recommendation", Text),
    Column("payload_json", JSON_SQL),
    Column("resolved", Boolean, default=False, index=True),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)

HOSPITALIZACION_RESUMENES = Table(
    "hospitalizacion_resumenes",
    HOSP_AGENT_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("consulta_id", Integer, index=True, nullable=False),
    Column("hospitalizacion_id", Integer, index=True),
    Column("nss", String(20), index=True),
    Column("paciente", String(220), index=True),
    Column("summary_date", Date, index=True, nullable=False),
    Column("summary_text", Text),
    Column("payload_json", JSON_SQL),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)


def ensure_hospital_agent_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    HOSP_AGENT_METADATA.create_all(bind=bind, checkfirst=True)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_upper(value: Any) -> str:
    return _safe_text(value).upper()


def _dump(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
            if isinstance(out, type(default)):
                return out
        except Exception:
            return default
    return default


def _bool_yes(value: Any) -> bool:
    return _safe_upper(value) in {"SI", "S", "YES", "TRUE", "1", "Y"}


def _extract_float(raw: Any) -> Optional[float]:
    txt = _safe_text(raw).replace(",", "")
    if not txt:
        return None
    out = ""
    dot = False
    minus = False
    for ch in txt:
        if ch.isdigit():
            out += ch
        elif ch == "." and not dot:
            out += ch
            dot = True
        elif ch == "-" and not minus and not out:
            out += ch
            minus = True
    if out in {"", "-", ".", "-."}:
        return None
    try:
        return float(out)
    except Exception:
        return None


def _map_lab_marker(test_name: str, test_code: str) -> Optional[str]:
    joined = f"{_safe_text(test_name)} {_safe_text(test_code)}".lower()
    if "creatin" in joined or joined.strip() in {"cr", "creat"}:
        return "creatinina"
    if "hemoglob" in joined or "hgb" in joined or joined.strip().startswith("hb"):
        return "hemoglobina"
    if "leuco" in joined or "wbc" in joined:
        return "leucocitos"
    if "plaquet" in joined or "plt" in joined:
        return "plaquetas"
    if "sodio" in joined or joined.strip() in {"na", "sodio"}:
        return "sodio"
    if "potasio" in joined or joined.strip() in {"k", "potasio"}:
        return "potasio"
    return None


def _risk_level(score: float) -> str:
    if score >= 0.7:
        return "ALTO"
    if score >= 0.4:
        return "MEDIO"
    return "BAJO"


def _unique_factors(factors: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for f in factors:
        key = _safe_upper(f)
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out


def _insert_prediction(
    sdb: Session,
    *,
    run_id: Optional[int],
    consulta_id: int,
    hospitalizacion_id: Optional[int],
    nss: str,
    paciente: str,
    prediction_type: str,
    prediction_value: float,
    ci_low: float,
    ci_high: float,
    risk_level: str,
    model_version: str,
    factors: List[str],
) -> None:
    sdb.execute(
        insert(HOSPITALIZACION_PREDICCIONES).values(
            run_id=run_id,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            nss=_safe_text(nss),
            paciente=_safe_text(paciente),
            prediction_date=date.today(),
            prediction_type=_safe_upper(prediction_type),
            prediction_value=float(prediction_value),
            ci_low=float(ci_low),
            ci_high=float(ci_high),
            risk_level=_safe_upper(risk_level),
            model_version=_safe_text(model_version),
            factors_json=_dump(factors),
            created_at=utcnow(),
        )
    )


def _insert_alert(
    sdb: Session,
    *,
    run_id: Optional[int],
    consulta_id: int,
    hospitalizacion_id: Optional[int],
    nss: str,
    paciente: str,
    alert_type: str,
    severity: str,
    message: str,
    recommendation: str,
    payload: Dict[str, Any],
) -> None:
    sdb.execute(
        insert(HOSPITALIZACION_ALERTAS).values(
            run_id=run_id,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            nss=_safe_text(nss),
            paciente=_safe_text(paciente),
            alert_ts=utcnow(),
            alert_type=_safe_upper(alert_type),
            severity=_safe_upper(severity),
            message=_safe_text(message),
            recommendation=_safe_text(recommendation),
            payload_json=_dump(payload),
            resolved=False,
            created_at=utcnow(),
        )
    )


def _insert_summary(
    sdb: Session,
    *,
    run_id: Optional[int],
    consulta_id: int,
    hospitalizacion_id: Optional[int],
    nss: str,
    paciente: str,
    summary_text: str,
    payload: Dict[str, Any],
) -> None:
    sdb.execute(
        insert(HOSPITALIZACION_RESUMENES).values(
            run_id=run_id,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            nss=_safe_text(nss),
            paciente=_safe_text(paciente),
            summary_date=date.today(),
            summary_text=_safe_text(summary_text),
            payload_json=_dump(payload),
            created_at=utcnow(),
        )
    )


def analyze_hospitalizacion_patients(
    db: Session,
    sdb: Session,
    main_module: Any,
    *,
    window_days: int = 30,
    run_id: Optional[int] = None,
    model_version: str = "hospitalizacion_agent_v1",
) -> Dict[str, Any]:
    ensure_hospital_agent_schema(sdb)

    since = date.today() - timedelta(days=max(1, min(window_days, 365)))
    rows = (
        db.query(main_module.HospitalizacionDB)
        .filter(main_module.HospitalizacionDB.fecha_ingreso >= since)
        .order_by(main_module.HospitalizacionDB.fecha_ingreso.asc())
        .all()
    )

    by_ingreso = Counter(_safe_upper(r.ingreso_tipo or "NO_REGISTRADO") for r in rows)
    by_urg_tipo = Counter(_safe_upper(r.urgencia_tipo or "NO_REGISTRADO") for r in rows)
    by_dx = Counter(_safe_upper(r.diagnostico or "NO_REGISTRADO") for r in rows)

    predictions_count = 0
    alert_count = 0
    summary_count = 0
    complication_scores: List[float] = []
    readmission_scores: List[float] = []
    los_pred_total: List[float] = []
    estancias: List[int] = []
    insights: List[Dict[str, Any]] = []

    for hosp in rows:
        if not hosp.consulta_id:
            continue

        now = utcnow()
        vitals_since = now - timedelta(days=3)
        labs_since = now - timedelta(days=3)

        current_stay_days = int(
            hosp.dias_hospitalizacion
            if hosp.dias_hospitalizacion is not None
            else max((date.today() - (hosp.fecha_ingreso or date.today())).days, 0)
        )
        estancias.append(current_stay_days)

        vitals_q = db.query(main_module.VitalDB).filter(main_module.VitalDB.timestamp >= vitals_since)
        if hosp.nss:
            vitals_q = vitals_q.filter(
                or_(
                    main_module.VitalDB.consulta_id == hosp.consulta_id,
                    main_module.VitalDB.patient_id == str(hosp.consulta_id),
                    main_module.VitalDB.patient_id == _safe_text(hosp.nss),
                )
            )
        else:
            vitals_q = vitals_q.filter(
                or_(
                    main_module.VitalDB.consulta_id == hosp.consulta_id,
                    main_module.VitalDB.patient_id == str(hosp.consulta_id),
                )
            )
        vitals = vitals_q.order_by(main_module.VitalDB.timestamp.asc()).all()

        labs_q = db.query(main_module.LabDB).filter(main_module.LabDB.timestamp >= labs_since)
        if hosp.nss:
            labs_q = labs_q.filter(
                or_(
                    main_module.LabDB.consulta_id == hosp.consulta_id,
                    main_module.LabDB.patient_id == str(hosp.consulta_id),
                    main_module.LabDB.patient_id == _safe_text(hosp.nss),
                )
            )
        else:
            labs_q = labs_q.filter(
                or_(
                    main_module.LabDB.consulta_id == hosp.consulta_id,
                    main_module.LabDB.patient_id == str(hosp.consulta_id),
                )
            )
        labs = labs_q.order_by(main_module.LabDB.timestamp.asc()).all()

        qx_reciente = (
            sdb.query(main_module.SurgicalProgramacionDB)
            .filter(main_module.SurgicalProgramacionDB.consulta_id == hosp.consulta_id)
            .filter(main_module.SurgicalProgramacionDB.estatus.in_(["REALIZADA", "POSTQUIRURGICA", "PROGRAMADA"]))
            .order_by(main_module.SurgicalProgramacionDB.id.desc())
            .first()
        )

        hr_values = [float(v.hr) for v in vitals if v.hr is not None]
        sbp_values = [float(v.sbp) for v in vitals if v.sbp is not None]
        temp_values = [float(v.temp) for v in vitals if v.temp is not None]

        marker_values: Dict[str, List[float]] = defaultdict(list)
        for lb in labs:
            marker = _map_lab_marker(lb.test_name, lb.test_code)
            if not marker:
                continue
            val = _extract_float(lb.value)
            if val is None:
                continue
            marker_values[marker].append(float(val))

        cr_vals = marker_values.get("creatinina", [])
        hb_vals = marker_values.get("hemoglobina", [])
        leu_vals = marker_values.get("leucocitos", [])
        plt_vals = marker_values.get("plaquetas", [])
        na_vals = marker_values.get("sodio", [])
        k_vals = marker_values.get("potasio", [])

        cr_latest = cr_vals[-1] if cr_vals else None
        hb_latest = hb_vals[-1] if hb_vals else None
        leu_latest = leu_vals[-1] if leu_vals else None
        na_latest = na_vals[-1] if na_vals else None
        k_latest = k_vals[-1] if k_vals else None

        delta_cr = (max(cr_vals) - min(cr_vals)) if len(cr_vals) >= 2 else 0.0

        risk_score = 0.05
        factors: List[str] = []

        if (hosp.edad or 0) >= 70:
            risk_score += 0.08
            factors.append("Edad >= 70")
        if _safe_upper(hosp.ingreso_tipo) == "URGENCIA":
            risk_score += 0.10
            factors.append("Ingreso de urgencia")
        if _bool_yes(hosp.uci):
            risk_score += 0.15
            factors.append("Manejo en UCI")
        if _safe_upper(hosp.estado_clinico) in {"DELICADO", "GRAVE"}:
            risk_score += 0.10
            factors.append("Estado clinico delicado/grave")
        if current_stay_days > 5:
            risk_score += 0.07
            factors.append("Estancia > 5 dias")
        if cr_latest is not None and cr_latest >= 2.0:
            risk_score += 0.12
            factors.append("Creatinina >= 2.0")
        if delta_cr >= 0.3:
            risk_score += 0.14
            factors.append("Delta creatinina >= 0.3")
        if hb_latest is not None and hb_latest < 8.0:
            risk_score += 0.10
            factors.append("Hemoglobina < 8")
        if leu_latest is not None and leu_latest > 10000:
            risk_score += 0.07
            factors.append("Leucocitos > 10000")
        if hr_values and statistics.mean(hr_values) > 100:
            risk_score += 0.06
            factors.append("Taquicardia persistente")
        if temp_values and max(temp_values) >= 38.0:
            risk_score += 0.08
            factors.append("Fiebre >= 38")
        if sbp_values and min(sbp_values) < 90:
            risk_score += 0.06
            factors.append("Hipotension SBP < 90")
        if qx_reciente is not None:
            risk_score += 0.05
            factors.append("Contexto postquirurgico")
            try:
                ecog_val = int(_extract_float(qx_reciente.ecog) or 0)
            except Exception:
                ecog_val = 0
            try:
                charlson_val = int(_extract_float(qx_reciente.charlson) or 0)
            except Exception:
                charlson_val = 0
            if ecog_val >= 2:
                risk_score += 0.05
                factors.append("ECOG >= 2")
            if charlson_val >= 3:
                risk_score += 0.06
                factors.append("Charlson >= 3")

        risk_score = float(max(0.01, min(0.98, risk_score)))
        factors = _unique_factors(factors)

        predicted_total_stay = float(max(current_stay_days + 1, round(current_stay_days + 1 + (risk_score * 7), 1)))
        ci_half = max(0.8, predicted_total_stay * 0.2)
        ci_low = max(0.0, round(predicted_total_stay - ci_half, 2))
        ci_high = round(predicted_total_stay + ci_half, 2)

        complication_risk = float(max(0.01, min(0.98, 0.02 + risk_score)))
        readmission_risk = float(
            max(
                0.01,
                min(
                    0.98,
                    0.03
                    + (risk_score * 0.72)
                    + (0.05 if current_stay_days > 7 else 0.0)
                    + (0.04 if _safe_upper(hosp.ingreso_tipo) == "URGENCIA" else 0.0),
                ),
            )
        )

        alerts: List[Dict[str, Any]] = []

        def _push_alert(a_type: str, severity: str, message: str, recommendation: str, payload: Dict[str, Any]) -> None:
            alerts.append(
                {
                    "type": _safe_upper(a_type),
                    "severity": _safe_upper(severity),
                    "message": _safe_text(message),
                    "recommendation": _safe_text(recommendation),
                    "payload": payload,
                }
            )

        if sum(1 for x in hr_values if x > 100) >= 3:
            _push_alert(
                "VITAL_SIGNS",
                "MEDIA",
                "Frecuencia cardiaca > 100 en multiples mediciones.",
                "Revisar foco infeccioso, hidratacion y control hemodinamico.",
                {"mean_hr": round(statistics.mean(hr_values), 2) if hr_values else None},
            )
        if sum(1 for x in temp_values if x >= 38.0) >= 2:
            _push_alert(
                "FIEBRE",
                "ALTA",
                "Fiebre persistente detectada en hospitalizacion.",
                "Solicitar BH, EGO/cultivos y valorar imagen segun evolucion.",
                {"max_temp": max(temp_values) if temp_values else None},
            )
        if delta_cr >= 0.3:
            _push_alert(
                "AKI",
                "ALTA",
                "Incremento de creatinina compatible con lesion renal aguda.",
                "Aplicar vigilancia de funcion renal y ajustar nefrotoxicos.",
                {"delta_cr": round(delta_cr, 3), "creatinina_latest": cr_latest},
            )
        if hb_latest is not None and hb_latest < 8.0:
            _push_alert(
                "ANEMIA_SEVERA",
                "ALTA",
                "Hemoglobina menor a 8 g/dL.",
                "Valorar sangrado activo y necesidad transfusional.",
                {"hb": hb_latest},
            )
        if leu_latest is not None and leu_latest > 12000:
            _push_alert(
                "LEUCOCITOSIS",
                "MEDIA",
                "Leucocitosis significativa.",
                "Correlacionar con clinica, cultivos y foco infeccioso.",
                {"leucocitos": leu_latest},
            )
        if na_latest is not None and (na_latest < 135 or na_latest > 145):
            _push_alert(
                "DISNATREMIA",
                "MEDIA",
                "Sodio fuera de rango.",
                "Corregir gradualmente y monitorizar neurologico.",
                {"sodio": na_latest},
            )
        if k_latest is not None and (k_latest < 3.5 or k_latest > 5.0):
            _push_alert(
                "DISPOTASEMIA",
                "ALTA" if (k_latest > 5.5 or k_latest < 3.0) else "MEDIA",
                "Potasio fuera de rango.",
                "Repetir control y corregir de forma protocolizada.",
                {"potasio": k_latest},
            )
        if readmission_risk >= 0.60:
            _push_alert(
                "READMISION_30D_RIESGO",
                "MEDIA",
                "Riesgo elevado de reingreso no planificado a 30 dias.",
                "Reforzar plan de alta y seguimiento temprano.",
                {"readmission_risk": round(readmission_risk, 3)},
            )

        summary = (
            f"Paciente {_safe_text(hosp.nombre_completo or 'SIN NOMBRE')} (NSS {_safe_text(hosp.nss or 'N/A')}), "
            f"estancia actual {current_stay_days} dias. Riesgo de complicacion {round(complication_risk, 3)} "
            f"(nivel {_risk_level(complication_risk)}), estancia total predicha {predicted_total_stay} dias "
            f"(IC {ci_low}-{ci_high}). Riesgo de reingreso 30d {round(readmission_risk, 3)}. "
            f"Factores principales: {', '.join(factors[:6]) if factors else 'sin factores mayores detectados'}.")

        _insert_prediction(
            sdb,
            run_id=run_id,
            consulta_id=int(hosp.consulta_id),
            hospitalizacion_id=hosp.id,
            nss=_safe_text(hosp.nss),
            paciente=_safe_text(hosp.nombre_completo),
            prediction_type="LENGTH_OF_STAY_DAYS",
            prediction_value=predicted_total_stay,
            ci_low=ci_low,
            ci_high=ci_high,
            risk_level=_risk_level(complication_risk),
            model_version=model_version,
            factors=factors,
        )
        _insert_prediction(
            sdb,
            run_id=run_id,
            consulta_id=int(hosp.consulta_id),
            hospitalizacion_id=hosp.id,
            nss=_safe_text(hosp.nss),
            paciente=_safe_text(hosp.nombre_completo),
            prediction_type="COMPLICATION_RISK",
            prediction_value=complication_risk,
            ci_low=max(0.0, round(complication_risk - 0.08, 4)),
            ci_high=min(1.0, round(complication_risk + 0.08, 4)),
            risk_level=_risk_level(complication_risk),
            model_version=model_version,
            factors=factors,
        )
        _insert_prediction(
            sdb,
            run_id=run_id,
            consulta_id=int(hosp.consulta_id),
            hospitalizacion_id=hosp.id,
            nss=_safe_text(hosp.nss),
            paciente=_safe_text(hosp.nombre_completo),
            prediction_type="READMISSION_RISK_30D",
            prediction_value=readmission_risk,
            ci_low=max(0.0, round(readmission_risk - 0.08, 4)),
            ci_high=min(1.0, round(readmission_risk + 0.08, 4)),
            risk_level=_risk_level(readmission_risk),
            model_version=model_version,
            factors=factors,
        )
        predictions_count += 3

        for alert in alerts:
            _insert_alert(
                sdb,
                run_id=run_id,
                consulta_id=int(hosp.consulta_id),
                hospitalizacion_id=hosp.id,
                nss=_safe_text(hosp.nss),
                paciente=_safe_text(hosp.nombre_completo),
                alert_type=alert["type"],
                severity=alert["severity"],
                message=alert["message"],
                recommendation=alert["recommendation"],
                payload=alert["payload"],
            )
            alert_count += 1

        _insert_summary(
            sdb,
            run_id=run_id,
            consulta_id=int(hosp.consulta_id),
            hospitalizacion_id=hosp.id,
            nss=_safe_text(hosp.nss),
            paciente=_safe_text(hosp.nombre_completo),
            summary_text=summary,
            payload={
                "predictions": {
                    "length_of_stay": predicted_total_stay,
                    "complication_risk": complication_risk,
                    "readmission_risk_30d": readmission_risk,
                },
                "alerts": alerts,
            },
        )
        summary_count += 1

        complication_scores.append(complication_risk)
        readmission_scores.append(readmission_risk)
        los_pred_total.append(predicted_total_stay)

        if alerts:
            worst = sorted(alerts, key=lambda x: {"ALTA": 3, "MEDIA": 2, "BAJA": 1}.get(x["severity"], 0), reverse=True)[0]
            insights.append(
                {
                    "type": "hospital_alert",
                    "severity": worst["severity"],
                    "text": f"{_safe_text(hosp.nombre_completo)} ({_safe_text(hosp.nss)}): {worst['message']}",
                    "consulta_id": hosp.consulta_id,
                    "hospitalizacion_id": hosp.id,
                }
            )

    sdb.commit()

    metrics = {
        "period_start": since.isoformat(),
        "period_end": date.today().isoformat(),
        "total_ingresos": len(rows),
        "por_tipo_ingreso": dict(by_ingreso),
        "por_urgencia_tipo": dict(by_urg_tipo),
        "top_diagnosticos": dict(by_dx.most_common(20)),
        "estancia_media_dias": round(sum(estancias) / len(estancias), 2) if estancias else None,
        "estancia_mediana_dias": statistics.median(estancias) if estancias else None,
        "indice_estancia_prolongada_pct": round((sum(1 for d in estancias if d > 5) / len(estancias)) * 100.0, 2)
        if estancias
        else 0.0,
        "predicciones_generadas": predictions_count,
        "alertas_generadas": alert_count,
        "resumenes_generados": summary_count,
        "riesgo_complicacion_promedio": round(sum(complication_scores) / len(complication_scores), 4)
        if complication_scores
        else None,
        "riesgo_reingreso_30d_promedio": round(sum(readmission_scores) / len(readmission_scores), 4)
        if readmission_scores
        else None,
        "estancia_predicha_media": round(sum(los_pred_total) / len(los_pred_total), 2) if los_pred_total else None,
    }

    if not insights and metrics["indice_estancia_prolongada_pct"] > 30:
        insights.append(
            {
                "type": "prolonged_stay",
                "severity": "ALTA",
                "text": "Indice de estancia prolongada mayor al 30% en hospitalizacion.",
            }
        )

    return {"metrics": metrics, "insights": insights}


def get_patient_predictions(sdb: Session, consulta_id: int, limit: int = 120) -> List[Dict[str, Any]]:
    ensure_hospital_agent_schema(sdb)
    rows = (
        sdb.execute(
            select(HOSPITALIZACION_PREDICCIONES)
            .where(HOSPITALIZACION_PREDICCIONES.c.consulta_id == int(consulta_id))
            .order_by(desc(HOSPITALIZACION_PREDICCIONES.c.id))
            .limit(max(1, min(limit, 1000)))
        )
        .mappings()
        .all()
    )
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "consulta_id": r["consulta_id"],
            "hospitalizacion_id": r["hospitalizacion_id"],
            "nss": r["nss"],
            "paciente": r["paciente"],
            "prediction_date": r["prediction_date"].isoformat() if r["prediction_date"] else None,
            "prediction_type": r["prediction_type"],
            "prediction_value": r["prediction_value"],
            "ci_low": r["ci_low"],
            "ci_high": r["ci_high"],
            "risk_level": r["risk_level"],
            "model_version": r["model_version"],
            "factors": _load(r["factors_json"], []),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def get_patient_alerts(
    sdb: Session,
    consulta_id: int,
    *,
    limit: int = 120,
    only_open: bool = False,
) -> List[Dict[str, Any]]:
    ensure_hospital_agent_schema(sdb)
    q = select(HOSPITALIZACION_ALERTAS).where(HOSPITALIZACION_ALERTAS.c.consulta_id == int(consulta_id))
    if only_open:
        q = q.where(HOSPITALIZACION_ALERTAS.c.resolved.is_(False))
    rows = sdb.execute(q.order_by(desc(HOSPITALIZACION_ALERTAS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "consulta_id": r["consulta_id"],
            "hospitalizacion_id": r["hospitalizacion_id"],
            "nss": r["nss"],
            "paciente": r["paciente"],
            "alert_ts": r["alert_ts"].isoformat() if r["alert_ts"] else None,
            "alert_type": r["alert_type"],
            "severity": r["severity"],
            "message": r["message"],
            "recommendation": r["recommendation"],
            "payload": _load(r["payload_json"], {}),
            "resolved": bool(r["resolved"]),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def get_patient_summaries(sdb: Session, consulta_id: int, limit: int = 60) -> List[Dict[str, Any]]:
    ensure_hospital_agent_schema(sdb)
    rows = (
        sdb.execute(
            select(HOSPITALIZACION_RESUMENES)
            .where(HOSPITALIZACION_RESUMENES.c.consulta_id == int(consulta_id))
            .order_by(desc(HOSPITALIZACION_RESUMENES.c.id))
            .limit(max(1, min(limit, 1000)))
        )
        .mappings()
        .all()
    )
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "consulta_id": r["consulta_id"],
            "hospitalizacion_id": r["hospitalizacion_id"],
            "nss": r["nss"],
            "paciente": r["paciente"],
            "summary_date": r["summary_date"].isoformat() if r["summary_date"] else None,
            "summary_text": r["summary_text"],
            "payload": _load(r["payload_json"], {}),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def list_recent_hospital_alerts(
    sdb: Session,
    *,
    limit: int = 200,
    days: int = 30,
    only_open: bool = False,
) -> List[Dict[str, Any]]:
    ensure_hospital_agent_schema(sdb)
    since_dt = utcnow() - timedelta(days=max(1, min(days, 365)))
    q = select(HOSPITALIZACION_ALERTAS).where(HOSPITALIZACION_ALERTAS.c.alert_ts >= since_dt)
    if only_open:
        q = q.where(HOSPITALIZACION_ALERTAS.c.resolved.is_(False))
    rows = sdb.execute(q.order_by(desc(HOSPITALIZACION_ALERTAS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "consulta_id": r["consulta_id"],
            "hospitalizacion_id": r["hospitalizacion_id"],
            "nss": r["nss"],
            "paciente": r["paciente"],
            "alert_ts": r["alert_ts"].isoformat() if r["alert_ts"] else None,
            "alert_type": r["alert_type"],
            "severity": r["severity"],
            "message": r["message"],
            "recommendation": r["recommendation"],
            "payload": _load(r["payload_json"], {}),
            "resolved": bool(r["resolved"]),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "source": "AI_HOSPITALIZACION",
        }
        for r in rows
    ]


def summarize_hospital_alerts(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_severity = Counter(_safe_upper(a.get("severity") or "NO_REGISTRADO") for a in alerts)
    by_type = Counter(_safe_upper(a.get("alert_type") or "NO_REGISTRADO") for a in alerts)
    by_patient = Counter(_safe_text(a.get("paciente") or "NO_REGISTRADO") for a in alerts)
    return {
        "total": len(alerts),
        "por_severidad": dict(by_severity),
        "por_tipo": dict(by_type),
        "top_pacientes": dict(by_patient.most_common(15)),
    }
