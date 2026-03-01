"""Flujos legacy de reporte extraídos de main.py (refactor aditivo)."""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models.inpatient_ai_models import (
    ALERT_ACTION_METADATA,
    CLINICAL_EVENT_LOG,
    IO_BLOCKS,
    LAB_RESULTS,
    UROLOGY_DEVICES,
    VITALS_TS,
    ensure_inpatient_time_series_schema,
)
from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
from app.core.observability import metrics_snapshot
from app.services.ui_error_observability_flow import ui_error_summary


_REPORTE_CACHE_LOCK = threading.Lock()
_REPORTE_CACHE_TTL_SEC = max(20, int(os.getenv("REPORTE_CACHE_TTL_SEC", "90") or 90))
_REPORTE_CACHE: Dict[str, Any] = {
    "expires_at": 0.0,
    "context": None,
    "generated_at": None,
}


def _fmt_pct(value: Any, fallback: str = "0.0%") -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return fallback


def _fmt_num(value: Any, fallback: str = "0") -> str:
    try:
        if value is None:
            return fallback
        value_f = float(value)
        if value_f.is_integer():
            return str(int(value_f))
        return f"{value_f:.2f}"
    except Exception:
        return fallback


def _top_pair(rows: Any) -> List[Any]:
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, (list, tuple)) and len(first) >= 2:
            return [first[0], first[1]]
        if isinstance(first, dict):
            label = first.get("cirujano") or first.get("procedimiento") or first.get("grupo") or first.get("label") or "N/A"
            value = first.get("cantidad") or first.get("sangrado_total_ml") or first.get("n") or first.get("value") or 0
            return [label, value]
    return ["N/A", 0]


def _build_prediccion_alertas(context: Dict[str, Any]) -> List[Dict[str, str]]:
    alerts: List[Dict[str, str]] = []
    cancelacion = ((context.get("cancelacion_global") or {}).get("tasa_pct")) or 0
    aki = (((context.get("incidencia_laboratorios") or {}).get("aki_delta_creatinina") or {}).get("incidencia_pct")) or 0
    estancia = ((context.get("indice_estancia_prolongada") or {}).get("indice_pct")) or 0
    cirujano_mes, sangrado_mes = _top_pair((context.get("sangrado_metricas_mes") or {}).get("cirujano_top"))
    cirujano_global, _ = _top_pair((context.get("sangrado_metricas_global") or {}).get("cirujano_top"))
    conversion = ((context.get("embudo_conversion") or {}).get("programacion_a_realizada_pct")) or 0

    if float(cancelacion or 0) >= 8.0:
        alerts.append(
            {
                "nivel": "ALTA",
                "titulo": "Cancelación quirúrgica elevada",
                "detalle": f"Tasa actual {float(cancelacion):.2f}% (umbral sugerido 8%).",
            }
        )
    if float(aki or 0) >= 15.0:
        alerts.append(
            {
                "nivel": "ALTA",
                "titulo": "Incremento de AKI",
                "detalle": f"Incidencia AKI por ΔCr en {float(aki):.2f}%. Reforzar vigilancia renal.",
            }
        )
    if float(estancia or 0) >= 30.0:
        alerts.append(
            {
                "nivel": "MEDIA",
                "titulo": "Estancia prolongada en ascenso",
                "detalle": f"Índice de estancia prolongada en {float(estancia):.2f}%.",
            }
        )
    if cirujano_mes != "N/A" and cirujano_mes == cirujano_global:
        alerts.append(
            {
                "nivel": "MEDIA",
                "titulo": "Patrón persistente de sangrado por cirujano",
                "detalle": (
                    f"{cirujano_mes} lidera el sangrado en periodo mensual y global "
                    f"(mes actual: {_fmt_num(sangrado_mes)} ml)."
                ),
            }
        )
    if 0 < float(conversion or 0) < 70.0:
        alerts.append(
            {
                "nivel": "MEDIA",
                "titulo": "Conversión baja Programada→Realizada",
                "detalle": f"Conversión actual {_fmt_pct(conversion)}. Revisar cuellos preoperatorios.",
            }
        )
    if not alerts:
        alerts.append(
            {
                "nivel": "CONTROL",
                "titulo": "Sin desvíos críticos detectados",
                "detalle": "Los indicadores clave se mantienen en rango operativo.",
            }
        )
    return alerts


def _build_inpatient_structured_metrics(db: Session) -> Dict[str, Any]:
    today_date = datetime.now().date()
    today_iso = today_date.isoformat()
    month_start = today_date.replace(day=1)
    if month_start.month == 12:
        month_end_excl = month_start.replace(year=month_start.year + 1, month=1, day=1)
    else:
        month_end_excl = month_start.replace(month=month_start.month + 1, day=1)

    def _age_group(v: Any) -> str:
        try:
            age = int(v)
        except Exception:
            return "NO_REGISTRADO"
        if age < 18:
            return "<18"
        if age < 40:
            return "18-39"
        if age < 60:
            return "40-59"
        if age < 75:
            return "60-74"
        return ">=75"

    def _norm_diag(v: Any) -> str:
        txt = str(v or "").strip().upper()
        return txt or "NO_REGISTRADO"

    def _counter_to_rows(counter: Dict[str, int], *, key_name: str = "label") -> List[Dict[str, Any]]:
        rows = [{key_name: k, "n": int(v)} for k, v in counter.items()]
        rows.sort(key=lambda x: (-x["n"], str(x[key_name])))
        return rows[:20]
    try:
        ensure_inpatient_time_series_schema(db)
    except Exception:
        pass
    try:
        total_vitals = int(db.execute(select(func.count()).select_from(VITALS_TS)).scalar() or 0)
        total_io = int(db.execute(select(func.count()).select_from(IO_BLOCKS)).scalar() or 0)
        total_labs = int(db.execute(select(func.count()).select_from(LAB_RESULTS)).scalar() or 0)
        total_events = int(db.execute(select(func.count()).select_from(CLINICAL_EVENT_LOG)).scalar() or 0)
        total_notes = int(db.execute(select(func.count()).select_from(INPATIENT_DAILY_NOTES)).scalar() or 0)
        total_alert_actions = int(db.execute(select(func.count()).select_from(ALERT_ACTION_METADATA)).scalar() or 0)
        total_alert_ack = int(
            db.execute(
                select(func.count()).select_from(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.ack_at.is_not(None))
            ).scalar()
            or 0
        )
        total_alert_resolved = int(
            db.execute(
                select(func.count()).select_from(ALERT_ACTION_METADATA).where(ALERT_ACTION_METADATA.c.resolved_at.is_not(None))
            ).scalar()
            or 0
        )
        devices_active = int(
            db.execute(
                select(func.count()).select_from(UROLOGY_DEVICES).where(UROLOGY_DEVICES.c.present == True)  # noqa: E712
            ).scalar()
            or 0
        )
        drain_types = [
            "PENROSE",
            "SARATOGA",
            "JACKSON",
            "NEFROSTOMIA",
            "CONDUCTO ILEAL",
            "URETEROSTOMA",
            "DRENAJE PELVICO",
        ]
        support_device_types = [
            "SONDA FOLEY",
            "CATETER JJ",
            "CATETER URETERAL",
            "CATETER URETERAL POR REPARACION POR FISTULA VESICOVAGINAL",
        ]
        with_drain = int(
            db.execute(
                select(func.count(func.distinct(UROLOGY_DEVICES.c.hospitalizacion_id))).where(
                    and_(
                        UROLOGY_DEVICES.c.present == True,  # noqa: E712
                        UROLOGY_DEVICES.c.device_type.in_(drain_types),
                        UROLOGY_DEVICES.c.hospitalizacion_id.is_not(None),
                    )
                )
            ).scalar()
            or 0
        )
        with_support_device = int(
            db.execute(
                select(func.count(func.distinct(UROLOGY_DEVICES.c.hospitalizacion_id))).where(
                    and_(
                        UROLOGY_DEVICES.c.present == True,  # noqa: E712
                        UROLOGY_DEVICES.c.device_type.in_(support_device_types),
                        UROLOGY_DEVICES.c.hospitalizacion_id.is_not(None),
                    )
                )
            ).scalar()
            or 0
        )
        active_hospitalizados = 0
        try:
            from app.core.app_context import main_proxy as m

            active_hospitalizados = int(
                db.execute(
                    select(func.count()).select_from(m.HospitalizacionDB).where(m.HospitalizacionDB.estatus == "ACTIVO")
                ).scalar()
                or 0
            )
        except Exception:
            active_hospitalizados = max(with_drain, with_support_device)
        without_drain = max(active_hospitalizados - with_drain, 0)
        without_support_device = max(active_hospitalizados - with_support_device, 0)

        drain_type_rows = db.execute(
            select(UROLOGY_DEVICES.c.device_type, func.count().label("n"))
            .where(and_(UROLOGY_DEVICES.c.present == True, UROLOGY_DEVICES.c.device_type.in_(drain_types)))  # noqa: E712
            .group_by(UROLOGY_DEVICES.c.device_type)
            .order_by(func.count().desc(), UROLOGY_DEVICES.c.device_type.asc())
        ).all()
        support_device_rows = db.execute(
            select(UROLOGY_DEVICES.c.device_type, func.count().label("n"))
            .where(and_(UROLOGY_DEVICES.c.present == True, UROLOGY_DEVICES.c.device_type.in_(support_device_types)))  # noqa: E712
            .group_by(UROLOGY_DEVICES.c.device_type)
            .order_by(func.count().desc(), UROLOGY_DEVICES.c.device_type.asc())
        ).all()
        drain_by_type = [{"tipo": str(r[0] or "N/E"), "n": int(r[1] or 0)} for r in drain_type_rows]
        support_device_by_type = [{"tipo": str(r[0] or "N/E"), "n": int(r[1] or 0)} for r in support_device_rows]

        notes_today = int(
            db.execute(
                select(func.count()).select_from(INPATIENT_DAILY_NOTES).where(
                    INPATIENT_DAILY_NOTES.c.note_date == today_date
                )
            ).scalar()
            or 0
        )
        events_today = int(
            db.execute(
                select(func.count()).select_from(CLINICAL_EVENT_LOG).where(
                    func.date(CLINICAL_EVENT_LOG.c.event_time) == today_iso
                )
            ).scalar()
            or 0
        )
        vitals_today = int(
            db.execute(
                select(func.count()).select_from(VITALS_TS).where(
                    func.date(VITALS_TS.c.recorded_at) == today_iso
                )
            ).scalar()
            or 0
        )
        io_today = int(
            db.execute(
                select(func.count()).select_from(IO_BLOCKS).where(
                    func.date(IO_BLOCKS.c.interval_start) == today_iso
                )
            ).scalar()
            or 0
        )
        labs_today = int(
            db.execute(
                select(func.count()).select_from(LAB_RESULTS).where(
                    func.date(LAB_RESULTS.c.collected_at) == today_iso
                )
            ).scalar()
            or 0
        )
        blocks_done = sum(1 for v in [notes_today > 0, events_today > 0, vitals_today > 0, io_today > 0, labs_today > 0] if v)
        completitud_hoy_pct = round((blocks_done / 5) * 100, 2)

        top_events_rows = db.execute(
            select(CLINICAL_EVENT_LOG.c.event_type, func.count().label("n"))
            .group_by(CLINICAL_EVENT_LOG.c.event_type)
            .order_by(func.count().desc(), CLINICAL_EVENT_LOG.c.event_type.asc())
            .limit(10)
        ).all()
        top_events = [{"event_type": str(r[0] or "N/E"), "n": int(r[1] or 0)} for r in top_events_rows]

        # --- Analítica mensual: índice urinario y soportes por sexo/edad/diagnóstico ---
        meta_by_hosp: Dict[int, Dict[str, Any]] = {}
        meta_by_consulta: Dict[int, Dict[str, Any]] = {}
        try:
            from app.core.app_context import main_proxy as m

            hosp_rows = (
                db.query(
                    m.HospitalizacionDB.id,
                    m.HospitalizacionDB.consulta_id,
                    m.HospitalizacionDB.nss,
                    m.HospitalizacionDB.sexo,
                    m.HospitalizacionDB.edad,
                    m.HospitalizacionDB.diagnostico,
                )
                .order_by(m.HospitalizacionDB.id.desc())
                .all()
            )
            for hr in hosp_rows:
                meta = {
                    "nss": str(getattr(hr, "nss", "") or "").strip(),
                    "sexo": str(getattr(hr, "sexo", "") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO",
                    "edad": getattr(hr, "edad", None),
                    "diagnostico": _norm_diag(getattr(hr, "diagnostico", None)),
                }
                hid = getattr(hr, "id", None)
                cid = getattr(hr, "consulta_id", None)
                if hid is not None and int(hid) not in meta_by_hosp:
                    meta_by_hosp[int(hid)] = meta
                if cid is not None and int(cid) not in meta_by_consulta:
                    meta_by_consulta[int(cid)] = meta
        except Exception:
            meta_by_hosp = {}
            meta_by_consulta = {}

        # Índice urinario (FOLEY) mensual.
        urinary_rows = db.execute(
            select(
                CLINICAL_EVENT_LOG.c.hospitalizacion_id,
                CLINICAL_EVENT_LOG.c.consulta_id,
                CLINICAL_EVENT_LOG.c.payload_json,
            ).where(
                and_(
                    CLINICAL_EVENT_LOG.c.event_type == "FOLEY_URESIS_RECORDED",
                    CLINICAL_EVENT_LOG.c.event_time >= datetime.combine(month_start, datetime.min.time()),
                    CLINICAL_EVENT_LOG.c.event_time < datetime.combine(month_end_excl, datetime.min.time()),
                )
            )
        ).mappings().all()
        by_patient_vals: Dict[str, List[float]] = {}
        by_sex_vals: Dict[str, List[float]] = {}
        by_age_vals: Dict[str, List[float]] = {}
        by_diag_vals: Dict[str, List[float]] = {}
        global_vals: List[float] = []
        for r in urinary_rows:
            payload = r.get("payload_json")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            idx = payload.get("urinary_index_ml_kg_h")
            try:
                idx_f = float(idx)
            except Exception:
                continue
            global_vals.append(idx_f)
            meta = meta_by_hosp.get(int(r["hospitalizacion_id"])) if r.get("hospitalizacion_id") is not None else None
            if meta is None and r.get("consulta_id") is not None:
                meta = meta_by_consulta.get(int(r["consulta_id"]))
            meta = meta or {"nss": f"CONSULTA-{r.get('consulta_id')}", "sexo": "NO_REGISTRADO", "edad": None, "diagnostico": "NO_REGISTRADO"}
            patient_key = str(meta.get("nss") or f"CONSULTA-{r.get('consulta_id')}")
            by_patient_vals.setdefault(patient_key, []).append(idx_f)
            by_sex_vals.setdefault(str(meta.get("sexo") or "NO_REGISTRADO"), []).append(idx_f)
            by_age_vals.setdefault(_age_group(meta.get("edad")), []).append(idx_f)
            by_diag_vals.setdefault(str(meta.get("diagnostico") or "NO_REGISTRADO"), []).append(idx_f)
        urinary_by_patient = [{"nss": k, "promedio": round(sum(v) / len(v), 4), "n": len(v)} for k, v in by_patient_vals.items()]
        urinary_by_patient.sort(key=lambda x: (-x["promedio"], x["nss"]))
        urinary_by_sex = [{"sexo": k, "promedio": round(sum(v) / len(v), 4), "n": len(v)} for k, v in by_sex_vals.items()]
        urinary_by_age = [{"grupo_edad": k, "promedio": round(sum(v) / len(v), 4), "n": len(v)} for k, v in by_age_vals.items()]
        urinary_by_diag = [{"diagnostico": k, "promedio": round(sum(v) / len(v), 4), "n": len(v)} for k, v in by_diag_vals.items()]
        urinary_by_sex.sort(key=lambda x: (-x["promedio"], x["sexo"]))
        urinary_by_age.sort(key=lambda x: (-x["promedio"], x["grupo_edad"]))
        urinary_by_diag.sort(key=lambda x: (-x["promedio"], x["diagnostico"]))

        # Drenajes/dispositivos mensuales por sexo/edad/diagnóstico.
        support_rows = db.execute(
            select(
                UROLOGY_DEVICES.c.hospitalizacion_id,
                UROLOGY_DEVICES.c.consulta_id,
                UROLOGY_DEVICES.c.device_type,
                UROLOGY_DEVICES.c.created_at,
            ).where(
                and_(
                    UROLOGY_DEVICES.c.created_at >= datetime.combine(month_start, datetime.min.time()),
                    UROLOGY_DEVICES.c.created_at < datetime.combine(month_end_excl, datetime.min.time()),
                )
            )
        ).mappings().all()
        drain_types_set = set(drain_types)
        support_types_set = set(support_device_types)
        drain_by_sex: Dict[str, int] = {}
        drain_by_age: Dict[str, int] = {}
        drain_by_diag: Dict[str, int] = {}
        support_by_sex: Dict[str, int] = {}
        support_by_age: Dict[str, int] = {}
        support_by_diag: Dict[str, int] = {}
        for r in support_rows:
            dtype = str(r.get("device_type") or "").upper()
            meta = meta_by_hosp.get(int(r["hospitalizacion_id"])) if r.get("hospitalizacion_id") is not None else None
            if meta is None and r.get("consulta_id") is not None:
                meta = meta_by_consulta.get(int(r["consulta_id"]))
            meta = meta or {"sexo": "NO_REGISTRADO", "edad": None, "diagnostico": "NO_REGISTRADO"}
            sexo = str(meta.get("sexo") or "NO_REGISTRADO")
            ageg = _age_group(meta.get("edad"))
            diag = str(meta.get("diagnostico") or "NO_REGISTRADO")
            if dtype in drain_types_set:
                drain_by_sex[sexo] = int(drain_by_sex.get(sexo, 0)) + 1
                drain_by_age[ageg] = int(drain_by_age.get(ageg, 0)) + 1
                drain_by_diag[diag] = int(drain_by_diag.get(diag, 0)) + 1
            if dtype in support_types_set:
                support_by_sex[sexo] = int(support_by_sex.get(sexo, 0)) + 1
                support_by_age[ageg] = int(support_by_age.get(ageg, 0)) + 1
                support_by_diag[diag] = int(support_by_diag.get(diag, 0)) + 1

        return {
            "ok": True,
            "total_vitals": total_vitals,
            "total_io": total_io,
            "total_labs": total_labs,
            "total_events": total_events,
            "total_daily_notes": total_notes,
            "alert_actions": {
                "total": total_alert_actions,
                "ack": total_alert_ack,
                "resolved": total_alert_resolved,
            },
            "active_devices": devices_active,
            "active_hospitalizados": active_hospitalizados,
            "drainage": {
                "with_drain": with_drain,
                "without_drain": without_drain,
                "by_type": drain_by_type,
            },
            "support_devices": {
                "with_device": with_support_device,
                "without_device": without_support_device,
                "by_type": support_device_by_type,
            },
            "urinary_index_month": {
                "promedio_global": round(sum(global_vals) / len(global_vals), 4) if global_vals else None,
                "by_patient": urinary_by_patient[:20],
                "by_sex": urinary_by_sex[:20],
                "by_age_group": urinary_by_age[:20],
                "by_diagnostico": urinary_by_diag[:20],
            },
            "drainage_month_breakdown": {
                "by_sex": _counter_to_rows(drain_by_sex, key_name="sexo"),
                "by_age_group": _counter_to_rows(drain_by_age, key_name="grupo_edad"),
                "by_diagnostico": _counter_to_rows(drain_by_diag, key_name="diagnostico"),
            },
            "support_devices_month_breakdown": {
                "by_sex": _counter_to_rows(support_by_sex, key_name="sexo"),
                "by_age_group": _counter_to_rows(support_by_age, key_name="grupo_edad"),
                "by_diagnostico": _counter_to_rows(support_by_diag, key_name="diagnostico"),
            },
            "today": {
                "notes": notes_today,
                "events": events_today,
                "vitals": vitals_today,
                "io": io_today,
                "labs": labs_today,
                "completitud_pct": completitud_hoy_pct,
            },
            "top_event_types": top_events,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "total_vitals": 0,
            "total_io": 0,
            "total_labs": 0,
            "total_events": 0,
            "total_daily_notes": 0,
            "alert_actions": {"total": 0, "ack": 0, "resolved": 0},
            "active_devices": 0,
            "active_hospitalizados": 0,
            "drainage": {"with_drain": 0, "without_drain": 0, "by_type": []},
            "support_devices": {"with_device": 0, "without_device": 0, "by_type": []},
            "urinary_index_month": {
                "promedio_global": None,
                "by_patient": [],
                "by_sex": [],
                "by_age_group": [],
                "by_diagnostico": [],
            },
            "drainage_month_breakdown": {"by_sex": [], "by_age_group": [], "by_diagnostico": []},
            "support_devices_month_breakdown": {"by_sex": [], "by_age_group": [], "by_diagnostico": []},
            "today": {"notes": 0, "events": 0, "vitals": 0, "io": 0, "labs": 0, "completitud_pct": 0},
            "top_event_types": [],
        }


def build_inpatient_structured_metrics(db: Session) -> Dict[str, Any]:
    return _build_inpatient_structured_metrics(db)


def _build_panel_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    total_pacientes = context.get("total", 0)
    total_onco = context.get("total_onco", 0)
    total_programados = context.get("total_programados", 0)
    total_realizadas = context.get("total_realizadas", 0)
    total_urgencias = context.get("total_urgencias_programadas", 0)
    completos = context.get("completos", 0)
    incompletos = context.get("incompletos", 0)
    consulta_ext_total = context.get("consulta_ext_total_atenciones", 0)
    consulta_ext_por_servicio = context.get("consulta_ext_por_servicio") or []
    consulta_ext_top = ["N/A", 0]
    if consulta_ext_por_servicio:
        try:
            consulta_ext_top = max(consulta_ext_por_servicio, key=lambda item: int(item[1] or 0))
        except Exception:
            consulta_ext_top = consulta_ext_por_servicio[0]

    ocupacion = (context.get("ocupacion_tendencia") or {}).get("promedio_pct")
    estancia_idx = (context.get("indice_estancia_prolongada") or {}).get("indice_pct")
    cancelacion = (context.get("cancelacion_global") or {}).get("tasa_pct")
    mediana_prog_real = (context.get("tiempo_programada_a_realizada") or {}).get("mediana_dias")
    aki = ((context.get("incidencia_laboratorios") or {}).get("aki_delta_creatinina") or {}).get("incidencia_pct")
    clostridium = ((context.get("incidencia_laboratorios") or {}).get("infeccion_clostridium") or {}).get("incidencia_pct")
    cohortes = context.get("cohortes_dinamicas") or {}
    embudo = context.get("embudo_operativo") or {}
    embudo_conv = context.get("embudo_conversion") or {}
    sangrado_mes = context.get("sangrado_metricas_mes") or {}
    top_cirujano, top_sangrado = _top_pair(sangrado_mes.get("cirujano_top"))
    top_proc, top_proc_sangrado = _top_pair(sangrado_mes.get("procedimiento_top"))
    cancelacion_conceptos = context.get("cancelacion_por_concepto") or []
    cancelacion_medicos = context.get("cancelacion_por_medico") or []
    top_cancel_concepto = cancelacion_conceptos[0] if cancelacion_conceptos else {}
    top_cancel_medico = cancelacion_medicos[0] if cancelacion_medicos else {}
    alertas = _build_prediccion_alertas(context)
    mi_stats = context.get("master_identity_stats") or {}
    mi_reingreso_pct = mi_stats.get("tasa_reingreso_window_pct")
    mi_recurrentes = mi_stats.get("top_recurrentes") or []
    top_recurrente = mi_recurrentes[0] if mi_recurrentes else {}
    top_recurrente_nss = top_recurrente.get("nss_canonico") or "N/A"
    top_recurrente_hosp = top_recurrente.get("hosp_ingresos_window") or 0
    structured = context.get("inpatient_structured_metrics") or {}
    structured_today = structured.get("today") or {}

    return {
        "hospitalizacion": {
            "titulo": "Hospitalización",
            "subtitulo": "Panorama operativo y seguridad intrahospitalaria",
            "kpis": [
                {"label": "Ingresos", "value": _fmt_num(embudo.get("ingreso"), "0")},
                {"label": "Altas", "value": _fmt_num(embudo.get("alta"), "0")},
                {"label": "Ocupación promedio", "value": _fmt_pct(ocupacion)},
                {"label": "Estancia prolongada", "value": _fmt_pct(estancia_idx)},
                {"label": "Incidencia AKI", "value": _fmt_pct(aki)},
                {"label": "Clostridium", "value": _fmt_pct(clostridium)},
                {"label": "Tasa reingreso (NSS)", "value": _fmt_pct(mi_reingreso_pct)},
                {"label": "Captura IA hoy", "value": _fmt_pct(structured_today.get("completitud_pct"))},
                {"label": "Notas estructuradas", "value": _fmt_num(structured.get("total_daily_notes"))},
            ],
            "insights": [
                f"Cohorte hospitalaria activa: oncológicos={_fmt_num(cohortes.get('oncologicos'), '0')}, litiasis={_fmt_num(cohortes.get('litiasis'), '0')}.",
                f"Embudo hospitalario Ingreso→Alta: {_fmt_num(embudo.get('ingreso'), '0')} → {_fmt_num(embudo.get('alta'), '0')}.",
                f"Conversión Postquirúrgica→Alta: {_fmt_pct(embudo_conv.get('postquirurgica_a_alta_pct'))}.",
                f"Top recurrencia por NSS: {top_recurrente_nss} con {_fmt_num(top_recurrente_hosp)} ingresos en ventana activa.",
                (
                    f"Captura estructurada hoy: notas={_fmt_num(structured_today.get('notes'))}, "
                    f"vitales={_fmt_num(structured_today.get('vitals'))}, io={_fmt_num(structured_today.get('io'))}, "
                    f"eventos={_fmt_num(structured_today.get('events'))}."
                ),
            ],
            "links": [
                {"label": "Seguridad y desenlaces", "target": "seccion-seguridad-clinica"},
                {"label": "Operación crítica", "target": "seccion-operacion-critica"},
                {"label": "Continuidad y reingresos", "target": "seccion-continuidad-reingresos"},
                {"label": "Epidemiología y calidad", "target": "seccion-epidemiologia"},
                {"label": "Hospitalización estructurada", "target": "seccion-hospitalizacion-estructurada"},
            ],
        },
        "quirofano": {
            "titulo": "Quirófano",
            "subtitulo": "Rendimiento quirúrgico, cancelación y sangrado",
            "kpis": [
                {"label": "Programadas", "value": _fmt_num(total_programados, "0")},
                {"label": "Realizadas", "value": _fmt_num(total_realizadas, "0")},
                {"label": "Urgencias", "value": _fmt_num(total_urgencias, "0")},
                {"label": "Cancelación global", "value": _fmt_pct(cancelacion)},
                {"label": "Mediana Prog→Real", "value": f"{_fmt_num(mediana_prog_real, 'N/A')} días"},
                {"label": "Top sangrado (mes)", "value": f"{top_cirujano} · {_fmt_num(top_sangrado)} ml"},
                {"label": "Top concepto cancelación", "value": str(top_cancel_concepto.get("codigo") or "N/A")},
                {"label": "Top médico cancelación", "value": str(top_cancel_medico.get("medico") or "N/A")},
            ],
            "insights": [
                f"Procedimiento con mayor sangrado total del mes: {top_proc} ({_fmt_num(top_proc_sangrado)} ml).",
                (
                    f"Concepto más frecuente de cancelación: {top_cancel_concepto.get('codigo')} · "
                    f"{top_cancel_concepto.get('concepto')} ({_fmt_num(top_cancel_concepto.get('canceladas'))} casos)."
                    if top_cancel_concepto
                    else "Sin cancelaciones con concepto registrado en el periodo."
                ),
                (
                    f"Médico con más diferimientos: {top_cancel_medico.get('medico')} "
                    f"({_fmt_num(top_cancel_medico.get('canceladas'))} cancelaciones)."
                    if top_cancel_medico
                    else "Sin desglose por médico en cancelaciones."
                ),
                "Priorizar balance programación/cancelación por procedimiento de alta carga.",
                "Revisar correlación sangrado por cirujano y complejidad (ECOG/Charlson/diagnóstico).",
            ],
            "links": [
                {"label": "Programación quirúrgica", "target": "seccion-programacion"},
                {"label": "Cancelaciones por concepto", "target": "seccion-cancelaciones-concepto"},
                {"label": "Bloque urgencias", "target": "seccion-urgencias"},
                {"label": "Bloque oncológico", "target": "seccion-oncologico"},
                {"label": "Bloque litiasis", "target": "seccion-litiasis"},
            ],
        },
        "consulta_externa": {
            "titulo": "Consulta Externa",
            "subtitulo": "Captura clínica, protocolos y carga asistencial",
            "kpis": [
                {"label": "Pacientes totales", "value": _fmt_num(total_pacientes, "0")},
                {"label": "Atenciones registradas", "value": _fmt_num(consulta_ext_total, "0")},
                {"label": "Oncológicos", "value": _fmt_num(total_onco, "0")},
                {"label": "Protocolos completos", "value": _fmt_num(completos, "0")},
                {"label": "Protocolos incompletos", "value": _fmt_num(incompletos, "0")},
                {"label": "Pendientes por programar", "value": _fmt_num(context.get("total_pendientes_programar"), "0")},
                {"label": "Conversión Ingreso→Programación", "value": _fmt_pct(embudo_conv.get("ingreso_a_programacion_pct"))},
            ],
            "insights": [
                "La consulta externa impacta directamente el embudo quirúrgico y la lista de espera.",
                "Controlar incompletitud de protocolo para reducir cancelaciones y tiempos de espera.",
                (
                    f"Servicio con mayor productividad: {consulta_ext_top[0]} "
                    f"({consulta_ext_top[1]} atenciones)."
                    if consulta_ext_por_servicio
                    else "Aún sin atenciones por servicio registradas en tabla operativa de consulta externa."
                ),
                "Vigilar cohorte oncológica para priorización oportuna y continuidad de atención.",
            ],
            "links": [
                {"label": "Resumen ejecutivo", "target": "resumen-ejecutivo"},
                {"label": "Consulta externa por servicio", "target": "seccion-consulta-externa"},
                {"label": "Programación quirúrgica", "target": "seccion-programacion"},
                {"label": "Análisis exploratorio", "target": "seccion-analisis-exploratorio"},
            ],
        },
        "curva_prediccion": {
            "titulo": "Curva de Predicción (fau_BOT)",
            "subtitulo": "Lecturas clínicas y patrones detectados sobre tendencias operativas",
            "kpis": [
                {"label": "Señales activas", "value": _fmt_num(len(alertas), "0")},
                {"label": "Conversión Prog→Real", "value": _fmt_pct(embudo_conv.get("programacion_a_realizada_pct"))},
                {"label": "Cancelación global", "value": _fmt_pct(cancelacion)},
                {"label": "AKI (ΔCr)", "value": _fmt_pct(aki)},
                {"label": "Estancia prolongada", "value": _fmt_pct(estancia_idx)},
                {"label": "Top cirujano sangrado", "value": str(top_cirujano)},
                {"label": "Reingreso (NSS)", "value": _fmt_pct(mi_reingreso_pct)},
            ],
            "insights": [f"[{item.get('nivel')}] {item.get('titulo')}: {item.get('detalle')}" for item in alertas],
            "links": [
                {"label": "Alertas clínicas", "target": "seccion-seguridad-clinica"},
                {"label": "Operación crítica", "target": "seccion-operacion-critica"},
                {"label": "Continuidad y reingresos", "target": "seccion-continuidad-reingresos"},
                {"label": "Epidemiología y calidad", "target": "seccion-epidemiologia"},
            ],
        },
    }


def _build_reporte_context(db: Session) -> Dict[str, Any]:
    from app.core.app_context import main_proxy as m
    from app.services.master_identity_flow import master_identity_operational_stats
    from app.services.reporte_bi_extracted import _default_consulta_ext_recommendations

    context = m.svc_agregar_timestamp(m.generar_reporte_bi(db))
    # Hardening aditivo: evita paneles vacíos cuando faltan llaves de contexto por fallback parcial.
    context.setdefault("consulta_ext_recomendaciones", _default_consulta_ext_recommendations())
    if not context.get("consulta_ext_recomendaciones"):
        context["consulta_ext_recomendaciones"] = _default_consulta_ext_recommendations()
    context.setdefault("cancelacion_por_concepto", [])
    context.setdefault("cancelacion_por_medico", [])
    context.setdefault("sangrado_metricas_mes", {})
    context.setdefault("sangrado_metricas_global", {})
    context.setdefault("embudo_operativo", {})
    context.setdefault("embudo_conversion", {})
    context.setdefault("incidencia_laboratorios", {})
    context.setdefault("indice_estancia_prolongada", {})
    context.setdefault("ocupacion_tendencia", {})
    context.setdefault("cohortes_dinamicas", {})
    context.setdefault("consulta_ext_por_servicio", [])
    context.setdefault("consulta_ext_total_atenciones", 0)
    try:
        context["master_identity_stats"] = master_identity_operational_stats(db, months=24, top_n=25)
    except Exception:
        context["master_identity_stats"] = {
            "ok": False,
            "window_months": 24,
            "total_master_identity": 0,
            "active_patients_window": 0,
            "total_events_window": 0,
            "hospitalizados_window": 0,
            "pacientes_reingresos_window": 0,
            "tasa_reingreso_window_pct": 0.0,
            "event_type_counts": {},
            "module_counts": {},
            "top_recurrentes": [],
        }
    context["inpatient_structured_metrics"] = _build_inpatient_structured_metrics(db)
    try:
        context["ui_error_summary"] = ui_error_summary(db, days=7, limit=12)
    except Exception:
        context["ui_error_summary"] = {
            "window_days": 7,
            "total": 0,
            "by_type": [],
            "by_path": [],
            "top_messages": [],
            "latest": [],
        }
    try:
        context["operability_metrics"] = metrics_snapshot(window_minutes=60)
    except Exception:
        context["operability_metrics"] = {
            "window_minutes": 60,
            "events": 0,
            "errors_5xx": 0,
            "error_rate_pct": 0.0,
            "latency": {"p95_ms": 0.0, "avg_ms": 0.0, "p50_ms": 0.0, "p99_ms": 0.0},
            "top_routes": [],
        }
    context["panel_payload"] = _build_panel_payload(context)
    return context


def _get_reporte_context_cached(db: Session, *, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force_refresh:
        with _REPORTE_CACHE_LOCK:
            expires_at = float(_REPORTE_CACHE.get("expires_at") or 0.0)
            cached_ctx = _REPORTE_CACHE.get("context")
            if cached_ctx is not None and now < expires_at:
                try:
                    out = copy.deepcopy(cached_ctx)
                except Exception:
                    out = dict(cached_ctx)
                out["cache_meta"] = {
                    "hit": True,
                    "ttl_sec": _REPORTE_CACHE_TTL_SEC,
                    "generated_at": _REPORTE_CACHE.get("generated_at"),
                }
                return out

    fresh = _build_reporte_context(db)
    with _REPORTE_CACHE_LOCK:
        _REPORTE_CACHE["context"] = copy.deepcopy(fresh)
        _REPORTE_CACHE["generated_at"] = datetime.now().isoformat(timespec="seconds")
        _REPORTE_CACHE["expires_at"] = time.time() + float(_REPORTE_CACHE_TTL_SEC)
    fresh["cache_meta"] = {
        "hit": False,
        "ttl_sec": _REPORTE_CACHE_TTL_SEC,
        "generated_at": _REPORTE_CACHE.get("generated_at"),
    }
    return fresh


def render_reporte_html(request: Request, db: Session) -> HTMLResponse:
    from app.core.app_context import main_proxy as m

    qp = getattr(request, "query_params", {})
    force_refresh = str(qp.get("nocache", "")).strip().lower() in {"1", "true", "yes"}
    context = _get_reporte_context_cached(db, force_refresh=force_refresh)
    return m.render_template(m.REPORTE_TEMPLATE, request=request, **context)


def render_qx_catalogos_json() -> JSONResponse:
    from app.core.app_context import main_proxy as m

    return JSONResponse(content=m.qx_catalogos_payload())
