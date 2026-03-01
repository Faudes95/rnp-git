from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.services.consulta_externa_flow import get_consulta_externa_stats


_DYNAMIC_SYMBOLS = [
    "ConsultaDB",
    "Integer",
    "Float",
    "SURVIVAL_EVENT_FIELD",
    "resolve_survival_event",
    "kaplan_meier",
    "LinearRegression",
    "np",
    "_new_surgical_session",
    "SurgicalProgramacionDB",
    "SurgicalUrgenciaProgramacionDB",
    "_build_pending_programar_dataset",
    "_build_realizadas_dataset",
    "SurgicalPostquirurgicaDB",
    "DimPaciente",
    "DataQualityLog",
    "count_by",
    "classify_age_group",
    "QUIROFANO_PROCEDIMIENTO_SUCCION",
    "_build_jj_metrics",
    "_build_hemoderivados_metrics",
    "build_programmed_age_counts",
    "_build_desglose_from_dict_rows",
    "EDAD_REPORTE_INDEX",
    "fig_to_base64",
    "plt",
    "HospitalizacionDB",
    "HospitalCensoDiarioDB",
    "LabDB",
    "HospitalGuardiaDB",
    "inspect",
    "text",
    "parse_int",
    "_hospital_stay_days",
    "_safe_pct",
    "_lab_key_from_text",
    "_parse_lab_numeric",
    "_lab_positive_clostridium",
    "_estimate_cancelation_risk",
    "_calc_percentile",
    "_distribution_stats_table",
    "_build_sangrado_metrics",
    "_extract_numeric_level",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing: List[str] = []
    for name in _DYNAMIC_SYMBOLS:
        if name in module_globals:
            continue
        try:
            module_globals[name] = getattr(m, name)
        except Exception:
            missing.append(name)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def _default_consulta_ext_recommendations() -> Dict[str, List[str]]:
    return {
        "CONSULTA EXTERNA": [
            "Registrar motivo de consulta y diagnóstico CIE de forma estructurada.",
            "Estandarizar medicación activa y alergias para continuidad terapéutica.",
            "Capturar plan y fecha objetivo de seguimiento para trazabilidad.",
        ],
        "UROENDOSCOPIA": [
            "Documentar tipo de procedimiento, hallazgos y complicaciones inmediatas.",
            "Registrar lateralidad, dispositivos (JJ/sonda) y plan de retiro.",
            "Relacionar resultado con diagnóstico principal para cohorte técnica.",
        ],
        "LEOCH": [
            "Capturar criterios de complejidad y motivo de referencia de alta prioridad.",
            "Registrar resultado clínico de la intervención y destino del paciente.",
            "Etiquetar estatus de protocolo para priorización quirúrgica.",
        ],
    }


def _merge_consulta_ext_recommendations(source: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    merged = _default_consulta_ext_recommendations()
    if not isinstance(source, dict):
        return merged
    for service, recs in source.items():
        key = str(service or "").strip().upper() or "NO_REGISTRADO"
        clean: List[str] = []
        if isinstance(recs, list):
            clean = [str(r).strip() for r in recs if str(r).strip()]
        merged[key] = clean or merged.get(key, [])
    return merged


def build_advanced_reporte_metrics(
    db: Session,
    *,
    consultas: List["ConsultaDB"],
    surgical_rows_all: List["SurgicalProgramacionDB"],
    realizadas_rows: List[Dict[str, Any]],
    postquirurgicas_rows: List["SurgicalPostquirurgicaDB"],
    dim_paciente_rows: List["DimPaciente"],
    data_quality_pending: int,
) -> Dict[str, Any]:
    _ensure_symbols()
    today_value = date.today()
    tracked_status = {"PROGRAMADA", "REALIZADA", "CANCELADA"}
    tracked_rows = [r for r in surgical_rows_all if (r.estatus or "").upper() in tracked_status]
    canceladas_rows = [r for r in tracked_rows if (r.estatus or "").upper() == "CANCELADA"]

    cancel_by_proc: Dict[str, Dict[str, int]] = {}
    for row in tracked_rows:
        proc = (row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        bucket = cancel_by_proc.setdefault(proc, {"total": 0, "canceladas": 0})
        bucket["total"] += 1
        if (row.estatus or "").upper() == "CANCELADA":
            bucket["canceladas"] += 1
    cancelacion_por_procedimiento = sorted(
        [
            {
                "procedimiento": proc,
                "total": vals["total"],
                "canceladas": vals["canceladas"],
                "tasa_pct": _safe_pct(vals["canceladas"], vals["total"]),
            }
            for proc, vals in cancel_by_proc.items()
        ],
        key=lambda item: (-float(item["tasa_pct"]), -int(item["canceladas"]), item["procedimiento"]),
    )[:25]

    cancel_by_concepto: Dict[Tuple[str, str, str], int] = {}
    cancel_by_medico: Dict[str, int] = {}
    cancel_by_diagnostico: Dict[str, int] = {}
    cancel_by_semana: Dict[str, int] = {}
    cancel_by_mes: Dict[str, int] = {}
    cancel_by_proc_concepto: Dict[Tuple[str, str], int] = {}
    cancel_detail_rows: List[Dict[str, Any]] = []

    for row in canceladas_rows:
        concepto = (getattr(row, "cancelacion_concepto", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        codigo = (getattr(row, "cancelacion_codigo", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        categoria = (getattr(row, "cancelacion_categoria", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        medico = (
            getattr(row, "agregado_medico", None)
            or getattr(row, "cirujano", None)
            or "NO_REGISTRADO"
        )
        medico = str(medico).strip().upper() or "NO_REGISTRADO"
        diagnostico = (
            getattr(row, "patologia", None)
            or getattr(row, "diagnostico_principal", None)
            or "NO_REGISTRADO"
        )
        diagnostico = str(diagnostico).strip().upper() or "NO_REGISTRADO"
        procedimiento = (
            getattr(row, "procedimiento_programado", None)
            or getattr(row, "procedimiento", None)
            or "NO_REGISTRADO"
        )
        procedimiento = str(procedimiento).strip().upper() or "NO_REGISTRADO"
        detalle = (getattr(row, "cancelacion_detalle", None) or "").strip().upper()
        fecha_cancel = (
            getattr(row, "cancelacion_fecha", None)
            or getattr(row, "actualizado_en", None)
            or getattr(row, "fecha_programada", None)
        )
        fecha_cancel_date = None
        if isinstance(fecha_cancel, datetime):
            fecha_cancel_date = fecha_cancel.date()
        elif isinstance(fecha_cancel, date):
            fecha_cancel_date = fecha_cancel
        semana_key = None
        mes_key = None
        if fecha_cancel_date is not None:
            iso = fecha_cancel_date.isocalendar()
            semana_key = f"{int(iso[0])}-S{int(iso[1]):02d}"
            mes_key = f"{fecha_cancel_date.year}-{fecha_cancel_date.month:02d}"
            cancel_by_semana[semana_key] = cancel_by_semana.get(semana_key, 0) + 1
            cancel_by_mes[mes_key] = cancel_by_mes.get(mes_key, 0) + 1

        key_concept = (codigo, concepto, categoria)
        cancel_by_concepto[key_concept] = cancel_by_concepto.get(key_concept, 0) + 1
        cancel_by_medico[medico] = cancel_by_medico.get(medico, 0) + 1
        cancel_by_diagnostico[diagnostico] = cancel_by_diagnostico.get(diagnostico, 0) + 1
        key_proc_concept = (procedimiento, concepto)
        cancel_by_proc_concepto[key_proc_concept] = cancel_by_proc_concepto.get(key_proc_concept, 0) + 1
        cancel_detail_rows.append(
            {
                "surgical_programacion_id": row.id,
                "consulta_id": row.consulta_id,
                "nss": (row.nss or "NO_REGISTRADO"),
                "paciente_nombre": (row.paciente_nombre or "NO_REGISTRADO"),
                "medico": medico,
                "diagnostico": diagnostico,
                "procedimiento": procedimiento,
                "codigo": codigo,
                "concepto": concepto,
                "categoria": categoria,
                "detalle": detalle or None,
                "fecha": fecha_cancel_date.isoformat() if fecha_cancel_date else None,
                "semana": semana_key,
                "mes": mes_key,
            }
        )

    cancelacion_por_concepto = sorted(
        [
            {
                "codigo": codigo,
                "concepto": concepto,
                "categoria": categoria,
                "canceladas": total,
                "tasa_sobre_canceladas_pct": _safe_pct(total, max(1, len(canceladas_rows))),
                "tasa_sobre_total_pct": _safe_pct(total, max(1, len(tracked_rows))),
            }
            for (codigo, concepto, categoria), total in cancel_by_concepto.items()
        ],
        key=lambda item: (-int(item["canceladas"]), item["codigo"], item["concepto"]),
    )[:50]
    cancelacion_por_medico = sorted(
        [
            {
                "medico": medico,
                "canceladas": total,
                "tasa_sobre_canceladas_pct": _safe_pct(total, max(1, len(canceladas_rows))),
            }
            for medico, total in cancel_by_medico.items()
        ],
        key=lambda item: (-int(item["canceladas"]), item["medico"]),
    )[:40]
    cancelacion_por_diagnostico = sorted(
        [{"diagnostico": diag, "canceladas": total} for diag, total in cancel_by_diagnostico.items()],
        key=lambda item: (-int(item["canceladas"]), item["diagnostico"]),
    )[:40]
    cancelacion_por_semana = sorted(
        [{"semana": semana, "canceladas": total} for semana, total in cancel_by_semana.items()],
        key=lambda item: item["semana"],
    )[-32:]
    cancelacion_por_mes = sorted(
        [{"mes": mes, "canceladas": total} for mes, total in cancel_by_mes.items()],
        key=lambda item: item["mes"],
    )[-24:]
    cancelacion_por_procedimiento_concepto = sorted(
        [
            {"procedimiento": proc, "concepto": concepto, "canceladas": total}
            for (proc, concepto), total in cancel_by_proc_concepto.items()
        ],
        key=lambda item: (-int(item["canceladas"]), item["procedimiento"], item["concepto"]),
    )[:60]

    delays_days: List[float] = []
    for row in surgical_rows_all:
        if (row.estatus or "").upper() != "REALIZADA":
            continue
        if not row.fecha_programada:
            continue
        fecha_real = row.fecha_realizacion or row.fecha_postquirurgica
        if fecha_real is None:
            continue
        delta = (fecha_real - row.fecha_programada).days
        if delta >= 0:
            delays_days.append(float(delta))

    procedure_by_consulta: Dict[int, str] = {}
    diagnosis_by_consulta: Dict[int, str] = {}
    hgz_by_consulta: Dict[int, str] = {}
    for row in sorted(
        surgical_rows_all,
        key=lambda r: (
            (r.actualizado_en or datetime.min),
            (r.creado_en or datetime.min),
            int(r.id or 0),
        ),
    ):
        if row.consulta_id is None:
            continue
        procedure_by_consulta[row.consulta_id] = (row.procedimiento_realizado or row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        diagnosis_by_consulta[row.consulta_id] = (row.patologia or row.diagnostico_principal or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        hgz_by_consulta[row.consulta_id] = (row.hgz or "NO_REGISTRADO").strip() or "NO_REGISTRADO"

    hospital_rows = db.query(HospitalizacionDB).all()
    estancia_values: List[float] = []
    estancia_diag: Dict[str, List[float]] = {}
    estancia_proc: Dict[str, List[float]] = {}
    estancia_hgz: Dict[str, List[float]] = {}
    long_stay_diag_counts: Dict[str, int] = {}
    long_stay_proc_counts: Dict[str, int] = {}
    long_stay_total = 0

    for row in hospital_rows:
        stay_days = _hospital_stay_days(row, today_value)
        if stay_days is None:
            continue
        stay_f = float(stay_days)
        estancia_values.append(stay_f)
        diag = (row.diagnostico or diagnosis_by_consulta.get(row.consulta_id) or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        proc = procedure_by_consulta.get(row.consulta_id, "NO_REGISTRADO")
        hgz = (row.hgz_envio or hgz_by_consulta.get(row.consulta_id) or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        estancia_diag.setdefault(diag, []).append(stay_f)
        estancia_proc.setdefault(proc, []).append(stay_f)
        estancia_hgz.setdefault(hgz, []).append(stay_f)
        if stay_days > 5:
            long_stay_total += 1
            long_stay_diag_counts[diag] = long_stay_diag_counts.get(diag, 0) + 1
            long_stay_proc_counts[proc] = long_stay_proc_counts.get(proc, 0) + 1

    total_camas = max(1, parse_int(os.getenv("HOSPITAL_TOTAL_CAMAS", "40")) or 40)
    censo_rows = (
        db.query(HospitalCensoDiarioDB)
        .order_by(HospitalCensoDiarioDB.fecha.asc())
        .all()
    )
    daily_occupancy: List[Dict[str, Any]] = []
    for row in censo_rows:
        hosp_count = int(row.total_hospitalizados or 0)
        daily_occupancy.append(
            {
                "fecha": row.fecha.isoformat() if row.fecha else None,
                "hospitalizados": hosp_count,
                "ocupacion_pct": round((hosp_count / float(total_camas)) * 100.0, 2),
            }
        )
    if not daily_occupancy:
        min_date = None
        max_date = None
        for row in hospital_rows:
            if row.fecha_ingreso is None:
                continue
            if min_date is None or row.fecha_ingreso < min_date:
                min_date = row.fecha_ingreso
            end = row.fecha_egreso or today_value
            if max_date is None or end > max_date:
                max_date = end
        if min_date and max_date:
            cursor = min_date
            while cursor <= max_date:
                active = 0
                for row in hospital_rows:
                    if row.fecha_ingreso is None:
                        continue
                    end = row.fecha_egreso or today_value
                    if row.fecha_ingreso <= cursor <= end:
                        active += 1
                daily_occupancy.append(
                    {
                        "fecha": cursor.isoformat(),
                        "hospitalizados": active,
                        "ocupacion_pct": round((active / float(total_camas)) * 100.0, 2),
                    }
                )
                cursor += timedelta(days=1)

    weekly_occ_map: Dict[str, List[float]] = {}
    monthly_occ_map: Dict[str, List[float]] = {}
    for item in daily_occupancy:
        fecha_raw = item.get("fecha")
        if not fecha_raw:
            continue
        try:
            fecha_obj = datetime.strptime(str(fecha_raw), "%Y-%m-%d").date()
        except Exception:
            continue
        y, week_num, _ = fecha_obj.isocalendar()
        week_key = f"{y}-S{int(week_num):02d}"
        month_key = fecha_obj.strftime("%Y-%m")
        weekly_occ_map.setdefault(week_key, []).append(float(item.get("ocupacion_pct") or 0.0))
        monthly_occ_map.setdefault(month_key, []).append(float(item.get("ocupacion_pct") or 0.0))

    weekly_occupancy = [
        {"semana": k, "ocupacion_promedio_pct": round(sum(v) / len(v), 2)}
        for k, v in sorted(weekly_occ_map.items())
        if v
    ]
    monthly_occupancy = [
        {"mes": k, "ocupacion_promedio_pct": round(sum(v) / len(v), 2)}
        for k, v in sorted(monthly_occ_map.items())
        if v
    ]

    tendencia_ocupacion = "ESTABLE"
    if len(monthly_occupancy) >= 2:
        prev_val = float(monthly_occupancy[-2]["ocupacion_promedio_pct"])
        last_val = float(monthly_occupancy[-1]["ocupacion_promedio_pct"])
        if (last_val - prev_val) >= 2.0:
            tendencia_ocupacion = "ALZA"
        elif (prev_val - last_val) >= 2.0:
            tendencia_ocupacion = "BAJA"

    labs_rows = db.query(LabDB).all()
    creat_by_consulta: Dict[int, List[float]] = {}
    all_lab_consultas: set = set()
    anemia_ids_10: set = set()
    anemia_ids_8: set = set()
    leuco_ids: set = set()
    plaquetas_ids: set = set()
    sodio_dis_ids: set = set()
    potasio_dis_ids: set = set()
    clostridium_ids: set = set()

    for lab in labs_rows:
        consulta_id = lab.consulta_id
        if consulta_id is None:
            continue
        all_lab_consultas.add(int(consulta_id))
        metric_key = _lab_key_from_text(lab.test_name, lab.test_code)
        if metric_key is None:
            continue
        num_val = _parse_lab_numeric(lab.value)
        if metric_key == "clostridium":
            if _lab_positive_clostridium(lab.value):
                clostridium_ids.add(int(consulta_id))
            continue
        if num_val is None:
            continue
        if metric_key == "creatinina":
            creat_by_consulta.setdefault(int(consulta_id), []).append(float(num_val))
        elif metric_key == "hemoglobina":
            if num_val < 10.0:
                anemia_ids_10.add(int(consulta_id))
            if num_val < 8.0:
                anemia_ids_8.add(int(consulta_id))
        elif metric_key == "leucocitos":
            if num_val > 10000:
                leuco_ids.add(int(consulta_id))
        elif metric_key == "plaquetas":
            if num_val < 150:
                plaquetas_ids.add(int(consulta_id))
        elif metric_key == "sodio":
            if num_val < 135 or num_val > 145:
                sodio_dis_ids.add(int(consulta_id))
        elif metric_key == "potasio":
            if num_val < 3.5 or num_val > 5.0:
                potasio_dis_ids.add(int(consulta_id))

    for row in hospital_rows:
        consulta_id = row.consulta_id
        if consulta_id is None:
            continue
        txt = f"{(row.diagnostico or '').lower()} {(row.observaciones or '').lower()}"
        if re.search(r"clostr|c\W*difficile|c\W*dif", txt):
            clostridium_ids.add(int(consulta_id))

    aki_ids: set = set()
    for consulta_id, values in creat_by_consulta.items():
        if len(values) < 2:
            continue
        if (max(values) - min(values)) >= 0.3:
            aki_ids.add(int(consulta_id))

    denom_labs = max(1, len(all_lab_consultas))
    incidencia_labs = {
        "denominador_labs_pacientes": len(all_lab_consultas),
        "aki_delta_creatinina": {"pacientes": len(aki_ids), "incidencia_pct": _safe_pct(len(aki_ids), denom_labs)},
        "anemia_hb_menor_10": {"pacientes": len(anemia_ids_10), "incidencia_pct": _safe_pct(len(anemia_ids_10), denom_labs)},
        "anemia_hb_menor_8": {"pacientes": len(anemia_ids_8), "incidencia_pct": _safe_pct(len(anemia_ids_8), denom_labs)},
        "leucocitosis_mayor_10000": {"pacientes": len(leuco_ids), "incidencia_pct": _safe_pct(len(leuco_ids), denom_labs)},
        "trombocitopenia_plt_menor_150": {"pacientes": len(plaquetas_ids), "incidencia_pct": _safe_pct(len(plaquetas_ids), denom_labs)},
        "disnatremias": {"pacientes": len(sodio_dis_ids), "incidencia_pct": _safe_pct(len(sodio_dis_ids), denom_labs)},
        "dispotasemias": {"pacientes": len(potasio_dis_ids), "incidencia_pct": _safe_pct(len(potasio_dis_ids), denom_labs)},
        "infeccion_clostridium": {"pacientes": len(clostridium_ids), "incidencia_pct": _safe_pct(len(clostridium_ids), denom_labs)},
    }

    riesgo_map: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for row in surgical_rows_all:
        edad_grupo = classify_age_group(row.edad)
        ecog = (row.ecog or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        charlson = (row.charlson or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        diag = (row.patologia or row.diagnostico_principal or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        proc = (row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        key = (edad_grupo, ecog, charlson, diag, proc)
        risk_value = row.riesgo_cancelacion_predicho
        if risk_value is None:
            risk_value = _estimate_cancelation_risk(
                edad=row.edad,
                ecog=row.ecog,
                charlson=row.charlson,
                dias_espera=row.dias_en_espera,
                requiere_intermed=row.requiere_intermed,
            )
        bucket = riesgo_map.setdefault(key, {"cantidad": 0, "sum_riesgo": 0.0})
        bucket["cantidad"] += 1
        bucket["sum_riesgo"] += float(risk_value or 0.0)
    riesgo_cruzado_top = sorted(
        [
            {
                "edad_grupo": k[0],
                "ecog": k[1],
                "charlson": k[2],
                "diagnostico": k[3],
                "procedimiento": k[4],
                "cantidad": v["cantidad"],
                "riesgo_promedio": round(v["sum_riesgo"] / max(1, v["cantidad"]), 3),
            }
            for k, v in riesgo_map.items()
        ],
        key=lambda item: (-int(item["cantidad"]), -float(item["riesgo_promedio"]), item["diagnostico"]),
    )[:120]

    urgencias_hosp = sum(1 for row in hospital_rows if (row.ingreso_tipo or "").upper() == "URGENCIA")
    urgencias_hosp += sum(1 for row in surgical_rows_all if (row.modulo_origen or "").upper() == "QUIROFANO_URGENCIA")
    programadas = sum(1 for row in surgical_rows_all if (row.estatus or "").upper() == "PROGRAMADA")
    onco_count = sum(1 for row in surgical_rows_all if (row.grupo_patologia or "").upper() == "ONCOLOGICO")
    litiasis_count = sum(1 for row in surgical_rows_all if (row.grupo_patologia or "").upper() == "LITIASIS_URINARIA")
    complicadas_postqx = 0
    for row in postquirurgicas_rows:
        txt = (row.complicaciones or "").strip().lower()
        if txt and not re.search(r"^sin\b|sin complic", txt):
            complicadas_postqx += 1

    alta_total = sum(1 for row in hospital_rows if row.fecha_egreso is not None or (row.estatus or "").upper() == "EGRESADO")
    programacion_total = len(tracked_rows)
    cirugias_realizadas_total = sum(1 for row in surgical_rows_all if (row.estatus or "").upper() == "REALIZADA")
    postqx_total = len(postquirurgicas_rows)
    ingresos_total = len(hospital_rows)

    embudo = {
        "ingreso": ingresos_total,
        "programacion": programacion_total,
        "cirugia_realizada": cirugias_realizadas_total,
        "postquirurgica": postqx_total,
        "alta": alta_total,
    }
    embudo_conversion = {
        "ingreso_a_programacion_pct": _safe_pct(programacion_total, max(1, ingresos_total)),
        "programacion_a_realizada_pct": _safe_pct(cirugias_realizadas_total, max(1, programacion_total)),
        "realizada_a_postquirurgica_pct": _safe_pct(postqx_total, max(1, cirugias_realizadas_total)),
        "postquirurgica_a_alta_pct": _safe_pct(alta_total, max(1, postqx_total)),
    }

    guardias_rows = db.query(HospitalGuardiaDB).all()
    productivity_turno: Dict[str, int] = {}
    productivity_medico: Dict[str, int] = {}
    for g in guardias_rows:
        turno = (g.turno or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        medico = (g.medico or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        productivity_turno[turno] = productivity_turno.get(turno, 0) + 1
        productivity_medico[medico] = productivity_medico.get(medico, 0) + 1

    productivity_dataset: List[Dict[str, Any]] = []
    guardia_identity_completitud = None
    try:
        inspector = inspect(db.bind)
        if "hospital_guardia_registros" in inspector.get_table_names():
            dataset_rows = db.execute(
                text(
                    "SELECT dataset, COUNT(*) AS total FROM hospital_guardia_registros GROUP BY dataset ORDER BY total DESC"
                )
            ).fetchall()
            productivity_dataset = [{"dataset": str(r[0]), "total": int(r[1])} for r in dataset_rows]
            identity = db.execute(
                text(
                    "SELECT COUNT(*) AS total, "
                    "SUM(CASE WHEN COALESCE(TRIM(COALESCE(nss,'')), '') <> '' OR COALESCE(TRIM(COALESCE(nombre,'')), '') <> '' OR COALESCE(TRIM(COALESCE(cama,'')), '') <> '' THEN 1 ELSE 0 END) AS filled "
                    "FROM hospital_guardia_registros"
                )
            ).fetchone()
            if identity:
                total_reg = int(identity[0] or 0)
                filled_reg = int(identity[1] or 0)
                guardia_identity_completitud = {
                    "total_registros": total_reg,
                    "con_identificadores": filled_reg,
                    "completitud_pct": _safe_pct(filled_reg, max(1, total_reg)),
                }
    except Exception:
        productivity_dataset = []

    alcaldia_counts_map: Dict[str, int] = {}
    for c in consultas:
        alcaldia = (c.alcaldia or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        alcaldia_counts_map[alcaldia] = alcaldia_counts_map.get(alcaldia, 0) + 1

    hgz_density_map: Dict[str, int] = {}
    for row in surgical_rows_all:
        hgz = (row.hgz or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        hgz_density_map[hgz] = hgz_density_map.get(hgz, 0) + 1
    for row in hospital_rows:
        hgz = (row.hgz_envio or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        hgz_density_map[hgz] = hgz_density_map.get(hgz, 0) + 1

    points_geocoded = 0
    for p in dim_paciente_rows:
        if p.lat is not None and p.lon is not None:
            points_geocoded += 1

    hosp_required = ["cama", "nombre_completo", "nss", "edad", "sexo", "diagnostico", "hgz_envio", "ingreso_tipo", "estatus_detalle"]
    surg_required = ["nss", "paciente_nombre", "edad", "sexo", "patologia", "procedimiento_programado", "hgz", "estatus"]

    def _row_fill_pct(row_obj: Any, keys: List[str]) -> float:
        total_keys = len(keys)
        if total_keys <= 0:
            return 0.0
        filled = 0
        for k in keys:
            val = getattr(row_obj, k, None)
            if val is None:
                continue
            if isinstance(val, str) and not val.strip():
                continue
            filled += 1
        return _safe_pct(filled, total_keys)

    hosp_fill_values = [_row_fill_pct(r, hosp_required) for r in hospital_rows] if hospital_rows else []
    surg_fill_values = [_row_fill_pct(r, surg_required) for r in surgical_rows_all] if surgical_rows_all else []

    chart_cancelacion_proc = None
    chart_cancelacion_concepto = None
    chart_ocupacion_mensual = None
    chart_labs_incidencia = None
    if plt is not None:
        if cancelacion_por_procedimiento:
            top_cancel = cancelacion_por_procedimiento[:10]
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar([x["procedimiento"] for x in top_cancel], [x["tasa_pct"] for x in top_cancel], color="#7f2d2d")
            ax.set_title("Tasa de cancelacion por procedimiento (%)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_cancelacion_proc = fig_to_base64(fig)
            plt.close(fig)
        if cancelacion_por_concepto:
            top_concept = cancelacion_por_concepto[:10]
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar([x["codigo"] for x in top_concept], [x["canceladas"] for x in top_concept], color="#9b6a2d")
            ax.set_title("Cancelaciones por concepto (top)")
            ax.tick_params(axis="x", rotation=30)
            fig.tight_layout()
            chart_cancelacion_concepto = fig_to_base64(fig)
            plt.close(fig)
        if monthly_occupancy:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(
                [x["mes"] for x in monthly_occupancy],
                [x["ocupacion_promedio_pct"] for x in monthly_occupancy],
                marker="o",
                color="#13322B",
            )
            ax.set_title("Ocupacion promedio mensual (%)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_ocupacion_mensual = fig_to_base64(fig)
            plt.close(fig)
        labels_labs = [
            "AKI dCr",
            "Hb<10",
            "Hb<8",
            "Leuco>10k",
            "Plt<150",
            "DisNa",
            "DisK",
            "Clostridium",
        ]
        values_labs = [
            incidencia_labs["aki_delta_creatinina"]["pacientes"],
            incidencia_labs["anemia_hb_menor_10"]["pacientes"],
            incidencia_labs["anemia_hb_menor_8"]["pacientes"],
            incidencia_labs["leucocitosis_mayor_10000"]["pacientes"],
            incidencia_labs["trombocitopenia_plt_menor_150"]["pacientes"],
            incidencia_labs["disnatremias"]["pacientes"],
            incidencia_labs["dispotasemias"]["pacientes"],
            incidencia_labs["infeccion_clostridium"]["pacientes"],
        ]
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.bar(labels_labs, values_labs, color="#B38E5D")
        ax.set_title("Incidencias clinicas por laboratorio")
        ax.tick_params(axis="x", rotation=30)
        fig.tight_layout()
        chart_labs_incidencia = fig_to_base64(fig)
        plt.close(fig)

    sangrado_mes = _build_sangrado_metrics(
        realizadas_rows,
        anio=today_value.year,
        mes=today_value.month,
        top_n=30,
    )
    sangrado_global = _build_sangrado_metrics(
        realizadas_rows,
        anio=None,
        mes=None,
        top_n=30,
    )

    return {
        "cancelacion_global": {
            "total_con_estatus": len(tracked_rows),
            "canceladas": len(canceladas_rows),
            "tasa_pct": _safe_pct(len(canceladas_rows), max(1, len(tracked_rows))),
        },
        "cancelacion_por_procedimiento": cancelacion_por_procedimiento,
        "cancelacion_por_concepto": cancelacion_por_concepto,
        "cancelacion_por_medico": cancelacion_por_medico,
        "cancelacion_por_diagnostico": cancelacion_por_diagnostico,
        "cancelacion_por_semana": cancelacion_por_semana,
        "cancelacion_por_mes": cancelacion_por_mes,
        "cancelacion_por_procedimiento_concepto": cancelacion_por_procedimiento_concepto,
        "cancelacion_detalle_rows": cancel_detail_rows[:300],
        "tiempo_programada_a_realizada": {
            "n": len(delays_days),
            "promedio_dias": round(sum(delays_days) / len(delays_days), 2) if delays_days else None,
            "mediana_dias": _calc_percentile(delays_days, 50),
            "p90_dias": _calc_percentile(delays_days, 90),
        },
        "estancia_global": {
            "n": len(estancia_values),
            "promedio_dias": round(sum(estancia_values) / len(estancia_values), 2) if estancia_values else None,
            "mediana_dias": _calc_percentile(estancia_values, 50),
            "p90_dias": _calc_percentile(estancia_values, 90),
        },
        "estancia_por_diagnostico": _distribution_stats_table(estancia_diag),
        "estancia_por_procedimiento": _distribution_stats_table(estancia_proc),
        "estancia_por_hgz": _distribution_stats_table(estancia_hgz),
        "ocupacion_tendencia": {
            "promedio_pct": round(sum(x["ocupacion_pct"] for x in daily_occupancy) / len(daily_occupancy), 2) if daily_occupancy else None,
            "trend": tendencia_ocupacion,
            "daily": daily_occupancy[-90:],
            "weekly": weekly_occupancy[-24:],
            "monthly": monthly_occupancy[-24:],
        },
        "indice_estancia_prolongada": {
            "total_hospitalizados": len(hospital_rows),
            "prolongadas": long_stay_total,
            "indice_pct": _safe_pct(long_stay_total, max(1, len(hospital_rows))),
            "por_diagnostico": sorted(long_stay_diag_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
            "por_procedimiento": sorted(long_stay_proc_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
        },
        "incidencia_laboratorios": incidencia_labs,
        "riesgo_cruzado_top": riesgo_cruzado_top,
        "cohortes_dinamicas": {
            "oncologicos": onco_count,
            "litiasis": litiasis_count,
            "urgencias": urgencias_hosp,
            "programados": programadas,
            "postqx_complicados": complicadas_postqx,
        },
        "embudo_operativo": embudo,
        "embudo_conversion": embudo_conversion,
        "productividad_guardia": {
            "por_turno": sorted(productivity_turno.items(), key=lambda kv: (-kv[1], kv[0])),
            "por_equipo_medico": sorted(productivity_medico.items(), key=lambda kv: (-kv[1], kv[0]))[:25],
            "por_dataset_guardia": productivity_dataset,
        },
        "mapa_epidemiologico": {
            "por_hgz": sorted(hgz_density_map.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
            "por_alcaldia": sorted(alcaldia_counts_map.items(), key=lambda kv: (-kv[1], kv[0]))[:20],
            "puntos_geocodificados": points_geocoded,
        },
        "calidad_captura": {
            "hospitalizacion_completitud_pct": round(sum(hosp_fill_values) / len(hosp_fill_values), 2) if hosp_fill_values else None,
            "quirurgico_completitud_pct": round(sum(surg_fill_values) / len(surg_fill_values), 2) if surg_fill_values else None,
            "guardia_identidad": guardia_identity_completitud,
            "data_quality_pendiente": int(data_quality_pending or 0),
        },
        "desenlaces_postquirurgicos": {
            "reingreso_30d_si": sum(1 for r in realizadas_rows if str(r.get("reingreso_30d") or "").upper() == "SI"),
            "reintervencion_30d_si": sum(1 for r in realizadas_rows if str(r.get("reintervencion_30d") or "").upper() == "SI"),
            "mortalidad_30d_si": sum(1 for r in realizadas_rows if str(r.get("mortalidad_30d") or "").upper() == "SI"),
            "reingreso_90d_si": sum(1 for r in realizadas_rows if str(r.get("reingreso_90d") or "").upper() == "SI"),
            "reintervencion_90d_si": sum(1 for r in realizadas_rows if str(r.get("reintervencion_90d") or "").upper() == "SI"),
            "mortalidad_90d_si": sum(1 for r in realizadas_rows if str(r.get("mortalidad_90d") or "").upper() == "SI"),
            "clavien_dindo": dict(count_by(realizadas_rows, lambda r: r.get("clavien_dindo") or "NO_REGISTRADO")),
            "transfusion": dict(count_by(realizadas_rows, lambda r: r.get("transfusion") or "NO_REGISTRADO")),
            "margen_quirurgico": dict(count_by(realizadas_rows, lambda r: r.get("margen_quirurgico") or "NO_REGISTRADO")),
            "neuropreservacion": dict(count_by(realizadas_rows, lambda r: r.get("neuropreservacion") or "NO_REGISTRADO")),
            "linfadenectomia": dict(count_by(realizadas_rows, lambda r: r.get("linfadenectomia") or "NO_REGISTRADO")),
            "stone_free": dict(count_by(realizadas_rows, lambda r: r.get("stone_free") or "NO_REGISTRADO")),
            "recurrencia_litiasis": dict(count_by(realizadas_rows, lambda r: r.get("recurrencia_litiasis") or "NO_REGISTRADO")),
        },
        "sangrado_metricas_mes": sangrado_mes,
        "sangrado_metricas_global": sangrado_global,
        "charts_advanced": {
            "cancelacion_proc": chart_cancelacion_proc,
            "cancelacion_concepto": chart_cancelacion_concepto,
            "ocupacion_mensual": chart_ocupacion_mensual,
            "incidencias_labs": chart_labs_incidencia,
        },
    }


def build_reporte_bi(db: Session) -> Dict[str, Any]:
    _ensure_symbols()
    consultas = db.query(ConsultaDB).all()
    total = len(consultas)
    total_onco = len([c for c in consultas if (c.diagnostico_principal or "").startswith("ca_")])
    completos = len([c for c in consultas if c.estatus_protocolo == "completo"])
    incompletos = len([c for c in consultas if c.estatus_protocolo == "incompleto"])

    notice = ""
    numeric_charts = []
    chart_diagnosticos = None
    chart_survival = None
    chart_waitlist = None

    if plt is None:
        notice = "Matplotlib no disponible. Instale matplotlib para generar gráficas."
        return {
            "total": total,
            "total_onco": total_onco,
            "completos": completos,
            "incompletos": incompletos,
            "numeric_charts": numeric_charts,
            "chart_diagnosticos": chart_diagnosticos,
            "chart_survival": chart_survival,
            "chart_waitlist": chart_waitlist,
            "notice": notice,
            "total_programados": 0,
            "total_urgencias_programadas": 0,
            "total_pendientes_programar": 0,
            "total_realizadas": 0,
            "pendientes_programar_rows": [],
            "realizadas_rows": [],
            "pendientes_por_edad": [],
            "pendientes_por_sexo": [],
            "pendientes_por_nss": [],
            "pendientes_por_hgz": [],
            "pendientes_por_procedimiento": [],
            "pendientes_por_diagnostico": [],
            "realizadas_por_edad": [],
            "realizadas_por_sexo": [],
            "realizadas_por_nss": [],
            "realizadas_por_hgz": [],
            "realizadas_por_procedimiento": [],
            "realizadas_por_diagnostico": [],
            "realizadas_por_cirujano": [],
            "realizadas_por_sangrado": [],
            "sexo_counts": [],
            "patologias_counts": [],
            "procedimientos_counts": [],
            "hgz_counts": [],
            "onco_diag_counts": [],
            "onco_ecog_counts": [],
            "onco_charlson_counts": [],
            "onco_edad_counts": [],
            "onco_pacientes": [],
            "litiasis_diag_counts": [],
            "litiasis_uh_counts": [],
            "litiasis_tamano_counts": [],
            "litiasis_subtipo_counts": [],
            "litiasis_ubicacion_counts": [],
            "litiasis_hidronefrosis_counts": [],
            "procedimiento_abordaje_counts": [],
            "succion_counts": [],
            "intermed_por_procedimiento": [],
            "urg_sexo_counts": [],
            "urg_patologias_counts": [],
            "urg_procedimientos_counts": [],
            "urg_hgz_counts": [],
            "urg_insumos_intermed": [],
            "urg_onco_diag_counts": [],
            "urg_onco_ecog_counts": [],
            "urg_onco_charlson_counts": [],
            "urg_onco_edad_counts": [],
            "urg_litiasis_diag_counts": [],
            "urg_litiasis_uh_counts": [],
            "urg_litiasis_tamano_counts": [],
            "urg_litiasis_subtipo_counts": [],
            "urg_litiasis_ubicacion_counts": [],
            "urg_litiasis_hidronefrosis_counts": [],
            "jj_metricas": {"total_jj_colocados": 0, "por_origen": [], "por_medico": [], "por_procedimiento": [], "por_semana": [], "por_mes": []},
            "hemoderivados_metricas": {
                "solicitudes_urgencias_total": 0,
                "solicitudes_programadas_total": 0,
                "solicitudes_urgencias_unidades": {"pg": 0, "pfc": 0, "cp": 0},
                "solicitudes_programadas_unidades": {"pg": 0, "pfc": 0, "cp": 0},
                "solicitudes_urgencias_por_procedimiento": [],
                "solicitudes_programadas_por_procedimiento": [],
                "solicitudes_urgencias_por_hgz": [],
                "solicitudes_programadas_por_hgz": [],
                "solicitudes_urgencias_rows": [],
                "solicitudes_programadas_rows": [],
                "uso_total_cirugias_realizadas": 0,
                "uso_unidades_totales": {"pg": 0, "pfc": 0, "cp": 0},
                "uso_por_origen": [],
                "uso_unidades_por_cirujano": [],
                "uso_unidades_por_procedimiento": [],
            },
            "urg_hemoderivados_rows": [],
            "prog_hemoderivados_rows": [],
            "edad_programados_counts": [],
            "chart_edad_combinada": None,
            "edad_combinado_counts": [],
            "cancelacion_global": {"total_con_estatus": 0, "canceladas": 0, "tasa_pct": 0.0},
            "cancelacion_por_procedimiento": [],
            "cancelacion_por_concepto": [],
            "cancelacion_por_medico": [],
            "cancelacion_por_diagnostico": [],
            "cancelacion_por_semana": [],
            "cancelacion_por_mes": [],
            "cancelacion_por_procedimiento_concepto": [],
            "cancelacion_detalle_rows": [],
            "tiempo_programada_a_realizada": {"n": 0, "promedio_dias": None, "mediana_dias": None, "p90_dias": None},
            "estancia_global": {"n": 0, "promedio_dias": None, "mediana_dias": None, "p90_dias": None},
            "estancia_por_diagnostico": [],
            "estancia_por_procedimiento": [],
            "estancia_por_hgz": [],
            "ocupacion_tendencia": {"promedio_pct": None, "trend": "ESTABLE", "daily": [], "weekly": [], "monthly": []},
            "indice_estancia_prolongada": {"total_hospitalizados": 0, "prolongadas": 0, "indice_pct": 0.0, "por_diagnostico": [], "por_procedimiento": []},
            "incidencia_laboratorios": {},
            "riesgo_cruzado_top": [],
            "cohortes_dinamicas": {"oncologicos": 0, "litiasis": 0, "urgencias": 0, "programados": 0, "postqx_complicados": 0},
            "embudo_operativo": {"ingreso": 0, "programacion": 0, "cirugia_realizada": 0, "postquirurgica": 0, "alta": 0},
            "embudo_conversion": {
                "ingreso_a_programacion_pct": 0.0,
                "programacion_a_realizada_pct": 0.0,
                "realizada_a_postquirurgica_pct": 0.0,
                "postquirurgica_a_alta_pct": 0.0,
            },
            "productividad_guardia": {"por_turno": [], "por_equipo_medico": [], "por_dataset_guardia": []},
            "mapa_epidemiologico": {"por_hgz": [], "por_alcaldia": [], "puntos_geocodificados": 0},
            "calidad_captura": {
                "hospitalizacion_completitud_pct": None,
                "quirurgico_completitud_pct": None,
                "guardia_identidad": None,
                "data_quality_pendiente": 0,
            },
            "desenlaces_postquirurgicos": {
                "reingreso_30d_si": 0,
                "reintervencion_30d_si": 0,
                "mortalidad_30d_si": 0,
                "reingreso_90d_si": 0,
                "reintervencion_90d_si": 0,
                "mortalidad_90d_si": 0,
                "clavien_dindo": {},
                "transfusion": {},
                "margen_quirurgico": {},
                "neuropreservacion": {},
                "linfadenectomia": {},
                "stone_free": {},
                "recurrencia_litiasis": {},
            },
            "sangrado_metricas_mes": {
                "periodo": {"anio": date.today().year, "mes": date.today().month},
                "total_cirugias_realizadas_periodo": 0,
                "cirugias_con_sangrado_registrado": 0,
                "sangrado_total_ml": 0.0,
                "sangrado_promedio_ml": None,
                "sangrado_mediana_ml": None,
                "sangrado_p90_ml": None,
                "cirujano_top": [],
                "procedimiento_top": [],
                "cirujano_procedimiento_top": [],
                "chart_cirujano_total": None,
                "chart_procedimiento_total": None,
                "chart_cirujano_procedimiento": None,
            },
            "sangrado_metricas_global": {
                "periodo": {"anio": None, "mes": None},
                "total_cirugias_realizadas_periodo": 0,
                "cirugias_con_sangrado_registrado": 0,
                "sangrado_total_ml": 0.0,
                "sangrado_promedio_ml": None,
                "sangrado_mediana_ml": None,
                "sangrado_p90_ml": None,
                "cirujano_top": [],
                "procedimiento_top": [],
                "cirujano_procedimiento_top": [],
                "chart_cirujano_total": None,
                "chart_procedimiento_total": None,
                "chart_cirujano_procedimiento": None,
            },
            "consulta_ext_stats": {
                "total_atenciones": 0,
                "por_servicio": [],
                "por_servicio_sexo": [],
                "por_servicio_edad": [],
                "por_servicio_diagnostico": [],
                "detalle": [],
                "recommendations": _default_consulta_ext_recommendations(),
            },
            "consulta_ext_total_atenciones": 0,
            "consulta_ext_por_servicio": [],
            "consulta_ext_por_servicio_sexo": [],
            "consulta_ext_por_servicio_edad": [],
            "consulta_ext_por_servicio_diagnostico": [],
            "consulta_ext_detalle": [],
            "consulta_ext_recomendaciones": _default_consulta_ext_recommendations(),
            "charts_advanced": {"cancelacion_proc": None, "cancelacion_concepto": None, "ocupacion_mensual": None, "incidencias_labs": None},
        }

    # Diagnósticos
    diag_counts: Dict[str, int] = {}
    for consulta in consultas:
        diag = consulta.diagnostico_principal or "SIN_DIAGNOSTICO"
        diag_counts[diag] = diag_counts.get(diag, 0) + 1
    if diag_counts:
        labels = list(diag_counts.keys())
        values = list(diag_counts.values())
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(labels, values, color="#13322B")
        ax.set_title("Diagnósticos")
        ax.tick_params(axis='x', rotation=45)
        chart_diagnosticos = fig_to_base64(fig)
        plt.close(fig)

    # Variables numéricas
    numeric_fields = []
    for col in ConsultaDB.__table__.columns:
        if col.name == "id":
            continue
        if isinstance(col.type, (Integer, Float)):
            numeric_fields.append(col.name)

    for field in numeric_fields:
        values = [getattr(c, field) for c in consultas if getattr(c, field) is not None]
        if len(values) < 2:
            continue
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.hist(values, bins=10, color="#B38E5D", edgecolor="#13322B")
        ax.set_title(field)
        numeric_charts.append(fig_to_base64(fig))
        plt.close(fig)

    # Kaplan-Meier para oncológicos (evento configurable)
    onco = [c for c in consultas if (c.diagnostico_principal or "").startswith("ca_")]
    if onco:
        durations = []
        events = []
        today = date.today()
        for c in onco:
            if c.fecha_registro:
                event, event_date = resolve_survival_event(c)
                end_date = event_date or today
                durations.append(max((end_date - c.fecha_registro).days, 1))
                events.append(1 if event else 0)
        if durations:
            times, surv = kaplan_meier(durations, events)
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.step(times, surv, where="post", color="#13322B")
            ax.set_ylim(0, 1.05)
            ax.set_title(f"Kaplan-Meier (Oncológicos) - Evento: {SURVIVAL_EVENT_FIELD}")
            ax.set_xlabel("Días desde registro")
            ax.set_ylabel("Supervivencia")
            chart_survival = fig_to_base64(fig)
            plt.close(fig)

    # Proyección lista de espera quirúrgica
    completos_por_fecha: Dict[date, int] = {}
    for c in consultas:
        if c.estatus_protocolo == "completo" and c.fecha_registro:
            completos_por_fecha[c.fecha_registro] = completos_por_fecha.get(c.fecha_registro, 0) + 1
    if completos_por_fecha:
        fechas_ordenadas = sorted(completos_por_fecha.keys())
        acumulado = []
        total_acum = 0
        for f in fechas_ordenadas:
            total_acum += completos_por_fecha[f]
            acumulado.append(total_acum)
        x = [f.toordinal() for f in fechas_ordenadas]
        y = acumulado
        if len(x) >= 2:
            if LinearRegression is not None and np is not None:
                model = LinearRegression().fit(np.array(x).reshape(-1, 1), np.array(y))
                futuros = [fechas_ordenadas[-1].toordinal() + i for i in range(1, 31)]
                y_pred = model.predict(np.array(futuros).reshape(-1, 1))
            elif np is not None:
                coef = np.polyfit(x, y, 1)
                futuros = [fechas_ordenadas[-1].toordinal() + i for i in range(1, 31)]
                y_pred = [coef[0] * f + coef[1] for f in futuros]
            else:
                futuros = []
                y_pred = []

            fig, ax = plt.subplots(figsize=(6, 4))
            ax.plot(fechas_ordenadas, y, label="Histórico", color="#13322B")
            if futuros:
                fechas_futuras = [date.fromordinal(f) for f in futuros]
                ax.plot(fechas_futuras, y_pred, label="Proyección", color="#B38E5D", linestyle="--")
            ax.set_title("Lista de espera quirúrgica (completos)")
            ax.legend()
            chart_waitlist = fig_to_base64(fig)
            plt.close(fig)

    # ==============================
    # Analítica quirúrgica programada / pendientes / realizadas
    # ==============================
    sdb = _new_surgical_session(enable_dual_write=True)
    surgical_rows_all: List[SurgicalProgramacionDB] = []
    urgencias_rows_all: List[SurgicalUrgenciaProgramacionDB] = []
    pendientes_programar_rows: List[Dict[str, Any]] = []
    realizadas_rows: List[Dict[str, Any]] = []
    postquirurgicas_rows: List[SurgicalPostquirurgicaDB] = []
    dim_paciente_rows: List[DimPaciente] = []
    data_quality_pending = 0
    try:
        surgical_rows_all = sdb.query(SurgicalProgramacionDB).all()
        urgencias_rows_all = sdb.query(SurgicalUrgenciaProgramacionDB).all()
        pendientes_programar_rows = _build_pending_programar_dataset(db, sdb=sdb, limit=3000)
        realizadas_rows = _build_realizadas_dataset(sdb=sdb, limit=3000)
        postquirurgicas_rows = sdb.query(SurgicalPostquirurgicaDB).all()
        dim_paciente_rows = sdb.query(DimPaciente).all()
        data_quality_pending = (
            sdb.query(DataQualityLog)
            .filter(DataQualityLog.corregido.is_(False))
            .count()
        )
    except Exception:
        surgical_rows_all = []
        urgencias_rows_all = []
        pendientes_programar_rows = []
        realizadas_rows = []
        postquirurgicas_rows = []
        dim_paciente_rows = []
        data_quality_pending = 0
    finally:
        sdb.close()

    surgical_rows = [
        r
        for r in surgical_rows_all
        if (r.estatus or "").upper() == "PROGRAMADA"
        and (r.modulo_origen or "").upper() != "QUIROFANO_URGENCIA"
    ]
    urgencias_rows = [r for r in urgencias_rows_all if (r.estatus or "").upper() == "PROGRAMADA"]

    total_programados = len(surgical_rows)
    sexo_counts = count_by(surgical_rows, lambda r: r.sexo or "NO_REGISTRADO")
    patologias_counts = count_by(surgical_rows, lambda r: r.patologia or "NO_REGISTRADO")
    procedimientos_counts = count_by(surgical_rows, lambda r: r.procedimiento_programado or r.procedimiento or "NO_REGISTRADO")
    hgz_counts = count_by(surgical_rows, lambda r: r.hgz or "NO_REGISTRADO")

    onco_rows = [r for r in surgical_rows if r.grupo_patologia == "ONCOLOGICO"]
    onco_diag_counts = count_by(onco_rows, lambda r: r.patologia or "NO_REGISTRADO")
    onco_ecog_counts = count_by(onco_rows, lambda r: r.ecog or "NO_REGISTRADO")
    onco_charlson_counts = count_by(onco_rows, lambda r: r.charlson or "NO_REGISTRADO")
    onco_edad_counts = count_by(onco_rows, lambda r: classify_age_group(r.edad))
    onco_pacientes = [
        {
            "nss": r.nss,
            "nombre": r.paciente_nombre,
            "edad": r.edad,
            "patologia": r.patologia,
            "ecog": r.ecog,
            "charlson": r.charlson,
        }
        for r in onco_rows
    ]

    litiasis_rows = [r for r in surgical_rows if r.grupo_patologia == "LITIASIS_URINARIA"]
    litiasis_diag_counts = count_by(litiasis_rows, lambda r: r.patologia or "NO_REGISTRADO")
    litiasis_uh_counts = count_by(
        [r for r in litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.uh_rango or "NO_REGISTRADO",
    )
    litiasis_tamano_counts = count_by(
        [r for r in litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_tamano_rango or "NO_REGISTRADO",
    )
    litiasis_subtipo_counts = count_by(
        [r for r in litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_subtipo_20 or "NO_REGISTRADO",
    )
    litiasis_ubicacion_counts = count_by(
        [r for r in litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_ubicacion or "NO_REGISTRADO",
    )
    litiasis_hidronefrosis_counts = count_by(
        [r for r in litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.hidronefrosis or "NO_REGISTRADO",
    )

    procedimiento_abordaje_counts = count_by(
        surgical_rows,
        lambda r: f"{(r.abordaje or 'NO_REGISTRADO')} | {(r.procedimiento_programado or r.procedimiento or 'NO_REGISTRADO')}",
    )
    succion_counts = count_by(
        [r for r in surgical_rows if (r.procedimiento_programado or "") == QUIROFANO_PROCEDIMIENTO_SUCCION],
        lambda r: r.sistema_succion or "NO_REGISTRADO",
    )
    intermed_rows = [r for r in surgical_rows if (r.requiere_intermed or "").upper() == "SI"]
    intermed_por_procedimiento = count_by(intermed_rows, lambda r: r.procedimiento_programado or r.procedimiento or "NO_REGISTRADO")

    urg_sexo_counts = count_by(urgencias_rows, lambda r: r.sexo or "NO_REGISTRADO")
    urg_patologias_counts = count_by(urgencias_rows, lambda r: r.patologia or "NO_REGISTRADO")
    urg_procedimientos_counts = count_by(urgencias_rows, lambda r: r.procedimiento_programado or "NO_REGISTRADO")
    urg_hgz_counts = count_by(urgencias_rows, lambda r: r.hgz or "NO_REGISTRADO")
    urg_insumos_intermed = count_by(
        [r for r in urgencias_rows if (r.requiere_intermed or "").upper() == "SI"],
        lambda r: r.procedimiento_programado or "NO_REGISTRADO",
    )
    urg_onco_rows = [r for r in urgencias_rows if (r.grupo_patologia or "").upper() == "ONCOLOGICO"]
    urg_onco_diag_counts = count_by(urg_onco_rows, lambda r: r.patologia or "NO_REGISTRADO")
    urg_onco_ecog_counts = count_by(urg_onco_rows, lambda r: r.ecog or "NO_REGISTRADO")
    urg_onco_charlson_counts = count_by(urg_onco_rows, lambda r: r.charlson or "NO_REGISTRADO")
    urg_onco_edad_counts = count_by(urg_onco_rows, lambda r: classify_age_group(r.edad))
    urg_litiasis_rows = [r for r in urgencias_rows if (r.grupo_patologia or "").upper() == "LITIASIS_URINARIA"]
    urg_litiasis_diag_counts = count_by(urg_litiasis_rows, lambda r: r.patologia or "NO_REGISTRADO")
    urg_litiasis_uh_counts = count_by(
        [r for r in urg_litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.uh_rango or "NO_REGISTRADO",
    )
    urg_litiasis_tamano_counts = count_by(
        [r for r in urg_litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_tamano_rango or "NO_REGISTRADO",
    )
    urg_litiasis_subtipo_counts = count_by(
        [r for r in urg_litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_subtipo_20 or "NO_REGISTRADO",
    )
    urg_litiasis_ubicacion_counts = count_by(
        [r for r in urg_litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.litiasis_ubicacion or "NO_REGISTRADO",
    )
    urg_litiasis_hidronefrosis_counts = count_by(
        [r for r in urg_litiasis_rows if (r.patologia or "") == "CALCULO DEL RIÑON"],
        lambda r: r.hidronefrosis or "NO_REGISTRADO",
    )

    jj_metricas = _build_jj_metrics(realizadas_rows)
    hemoderivados_metricas = _build_hemoderivados_metrics(
        surgical_programadas_rows=surgical_rows,
        urgencias_programadas_rows=urgencias_rows,
        realizadas_rows=realizadas_rows,
    )

    edad_programados_counts = build_programmed_age_counts(surgical_rows)
    pendientes_desglose = _build_desglose_from_dict_rows(pendientes_programar_rows)
    realizadas_desglose = _build_desglose_from_dict_rows(realizadas_rows)

    # Desglose combinado: edad + diagnóstico + procedimiento + ECOG + sexo
    edad_combinado_map: Dict[Tuple[str, str, str, str, str], int] = {}
    combos_por_edad: Dict[str, set] = {}
    for row in surgical_rows:
        edad_bucket = classify_age_group(row.edad)
        diag = (row.patologia or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        proc = (row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        ecog = (row.ecog or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        sexo = (row.sexo or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        key = (edad_bucket, diag, proc, ecog, sexo)
        edad_combinado_map[key] = edad_combinado_map.get(key, 0) + 1
        combos_por_edad.setdefault(edad_bucket, set()).add((diag, proc, ecog, sexo))

    edad_combinado_counts = [
        {
            "edad": edad,
            "diagnostico": diag,
            "procedimiento": proc,
            "ecog": ecog,
            "sexo": sexo,
            "cantidad": cantidad,
        }
        for (edad, diag, proc, ecog, sexo), cantidad in sorted(
            edad_combinado_map.items(),
            key=lambda item: (
                EDAD_REPORTE_INDEX.get(item[0][0], 999),
                -item[1],
                item[0][1],
                item[0][2],
                item[0][3],
                item[0][4],
            ),
        )
    ]

    chart_edad_combinada = None
    labels_edad = [bucket for bucket, _ in edad_programados_counts]
    total_por_edad = [count for _, count in edad_programados_counts]
    promedio_combinado = []
    for bucket, count in edad_programados_counts:
        base = len(combos_por_edad.get(bucket, set()))
        promedio_combinado.append((count / base) if base else 0)

    if labels_edad and any(v > 0 for v in total_por_edad):
        fig, ax1 = plt.subplots(figsize=(18, 5))
        ax1.bar(labels_edad, total_por_edad, color="#13322B", alpha=0.85, label="Pacientes por edad")
        ax1.set_ylabel("Pacientes programados")
        ax1.tick_params(axis="x", rotation=50)
        ax2 = ax1.twinx()
        ax2.plot(labels_edad, promedio_combinado, color="#B38E5D", marker="o", linewidth=2, label="Promedio combinado por edad")
        ax2.set_ylabel("Promedio combinado")
        ax1.set_title("Promedio de pacientes programados por edad (integrado con diagnóstico, procedimiento, ECOG y sexo)")
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc="upper right")
        chart_edad_combinada = fig_to_base64(fig)
        plt.close(fig)

    advanced_metrics = build_advanced_reporte_metrics(
        db,
        consultas=consultas,
        surgical_rows_all=surgical_rows_all,
        realizadas_rows=realizadas_rows,
        postquirurgicas_rows=postquirurgicas_rows,
        dim_paciente_rows=dim_paciente_rows,
        data_quality_pending=data_quality_pending,
    )
    try:
        consulta_ext_stats = get_consulta_externa_stats(db)
    except Exception:
        consulta_ext_stats = {
            "total_atenciones": 0,
            "por_servicio": [],
            "por_servicio_sexo": [],
            "por_servicio_edad": [],
            "por_servicio_diagnostico": [],
            "detalle": [],
            "recommendations": _default_consulta_ext_recommendations(),
        }
    consulta_ext_recomendaciones = _merge_consulta_ext_recommendations(consulta_ext_stats.get("recommendations"))

    return {
        "total": total,
        "total_onco": total_onco,
        "completos": completos,
        "incompletos": incompletos,
        "numeric_charts": numeric_charts,
        "chart_diagnosticos": chart_diagnosticos,
        "chart_survival": chart_survival,
        "chart_waitlist": chart_waitlist,
        "notice": notice,
        "total_programados": total_programados,
        "total_urgencias_programadas": len(urgencias_rows),
        "total_pendientes_programar": len(pendientes_programar_rows),
        "total_realizadas": len(realizadas_rows),
        "pendientes_programar_rows": pendientes_programar_rows,
        "realizadas_rows": realizadas_rows,
        "pendientes_por_edad": pendientes_desglose["por_edad"],
        "pendientes_por_sexo": pendientes_desglose["por_sexo"],
        "pendientes_por_nss": pendientes_desglose["por_nss"],
        "pendientes_por_hgz": pendientes_desglose["por_hgz"],
        "pendientes_por_procedimiento": pendientes_desglose["por_procedimiento"],
        "pendientes_por_diagnostico": pendientes_desglose["por_diagnostico"],
        "realizadas_por_edad": realizadas_desglose["por_edad"],
        "realizadas_por_sexo": realizadas_desglose["por_sexo"],
        "realizadas_por_nss": realizadas_desglose["por_nss"],
        "realizadas_por_hgz": realizadas_desglose["por_hgz"],
        "realizadas_por_procedimiento": realizadas_desglose["por_procedimiento"],
        "realizadas_por_diagnostico": realizadas_desglose["por_diagnostico"],
        "realizadas_por_cirujano": realizadas_desglose["por_cirujano"],
        "realizadas_por_sangrado": realizadas_desglose["por_sangrado"],
        "sexo_counts": sexo_counts,
        "patologias_counts": patologias_counts,
        "procedimientos_counts": procedimientos_counts,
        "hgz_counts": hgz_counts,
        "onco_diag_counts": onco_diag_counts,
        "onco_ecog_counts": onco_ecog_counts,
        "onco_charlson_counts": onco_charlson_counts,
        "onco_edad_counts": onco_edad_counts,
        "onco_pacientes": onco_pacientes,
        "litiasis_diag_counts": litiasis_diag_counts,
        "litiasis_uh_counts": litiasis_uh_counts,
        "litiasis_tamano_counts": litiasis_tamano_counts,
        "litiasis_subtipo_counts": litiasis_subtipo_counts,
        "litiasis_ubicacion_counts": litiasis_ubicacion_counts,
        "litiasis_hidronefrosis_counts": litiasis_hidronefrosis_counts,
        "procedimiento_abordaje_counts": procedimiento_abordaje_counts,
        "succion_counts": succion_counts,
        "intermed_por_procedimiento": intermed_por_procedimiento,
        "urg_sexo_counts": urg_sexo_counts,
        "urg_patologias_counts": urg_patologias_counts,
        "urg_procedimientos_counts": urg_procedimientos_counts,
        "urg_hgz_counts": urg_hgz_counts,
        "urg_insumos_intermed": urg_insumos_intermed,
        "urg_onco_diag_counts": urg_onco_diag_counts,
        "urg_onco_ecog_counts": urg_onco_ecog_counts,
        "urg_onco_charlson_counts": urg_onco_charlson_counts,
        "urg_onco_edad_counts": urg_onco_edad_counts,
        "urg_litiasis_diag_counts": urg_litiasis_diag_counts,
        "urg_litiasis_uh_counts": urg_litiasis_uh_counts,
        "urg_litiasis_tamano_counts": urg_litiasis_tamano_counts,
        "urg_litiasis_subtipo_counts": urg_litiasis_subtipo_counts,
        "urg_litiasis_ubicacion_counts": urg_litiasis_ubicacion_counts,
        "urg_litiasis_hidronefrosis_counts": urg_litiasis_hidronefrosis_counts,
        "jj_metricas": jj_metricas,
        "hemoderivados_metricas": hemoderivados_metricas,
        "urg_hemoderivados_rows": hemoderivados_metricas.get("solicitudes_urgencias_rows", []),
        "prog_hemoderivados_rows": hemoderivados_metricas.get("solicitudes_programadas_rows", []),
        "edad_programados_counts": edad_programados_counts,
        "chart_edad_combinada": chart_edad_combinada,
        "edad_combinado_counts": edad_combinado_counts,
        "cancelacion_global": advanced_metrics.get("cancelacion_global", {}),
        "cancelacion_por_procedimiento": advanced_metrics.get("cancelacion_por_procedimiento", []),
        "cancelacion_por_concepto": advanced_metrics.get("cancelacion_por_concepto", []),
        "cancelacion_por_medico": advanced_metrics.get("cancelacion_por_medico", []),
        "cancelacion_por_diagnostico": advanced_metrics.get("cancelacion_por_diagnostico", []),
        "cancelacion_por_semana": advanced_metrics.get("cancelacion_por_semana", []),
        "cancelacion_por_mes": advanced_metrics.get("cancelacion_por_mes", []),
        "cancelacion_por_procedimiento_concepto": advanced_metrics.get("cancelacion_por_procedimiento_concepto", []),
        "cancelacion_detalle_rows": advanced_metrics.get("cancelacion_detalle_rows", []),
        "tiempo_programada_a_realizada": advanced_metrics.get("tiempo_programada_a_realizada", {}),
        "estancia_global": advanced_metrics.get("estancia_global", {}),
        "estancia_por_diagnostico": advanced_metrics.get("estancia_por_diagnostico", []),
        "estancia_por_procedimiento": advanced_metrics.get("estancia_por_procedimiento", []),
        "estancia_por_hgz": advanced_metrics.get("estancia_por_hgz", []),
        "ocupacion_tendencia": advanced_metrics.get("ocupacion_tendencia", {}),
        "indice_estancia_prolongada": advanced_metrics.get("indice_estancia_prolongada", {}),
        "incidencia_laboratorios": advanced_metrics.get("incidencia_laboratorios", {}),
        "riesgo_cruzado_top": advanced_metrics.get("riesgo_cruzado_top", []),
        "cohortes_dinamicas": advanced_metrics.get("cohortes_dinamicas", {}),
        "embudo_operativo": advanced_metrics.get("embudo_operativo", {}),
        "embudo_conversion": advanced_metrics.get("embudo_conversion", {}),
        "productividad_guardia": advanced_metrics.get("productividad_guardia", {}),
        "mapa_epidemiologico": advanced_metrics.get("mapa_epidemiologico", {}),
        "calidad_captura": advanced_metrics.get("calidad_captura", {}),
        "desenlaces_postquirurgicos": advanced_metrics.get("desenlaces_postquirurgicos", {}),
        "sangrado_metricas_mes": advanced_metrics.get("sangrado_metricas_mes", {}),
        "sangrado_metricas_global": advanced_metrics.get("sangrado_metricas_global", {}),
        "consulta_ext_stats": consulta_ext_stats,
        "consulta_ext_total_atenciones": consulta_ext_stats.get("total_atenciones", 0),
        "consulta_ext_por_servicio": consulta_ext_stats.get("por_servicio", []),
        "consulta_ext_por_servicio_sexo": consulta_ext_stats.get("por_servicio_sexo", []),
        "consulta_ext_por_servicio_edad": consulta_ext_stats.get("por_servicio_edad", []),
        "consulta_ext_por_servicio_diagnostico": consulta_ext_stats.get("por_servicio_diagnostico", []),
        "consulta_ext_detalle": consulta_ext_stats.get("detalle", []),
        "consulta_ext_recomendaciones": consulta_ext_recomendaciones,
        "charts_advanced": advanced_metrics.get("charts_advanced", {}),
    }
