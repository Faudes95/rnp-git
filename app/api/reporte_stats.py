from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

router = APIRouter(tags=["reporte-estadistico"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_surgical_db()


@router.get("/api/stats/hospitalizacion/estructurada/resumen", response_class=JSONResponse)
def api_stats_hospitalizacion_estructurada_resumen(db: Session = Depends(_get_db)):
    from app.services.reporte_flow import build_inpatient_structured_metrics

    return JSONResponse(content=build_inpatient_structured_metrics(db))


@router.get("/reporte/pendientes-programar", response_class=HTMLResponse)
def reporte_pendientes_programar(
    request: Request,
    limit: int = 1200,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 5000))
    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=safe_limit)
    desglose = m._build_desglose_from_dict_rows(filas)
    chart_pendientes_prioridad = m._build_bar_chart_from_counts(
        desglose["por_prioridad"],
        title="Pendientes de programar por prioridad clínica",
        color="#B38E5D",
    )
    chart_pendientes_espera = m._build_bar_chart_from_counts(
        desglose["por_espera"],
        title="Pendientes de programar por rango de espera",
        color="#13322B",
    )
    chart_pendientes_diag = m._build_bar_chart_from_counts(
        desglose["por_diagnostico"],
        title="Pendientes de programar por diagnóstico",
        color="#24584f",
    )
    return m.render_template(
        "reporte_pendientes_programar.html",
        request=request,
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M"),
        total=len(filas),
        filas=filas,
        por_edad=desglose["por_edad"],
        por_sexo=desglose["por_sexo"],
        por_nss=desglose["por_nss"],
        por_hgz=desglose["por_hgz"],
        por_procedimiento=desglose["por_procedimiento"],
        por_diagnostico=desglose["por_diagnostico"],
        por_prioridad=desglose["por_prioridad"],
        por_espera=desglose["por_espera"],
        chart_pendientes_prioridad=chart_pendientes_prioridad,
        chart_pendientes_espera=chart_pendientes_espera,
        chart_pendientes_diag=chart_pendientes_diag,
    )


@router.get("/reporte/cirugias-realizadas", response_class=HTMLResponse)
def reporte_cirugias_realizadas(
    request: Request,
    limit: int = 1200,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 5000))
    filas = m._build_realizadas_dataset(sdb=sdb, limit=safe_limit)
    desglose = m._build_desglose_from_dict_rows(filas)
    chart_realizadas_procedimiento = m._build_bar_chart_from_counts(
        desglose["por_procedimiento"],
        title="Cirugías realizadas por procedimiento",
        color="#13322B",
    )
    sangrados = []
    for row in filas:
        value = row.get("sangrado_ml")
        if value is None:
            continue
        try:
            sangrados.append(float(value))
        except Exception:
            continue
    chart_realizadas_sangrado = m._build_hist_chart_from_values(
        sangrados,
        title="Distribución de sangrado en cirugías realizadas (mL)",
        bins=10,
        color="#B38E5D",
    )
    return m.render_template(
        "reporte_cirugias_realizadas.html",
        request=request,
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M"),
        total=len(filas),
        filas=filas,
        por_edad=desglose["por_edad"],
        por_sexo=desglose["por_sexo"],
        por_nss=desglose["por_nss"],
        por_hgz=desglose["por_hgz"],
        por_procedimiento=desglose["por_procedimiento"],
        por_diagnostico=desglose["por_diagnostico"],
        por_cirujano=desglose["por_cirujano"],
        por_sangrado=desglose["por_sangrado"],
        chart_realizadas_procedimiento=chart_realizadas_procedimiento,
        chart_realizadas_sangrado=chart_realizadas_sangrado,
    )


@router.get("/reporte/sangrado", response_class=HTMLResponse)
def reporte_sangrado(
    request: Request,
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    today_value = date.today()
    target_year = int(anio) if anio is not None else today_value.year
    target_month = int(mes) if mes is not None else today_value.month
    if target_month < 1 or target_month > 12:
        target_month = today_value.month
    filas = m._build_realizadas_dataset(sdb=sdb, limit=7000)
    metricas_mes = m._build_sangrado_metrics(filas, anio=target_year, mes=target_month, top_n=40)
    metricas_global = m._build_sangrado_metrics(filas, anio=None, mes=None, top_n=40)
    return m.render_template(
        "reporte_sangrado.html",
        request=request,
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M"),
        target_year=target_year,
        target_month=target_month,
        metricas_mes=metricas_mes,
        metricas_global=metricas_global,
    )


@router.get("/reporte/cola-preventiva", response_class=HTMLResponse)
def reporte_cola_preventiva(
    request: Request,
    limit: int = 300,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 5000))
    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=5000)
    desglose = m._build_desglose_from_dict_rows(filas)
    ranking = m._rank_preventive_rows(filas, limit=safe_limit)
    chart_prioridad = m._build_bar_chart_from_counts(
        desglose["por_prioridad"],
        title="Cola preventiva por prioridad clínica",
        color="#B38E5D",
    )
    chart_espera = m._build_bar_chart_from_counts(
        desglose["por_espera"],
        title="Cola preventiva por rango de espera",
        color="#13322B",
    )
    chart_diagnostico = m._build_bar_chart_from_counts(
        desglose["por_diagnostico"],
        title="Cola preventiva por diagnóstico",
        color="#24584f",
    )
    return m.render_template(
        "reporte_cola_preventiva.html",
        request=request,
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M"),
        total=len(filas),
        ranking=ranking,
        por_prioridad=desglose["por_prioridad"],
        por_espera=desglose["por_espera"],
        por_diagnostico=desglose["por_diagnostico"],
        por_hgz=desglose["por_hgz"],
        chart_prioridad=chart_prioridad,
        chart_espera=chart_espera,
        chart_diagnostico=chart_diagnostico,
    )


@router.get("/api/stats/pendientes-programar/resumen", response_class=JSONResponse)
def api_stats_pendientes_programar_resumen(
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=5000)
    desglose = m._build_desglose_from_dict_rows(filas)
    return JSONResponse(
        content={
            "total_pendientes_programar": len(filas),
            "por_edad": {k: int(v) for k, v in desglose["por_edad"]},
            "por_sexo": {k: int(v) for k, v in desglose["por_sexo"]},
            "por_nss": {k: int(v) for k, v in desglose["por_nss"]},
            "por_hgz": {k: int(v) for k, v in desglose["por_hgz"]},
            "por_procedimiento": {k: int(v) for k, v in desglose["por_procedimiento"]},
            "por_diagnostico": {k: int(v) for k, v in desglose["por_diagnostico"]},
            "por_prioridad": {k: int(v) for k, v in desglose["por_prioridad"]},
            "por_espera": {k: int(v) for k, v in desglose["por_espera"]},
            "top_riesgo_cancelacion": sorted(
                [
                    {
                        "consulta_id": row.get("consulta_id"),
                        "paciente_nombre": row.get("paciente_nombre"),
                        "nss": row.get("nss"),
                        "diagnostico": row.get("diagnostico"),
                        "riesgo_cancelacion_predicho": row.get("riesgo_cancelacion_predicho"),
                    }
                    for row in filas
                ],
                key=lambda item: float(item.get("riesgo_cancelacion_predicho") or 0),
                reverse=True,
            )[:20],
        }
    )


@router.get("/api/stats/pendientes-programar", response_class=JSONResponse)
def api_stats_pendientes_programar_compat(
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_pendientes_programar_resumen(db=db, sdb=sdb)


@router.get("/api/stats/pendientes-programar/desglose", response_class=JSONResponse)
def api_stats_pendientes_programar_desglose(
    limit: int = 1200,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 5000))
    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=safe_limit)
    return JSONResponse(content=jsonable_encoder({"total": len(filas), "rows": filas}))


@router.get("/api/stats/cirugias-realizadas/resumen", response_class=JSONResponse)
def api_stats_cirugias_realizadas_resumen(
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_realizadas_dataset(sdb=sdb, limit=5000)
    desglose = m._build_desglose_from_dict_rows(filas)

    def _c(field: str) -> Dict[str, int]:
        return {k: int(v) for k, v in m.count_by(filas, lambda r: r.get(field) if isinstance(r, dict) else None)}

    return JSONResponse(
        content={
            "total_cirugias_realizadas": len(filas),
            "por_edad": {k: int(v) for k, v in desglose["por_edad"]},
            "por_sexo": {k: int(v) for k, v in desglose["por_sexo"]},
            "por_nss": {k: int(v) for k, v in desglose["por_nss"]},
            "por_hgz": {k: int(v) for k, v in desglose["por_hgz"]},
            "por_procedimiento": {k: int(v) for k, v in desglose["por_procedimiento"]},
            "por_diagnostico": {k: int(v) for k, v in desglose["por_diagnostico"]},
            "por_cirujano": {k: int(v) for k, v in desglose["por_cirujano"]},
            "por_sangrado": {k: int(v) for k, v in desglose["por_sangrado"]},
            "por_transfusion": _c("transfusion"),
            "por_clavien_dindo": _c("clavien_dindo"),
            "por_margen_quirurgico": _c("margen_quirurgico"),
            "por_neuropreservacion": _c("neuropreservacion"),
            "por_linfadenectomia": _c("linfadenectomia"),
            "por_stone_free": _c("stone_free"),
            "por_recurrencia_litiasis": _c("recurrencia_litiasis"),
            "desenlaces_30_90": {
                "reingreso_30d_si": sum(1 for r in filas if str(r.get("reingreso_30d") or "").upper() == "SI"),
                "reintervencion_30d_si": sum(1 for r in filas if str(r.get("reintervencion_30d") or "").upper() == "SI"),
                "mortalidad_30d_si": sum(1 for r in filas if str(r.get("mortalidad_30d") or "").upper() == "SI"),
                "reingreso_90d_si": sum(1 for r in filas if str(r.get("reingreso_90d") or "").upper() == "SI"),
                "reintervencion_90d_si": sum(1 for r in filas if str(r.get("reintervencion_90d") or "").upper() == "SI"),
                "mortalidad_90d_si": sum(1 for r in filas if str(r.get("mortalidad_90d") or "").upper() == "SI"),
            },
        }
    )


@router.get("/api/stats/cirugias-realizadas", response_class=JSONResponse)
def api_stats_cirugias_realizadas_compat(
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_cirugias_realizadas_resumen(sdb=sdb)


@router.get("/api/stats/cirugias-realizadas/desglose", response_class=JSONResponse)
def api_stats_cirugias_realizadas_desglose(
    limit: int = 1200,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 5000))
    filas = m._build_realizadas_dataset(sdb=sdb, limit=safe_limit)
    return JSONResponse(content=jsonable_encoder({"total": len(filas), "rows": filas}))


@router.get("/api/stats/sangrado/resumen", response_class=JSONResponse)
def api_stats_sangrado_resumen(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_realizadas_dataset(sdb=sdb, limit=7000)
    metricas = m._build_sangrado_metrics(filas, anio=anio, mes=mes, top_n=50)
    return JSONResponse(content=metricas)


@router.get("/api/stats/sangrado", response_class=JSONResponse)
def api_stats_sangrado_compat(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_sangrado_resumen(anio=anio, mes=mes, sdb=sdb)


@router.get("/api/stats/sangrado/cirujano-procedimiento", response_class=JSONResponse)
def api_stats_sangrado_cirujano_procedimiento(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    limit: int = 150,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_realizadas_dataset(sdb=sdb, limit=7000)
    metricas = m._build_sangrado_metrics(filas, anio=anio, mes=mes, top_n=max(10, min(int(limit), 500)))
    return JSONResponse(
        content={
            "periodo": metricas.get("periodo", {}),
            "cirujano_procedimiento_top": metricas.get("cirujano_procedimiento_top", []),
            "cirujano_top": metricas.get("cirujano_top", []),
            "procedimiento_top": metricas.get("procedimiento_top", []),
            "transfusion_global": metricas.get("transfusion_global", {}),
            "transfusion_por_cirujano": metricas.get("transfusion_por_cirujano", []),
            "transfusion_por_procedimiento": metricas.get("transfusion_por_procedimiento", []),
            "complejidad_estratos": metricas.get("complejidad_estratos", []),
            "ajuste_complejidad_cirujano": metricas.get("ajuste_complejidad_cirujano", []),
        }
    )


@router.get("/api/stats/quirofano/cancelaciones/resumen", response_class=JSONResponse)
def api_stats_quirofano_cancelaciones_resumen(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    concepto: Optional[str] = None,
    medico: Optional[str] = None,
    limit: int = 8000,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 25000))
    rows = (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.estatus == "CANCELADA")
        .order_by(m.SurgicalProgramacionDB.cancelacion_fecha.desc(), m.SurgicalProgramacionDB.id.desc())
        .limit(safe_limit)
        .all()
    )

    concepto_filter = str(concepto or "").strip().upper()
    medico_filter = str(medico or "").strip().upper()
    data_rows = []
    by_concepto: Dict[str, int] = {}
    by_medico: Dict[str, int] = {}
    by_diagnostico: Dict[str, int] = {}
    by_procedimiento: Dict[str, int] = {}
    by_mes: Dict[str, int] = {}
    by_semana: Dict[str, int] = {}
    by_proc_concepto: Dict[str, int] = {}

    for row in rows:
        fecha_raw = getattr(row, "cancelacion_fecha", None) or getattr(row, "actualizado_en", None) or getattr(row, "fecha_programada", None)
        fecha_val = None
        if isinstance(fecha_raw, datetime):
            fecha_val = fecha_raw.date()
        elif isinstance(fecha_raw, date):
            fecha_val = fecha_raw
        if fecha_val is not None:
            if anio is not None and int(fecha_val.year) != int(anio):
                continue
            if mes is not None and int(fecha_val.month) != int(mes):
                continue
        elif anio is not None or mes is not None:
            continue

        concepto_val = str(getattr(row, "cancelacion_concepto", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        codigo_val = str(getattr(row, "cancelacion_codigo", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        categoria_val = str(getattr(row, "cancelacion_categoria", None) or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        medico_val = (
            str(getattr(row, "agregado_medico", None) or getattr(row, "cirujano", None) or "NO_REGISTRADO")
            .strip()
            .upper()
            or "NO_REGISTRADO"
        )
        diagnostico_val = (
            str(getattr(row, "patologia", None) or getattr(row, "diagnostico_principal", None) or "NO_REGISTRADO")
            .strip()
            .upper()
            or "NO_REGISTRADO"
        )
        procedimiento_val = (
            str(getattr(row, "procedimiento_programado", None) or getattr(row, "procedimiento", None) or "NO_REGISTRADO")
            .strip()
            .upper()
            or "NO_REGISTRADO"
        )
        if concepto_filter and concepto_filter not in concepto_val and concepto_filter not in codigo_val:
            continue
        if medico_filter and medico_filter not in medico_val:
            continue

        semana_key = None
        mes_key = None
        if fecha_val is not None:
            iso = fecha_val.isocalendar()
            semana_key = f"{int(iso[0])}-S{int(iso[1]):02d}"
            mes_key = f"{fecha_val.year}-{fecha_val.month:02d}"
            by_semana[semana_key] = by_semana.get(semana_key, 0) + 1
            by_mes[mes_key] = by_mes.get(mes_key, 0) + 1
        by_concepto[f"{codigo_val} · {concepto_val}"] = by_concepto.get(f"{codigo_val} · {concepto_val}", 0) + 1
        by_medico[medico_val] = by_medico.get(medico_val, 0) + 1
        by_diagnostico[diagnostico_val] = by_diagnostico.get(diagnostico_val, 0) + 1
        by_procedimiento[procedimiento_val] = by_procedimiento.get(procedimiento_val, 0) + 1
        proc_concept_key = f"{procedimiento_val} || {codigo_val} · {concepto_val}"
        by_proc_concepto[proc_concept_key] = by_proc_concepto.get(proc_concept_key, 0) + 1
        data_rows.append(
            {
                "surgical_programacion_id": row.id,
                "consulta_id": row.consulta_id,
                "nss": row.nss,
                "paciente_nombre": row.paciente_nombre,
                "diagnostico": diagnostico_val,
                "procedimiento": procedimiento_val,
                "medico": medico_val,
                "cancelacion_codigo": codigo_val,
                "cancelacion_concepto": concepto_val,
                "cancelacion_categoria": categoria_val,
                "cancelacion_detalle": getattr(row, "cancelacion_detalle", None),
                "fecha_cancelacion": fecha_val.isoformat() if fecha_val else None,
                "semana": semana_key,
                "mes": mes_key,
            }
        )

    total = len(data_rows)
    return JSONResponse(
        content={
            "total_canceladas": total,
            "filtros": {
                "anio": anio,
                "mes": mes,
                "concepto": concepto_filter or None,
                "medico": medico_filter or None,
                "limit": safe_limit,
            },
            "por_concepto": sorted(
                [{"concepto": k, "canceladas": int(v), "porcentaje": round((int(v) / float(total)) * 100.0, 2) if total else 0.0} for k, v in by_concepto.items()],
                key=lambda item: (-int(item["canceladas"]), item["concepto"]),
            ),
            "por_medico": sorted(
                [{"medico": k, "canceladas": int(v), "porcentaje": round((int(v) / float(total)) * 100.0, 2) if total else 0.0} for k, v in by_medico.items()],
                key=lambda item: (-int(item["canceladas"]), item["medico"]),
            ),
            "por_diagnostico": sorted(
                [{"diagnostico": k, "canceladas": int(v)} for k, v in by_diagnostico.items()],
                key=lambda item: (-int(item["canceladas"]), item["diagnostico"]),
            ),
            "por_procedimiento": sorted(
                [{"procedimiento": k, "canceladas": int(v)} for k, v in by_procedimiento.items()],
                key=lambda item: (-int(item["canceladas"]), item["procedimiento"]),
            ),
            "por_procedimiento_concepto": sorted(
                [{"procedimiento_concepto": k, "canceladas": int(v)} for k, v in by_proc_concepto.items()],
                key=lambda item: (-int(item["canceladas"]), item["procedimiento_concepto"]),
            )[:150],
            "por_semana": [{"semana": k, "canceladas": int(v)} for k, v in sorted(by_semana.items(), key=lambda item: item[0])],
            "por_mes": [{"mes": k, "canceladas": int(v)} for k, v in sorted(by_mes.items(), key=lambda item: item[0])],
            "rows": data_rows[:500],
        }
    )


@router.get("/api/stats/quirofano/cancelaciones", response_class=JSONResponse)
def api_stats_quirofano_cancelaciones_compat(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    concepto: Optional[str] = None,
    medico: Optional[str] = None,
    limit: int = 8000,
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_quirofano_cancelaciones_resumen(
        anio=anio,
        mes=mes,
        concepto=concepto,
        medico=medico,
        limit=limit,
        sdb=sdb,
    )


@router.get("/api/stats/urgencias/resumen", response_class=JSONResponse)
def api_stats_urgencias_resumen(
    limit: int = 2000,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 8000))
    rows = m._build_urgencias_programadas_dataset(sdb=sdb, limit=safe_limit)
    desglose = m._build_desglose_from_dict_rows(rows)
    onco = [r for r in rows if str(r.get("grupo_patologia") or "").upper() == "ONCOLOGICO"]
    litiasis = [r for r in rows if str(r.get("grupo_patologia") or "").upper() == "LITIASIS_URINARIA"]
    return JSONResponse(
        content={
            "total_urgencias_programadas": len(rows),
            "por_edad": {k: int(v) for k, v in desglose["por_edad"]},
            "por_sexo": {k: int(v) for k, v in desglose["por_sexo"]},
            "por_nss": {k: int(v) for k, v in desglose["por_nss"]},
            "por_hgz": {k: int(v) for k, v in desglose["por_hgz"]},
            "por_procedimiento": {k: int(v) for k, v in desglose["por_procedimiento"]},
            "por_diagnostico": {k: int(v) for k, v in desglose["por_diagnostico"]},
            "oncologicos": {
                "total": len(onco),
                "por_diagnostico": {k: int(v) for k, v in m.count_by(onco, lambda r: r.get("diagnostico") or "NO_REGISTRADO")},
                "por_ecog": {k: int(v) for k, v in m.count_by(onco, lambda r: r.get("ecog") or "NO_REGISTRADO")},
                "por_charlson": {k: int(v) for k, v in m.count_by(onco, lambda r: r.get("charlson") or "NO_REGISTRADO")},
                "por_edad": {k: int(v) for k, v in m.count_by(onco, lambda r: r.get("edad_grupo") or "NO_REGISTRADO")},
            },
            "litiasis": {
                "total": len(litiasis),
                "por_diagnostico": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("diagnostico") or "NO_REGISTRADO")},
                "por_uh": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("uh_rango") or "NO_REGISTRADO")},
                "por_tamano": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("litiasis_tamano_rango") or "NO_REGISTRADO")},
                "por_subtipo_20": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("litiasis_subtipo_20") or "NO_REGISTRADO")},
                "por_ubicacion": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("litiasis_ubicacion") or "NO_REGISTRADO")},
                "por_hidronefrosis": {k: int(v) for k, v in m.count_by(litiasis, lambda r: r.get("hidronefrosis") or "NO_REGISTRADO")},
            },
            "insumos_intermed_por_procedimiento": {
                k: int(v)
                for k, v in m.count_by(
                    [r for r in rows if str(r.get("requiere_intermed") or "").upper() == "SI"],
                    lambda r: r.get("procedimiento") or "NO_REGISTRADO",
                )
            },
            "solicitudes_hemoderivados": {
                "total": sum(1 for r in rows if str(r.get("solicita_hemoderivados") or "").upper() == "SI"),
                "por_procedimiento": {
                    k: int(v)
                    for k, v in m.count_by(
                        [r for r in rows if str(r.get("solicita_hemoderivados") or "").upper() == "SI"],
                        lambda r: r.get("procedimiento") or "NO_REGISTRADO",
                    )
                },
                "por_hgz": {
                    k: int(v)
                    for k, v in m.count_by(
                        [r for r in rows if str(r.get("solicita_hemoderivados") or "").upper() == "SI"],
                        lambda r: r.get("hgz") or "NO_REGISTRADO",
                    )
                },
                "unidades_totales": {
                    "pg": int(sum(m.parse_int(r.get("hemoderivados_pg_solicitados")) or 0 for r in rows)),
                    "pfc": int(sum(m.parse_int(r.get("hemoderivados_pfc_solicitados")) or 0 for r in rows)),
                    "cp": int(sum(m.parse_int(r.get("hemoderivados_cp_solicitados")) or 0 for r in rows)),
                },
            },
            "rows": rows,
        }
    )


@router.get("/api/stats/urgencias", response_class=JSONResponse)
def api_stats_urgencias_compat(
    limit: int = 2000,
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_urgencias_resumen(limit=limit, sdb=sdb)


@router.get("/api/stats/litiasis/jj", response_class=JSONResponse)
def api_stats_litiasis_jj(
    limit: int = 7000,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 12000))
    filas = m._build_realizadas_dataset(sdb=sdb, limit=safe_limit)
    metricas = m._build_jj_metrics(filas)
    return JSONResponse(content=metricas)


@router.get("/api/stats/hemoderivados/resumen", response_class=JSONResponse)
def api_stats_hemoderivados_resumen(
    limit: int = 7000,
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    safe_limit = max(1, min(int(limit), 12000))
    surgical_rows = (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.estatus == "PROGRAMADA")
        .filter(
            (m.SurgicalProgramacionDB.modulo_origen.is_(None))
            | (m.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA")
        )
        .limit(safe_limit)
        .all()
    )
    urg_rows = (
        sdb.query(m.SurgicalUrgenciaProgramacionDB)
        .filter(m.SurgicalUrgenciaProgramacionDB.estatus == "PROGRAMADA")
        .limit(safe_limit)
        .all()
    )
    realizadas_rows = m._build_realizadas_dataset(sdb=sdb, limit=safe_limit)
    metricas = m._build_hemoderivados_metrics(
        surgical_programadas_rows=surgical_rows,
        urgencias_programadas_rows=urg_rows,
        realizadas_rows=realizadas_rows,
    )
    return JSONResponse(content=metricas)


@router.get("/api/stats/hemoderivados", response_class=JSONResponse)
def api_stats_hemoderivados_compat(
    limit: int = 7000,
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_hemoderivados_resumen(limit=limit, sdb=sdb)


@router.get("/api/stats/cohortes/resumen", response_class=JSONResponse)
def api_stats_cohortes_resumen(
    source: str = "programadas",
    top: int = 150,
    sdb: Session = Depends(_get_surgical_db),
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    source_key = (source or "programadas").strip().lower()
    if source_key == "pendientes":
        rows = m._build_pending_programar_dataset(db, sdb=sdb, limit=5000)
    elif source_key == "realizadas":
        rows = m._build_realizadas_dataset(sdb=sdb, limit=5000)
    else:
        source_key = "programadas"
        rows = m._build_programadas_dataset(sdb=sdb, limit=5000)

    cohort_map: Dict[Tuple[str, str, str, str, str], int] = {}
    for row in rows:
        edad_key = str(row.get("edad_grupo") or m.classify_age_group(m.parse_int(row.get("edad"))))
        diag_key = str(row.get("diagnostico") or "NO_REGISTRADO")
        proc_key = str(row.get("procedimiento") or "NO_REGISTRADO")
        ecog_key = str(row.get("ecog") or "NO_REGISTRADO")
        sexo_key = str(row.get("sexo") or "NO_REGISTRADO")
        key = (edad_key, diag_key, proc_key, ecog_key, sexo_key)
        cohort_map[key] = cohort_map.get(key, 0) + 1

    items = [
        {
            "edad_grupo": edad,
            "diagnostico": diag,
            "procedimiento": proc,
            "ecog": ecog,
            "sexo": sexo,
            "cantidad": count,
        }
        for (edad, diag, proc, ecog, sexo), count in sorted(
            cohort_map.items(),
            key=lambda kv: (-kv[1], m.EDAD_REPORTE_INDEX.get(kv[0][0], 999), kv[0][1], kv[0][2], kv[0][4]),
        )[: max(1, min(int(top), 2000))]
    ]
    return JSONResponse(
        content={
            "source": source_key,
            "total_rows": len(rows),
            "total_cohortes": len(cohort_map),
            "items": items,
        }
    )


@router.get("/api/stats/cohortes", response_class=JSONResponse)
def api_stats_cohortes_compat(
    source: str = "programadas",
    top: int = 150,
    sdb: Session = Depends(_get_surgical_db),
    db: Session = Depends(_get_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_cohortes_resumen(source=source, top=top, sdb=sdb, db=db)


@router.get("/api/stats/cola-preventiva/resumen", response_class=JSONResponse)
def api_stats_cola_preventiva_resumen(
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=5000)
    desglose = m._build_desglose_from_dict_rows(filas)
    return JSONResponse(
        content={
            "total_en_cola_preventiva": len(filas),
            "por_prioridad": {k: int(v) for k, v in desglose["por_prioridad"]},
            "por_espera": {k: int(v) for k, v in desglose["por_espera"]},
            "por_diagnostico": {k: int(v) for k, v in desglose["por_diagnostico"]},
            "por_hgz": {k: int(v) for k, v in desglose["por_hgz"]},
            "promedio_dias_espera": round(
                sum((row.get("dias_en_espera") or 0) for row in filas) / max(1, len(filas)),
                2,
            ),
        }
    )


@router.get("/api/stats/cola-preventiva", response_class=JSONResponse)
def api_stats_cola_preventiva_compat(
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    """Compatibilidad aditiva de endpoint legacy sin sufijo /resumen."""
    return api_stats_cola_preventiva_resumen(db=db, sdb=sdb)


@router.get("/api/stats/cola-preventiva/desglose", response_class=JSONResponse)
def api_stats_cola_preventiva_desglose(
    limit: int = 200,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    filas = m._build_pending_programar_dataset(db, sdb=sdb, limit=5000)
    ranked = m._rank_preventive_rows(filas, limit=limit)
    return JSONResponse(content=jsonable_encoder({"total": len(filas), "rows": ranked}))
