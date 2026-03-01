from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple


def predict_risk_payload(
    *,
    edad: int,
    sexo: str,
    ecog: Optional[str],
    charlson: Optional[str],
    cargar_modelo_riesgo_fn: Callable[[], Any],
    normalize_upper_fn: Callable[[Optional[str]], str],
    parse_int_from_text_fn: Callable[[Optional[str]], int],
) -> Tuple[int, Dict[str, Any]]:
    model = cargar_modelo_riesgo_fn()
    if model is None:
        return 400, {"error": "Modelo no entrenado"}
    sexo_cod = 1 if normalize_upper_fn(sexo) == "MASCULINO" else 0
    ecog_cod = parse_int_from_text_fn(ecog)
    charlson_cod = parse_int_from_text_fn(charlson)
    try:
        proba = float(model.predict_proba([[edad, sexo_cod, ecog_cod, charlson_cod]])[0][1])
    except Exception as exc:
        return 500, {"error": str(exc)}
    return 200, {"riesgo_complicacion": round(proba, 4)}


def stats_oncology_payload(
    *,
    sdb: Any,
    hecho_programacion_cls: Any,
    dim_diagnostico_cls: Any,
    dim_paciente_cls: Any,
) -> Dict[str, Any]:
    rows = sdb.query(hecho_programacion_cls).filter(hecho_programacion_cls.grupo_patologia == "ONCOLOGICO").all()
    diag_map = {d.id: d.nombre for d in sdb.query(dim_diagnostico_cls).all()}
    patient_map = {p.id: p for p in sdb.query(dim_paciente_cls).all()}
    por_diagnostico: Dict[str, int] = {}
    por_ecog: Dict[str, int] = {}
    por_charlson: Dict[str, int] = {}
    por_edad: Dict[str, int] = {}
    for row in rows:
        diag_name = diag_map.get(row.diagnostico_id, "NO_REGISTRADO")
        por_diagnostico[diag_name] = por_diagnostico.get(diag_name, 0) + 1
        ecog = row.ecog or "NO_REGISTRADO"
        por_ecog[ecog] = por_ecog.get(ecog, 0) + 1
        charlson = row.charlson or "NO_REGISTRADO"
        por_charlson[charlson] = por_charlson.get(charlson, 0) + 1
        pac = patient_map.get(row.paciente_id)
        edad = pac.edad_quinquenio if pac and pac.edad_quinquenio else "NO_REGISTRADO"
        por_edad[edad] = por_edad.get(edad, 0) + 1
    return {
        "total_oncologicos_programados": len(rows),
        "por_diagnostico": por_diagnostico,
        "por_ecog": por_ecog,
        "por_charlson": por_charlson,
        "por_edad_quinquenio": por_edad,
    }


def stats_lithiasis_payload(
    *,
    sdb: Any,
    hecho_programacion_cls: Any,
) -> Dict[str, Any]:
    rows = sdb.query(hecho_programacion_cls).filter(hecho_programacion_cls.grupo_patologia == "LITIASIS_URINARIA").all()
    por_uh: Dict[str, int] = {}
    por_tamano: Dict[str, int] = {}
    for row in rows:
        uh = row.uh_rango or "NO_REGISTRADO"
        tam = row.litiasis_tamano or "NO_REGISTRADO"
        por_uh[uh] = por_uh.get(uh, 0) + 1
        por_tamano[tam] = por_tamano.get(tam, 0) + 1
    return {
        "total_litiasis_programados": len(rows),
        "por_uh": por_uh,
        "por_tamano": por_tamano,
    }


def stats_surgery_payload(
    *,
    sdb: Any,
    hecho_programacion_cls: Any,
    dim_procedimiento_cls: Any,
    surgical_programacion_cls: Any,
    safe_pct_fn: Callable[[int, int], float],
    calc_percentile_fn: Callable[[List[float], float], Optional[float]],
) -> Dict[str, Any]:
    rows = sdb.query(hecho_programacion_cls).all()
    proc_map = {p.id: p.nombre for p in sdb.query(dim_procedimiento_cls).all()}
    por_procedimiento: Dict[str, int] = {}
    por_grupo: Dict[str, int] = {}
    for row in rows:
        proc_name = proc_map.get(row.procedimiento_id, "NO_REGISTRADO")
        por_procedimiento[proc_name] = por_procedimiento.get(proc_name, 0) + 1
        grp = row.grupo_procedimiento or "NO_REGISTRADO"
        por_grupo[grp] = por_grupo.get(grp, 0) + 1

    tracked = sdb.query(surgical_programacion_cls).filter(
        surgical_programacion_cls.estatus.in_(["PROGRAMADA", "REALIZADA", "CANCELADA"])
    ).all()
    canceladas = [row for row in tracked if (row.estatus or "").upper() == "CANCELADA"]
    turnaround: List[float] = []
    for row in tracked:
        if (row.estatus or "").upper() != "REALIZADA":
            continue
        if not row.fecha_programada:
            continue
        fecha_fin = row.fecha_realizacion or row.fecha_postquirurgica
        if fecha_fin is None:
            continue
        delta = (fecha_fin - row.fecha_programada).days
        if delta >= 0:
            turnaround.append(float(delta))
    return {
        "total_procedimientos": len(rows),
        "por_procedimiento": por_procedimiento,
        "por_grupo_procedimiento": por_grupo,
        "cancelacion_global_pct": safe_pct_fn(len(canceladas), max(1, len(tracked))),
        "tiempo_programada_realizada_mediana_dias": calc_percentile_fn(turnaround, 50),
        "tiempo_programada_realizada_p90_dias": calc_percentile_fn(turnaround, 90),
    }


def trends_diagnosticos_payload(
    *,
    sdb: Any,
    dim_fecha_cls: Any,
    dim_diagnostico_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
) -> List[Dict[str, Any]]:
    query = (
        sdb.query(
            dim_fecha_cls.anio,
            dim_fecha_cls.mes,
            dim_diagnostico_cls.nombre,
            sql_func.count(hecho_programacion_cls.id).label("total"),
        )
        .join(hecho_programacion_cls, hecho_programacion_cls.fecha_id == dim_fecha_cls.id)
        .join(dim_diagnostico_cls, dim_diagnostico_cls.id == hecho_programacion_cls.diagnostico_id)
        .group_by(dim_fecha_cls.anio, dim_fecha_cls.mes, dim_diagnostico_cls.nombre)
        .order_by(dim_fecha_cls.anio, dim_fecha_cls.mes, dim_diagnostico_cls.nombre)
    )
    if anio is not None:
        query = query.filter(dim_fecha_cls.anio == anio)
    if mes is not None:
        query = query.filter(dim_fecha_cls.mes == mes)
    if diagnostico:
        query = query.filter(dim_diagnostico_cls.nombre == diagnostico)
    return [
        {"anio": row.anio, "mes": row.mes, "diagnostico": row.nombre, "cantidad": int(row.total)}
        for row in query.all()
    ]


def trends_procedimientos_payload(
    *,
    sdb: Any,
    dim_fecha_cls: Any,
    dim_procedimiento_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
    anio: Optional[int],
    mes: Optional[int],
    procedimiento: Optional[str],
) -> List[Dict[str, Any]]:
    query = (
        sdb.query(
            dim_fecha_cls.anio,
            dim_fecha_cls.mes,
            dim_procedimiento_cls.nombre,
            sql_func.count(hecho_programacion_cls.id).label("total"),
        )
        .join(hecho_programacion_cls, hecho_programacion_cls.fecha_id == dim_fecha_cls.id)
        .join(dim_procedimiento_cls, dim_procedimiento_cls.id == hecho_programacion_cls.procedimiento_id)
        .group_by(dim_fecha_cls.anio, dim_fecha_cls.mes, dim_procedimiento_cls.nombre)
        .order_by(dim_fecha_cls.anio, dim_fecha_cls.mes, dim_procedimiento_cls.nombre)
    )
    if anio is not None:
        query = query.filter(dim_fecha_cls.anio == anio)
    if mes is not None:
        query = query.filter(dim_fecha_cls.mes == mes)
    if procedimiento:
        query = query.filter(dim_procedimiento_cls.nombre == procedimiento)
    return [
        {"anio": row.anio, "mes": row.mes, "procedimiento": row.nombre, "cantidad": int(row.total)}
        for row in query.all()
    ]


def trends_lista_espera_payload(
    *,
    sdb: Any,
    dim_fecha_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
) -> List[Dict[str, Any]]:
    query = (
        sdb.query(dim_fecha_cls.fecha, sql_func.count(hecho_programacion_cls.id).label("pendientes"))
        .join(hecho_programacion_cls, hecho_programacion_cls.fecha_id == dim_fecha_cls.id)
        .filter(hecho_programacion_cls.estatus == "PROGRAMADA")
        .group_by(dim_fecha_cls.fecha)
        .order_by(dim_fecha_cls.fecha)
    )
    return [{"fecha": row.fecha.isoformat(), "pendientes": int(row.pendientes)} for row in query.all()]


def cie11_search_payload(
    *,
    q: str,
    sdb: Any,
    catalogo_cie11_cls: Any,
    surgical_cie11_map: Dict[str, str],
) -> List[Dict[str, str]]:
    term = (q or "").strip().upper()
    if not term:
        return []
    db_hits = sdb.query(catalogo_cie11_cls).filter(catalogo_cie11_cls.descripcion.ilike(f"%{term}%")).limit(20).all()
    if db_hits:
        return [{"codigo": row.codigo, "descripcion": row.descripcion} for row in db_hits]
    fallback = [{"codigo": code, "descripcion": name} for name, code in surgical_cie11_map.items() if term in name]
    return fallback[:20]


def survival_km_payload(
    *,
    diagnostico: str,
    consultas: List[Any],
    resolve_survival_event_fn: Callable[[Any], Tuple[bool, Optional[date]]],
    kaplan_meier_fn: Callable[[List[int], List[int]], Tuple[List[int], List[float]]],
    kaplan_meier_fitter_cls: Any,
) -> Dict[str, Any]:
    durations: List[int] = []
    events: List[int] = []
    today = date.today()
    for consulta in consultas:
        if not consulta.fecha_registro:
            continue
        event, event_date = resolve_survival_event_fn(consulta)
        end = event_date or today
        durations.append(max((end - consulta.fecha_registro).days, 1))
        events.append(1 if event else 0)
    if not durations:
        return {"diagnostico": diagnostico, "tabla_supervivencia": [], "mediana_tiempo": None}
    if kaplan_meier_fitter_cls is not None:
        kmf = kaplan_meier_fitter_cls()
        kmf.fit(durations, event_observed=events, label=diagnostico)
        table = kmf.survival_function_.reset_index().to_dict(orient="records")
        mediana = float(kmf.median_survival_time_) if kmf.median_survival_time_ is not None else None
        return {"diagnostico": diagnostico, "tabla_supervivencia": table, "mediana_tiempo": mediana}
    timeline, survival = kaplan_meier_fn(durations, events)
    return {
        "diagnostico": diagnostico,
        "tabla_supervivencia": [{"timeline": t_i, diagnostico: s_i} for t_i, s_i in zip(timeline, survival)],
        "mediana_tiempo": None,
    }


def survival_logrank_payload(
    *,
    diagnostico1: str,
    diagnostico2: str,
    build_group_fn: Callable[[str], Tuple[List[int], List[int]]],
    logrank_test_fn: Any,
) -> Tuple[int, Dict[str, Any]]:
    if logrank_test_fn is None:
        return 200, {"p_value": None, "message": "lifelines no disponible"}
    t1, e1 = build_group_fn(diagnostico1)
    t2, e2 = build_group_fn(diagnostico2)
    if not t1 or not t2:
        return 400, {"error": "No hay datos suficientes para ambos diagnósticos"}
    result = logrank_test_fn(t1, t2, event_observed_A=e1, event_observed_B=e2)
    return 200, {"p_value": float(result.p_value)}
