from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple


def predict_risk_response(
    *,
    edad: int,
    sexo: str,
    ecog: Optional[str],
    charlson: Optional[str],
    predict_risk_payload_fn: Callable[..., Tuple[int, Dict[str, Any]]],
    cargar_modelo_riesgo_fn: Callable[[], Any],
    normalize_upper_fn: Callable[[Optional[str]], Optional[str]],
    parse_int_from_text_fn: Callable[[Any], Optional[int]],
) -> Tuple[int, Dict[str, Any]]:
    return predict_risk_payload_fn(
        edad=edad,
        sexo=sexo,
        ecog=ecog,
        charlson=charlson,
        cargar_modelo_riesgo_fn=cargar_modelo_riesgo_fn,
        normalize_upper_fn=normalize_upper_fn,
        parse_int_from_text_fn=parse_int_from_text_fn,
    )


def survival_logrank_response(
    *,
    diagnostico1: str,
    diagnostico2: str,
    db: Any,
    consulta_model: Any,
    resolve_survival_event_fn: Callable[[Any], Tuple[bool, Optional[date]]],
    survival_logrank_payload_fn: Callable[..., Tuple[int, Dict[str, Any]]],
    logrank_test_fn: Any,
) -> Tuple[int, Dict[str, Any]]:
    def build_group(diag: str) -> Tuple[List[int], List[int]]:
        rows = db.query(consulta_model).filter(consulta_model.diagnostico_principal == diag).all()
        durations: List[int] = []
        events: List[int] = []
        today = date.today()
        for c in rows:
            if not c.fecha_registro:
                continue
            event, event_date = resolve_survival_event_fn(c)
            end = event_date or today
            durations.append(max((end - c.fecha_registro).days, 1))
            events.append(1 if event else 0)
        return durations, events

    return survival_logrank_payload_fn(
        diagnostico1=diagnostico1,
        diagnostico2=diagnostico2,
        build_group_fn=build_group,
        logrank_test_fn=logrank_test_fn,
    )


def research_export_csv_content(
    *,
    sdb: Any,
    hecho_programacion_cls: Any,
    dim_paciente_cls: Any,
    build_research_records_fn: Callable[[List[Any]], List[Dict[str, Any]]],
    records_to_csv_fn: Callable[..., str],
    pd_module: Any,
) -> str:
    rows = (
        sdb.query(
            hecho_programacion_cls,
            dim_paciente_cls.edad_quinquenio,
            dim_paciente_cls.sexo,
        )
        .outerjoin(dim_paciente_cls, dim_paciente_cls.id == hecho_programacion_cls.paciente_id)
        .all()
    )
    records = build_research_records_fn(rows)
    return records_to_csv_fn(records, pd_module=pd_module)


def geostats_hgz_payload(
    *,
    sdb: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
) -> List[Dict[str, Any]]:
    rows = (
        sdb.query(
            hecho_programacion_cls.hgz,
            sql_func.count(hecho_programacion_cls.id).label("total"),
        )
        .group_by(hecho_programacion_cls.hgz)
        .all()
    )
    return [{"hgz": h or "NO_REGISTRADO", "total": int(t)} for h, t in rows]


def dashboard_payload(
    *,
    section: str,
    sdb: Any,
    dashboard_service: Any,
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    limit: int = 300,
) -> Dict[str, Any]:
    if section == "resumen":
        return dashboard_service.dashboard_resumen_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
        )
    if section == "tendencia":
        return dashboard_service.dashboard_tendencia_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
        )
    if section == "diagnosticos":
        return dashboard_service.dashboard_diagnosticos_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
        )
    if section == "sexo":
        return dashboard_service.dashboard_sexo_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
        )
    if section == "procedimientos_top":
        return dashboard_service.dashboard_procedimientos_top_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
        )
    if section == "detalle":
        return dashboard_service.dashboard_detalle_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
            limit=limit,
        )
    if section == "hgz":
        return dashboard_service.dashboard_hgz_payload(
            sdb,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
        )
    return {}

