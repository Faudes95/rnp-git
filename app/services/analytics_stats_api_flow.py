from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


def stats_oncology_response(
    *,
    sdb: Any,
    stats_oncology_payload_fn: Callable[..., Dict[str, Any]],
    hecho_programacion_cls: Any,
    dim_diagnostico_cls: Any,
    dim_paciente_cls: Any,
) -> Dict[str, Any]:
    return stats_oncology_payload_fn(
        sdb=sdb,
        hecho_programacion_cls=hecho_programacion_cls,
        dim_diagnostico_cls=dim_diagnostico_cls,
        dim_paciente_cls=dim_paciente_cls,
    )


def stats_lithiasis_response(
    *,
    sdb: Any,
    stats_lithiasis_payload_fn: Callable[..., Dict[str, Any]],
    hecho_programacion_cls: Any,
) -> Dict[str, Any]:
    return stats_lithiasis_payload_fn(
        sdb=sdb,
        hecho_programacion_cls=hecho_programacion_cls,
    )


def stats_surgery_response(
    *,
    sdb: Any,
    stats_surgery_payload_fn: Callable[..., Dict[str, Any]],
    hecho_programacion_cls: Any,
    dim_procedimiento_cls: Any,
    surgical_programacion_cls: Any,
    safe_pct_fn: Callable[[int, int], float],
    calc_percentile_fn: Callable[[list[float], float], Optional[float]],
) -> Dict[str, Any]:
    return stats_surgery_payload_fn(
        sdb=sdb,
        hecho_programacion_cls=hecho_programacion_cls,
        dim_procedimiento_cls=dim_procedimiento_cls,
        surgical_programacion_cls=surgical_programacion_cls,
        safe_pct_fn=safe_pct_fn,
        calc_percentile_fn=calc_percentile_fn,
    )


def trends_diagnosticos_response(
    *,
    sdb: Any,
    trends_diagnosticos_payload_fn: Callable[..., Any],
    dim_fecha_cls: Any,
    dim_diagnostico_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
) -> Any:
    return trends_diagnosticos_payload_fn(
        sdb=sdb,
        dim_fecha_cls=dim_fecha_cls,
        dim_diagnostico_cls=dim_diagnostico_cls,
        hecho_programacion_cls=hecho_programacion_cls,
        sql_func=sql_func,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
    )


def trends_procedimientos_response(
    *,
    sdb: Any,
    trends_procedimientos_payload_fn: Callable[..., Any],
    dim_fecha_cls: Any,
    dim_procedimiento_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
    anio: Optional[int],
    mes: Optional[int],
    procedimiento: Optional[str],
) -> Any:
    return trends_procedimientos_payload_fn(
        sdb=sdb,
        dim_fecha_cls=dim_fecha_cls,
        dim_procedimiento_cls=dim_procedimiento_cls,
        hecho_programacion_cls=hecho_programacion_cls,
        sql_func=sql_func,
        anio=anio,
        mes=mes,
        procedimiento=procedimiento,
    )


def trends_lista_espera_response(
    *,
    sdb: Any,
    trends_lista_espera_payload_fn: Callable[..., Any],
    dim_fecha_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
) -> Any:
    return trends_lista_espera_payload_fn(
        sdb=sdb,
        dim_fecha_cls=dim_fecha_cls,
        hecho_programacion_cls=hecho_programacion_cls,
        sql_func=sql_func,
    )


def cie11_search_response(
    *,
    q: str,
    sdb: Any,
    cie11_search_payload_fn: Callable[..., Any],
    catalogo_cie11_cls: Any,
    surgical_cie11_map: dict[str, str],
) -> Any:
    return cie11_search_payload_fn(
        q=q,
        sdb=sdb,
        catalogo_cie11_cls=catalogo_cie11_cls,
        surgical_cie11_map=surgical_cie11_map,
    )


def survival_km_response(
    *,
    diagnostico: str,
    db: Any,
    consulta_model: Any,
    survival_km_payload_fn: Callable[..., Dict[str, Any]],
    resolve_survival_event_fn: Callable[[Any], Tuple[bool, Any]],
    kaplan_meier_fn: Callable[..., Any],
    kaplan_meier_fitter_cls: Any,
) -> Dict[str, Any]:
    consultas = db.query(consulta_model).filter(consulta_model.diagnostico_principal == diagnostico).all()
    return survival_km_payload_fn(
        diagnostico=diagnostico,
        consultas=consultas,
        resolve_survival_event_fn=resolve_survival_event_fn,
        kaplan_meier_fn=kaplan_meier_fn,
        kaplan_meier_fitter_cls=kaplan_meier_fitter_cls,
    )


def stats_response_by_kind(
    *,
    kind: str,
    sdb: Any,
    stats_oncology_payload_fn: Callable[..., Dict[str, Any]],
    stats_lithiasis_payload_fn: Callable[..., Dict[str, Any]],
    stats_surgery_payload_fn: Callable[..., Dict[str, Any]],
    hecho_programacion_cls: Any,
    dim_diagnostico_cls: Any,
    dim_paciente_cls: Any,
    dim_procedimiento_cls: Any,
    surgical_programacion_cls: Any,
    safe_pct_fn: Callable[[int, int], float],
    calc_percentile_fn: Callable[[list[float], float], Optional[float]],
) -> Dict[str, Any]:
    if kind == "oncology":
        return stats_oncology_response(
            sdb=sdb,
            stats_oncology_payload_fn=stats_oncology_payload_fn,
            hecho_programacion_cls=hecho_programacion_cls,
            dim_diagnostico_cls=dim_diagnostico_cls,
            dim_paciente_cls=dim_paciente_cls,
        )
    if kind == "lithiasis":
        return stats_lithiasis_response(
            sdb=sdb,
            stats_lithiasis_payload_fn=stats_lithiasis_payload_fn,
            hecho_programacion_cls=hecho_programacion_cls,
        )
    return stats_surgery_response(
        sdb=sdb,
        stats_surgery_payload_fn=stats_surgery_payload_fn,
        hecho_programacion_cls=hecho_programacion_cls,
        dim_procedimiento_cls=dim_procedimiento_cls,
        surgical_programacion_cls=surgical_programacion_cls,
        safe_pct_fn=safe_pct_fn,
        calc_percentile_fn=calc_percentile_fn,
    )


def trends_response_by_kind(
    *,
    kind: str,
    sdb: Any,
    trends_diagnosticos_payload_fn: Callable[..., Any],
    trends_procedimientos_payload_fn: Callable[..., Any],
    trends_lista_espera_payload_fn: Callable[..., Any],
    dim_fecha_cls: Any,
    dim_diagnostico_cls: Any,
    dim_procedimiento_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    procedimiento: Optional[str],
) -> Any:
    if kind == "diagnosticos":
        return trends_diagnosticos_response(
            sdb=sdb,
            trends_diagnosticos_payload_fn=trends_diagnosticos_payload_fn,
            dim_fecha_cls=dim_fecha_cls,
            dim_diagnostico_cls=dim_diagnostico_cls,
            hecho_programacion_cls=hecho_programacion_cls,
            sql_func=sql_func,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
        )
    if kind == "procedimientos":
        return trends_procedimientos_response(
            sdb=sdb,
            trends_procedimientos_payload_fn=trends_procedimientos_payload_fn,
            dim_fecha_cls=dim_fecha_cls,
            dim_procedimiento_cls=dim_procedimiento_cls,
            hecho_programacion_cls=hecho_programacion_cls,
            sql_func=sql_func,
            anio=anio,
            mes=mes,
            procedimiento=procedimiento,
        )
    return trends_lista_espera_response(
        sdb=sdb,
        trends_lista_espera_payload_fn=trends_lista_espera_payload_fn,
        dim_fecha_cls=dim_fecha_cls,
        hecho_programacion_cls=hecho_programacion_cls,
        sql_func=sql_func,
    )
