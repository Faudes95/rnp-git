from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session


_DYNAMIC_SYMBOLS = [
    "SurgicalProgramacionDB",
    "SURGICAL_IS_SQLITE",
    "func",
    "Integer",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing: List[str] = []
    for symbol in _DYNAMIC_SYMBOLS:
        if symbol in module_globals:
            continue
        try:
            module_globals[symbol] = getattr(m, symbol)
        except Exception:
            missing.append(symbol)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def dashboard_apply_filters(
    query,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
):
    _ensure_symbols()
    query = query.filter(SurgicalProgramacionDB.estatus == "PROGRAMADA")
    if anio is not None:
        if SURGICAL_IS_SQLITE:
            query = query.filter(func.strftime("%Y", SurgicalProgramacionDB.fecha_programada) == str(int(anio)))
        else:
            query = query.filter(func.extract("year", SurgicalProgramacionDB.fecha_programada) == int(anio))
    if mes is not None:
        if SURGICAL_IS_SQLITE:
            query = query.filter(func.strftime("%m", SurgicalProgramacionDB.fecha_programada) == f"{int(mes):02d}")
        else:
            query = query.filter(func.extract("month", SurgicalProgramacionDB.fecha_programada) == int(mes))
    if diagnostico:
        query = query.filter(SurgicalProgramacionDB.patologia == diagnostico)
    if hgz:
        query = query.filter(SurgicalProgramacionDB.hgz == hgz)
    return query


def dashboard_year_month_expressions():
    _ensure_symbols()
    if SURGICAL_IS_SQLITE:
        year_expr = func.cast(func.strftime("%Y", SurgicalProgramacionDB.fecha_programada), Integer)
        month_expr = func.cast(func.strftime("%m", SurgicalProgramacionDB.fecha_programada), Integer)
    else:
        year_expr = func.cast(func.extract("year", SurgicalProgramacionDB.fecha_programada), Integer)
        month_expr = func.cast(func.extract("month", SurgicalProgramacionDB.fecha_programada), Integer)
    return year_expr, month_expr


def dashboard_resumen_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
) -> Dict[str, Any]:
    _ensure_symbols()
    base = dashboard_apply_filters(sdb.query(SurgicalProgramacionDB), anio, mes, diagnostico, hgz)
    total = base.count()
    oncologicos = dashboard_apply_filters(
        sdb.query(SurgicalProgramacionDB).filter(SurgicalProgramacionDB.grupo_patologia == "ONCOLOGICO"),
        anio,
        mes,
        diagnostico,
        hgz,
    ).count()
    litiasis = dashboard_apply_filters(
        sdb.query(SurgicalProgramacionDB).filter(SurgicalProgramacionDB.grupo_patologia == "LITIASIS_URINARIA"),
        anio,
        mes,
        diagnostico,
        hgz,
    ).count()
    proc_unicos = (
        dashboard_apply_filters(
            sdb.query(SurgicalProgramacionDB.procedimiento_programado).distinct(),
            anio,
            mes,
            diagnostico,
            hgz,
        ).count()
    )
    total_i = int(total)
    oncologicos_i = int(oncologicos)
    litiasis_i = int(litiasis)
    proc_unicos_i = int(proc_unicos)
    return {
        "total": total_i,
        "oncologicos": oncologicos_i,
        "litiasis": litiasis_i,
        "proc_unicos": proc_unicos_i,
        "total_procedimientos": total_i,
        "total_oncologicos": oncologicos_i,
        "total_litiasis": litiasis_i,
    }


def dashboard_tendencia_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    year_expr, month_expr = dashboard_year_month_expressions()
    query = sdb.query(
        year_expr.label("anio"),
        month_expr.label("mes"),
        func.count(SurgicalProgramacionDB.id).label("cantidad"),
    )
    query = dashboard_apply_filters(query, anio, mes, diagnostico, hgz)
    query = query.group_by(year_expr, month_expr).order_by(year_expr, month_expr)
    return [{"anio": int(r.anio), "mes": int(r.mes), "cantidad": int(r.cantidad)} for r in query.all() if r.anio and r.mes]


def dashboard_diagnosticos_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    query = sdb.query(
        SurgicalProgramacionDB.patologia.label("label"),
        func.count(SurgicalProgramacionDB.id).label("value"),
    )
    query = dashboard_apply_filters(query, anio, mes, diagnostico, hgz)
    query = query.group_by(SurgicalProgramacionDB.patologia).order_by(func.count(SurgicalProgramacionDB.id).desc(), SurgicalProgramacionDB.patologia)
    return [{"label": r.label or "NO_REGISTRADO", "value": int(r.value)} for r in query.limit(20).all()]


def dashboard_sexo_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    query = sdb.query(
        SurgicalProgramacionDB.sexo.label("label"),
        func.count(SurgicalProgramacionDB.id).label("value"),
    )
    query = dashboard_apply_filters(query, anio, mes, diagnostico, hgz)
    query = query.group_by(SurgicalProgramacionDB.sexo).order_by(func.count(SurgicalProgramacionDB.id).desc())
    return [{"label": r.label or "NO_REGISTRADO", "value": int(r.value)} for r in query.all()]


def dashboard_procedimientos_top_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    query = sdb.query(
        SurgicalProgramacionDB.procedimiento_programado.label("label"),
        func.count(SurgicalProgramacionDB.id).label("value"),
    )
    query = dashboard_apply_filters(query, anio, mes, diagnostico, hgz)
    query = query.group_by(SurgicalProgramacionDB.procedimiento_programado).order_by(func.count(SurgicalProgramacionDB.id).desc())
    return [{"label": r.label or "NO_REGISTRADO", "value": int(r.value)} for r in query.limit(10).all()]


def dashboard_detalle_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
    hgz: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    safe_limit = max(1, min(int(limit), 1000))
    query = dashboard_apply_filters(sdb.query(SurgicalProgramacionDB), anio, mes, diagnostico, hgz)
    rows = query.order_by(SurgicalProgramacionDB.fecha_programada.desc(), SurgicalProgramacionDB.id.desc()).limit(safe_limit).all()
    return [
        {
            "id": row.id,
            "fecha": row.fecha_programada.isoformat() if row.fecha_programada else "",
            "diagnostico": row.patologia or "NO_REGISTRADO",
            "procedimiento": row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO",
            "sexo": row.sexo or "NO_REGISTRADO",
            "edad": row.edad if row.edad is not None else "NO_REGISTRADO",
            "hgz": row.hgz or "NO_REGISTRADO",
        }
        for row in rows
    ]


def dashboard_hgz_payload(
    sdb: Session,
    *,
    anio: Optional[int],
    mes: Optional[int],
    diagnostico: Optional[str],
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    query = sdb.query(
        SurgicalProgramacionDB.hgz.label("hgz"),
        func.count(SurgicalProgramacionDB.id).label("total"),
    )
    query = dashboard_apply_filters(query, anio, mes, diagnostico, None)
    query = query.filter(SurgicalProgramacionDB.hgz.isnot(None)).filter(SurgicalProgramacionDB.hgz != "")
    query = query.group_by(SurgicalProgramacionDB.hgz).order_by(func.count(SurgicalProgramacionDB.id).desc(), SurgicalProgramacionDB.hgz)
    return [{"hgz": r.hgz, "total": int(r.total)} for r in query.all()]

