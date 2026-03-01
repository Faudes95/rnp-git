from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


def build_jj_metrics(
    realizadas_rows: List[Dict[str, Any]],
    *,
    parse_any_date_fn: Callable[[Any], Any],
) -> Dict[str, Any]:
    rows = [
        row
        for row in realizadas_rows
        if str(row.get("cateter_jj_colocado") or "").upper() == "SI"
    ]
    by_origin: Dict[str, int] = {}
    by_medico: Dict[str, int] = {}
    by_proc: Dict[str, int] = {}
    by_week: Dict[str, int] = {}
    by_month: Dict[str, int] = {}

    for row in rows:
        origin = "URGENCIA" if str(row.get("modulo_origen") or "").upper() == "QUIROFANO_URGENCIA" else "PROGRAMADA"
        medico = str(row.get("cirujano") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        proc = str(row.get("procedimiento") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        fecha_jj = parse_any_date_fn(row.get("fecha_colocacion_jj")) or parse_any_date_fn(row.get("fecha_realizacion"))

        by_origin[origin] = by_origin.get(origin, 0) + 1
        by_medico[medico] = by_medico.get(medico, 0) + 1
        by_proc[proc] = by_proc.get(proc, 0) + 1

        if fecha_jj is not None:
            iso = fecha_jj.isocalendar()
            key_week = f"{int(iso[0])}-W{int(iso[1]):02d}"
            key_month = f"{fecha_jj.year}-{fecha_jj.month:02d}"
            by_week[key_week] = by_week.get(key_week, 0) + 1
            by_month[key_month] = by_month.get(key_month, 0) + 1

    def _sorted_items(source: Dict[str, int], top_n: int = 60) -> List[Tuple[str, int]]:
        return sorted(source.items(), key=lambda kv: (-int(kv[1]), kv[0]))[: max(1, int(top_n))]

    return {
        "total_jj_colocados": len(rows),
        "por_origen": _sorted_items(by_origin, top_n=10),
        "por_medico": _sorted_items(by_medico, top_n=80),
        "por_procedimiento": _sorted_items(by_proc, top_n=80),
        "por_semana": sorted(by_week.items(), key=lambda kv: kv[0]),
        "por_mes": sorted(by_month.items(), key=lambda kv: kv[0]),
    }


def build_desglose_from_dict_rows(
    rows: List[Dict[str, Any]],
    *,
    count_by_fn: Callable[[List[Any], Callable[[Any], Any]], List[Tuple[str, int]]],
) -> Dict[str, List[Tuple[str, int]]]:
    def _count(field: str) -> List[Tuple[str, int]]:
        return count_by_fn(rows, lambda row: row.get(field) if isinstance(row, dict) else None)

    return {
        "por_edad": _count("edad_grupo"),
        "por_sexo": _count("sexo"),
        "por_nss": _count("nss"),
        "por_hgz": _count("hgz"),
        "por_procedimiento": _count("procedimiento"),
        "por_diagnostico": _count("diagnostico"),
        "por_prioridad": _count("prioridad_clinica"),
        "por_espera": _count("dias_en_espera_rango"),
        "por_cirujano": _count("cirujano"),
        "por_sangrado": _count("sangrado_ml"),
    }


def build_bar_chart_from_counts(
    counts: List[Tuple[str, int]],
    *,
    title: str,
    color: str = "#13322B",
    max_items: int = 12,
    plt_module: Any,
    fig_to_base64_fn: Callable[[Any], str],
) -> Optional[str]:
    if plt_module is None or not counts:
        return None
    items = counts[: max(1, int(max_items))]
    labels = [str(k) for k, _ in items]
    values = [int(v) for _, v in items]
    if not any(v > 0 for v in values):
        return None
    fig, ax = plt_module.subplots(figsize=(10, 4))
    ax.bar(labels, values, color=color, edgecolor="#0f1f1d", linewidth=0.5)
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=35)
    ax.set_ylabel("Cantidad")
    fig.tight_layout()
    chart = fig_to_base64_fn(fig)
    plt_module.close(fig)
    return chart


def build_hist_chart_from_values(
    values: List[float],
    *,
    title: str,
    bins: int = 12,
    color: str = "#B38E5D",
    plt_module: Any,
    fig_to_base64_fn: Callable[[Any], str],
) -> Optional[str]:
    if plt_module is None or not values:
        return None
    fig, ax = plt_module.subplots(figsize=(9, 4))
    ax.hist(values, bins=max(4, int(bins)), color=color, edgecolor="#0f1f1d", alpha=0.9)
    ax.set_title(title)
    ax.set_ylabel("Cantidad")
    fig.tight_layout()
    chart = fig_to_base64_fn(fig)
    plt_module.close(fig)
    return chart


def rank_preventive_rows(rows: List[Dict[str, Any]], limit: int = 200) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 5000))
    ranked = sorted(
        rows,
        key=lambda row: (
            -float(row.get("score_preventivo") or 0),
            -float(row.get("riesgo_cancelacion_predicho") or 0),
            -(int(row.get("dias_en_espera") or 0)),
            -(int(row.get("consulta_id") or 0)),
        ),
    )
    return ranked[:safe_limit]
