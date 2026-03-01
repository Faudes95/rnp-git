from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class SangradoDeps:
    as_date: Callable[[Any], Optional[date]]
    calc_percentile: Callable[[List[float], float], Optional[float]]
    safe_pct: Callable[[int, int], float]
    extract_numeric_level: Callable[[Any], int]
    plt: Any
    fig_to_base64: Callable[[Any], str]


def build_hemoderivados_metrics(
    *,
    surgical_programadas_rows: List[Any],
    urgencias_programadas_rows: List[Any],
    realizadas_rows: List[Dict[str, Any]],
    count_by_fn: Callable[[Any, Any], List[Tuple[str, int]]],
) -> Dict[str, Any]:
    def _to_int(raw: Any) -> int:
        try:
            value = int(float(raw))
            return value if value > 0 else 0
        except Exception:
            return 0

    def _yes(raw: Any) -> bool:
        return str(raw or "").strip().upper() == "SI"

    urg_solicitudes = [
        r
        for r in urgencias_programadas_rows
        if _yes(getattr(r, "solicita_hemoderivados", None))
    ]
    prog_solicitudes = [
        r
        for r in surgical_programadas_rows
        if _yes(getattr(r, "solicita_hemoderivados", None))
    ]

    urg_rows = []
    for r in urg_solicitudes[:250]:
        urg_rows.append(
            {
                "id": r.id,
                "fecha": r.fecha_urgencia.isoformat() if r.fecha_urgencia else None,
                "nss": r.nss or "NO_REGISTRADO",
                "paciente_nombre": r.paciente_nombre or "NO_REGISTRADO",
                "procedimiento": r.procedimiento_programado or "NO_REGISTRADO",
                "hgz": r.hgz or "NO_REGISTRADO",
                "pg": _to_int(r.hemoderivados_pg_solicitados),
                "pfc": _to_int(r.hemoderivados_pfc_solicitados),
                "cp": _to_int(r.hemoderivados_cp_solicitados),
            }
        )

    prog_rows = []
    for r in prog_solicitudes[:250]:
        prog_rows.append(
            {
                "id": r.id,
                "fecha": r.fecha_programada.isoformat() if r.fecha_programada else None,
                "nss": r.nss or "NO_REGISTRADO",
                "paciente_nombre": r.paciente_nombre or "NO_REGISTRADO",
                "procedimiento": r.procedimiento_programado or r.procedimiento or "NO_REGISTRADO",
                "hgz": r.hgz or "NO_REGISTRADO",
                "pg": _to_int(r.hemoderivados_pg_solicitados),
                "pfc": _to_int(r.hemoderivados_pfc_solicitados),
                "cp": _to_int(r.hemoderivados_cp_solicitados),
            }
        )

    solicitudes_urg_por_proc = count_by_fn(urg_solicitudes, lambda r: r.procedimiento_programado or "NO_REGISTRADO")
    solicitudes_prog_por_proc = count_by_fn(prog_solicitudes, lambda r: r.procedimiento_programado or r.procedimiento or "NO_REGISTRADO")
    solicitudes_urg_por_hgz = count_by_fn(urg_solicitudes, lambda r: r.hgz or "NO_REGISTRADO")
    solicitudes_prog_por_hgz = count_by_fn(prog_solicitudes, lambda r: r.hgz or "NO_REGISTRADO")

    solicitudes_urg_units = {
        "pg": sum(_to_int(r.hemoderivados_pg_solicitados) for r in urg_solicitudes),
        "pfc": sum(_to_int(r.hemoderivados_pfc_solicitados) for r in urg_solicitudes),
        "cp": sum(_to_int(r.hemoderivados_cp_solicitados) for r in urg_solicitudes),
    }
    solicitudes_prog_units = {
        "pg": sum(_to_int(r.hemoderivados_pg_solicitados) for r in prog_solicitudes),
        "pfc": sum(_to_int(r.hemoderivados_pfc_solicitados) for r in prog_solicitudes),
        "cp": sum(_to_int(r.hemoderivados_cp_solicitados) for r in prog_solicitudes),
    }

    uso_rows = [
        r
        for r in realizadas_rows
        if _yes(r.get("uso_hemoderivados")) or _yes(r.get("transfusion")) or _to_int(r.get("hemoderivados_pg_utilizados")) > 0 or _to_int(r.get("hemoderivados_pfc_utilizados")) > 0 or _to_int(r.get("hemoderivados_cp_utilizados")) > 0
    ]
    uso_units = {
        "pg": sum(_to_int(r.get("hemoderivados_pg_utilizados")) for r in uso_rows),
        "pfc": sum(_to_int(r.get("hemoderivados_pfc_utilizados")) for r in uso_rows),
        "cp": sum(_to_int(r.get("hemoderivados_cp_utilizados")) for r in uso_rows),
    }

    uso_por_origen = count_by_fn(
        uso_rows,
        lambda r: "URGENCIA" if str(r.get("modulo_origen") or "").upper() == "QUIROFANO_URGENCIA" else "PROGRAMADA",
    )
    uso_por_cirujano: Dict[str, int] = {}
    uso_por_procedimiento: Dict[str, int] = {}
    for r in uso_rows:
        cir = str(r.get("cirujano") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        proc = str(r.get("procedimiento") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO"
        pg = _to_int(r.get("hemoderivados_pg_utilizados"))
        pfc = _to_int(r.get("hemoderivados_pfc_utilizados"))
        cp = _to_int(r.get("hemoderivados_cp_utilizados"))
        total_units = pg + pfc + cp
        uso_por_cirujano[cir] = uso_por_cirujano.get(cir, 0) + total_units
        uso_por_procedimiento[proc] = uso_por_procedimiento.get(proc, 0) + total_units

    return {
        "solicitudes_urgencias_total": len(urg_solicitudes),
        "solicitudes_programadas_total": len(prog_solicitudes),
        "solicitudes_urgencias_unidades": solicitudes_urg_units,
        "solicitudes_programadas_unidades": solicitudes_prog_units,
        "solicitudes_urgencias_por_procedimiento": solicitudes_urg_por_proc,
        "solicitudes_programadas_por_procedimiento": solicitudes_prog_por_proc,
        "solicitudes_urgencias_por_hgz": solicitudes_urg_por_hgz,
        "solicitudes_programadas_por_hgz": solicitudes_prog_por_hgz,
        "solicitudes_urgencias_rows": urg_rows,
        "solicitudes_programadas_rows": prog_rows,
        "uso_total_cirugias_realizadas": len(uso_rows),
        "uso_unidades_totales": uso_units,
        "uso_por_origen": uso_por_origen,
        "uso_unidades_por_cirujano": sorted(uso_por_cirujano.items(), key=lambda kv: (-kv[1], kv[0]))[:120],
        "uso_unidades_por_procedimiento": sorted(uso_por_procedimiento.items(), key=lambda kv: (-kv[1], kv[0]))[:120],
    }


def build_sangrado_metrics(
    realizadas_rows: List[Dict[str, Any]],
    *,
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    top_n: int = 25,
    deps: SangradoDeps,
) -> Dict[str, Any]:
    def _is_yes(raw_value: Any) -> bool:
        txt = str(raw_value or "").strip().upper()
        if not txt:
            return False
        return txt.startswith("SI") or txt in {"YES", "TRUE", "1", "POSITIVO"}

    def _diagnosis_complexity_weight(diagnostico: str, grupo_patologia: str) -> float:
        diag = (diagnostico or "").upper()
        grp = (grupo_patologia or "").upper()
        if "CANCER DE PROSTATA" in diag:
            return 2.0
        if "CANCER DE VEJIGA" in diag:
            return 2.0
        if "CANCER RENAL" in diag:
            return 1.9
        if "CANCER UROTELIAL TRACTO SUPERIOR" in diag:
            return 1.9
        if "TUMOR SUPRARRENAL" in diag:
            return 1.8
        if "CANCER DE TESTICULO" in diag or "CANCER DE PENE" in diag:
            return 1.7
        if grp == "ONCOLOGICO":
            return 1.6
        if grp == "LITIASIS_URINARIA":
            return 1.0
        return 0.6

    def _complexity_stratum(score: float) -> str:
        if score < 2.8:
            return "BAJA"
        if score < 4.2:
            return "MEDIA"
        return "ALTA"

    period_rows: List[Dict[str, Any]] = []
    for row in realizadas_rows:
        fecha_real = deps.as_date(row.get("fecha_realizacion"))
        if fecha_real is None:
            continue
        if anio is not None and int(fecha_real.year) != int(anio):
            continue
        if mes is not None and int(fecha_real.month) != int(mes):
            continue
        period_rows.append(row)

    with_sangrado: List[Dict[str, Any]] = []
    sangrado_values: List[float] = []
    for row in period_rows:
        try:
            val = row.get("sangrado_ml")
            if val is None:
                continue
            num = float(val)
            if num < 0:
                continue
            sangrado_values.append(num)
            row_copy = dict(row)
            row_copy["sangrado_ml"] = num
            with_sangrado.append(row_copy)
        except Exception:
            continue

    cirujano_stats: Dict[str, Dict[str, Any]] = {}
    procedimiento_stats: Dict[str, Dict[str, Any]] = {}
    combo_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in with_sangrado:
        cirujano = (str(row.get("cirujano") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO")
        procedimiento = (str(row.get("procedimiento") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO")
        sangrado = float(row.get("sangrado_ml") or 0.0)

        c_bucket = cirujano_stats.setdefault(
            cirujano,
            {"cirujano": cirujano, "cirugias": 0, "sangrado_total_ml": 0.0, "sangrado_values": []},
        )
        c_bucket["cirugias"] += 1
        c_bucket["sangrado_total_ml"] += sangrado
        c_bucket["sangrado_values"].append(sangrado)

        p_bucket = procedimiento_stats.setdefault(
            procedimiento,
            {"procedimiento": procedimiento, "cirugias": 0, "sangrado_total_ml": 0.0, "sangrado_values": []},
        )
        p_bucket["cirugias"] += 1
        p_bucket["sangrado_total_ml"] += sangrado
        p_bucket["sangrado_values"].append(sangrado)

        k_combo = (cirujano, procedimiento)
        cp_bucket = combo_stats.setdefault(
            k_combo,
            {
                "cirujano": cirujano,
                "procedimiento": procedimiento,
                "cirugias": 0,
                "sangrado_total_ml": 0.0,
                "sangrado_values": [],
            },
        )
        cp_bucket["cirugias"] += 1
        cp_bucket["sangrado_total_ml"] += sangrado
        cp_bucket["sangrado_values"].append(sangrado)

    def _finalize(rows_iter: List[Dict[str, Any]], field_name: str) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        for row in rows_iter:
            values = [float(x) for x in row.get("sangrado_values", []) if x is not None]
            if not values:
                continue
            output.append(
                {
                    field_name: row.get(field_name),
                    "cirugias": int(row.get("cirugias") or 0),
                    "sangrado_total_ml": round(float(row.get("sangrado_total_ml") or 0.0), 2),
                    "sangrado_promedio_ml": round(sum(values) / len(values), 2),
                    "mediana_ml": deps.calc_percentile(values, 50),
                    "p90_ml": deps.calc_percentile(values, 90),
                    "sangrado_alto_500ml": sum(1 for x in values if float(x) >= 500.0),
                }
            )
        output.sort(
            key=lambda item: (
                -float(item.get("sangrado_total_ml") or 0.0),
                -float(item.get("sangrado_promedio_ml") or 0.0),
                -int(item.get("cirugias") or 0),
                str(item.get(field_name) or ""),
            )
        )
        return output[: max(1, int(top_n))]

    cirujano_rows = _finalize(list(cirujano_stats.values()), "cirujano")
    procedimiento_rows = _finalize(list(procedimiento_stats.values()), "procedimiento")

    combo_rows: List[Dict[str, Any]] = []
    for row in combo_stats.values():
        values = [float(x) for x in row.get("sangrado_values", []) if x is not None]
        if not values:
            continue
        combo_rows.append(
            {
                "cirujano": row.get("cirujano"),
                "procedimiento": row.get("procedimiento"),
                "cirugias": int(row.get("cirugias") or 0),
                "sangrado_total_ml": round(float(row.get("sangrado_total_ml") or 0.0), 2),
                "sangrado_promedio_ml": round(sum(values) / len(values), 2),
                "mediana_ml": deps.calc_percentile(values, 50),
                "p90_ml": deps.calc_percentile(values, 90),
                "sangrado_alto_500ml": sum(1 for x in values if float(x) >= 500.0),
            }
        )
    combo_rows.sort(
        key=lambda item: (
            -float(item.get("sangrado_total_ml") or 0.0),
            -float(item.get("sangrado_promedio_ml") or 0.0),
            -int(item.get("cirugias") or 0),
            str(item.get("cirujano") or ""),
            str(item.get("procedimiento") or ""),
        )
    )
    combo_rows = combo_rows[: max(1, int(top_n))]

    tx_cirujano_stats: Dict[str, Dict[str, Any]] = {}
    tx_procedimiento_stats: Dict[str, Dict[str, Any]] = {}
    transfusion_total = 0
    for row in period_rows:
        cirujano = (str(row.get("cirujano") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO")
        procedimiento = (str(row.get("procedimiento") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO")
        transfusion_si = _is_yes(row.get("transfusion"))
        transfusion_total += 1 if transfusion_si else 0

        c_bucket = tx_cirujano_stats.setdefault(
            cirujano,
            {"cirujano": cirujano, "cirugias": 0, "transfusiones_si": 0},
        )
        c_bucket["cirugias"] += 1
        c_bucket["transfusiones_si"] += 1 if transfusion_si else 0

        p_bucket = tx_procedimiento_stats.setdefault(
            procedimiento,
            {"procedimiento": procedimiento, "cirugias": 0, "transfusiones_si": 0},
        )
        p_bucket["cirugias"] += 1
        p_bucket["transfusiones_si"] += 1 if transfusion_si else 0

    transfusion_por_cirujano = sorted(
        [
            {
                "cirujano": item["cirujano"],
                "cirugias": int(item["cirugias"]),
                "transfusiones_si": int(item["transfusiones_si"]),
                "tasa_transfusion_pct": deps.safe_pct(int(item["transfusiones_si"]), max(1, int(item["cirugias"]))),
            }
            for item in tx_cirujano_stats.values()
        ],
        key=lambda row: (
            -float(row.get("tasa_transfusion_pct") or 0.0),
            -int(row.get("transfusiones_si") or 0),
            -int(row.get("cirugias") or 0),
            str(row.get("cirujano") or ""),
        ),
    )[: max(1, int(top_n))]

    transfusion_por_procedimiento = sorted(
        [
            {
                "procedimiento": item["procedimiento"],
                "cirugias": int(item["cirugias"]),
                "transfusiones_si": int(item["transfusiones_si"]),
                "tasa_transfusion_pct": deps.safe_pct(int(item["transfusiones_si"]), max(1, int(item["cirugias"]))),
            }
            for item in tx_procedimiento_stats.values()
        ],
        key=lambda row: (
            -float(row.get("tasa_transfusion_pct") or 0.0),
            -int(row.get("transfusiones_si") or 0),
            -int(row.get("cirugias") or 0),
            str(row.get("procedimiento") or ""),
        ),
    )[: max(1, int(top_n))]

    complexity_rows: List[Dict[str, Any]] = []
    for row in with_sangrado:
        ecog_num = deps.extract_numeric_level(row.get("ecog"))
        charlson_num = deps.extract_numeric_level(row.get("charlson"))
        diag = str(row.get("diagnostico") or "NO_REGISTRADO")
        grp = str(row.get("grupo_patologia") or "NO_REGISTRADO")
        score = 1.0 + (min(ecog_num, 4) * 0.65) + (min(charlson_num, 10) * 0.20) + _diagnosis_complexity_weight(diag, grp)
        score = round(score, 3)
        complexity_rows.append(
            {
                "cirujano": str(row.get("cirujano") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO",
                "procedimiento": str(row.get("procedimiento") or "NO_REGISTRADO").strip().upper() or "NO_REGISTRADO",
                "sangrado_ml": float(row.get("sangrado_ml") or 0.0),
                "complexity_score": score,
                "stratum": _complexity_stratum(score),
            }
        )

    by_stratum: Dict[str, List[float]] = {"BAJA": [], "MEDIA": [], "ALTA": []}
    for row in complexity_rows:
        by_stratum.setdefault(row["stratum"], []).append(float(row["sangrado_ml"]))
    expected_by_stratum = {
        k: (sum(v) / len(v) if v else 0.0)
        for k, v in by_stratum.items()
    }

    cirujano_adjusted_map: Dict[str, Dict[str, Any]] = {}
    for row in complexity_rows:
        cirujano = row["cirujano"]
        observed = float(row["sangrado_ml"])
        expected = float(expected_by_stratum.get(row["stratum"], 0.0))
        bucket = cirujano_adjusted_map.setdefault(
            cirujano,
            {
                "cirujano": cirujano,
                "cirugias": 0,
                "sangrado_observado_total_ml": 0.0,
                "sangrado_esperado_total_ml": 0.0,
                "complexity_values": [],
            },
        )
        bucket["cirugias"] += 1
        bucket["sangrado_observado_total_ml"] += observed
        bucket["sangrado_esperado_total_ml"] += expected
        bucket["complexity_values"].append(float(row["complexity_score"]))

    ajuste_complejidad_cirujano: List[Dict[str, Any]] = []
    for item in cirujano_adjusted_map.values():
        n = int(item["cirugias"])
        observed_total = float(item["sangrado_observado_total_ml"])
        expected_total = float(item["sangrado_esperado_total_ml"])
        oe_ratio = (observed_total / expected_total) if expected_total > 0 else None
        avg_complexity = round(sum(item["complexity_values"]) / len(item["complexity_values"]), 3) if item["complexity_values"] else None
        ajuste_complejidad_cirujano.append(
            {
                "cirujano": item["cirujano"],
                "cirugias": n,
                "complejidad_promedio": avg_complexity,
                "sangrado_observado_total_ml": round(observed_total, 2),
                "sangrado_esperado_total_ml": round(expected_total, 2),
                "sangrado_promedio_observado_ml": round(observed_total / n, 2) if n > 0 else None,
                "sangrado_promedio_esperado_ml": round(expected_total / n, 2) if n > 0 else None,
                "indice_oe": round(oe_ratio, 3) if oe_ratio is not None else None,
                "delta_oe_pct": round((oe_ratio - 1.0) * 100.0, 2) if oe_ratio is not None else None,
            }
        )
    ajuste_complejidad_cirujano.sort(
        key=lambda row: (
            -(float(row["indice_oe"]) if row.get("indice_oe") is not None else -99999.0),
            -int(row.get("cirugias") or 0),
            str(row.get("cirujano") or ""),
        )
    )
    ajuste_complejidad_cirujano = ajuste_complejidad_cirujano[: max(1, int(top_n))]

    complejidad_estratos = [
        {
            "estrato": key,
            "cirugias": len(values),
            "sangrado_promedio_estrato_ml": round(sum(values) / len(values), 2) if values else None,
        }
        for key, values in by_stratum.items()
    ]

    chart_cirujano_total = None
    chart_procedimiento_total = None
    chart_combo_total = None
    chart_transfusion_cirujano = None
    chart_transfusion_procedimiento = None
    chart_ajuste_complejidad_oe = None
    if deps.plt is not None:
        if cirujano_rows:
            top = cirujano_rows[:10]
            fig, ax = deps.plt.subplots(figsize=(10, 4))
            ax.bar([x["cirujano"] for x in top], [x["sangrado_total_ml"] for x in top], color="#7f2d2d")
            ax.set_title("Sangrado total por cirujano (mL)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_cirujano_total = deps.fig_to_base64(fig)
            deps.plt.close(fig)
        if procedimiento_rows:
            top = procedimiento_rows[:12]
            fig, ax = deps.plt.subplots(figsize=(10, 4))
            ax.bar([x["procedimiento"] for x in top], [x["sangrado_total_ml"] for x in top], color="#13322B")
            ax.set_title("Sangrado total por procedimiento (mL)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_procedimiento_total = deps.fig_to_base64(fig)
            deps.plt.close(fig)
        if combo_rows:
            top = combo_rows[:12]
            fig, ax = deps.plt.subplots(figsize=(11, 4))
            labels = [f"{x['cirujano']} | {x['procedimiento']}" for x in top]
            ax.bar(labels, [x["sangrado_total_ml"] for x in top], color="#B38E5D")
            ax.set_title("Sangrado total por cirujano + procedimiento (mL)")
            ax.tick_params(axis="x", rotation=40)
            fig.tight_layout()
            chart_combo_total = deps.fig_to_base64(fig)
            deps.plt.close(fig)
        if transfusion_por_cirujano:
            top = transfusion_por_cirujano[:10]
            fig, ax = deps.plt.subplots(figsize=(10, 4))
            ax.bar([x["cirujano"] for x in top], [x["tasa_transfusion_pct"] for x in top], color="#8a1538")
            ax.set_title("Tasa de transfusion por cirujano (%)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_transfusion_cirujano = deps.fig_to_base64(fig)
            deps.plt.close(fig)
        if transfusion_por_procedimiento:
            top = transfusion_por_procedimiento[:12]
            fig, ax = deps.plt.subplots(figsize=(10, 4))
            ax.bar([x["procedimiento"] for x in top], [x["tasa_transfusion_pct"] for x in top], color="#24584f")
            ax.set_title("Tasa de transfusion por procedimiento (%)")
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            chart_transfusion_procedimiento = deps.fig_to_base64(fig)
            deps.plt.close(fig)
        if ajuste_complejidad_cirujano:
            top = [x for x in ajuste_complejidad_cirujano if x.get("indice_oe") is not None][:10]
            if top:
                fig, ax = deps.plt.subplots(figsize=(10, 4))
                ax.bar([x["cirujano"] for x in top], [x["indice_oe"] for x in top], color="#B38E5D")
                ax.axhline(1.0, color="#13322B", linestyle="--", linewidth=1.2)
                ax.set_title("Indice O/E de sangrado ajustado por complejidad (cirujano)")
                ax.tick_params(axis="x", rotation=35)
                fig.tight_layout()
                chart_ajuste_complejidad_oe = deps.fig_to_base64(fig)
                deps.plt.close(fig)

    return {
        "periodo": {"anio": anio, "mes": mes},
        "total_cirugias_realizadas_periodo": len(period_rows),
        "cirugias_con_sangrado_registrado": len(with_sangrado),
        "sangrado_total_ml": round(sum(sangrado_values), 2) if sangrado_values else 0.0,
        "sangrado_promedio_ml": round(sum(sangrado_values) / len(sangrado_values), 2) if sangrado_values else None,
        "sangrado_mediana_ml": deps.calc_percentile(sangrado_values, 50),
        "sangrado_p90_ml": deps.calc_percentile(sangrado_values, 90),
        "cirujano_top": cirujano_rows,
        "procedimiento_top": procedimiento_rows,
        "cirujano_procedimiento_top": combo_rows,
        "transfusion_global": {
            "total_cirugias_periodo": len(period_rows),
            "transfusiones_si": int(transfusion_total),
            "tasa_transfusion_pct": deps.safe_pct(int(transfusion_total), max(1, len(period_rows))),
        },
        "transfusion_por_cirujano": transfusion_por_cirujano,
        "transfusion_por_procedimiento": transfusion_por_procedimiento,
        "complejidad_estratos": complejidad_estratos,
        "ajuste_complejidad_cirujano": ajuste_complejidad_cirujano,
        "chart_cirujano_total": chart_cirujano_total,
        "chart_procedimiento_total": chart_procedimiento_total,
        "chart_cirujano_procedimiento": chart_combo_total,
        "chart_transfusion_cirujano": chart_transfusion_cirujano,
        "chart_transfusion_procedimiento": chart_transfusion_procedimiento,
        "chart_ajuste_complejidad_oe": chart_ajuste_complejidad_oe,
    }

