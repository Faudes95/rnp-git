"""Servicios de reporte/BI para extracción progresiva."""

from datetime import datetime
from typing import Any, Dict


def agregar_timestamp(context: Dict[str, Any], now: datetime | None = None) -> Dict[str, Any]:
    out = dict(context)
    ts = now or datetime.now()
    out["fecha"] = ts.strftime("%Y-%m-%d %H:%M")
    return out


def resumen_serializable_reporte(context: Dict[str, Any]) -> Dict[str, Any]:
    """Genera un resumen JSON seguro sin alterar el reporte HTML original."""
    cancelacion_global = context.get("cancelacion_global") or {}
    tiempo_qx = context.get("tiempo_programada_a_realizada") or {}
    embudo = context.get("embudo_operativo") or {}
    labs = context.get("incidencia_laboratorios") or {}
    sangrado_mes = context.get("sangrado_metricas_mes") or {}
    ajuste_rows = sangrado_mes.get("ajuste_complejidad_cirujano") or []
    top_oe = ajuste_rows[0] if ajuste_rows else {}
    return {
        "total": context.get("total", 0),
        "total_onco": context.get("total_onco", 0),
        "completos": context.get("completos", 0),
        "incompletos": context.get("incompletos", 0),
        "total_programados": context.get("total_programados", 0),
        "total_pendientes_programar": context.get("total_pendientes_programar", 0),
        "total_realizadas": context.get("total_realizadas", 0),
        "sexo_counts_total": len(context.get("sexo_counts", []) or []),
        "patologias_counts_total": len(context.get("patologias_counts", []) or []),
        "procedimientos_counts_total": len(context.get("procedimientos_counts", []) or []),
        "hgz_counts_total": len(context.get("hgz_counts", []) or []),
        "onco_diag_counts_total": len(context.get("onco_diag_counts", []) or []),
        "litiasis_diag_counts_total": len(context.get("litiasis_diag_counts", []) or []),
        "edad_programados_counts_total": len(context.get("edad_programados_counts", []) or []),
        "cancelacion_tasa_pct": cancelacion_global.get("tasa_pct", 0.0),
        "tiempo_programada_realizada_mediana_dias": tiempo_qx.get("mediana_dias"),
        "embudo_ingreso": embudo.get("ingreso", 0),
        "embudo_programacion": embudo.get("programacion", 0),
        "embudo_realizada": embudo.get("cirugia_realizada", 0),
        "aki_delta_creatinina_pacientes": ((labs.get("aki_delta_creatinina") or {}).get("pacientes", 0)),
        "clostridium_pacientes": ((labs.get("infeccion_clostridium") or {}).get("pacientes", 0)),
        "sangrado_total_mes_ml": sangrado_mes.get("sangrado_total_ml", 0.0),
        "sangrado_promedio_mes_ml": sangrado_mes.get("sangrado_promedio_ml"),
        "sangrado_p90_mes_ml": sangrado_mes.get("sangrado_p90_ml"),
        "transfusion_tasa_mes_pct": ((sangrado_mes.get("transfusion_global") or {}).get("tasa_transfusion_pct", 0.0)),
        "ajuste_oe_cirujano_top": {
            "cirujano": top_oe.get("cirujano"),
            "indice_oe": top_oe.get("indice_oe"),
            "delta_oe_pct": top_oe.get("delta_oe_pct"),
        },
        "notice": context.get("notice", ""),
        "fecha": context.get("fecha"),
    }
