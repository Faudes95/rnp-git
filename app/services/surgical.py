"""Servicios de analítica quirúrgica aditivos."""

from typing import Any, Dict, Iterable, List, Tuple


def _count_values(values: Iterable[str]) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for value in values:
        key = (value or "NO_REGISTRADO").strip() or "NO_REGISTRADO"
        counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def resumen_programaciones(rows: Iterable[Any]) -> Dict[str, Any]:
    rows_list = list(rows)
    sexo_counts = _count_values(getattr(r, "sexo", None) for r in rows_list)
    patologia_counts = _count_values(getattr(r, "patologia", None) for r in rows_list)
    procedimiento_counts = _count_values(
        (getattr(r, "procedimiento_programado", None) or getattr(r, "procedimiento", None))
        for r in rows_list
    )
    estatus_counts = _count_values(getattr(r, "estatus", None) for r in rows_list)
    hgz_counts = _count_values(getattr(r, "hgz", None) for r in rows_list)
    return {
        "total": len(rows_list),
        "sexo_counts": sexo_counts,
        "patologia_counts": patologia_counts,
        "procedimiento_counts": procedimiento_counts,
        "estatus_counts": estatus_counts,
        "hgz_counts": hgz_counts,
    }

