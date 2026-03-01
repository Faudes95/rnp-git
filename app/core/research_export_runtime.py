from __future__ import annotations

import csv
import io
from typing import Any, Dict, List


RESEARCH_EXPORT_FIELDS = [
    "fecha_id",
    "edad_quinquenio",
    "sexo",
    "grupo_patologia",
    "grupo_procedimiento",
    "ecog",
    "charlson",
    "uh_rango",
    "litiasis_tamano",
    "estatus",
]


def build_research_records(rows: List[Any]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for hecho, edad_quinquenio, sexo in rows:
        records.append(
            {
                "fecha_id": hecho.fecha_id,
                "edad_quinquenio": edad_quinquenio,
                "sexo": sexo,
                "grupo_patologia": hecho.grupo_patologia,
                "grupo_procedimiento": hecho.grupo_procedimiento,
                "ecog": hecho.ecog,
                "charlson": hecho.charlson,
                "uh_rango": hecho.uh_rango,
                "litiasis_tamano": hecho.litiasis_tamano,
                "estatus": hecho.estatus,
            }
        )
    return records


def records_to_csv(records: List[Dict[str, Any]], *, pd_module: Any) -> str:
    if pd_module is not None:
        return pd_module.DataFrame(records).to_csv(index=False)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=RESEARCH_EXPORT_FIELDS)
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)
    return buffer.getvalue()
