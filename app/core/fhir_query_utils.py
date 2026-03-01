from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Callable, List, Optional, Tuple

from sqlalchemy.orm import Session


FHIR_CURP_REGEX = r"^[A-Z]{4}\d{6}[HM]"


def parse_fhir_date_filter(date_filter: str) -> Tuple[str, date]:
    value = (date_filter or "").strip()
    if not value:
        raise ValueError("Filtro de fecha vacío")
    op = "eq"
    raw = value
    for prefix in ("ge", "le", "gt", "lt", "eq"):
        if value.startswith(prefix):
            op = prefix
            raw = value[len(prefix) :]
            break
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception as exc:
        raise ValueError(
            "Formato de fecha inválido. Use YYYY-MM-DD o prefijos ge/le/gt/lt/eq"
        ) from exc
    return op, parsed


def apply_date_filter_to_date_column(query: Any, column: Any, date_filter: Optional[str]) -> Any:
    if not date_filter:
        return query
    op, parsed = parse_fhir_date_filter(date_filter)
    if op == "ge":
        return query.filter(column >= parsed)
    if op == "le":
        return query.filter(column <= parsed)
    if op == "gt":
        return query.filter(column > parsed)
    if op == "lt":
        return query.filter(column < parsed)
    return query.filter(column == parsed)


def lookup_consulta_ids_by_subject(
    subject: str,
    db: Session,
    *,
    consulta_model: Any,
    normalize_curp_fn: Callable[[str], str],
) -> List[int]:
    if not subject or not subject.startswith("Patient/"):
        return []
    ref = subject.replace("Patient/", "").strip()
    if re.match(FHIR_CURP_REGEX, ref):
        rows = db.query(consulta_model.id).filter(consulta_model.curp == normalize_curp_fn(ref)).all()
        return [r.id for r in rows]
    if ref.isdigit():
        return [int(ref)]
    rows = db.query(consulta_model.id).filter(consulta_model.nombre.contains(ref)).all()
    return [r.id for r in rows]

