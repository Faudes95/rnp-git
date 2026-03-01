from __future__ import annotations

from typing import Any, Callable, Optional


def backfill_quirofano_to_surgical_flow(
    *,
    new_clinical_session_fn: Callable[..., Any],
    quirofano_model: Any,
    consulta_model: Any,
    sync_quirofano_to_surgical_db_fn: Callable[[Any, Any], None],
    limit: Optional[int] = None,
) -> int:
    """
    Sincroniza programaciones históricas desde la BD principal a la BD quirúrgica.
    """
    db = new_clinical_session_fn(enable_dual_write=True)
    try:
        query = (
            db.query(quirofano_model, consulta_model)
            .join(consulta_model, consulta_model.id == quirofano_model.consulta_id)
            .order_by(quirofano_model.id.desc())
        )
        if limit:
            query = query.limit(limit)
        rows = query.all()
        for cirugia, consulta in rows:
            sync_quirofano_to_surgical_db_fn(consulta, cirugia)
        return len(rows)
    finally:
        db.close()

