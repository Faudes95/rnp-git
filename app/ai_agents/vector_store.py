from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session


def _to_vector_literal(vec: List[float]) -> str:
    safe_vals: List[str] = []
    for v in vec:
        try:
            safe_vals.append(str(float(v)))
        except Exception:
            safe_vals.append("0")
    return "[" + ",".join(safe_vals) + "]"


def ensure_consulta_vector_schema(bind_or_session: Any, *, dim: int = 384) -> bool:
    """Activa pgvector + columna vectorial para consultas si es PostgreSQL.

    No rompe si no hay permisos/extensión.
    """
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return False
    if getattr(bind, "dialect", None) is None or bind.dialect.name != "postgresql":
        return False

    ok = False
    with bind.begin() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        except Exception:
            # Puede fallar por permisos; mantenemos fallback JSON.
            return False

        insp = inspect(conn)
        if "consultas" not in insp.get_table_names():
            return False
        cols = {c["name"] for c in insp.get_columns("consultas")}
        if "embedding_diagnostico_vector" not in cols:
            try:
                conn.execute(text(f"ALTER TABLE consultas ADD COLUMN embedding_diagnostico_vector vector({int(dim)})"))
            except Exception:
                return False
        try:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_consultas_embedding_diagnostico_vector "
                    "ON consultas USING ivfflat (embedding_diagnostico_vector vector_cosine_ops) WITH (lists = 100)"
                )
            )
        except Exception:
            # Índice opcional; si falla, igual puede funcionar la búsqueda.
            pass
        ok = True
    return ok


def sync_consulta_embedding_vector(
    db: Session,
    *,
    consulta_id: int,
    embedding: List[float],
    commit: bool = False,
) -> bool:
    """Sincroniza embedding JSON -> vector pgvector para consultas."""
    if not embedding:
        return False
    bind = getattr(db, "bind", None)
    if bind is None or bind.dialect.name != "postgresql":
        return False
    if not ensure_consulta_vector_schema(db):
        return False
    vec = _to_vector_literal(embedding)
    try:
        db.execute(
            text(
                "UPDATE consultas "
                "SET embedding_diagnostico_vector = CAST(:vec AS vector) "
                "WHERE id = :consulta_id"
            ),
            {"vec": vec, "consulta_id": int(consulta_id)},
        )
        if commit:
            db.commit()
        return True
    except Exception:
        if commit:
            db.rollback()
        return False


def search_consultas_by_vector(
    db: Session,
    *,
    query_vector: List[float],
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Búsqueda semántica usando pgvector en PostgreSQL.

    Retorna lista vacía si pgvector no está disponible.
    """
    if not query_vector:
        return []
    bind = getattr(db, "bind", None)
    if bind is None or bind.dialect.name != "postgresql":
        return []
    if not ensure_consulta_vector_schema(db):
        return []

    vec = _to_vector_literal(query_vector)
    max_rows = max(1, min(int(limit or 20), 200))
    try:
        rows = db.execute(
            text(
                "SELECT id, curp, nombre, diagnostico_principal, "
                "(1 - (embedding_diagnostico_vector <=> CAST(:vec AS vector))) AS similitud "
                "FROM consultas "
                "WHERE embedding_diagnostico_vector IS NOT NULL "
                "ORDER BY embedding_diagnostico_vector <=> CAST(:vec AS vector) "
                "LIMIT :lim"
            ),
            {"vec": vec, "lim": max_rows},
        ).mappings().all()
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            sim = float(r.get("similitud") or 0.0)
        except Exception:
            sim = 0.0
        out.append(
            {
                "id": r.get("id"),
                "curp": r.get("curp"),
                "nombre": r.get("nombre"),
                "diagnostico_principal": r.get("diagnostico_principal"),
                "similitud": f"{sim:.3f}",
            }
        )
    return out

