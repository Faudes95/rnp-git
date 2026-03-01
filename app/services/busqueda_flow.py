from typing import Any, Optional

from fastapi import Request
from sqlalchemy.orm import Session

from app.ai_agents.vector_store import search_consultas_by_vector


async def busqueda_flow(request: Request, q: Optional[str], db: Session) -> Any:
    from app.core.app_context import main_proxy as m

    resultados = []
    query = (q or "").strip()
    if query:
        curp_q = m.normalize_curp(query)
        nss_q = m.normalize_nss(query)
        if m.re.match(r"^[A-Z]{4}\d{6}[HM]", curp_q):
            resultados = db.query(m.ConsultaDB).filter(m.ConsultaDB.curp == curp_q).all()
        elif m.re.match(r"^\d{10}$", nss_q):
            resultados = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == nss_q).all()
        else:
            resultados = db.query(m.ConsultaDB).filter(m.ConsultaDB.nombre.contains(query)).all()
    return m.render_template(
        m.BUSQUEDA_TEMPLATE,
        request=request,
        query=query,
        resultados=resultados,
    )


async def busqueda_semantica_flow(request: Request, q: Optional[str], db: Session) -> Any:
    from app.core.app_context import main_proxy as m

    resultados = []
    message = ""
    query = (q or "").strip()
    if query:
        model = m.get_semantic_model()
        if model is None:
            message = "Modelo semántico no disponible. Instale sentence-transformers para habilitar esta función."
        else:
            query_vec = model.encode(query).tolist()
            # Ruta prioritaria PostgreSQL + pgvector (rápida).
            resultados = search_consultas_by_vector(
                db,
                query_vector=query_vec,
                limit=20,
            )

            # Fallback legacy JSON en memoria para compatibilidad con SQLite.
            if not resultados:
                consultas = (
                    db.query(m.ConsultaDB)
                    .filter(m.ConsultaDB.embedding_diagnostico.isnot(None))
                    .all()
                )
                ranked = []
                for consulta in consultas:
                    sim = m.cosine_similarity(consulta.embedding_diagnostico, query_vec)
                    ranked.append((sim, consulta))
                ranked.sort(key=lambda x: x[0], reverse=True)
                for sim, consulta in ranked[:20]:
                    resultados.append(
                        {
                            "id": consulta.id,
                            "curp": consulta.curp,
                            "nombre": consulta.nombre,
                            "diagnostico_principal": consulta.diagnostico_principal,
                            "similitud": f"{sim:.3f}",
                        }
                    )
    return m.render_template(
        m.BUSQUEDA_SEMANTICA_TEMPLATE,
        request=request,
        query=query,
        resultados=resultados,
        message=message,
    )
