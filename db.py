from typing import Optional, List

from main import SessionLocal, ConsultaDB
from app.ai_agents.vector_store import sync_consulta_embedding_vector


def save_embedding(note_id: int, vec: Optional[List[float]]):
    if vec is None:
        return
    session = SessionLocal()
    try:
        consulta = session.query(ConsultaDB).filter(ConsultaDB.id == note_id).first()
        if not consulta:
            return
        consulta.embedding_diagnostico = vec
        session.add(consulta)
        session.commit()
        sync_consulta_embedding_vector(
            session,
            consulta_id=int(note_id),
            embedding=vec,
            commit=True,
        )
    finally:
        session.close()
