import argparse

from main import SessionLocal, ConsultaDB
from embeddings import compute_embedding


def build_text(consulta: ConsultaDB) -> str:
    parts = [
        consulta.diagnostico_principal,
        consulta.padecimiento_actual,
        consulta.exploracion_fisica,
        consulta.estudios_hallazgos,
        consulta.plan_especifico,
    ]
    return ". ".join([p for p in parts if p])


def run_backfill(limit: int = 0, force: bool = False):
    session = SessionLocal()
    try:
        query = session.query(ConsultaDB)
        if not force:
            query = query.filter(ConsultaDB.embedding_diagnostico.is_(None))
        if limit and limit > 0:
            query = query.limit(limit)
        total = 0
        for consulta in query.all():
            text = build_text(consulta)
            if not text:
                continue
            vec = compute_embedding(text)
            if vec is None:
                continue
            consulta.embedding_diagnostico = vec
            session.add(consulta)
            total += 1
        session.commit()
        print(f"Backfill completado. Embeddings actualizados: {total}")
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    run_backfill(limit=args.limit, force=args.force)
