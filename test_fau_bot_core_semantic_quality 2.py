import os
from datetime import datetime, UTC

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"
os.environ["FAU_CORE_USE_FALLBACK_EMBEDDING"] = "true"

from fau_bot_core.db import output_conn  # noqa: E402
from fau_bot_core.service import SERVICE  # noqa: E402
from fau_bot_core.vector_knowledge import upsert_knowledge_document_chunked  # noqa: E402


def test_fau_core_load_default_knowledge_chunked():
    payload = SERVICE.load_default_knowledge()
    assert int(payload.get("documents") or 0) >= 10
    assert int(payload.get("loaded") or 0) >= int(payload.get("documents") or 0)
    assert int(payload.get("chunk_size") or 0) >= 300


def test_fau_core_semantic_search_lithiasis_recall():
    rows = SERVICE.knowledge_search("litos ureterales obstructivos con sepsis", area="LITIASIS", limit=5)
    assert rows, "Se esperaban resultados de litiasis"
    top = rows[0]
    assert top.get("area") == "LITIASIS"
    assert float(top.get("score") or 0.0) > 0.0
    assert "score_lexical" in top


def test_fau_core_semantic_search_chunk_consolidation():
    large_content = " ".join(
        [
            "Nefrectomia radical con alto riesgo de sangrado intraoperatorio y transfusion.",
            "Control hemostatico estricto, monitorizacion hemodinamica y plan de hemoderivados.",
            "La evaluacion de comorbilidad y ECOG impacta resultados postoperatorios.",
            "Documentar perdidas sanguineas, reintervencion y seguimiento de hemoglobina.",
        ]
        * 18
    )
    with output_conn() as conn:
        docs = upsert_knowledge_document_chunked(
            conn,
            source="PYTEST",
            title=f"Documento calidad semantica {datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
            area="ONCOLOGIA",
            content=large_content,
            tags=["pytest", "chunking"],
        )
        assert len(docs) >= 2

    rows = SERVICE.knowledge_search("sangrado transfusion nefrectomia", area="ONCOLOGIA", limit=20)
    assert rows
    # Debe consolidar chunks por documento base y exponer matched_chunks.
    assert all("matched_chunks" in row for row in rows[:5])
