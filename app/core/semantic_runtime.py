from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

try:
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover - dependencia opcional
    SentenceTransformer = None


_semantic_model = None


def get_semantic_model():
    global _semantic_model
    try:
        from embeddings import get_model as external_get_model

        ext_model = external_get_model()
        if ext_model is not None:
            _semantic_model = ext_model
            return _semantic_model
    except Exception:
        pass

    if SentenceTransformer is None:
        return None
    if _semantic_model is None:
        _semantic_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _semantic_model


def build_embedding_text(data: Dict[str, Any]) -> str:
    parts = [
        data.get("diagnostico_principal"),
        data.get("padecimiento_actual"),
        data.get("exploracion_fisica"),
        data.get("estudios_hallazgos"),
        data.get("plan_especifico"),
    ]
    return ". ".join([part for part in parts if part])


def compute_embedding(text: str) -> Optional[List[float]]:
    if not text:
        return None
    model = get_semantic_model()
    if model is None:
        return None
    try:
        return model.encode(text).tolist()
    except Exception:
        return None


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def enqueue_embedding(note_id: int, text: str, *, enabled: bool = True) -> None:
    if not text:
        return
    if not enabled:
        return
    try:
        from ai_tasks import embedding_task

        embedding_task.delay(note_id, text)
    except Exception:
        return
