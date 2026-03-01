import os
import threading
from typing import Optional, List

try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

_MODEL = None
_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
_MODEL_LOCK = threading.Lock()
_DISABLE_SENTENCE_TRANSFORMERS = os.getenv("EMBEDDING_DISABLE_SENTENCE_TRANSFORMERS", "false").lower() in (
    "1",
    "true",
    "yes",
)


def get_model():
    global _MODEL
    if _DISABLE_SENTENCE_TRANSFORMERS:
        return None
    if SentenceTransformer is None:
        return None
    if _MODEL is None:
        with _MODEL_LOCK:
            if _MODEL is None:
                _MODEL = SentenceTransformer(_MODEL_NAME)
    return _MODEL


def compute_embedding(text: str) -> Optional[List[float]]:
    if not text:
        return None
    model = get_model()
    if model is None:
        return None
    try:
        return model.encode(text).tolist()
    except Exception:
        return None
