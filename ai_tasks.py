from ai_queue import celery
from embeddings import compute_embedding
from db import save_embedding


@celery.task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def embedding_task(self, note_id, text):
    vec = compute_embedding(text)
    save_embedding(note_id, vec)
    return True
