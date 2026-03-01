from ai_queue import celery


@celery.task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def analyze_image(self, image_id, path):
    # Placeholder: integrar modelo de visión clínica aquí
    return {"image_id": image_id, "status": "processed", "path": path}
