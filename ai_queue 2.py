from celery import Celery

celery = Celery(
    "clinical_ai",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=["main", "ai_tasks"],
)

# Carga explícita de tareas registradas en el proyecto para worker lanzado con "-A ai_queue.celery".
celery.conf.update(
    imports=("main", "ai_tasks"),
)
