from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def register_main_celery_tasks(
    *,
    celery_app: Any,
    main_module_name: str,
    worker_configure_default_beat_schedule: Callable[[Any], None],
    worker_run_actualizar_data_mart_sync: Callable[..., Any],
    worker_run_backfill_quirofano: Callable[..., Any],
    worker_run_entrenar_modelo_riesgo_v2: Callable[..., Any],
    worker_run_fau_bot_central_cycle: Callable[..., Any],
    worker_run_fau_bot_self_improvement: Callable[..., Any],
    worker_run_quirofano_programacion_analizar: Callable[..., Any],
    worker_run_quirofano_agent_window: Callable[..., Any],
    worker_run_carga_masiva_excel: Callable[..., Any],
) -> Dict[str, Optional[Callable[..., Any]]]:
    tasks: Dict[str, Optional[Callable[..., Any]]] = {
        "async_actualizar_data_mart_task": None,
        "async_backfill_quirofano_task": None,
        "async_entrenar_modelo_riesgo_v2_task": None,
        "async_fau_bot_central_cycle_task": None,
        "async_fau_bot_self_improvement_task": None,
        "async_quirofano_programacion_analizar_task": None,
        "async_quirofano_agent_window_task": None,
        "async_carga_masiva_excel_task": None,
    }
    if celery_app is None:
        return tasks

    worker_configure_default_beat_schedule(celery_app)

    def _main_module():
        return __import__(main_module_name)

    @celery_app.task(bind=True, name="clinical_ai.async_actualizar_data_mart")
    def async_actualizar_data_mart_task(self):
        return worker_run_actualizar_data_mart_sync(
            _main_module(),
            incremental=True,
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_backfill_quirofano")
    def async_backfill_quirofano_task(self, limit: int = 500):
        return worker_run_backfill_quirofano(
            _main_module(),
            limit=limit,
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_entrenar_modelo_riesgo_v2")
    def async_entrenar_modelo_riesgo_v2_task(self):
        return worker_run_entrenar_modelo_riesgo_v2(
            _main_module(),
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_fau_bot_central_cycle")
    def async_fau_bot_central_cycle_task(self, window_days: int = 30):
        return worker_run_fau_bot_central_cycle(
            _main_module(),
            window_days=window_days,
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_fau_bot_self_improvement")
    def async_fau_bot_self_improvement_task(self):
        return worker_run_fau_bot_self_improvement(
            _main_module(),
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_quirofano_programacion_analizar")
    def async_quirofano_programacion_analizar_task(self, programacion_id: int):
        return worker_run_quirofano_programacion_analizar(
            programacion_id=programacion_id,
            main_module=_main_module(),
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_quirofano_agent_window")
    def async_quirofano_agent_window_task(self, window_days: int = 30, limit: int = 700):
        return worker_run_quirofano_agent_window(
            window_days=window_days,
            limit=limit,
            main_module=_main_module(),
            task_id=str(getattr(self.request, "id", "") or ""),
        )

    @celery_app.task(bind=True, name="clinical_ai.async_carga_masiva_excel")
    def async_carga_masiva_excel_task(self, file_content_base64: str, filename: str, usuario: str = "system"):
        return worker_run_carga_masiva_excel(
            _main_module(),
            task_ctx=self,
            file_content_base64=file_content_base64,
            filename=filename,
            usuario=usuario,
        )

    tasks.update(
        {
            "async_actualizar_data_mart_task": async_actualizar_data_mart_task,
            "async_backfill_quirofano_task": async_backfill_quirofano_task,
            "async_entrenar_modelo_riesgo_v2_task": async_entrenar_modelo_riesgo_v2_task,
            "async_fau_bot_central_cycle_task": async_fau_bot_central_cycle_task,
            "async_fau_bot_self_improvement_task": async_fau_bot_self_improvement_task,
            "async_quirofano_programacion_analizar_task": async_quirofano_programacion_analizar_task,
            "async_quirofano_agent_window_task": async_quirofano_agent_window_task,
            "async_carga_masiva_excel_task": async_carga_masiva_excel_task,
        }
    )
    return tasks
