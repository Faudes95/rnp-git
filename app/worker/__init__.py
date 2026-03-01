"""Worker app: tareas extraídas de main.py de manera aditiva."""

from .tasks import (
    configure_default_beat_schedule,
    run_actualizar_data_mart_sync,
    run_backfill_quirofano,
    run_carga_masiva_excel,
    run_entrenar_modelo_riesgo_v2,
    run_fau_bot_central_cycle,
    run_fau_bot_self_improvement,
    run_quirofano_agent_window,
    run_quirofano_programacion_analizar,
)

__all__ = [
    "configure_default_beat_schedule",
    "run_actualizar_data_mart_sync",
    "run_backfill_quirofano",
    "run_carga_masiva_excel",
    "run_entrenar_modelo_riesgo_v2",
    "run_fau_bot_central_cycle",
    "run_fau_bot_self_improvement",
    "run_quirofano_agent_window",
    "run_quirofano_programacion_analizar",
]
