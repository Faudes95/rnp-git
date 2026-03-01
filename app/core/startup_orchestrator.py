from __future__ import annotations

from typing import Any, Callable, Dict


def run_startup_interconexion(
    *,
    run_surgical_alembic_upgrade_if_enabled: Callable[[], Dict[str, Any]],
    logger: Any,
    ensure_clinical_sensitive_schema: Callable[[], Any],
    ensure_hospitalizacion_schema: Callable[[], Any],
    ensure_surgical_schema: Callable[[], Any],
    ensure_surgical_postquirurgica_schema: Callable[[], Any],
    ensure_modelos_ml_schema: Callable[[], Any],
    ensure_carga_masiva_schema: Callable[[], Any],
    ensure_dim_paciente_geo_schema: Callable[[], Any],
    ensure_form_metadata_schema: Callable[[Any], Any],
    seed_default_form_metadata: Callable[[Any], Any],
    ensure_outbox_schema: Callable[[Any], Any],
    ensure_event_log_schema: Callable[[Any], Any],
    ensure_job_registry_schema: Callable[[Any], Any],
    ensure_hospital_guardia_schema: Callable[[Any], Any],
    ensure_patient_files_dir: Callable[[], Any],
    prewarm_template_file_cache: Callable[[], Any],
    log_suppressed_exception: Callable[[str, Exception], Any],
    backfill_quirofano_to_surgical: Callable[..., Any],
    new_surgical_session: Callable[..., Any],
    poblar_dimensiones_catalogo: Callable[[Any], Any],
    poblar_dim_fecha: Callable[[Any], Any],
    actualizar_data_mart: Callable[[Any], Any],
    launch_background_task: Callable[..., Any],
    startup_mode: str,
    startup_delay_sec: float,
    engine: Any,
    surgical_engine: Any,
) -> None:
    migration_result = run_surgical_alembic_upgrade_if_enabled()
    if migration_result.get("enabled") and not migration_result.get("ok"):
        logger.warning({"event": "surgical_alembic_fallback", "detail": migration_result.get("error", "unknown")})
    ensure_clinical_sensitive_schema()
    ensure_hospitalizacion_schema()
    ensure_surgical_schema()
    ensure_surgical_postquirurgica_schema()
    ensure_modelos_ml_schema()
    ensure_carga_masiva_schema()
    ensure_dim_paciente_geo_schema()
    ensure_form_metadata_schema(engine)
    seed_default_form_metadata(engine)
    ensure_outbox_schema(surgical_engine)
    ensure_event_log_schema(surgical_engine)
    ensure_job_registry_schema(surgical_engine)
    ensure_hospital_guardia_schema(engine)
    ensure_patient_files_dir()
    try:
        prewarm_template_file_cache()
    except Exception as exc:
        log_suppressed_exception("template_prewarm_failed", exc)

    def _heavy_bootstrap() -> Dict[str, Any]:
        backfill_quirofano_to_surgical(limit=500)
        sdb = new_surgical_session(enable_dual_write=True)
        try:
            poblar_dimensiones_catalogo(sdb)
            poblar_dim_fecha(sdb)
            actualizar_data_mart(sdb, incremental=True)
            return {"ok": True}
        except Exception as exc:
            log_suppressed_exception("startup_interconexion_failed", exc)
            sdb.rollback()
            return {"ok": False, "error": str(exc)}
        finally:
            sdb.close()

    if startup_mode == "off":
        logger.info({"event": "startup_interconexion_heavy_skipped", "mode": "off"})
        return
    if startup_mode == "background":
        launch_background_task(
            task_name="startup_interconexion_heavy",
            logger=logger,
            fn=_heavy_bootstrap,
            delay_sec=startup_delay_sec,
        )
        return

    _heavy_bootstrap()


def run_startup_ai_agents_bootstrap(
    *,
    ensure_consulta_vector_schema: Callable[[Any], Any],
    engine: Any,
    logger: Any,
    log_suppressed_exception: Callable[[str, Exception], Any],
    schedule_model_warmup: Callable[..., Dict[str, Any]],
    ai_warmup_models: Callable[..., Any],
    model_paths: list[str],
    warmup_mode: str,
    warmup_delay_sec: float,
) -> None:
    try:
        vector_ok = ensure_consulta_vector_schema(engine)
        logger.info({"event": "startup_pgvector_consultas", "enabled": bool(vector_ok)})
    except Exception as exc:
        log_suppressed_exception("startup_pgvector_consultas_failed", exc)

    try:
        warmup_status = schedule_model_warmup(
            task_name="startup_model_warmup",
            logger=logger,
            warmup_fn=ai_warmup_models,
            model_paths=model_paths,
            mode=warmup_mode,
            delay_sec=warmup_delay_sec,
        )
        logger.info({"event": "startup_model_warmup_mode", **warmup_status})
    except Exception as exc:
        log_suppressed_exception("startup_model_warmup_failed", exc)
