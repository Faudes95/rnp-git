from __future__ import annotations

import os
from typing import Any


def run_startup_interconexion_wired(main_module: Any) -> None:
    main_module.run_startup_interconexion(
        run_surgical_alembic_upgrade_if_enabled=main_module.run_surgical_alembic_upgrade_if_enabled,
        logger=main_module.logger,
        ensure_clinical_sensitive_schema=main_module.ensure_clinical_sensitive_schema,
        ensure_hospitalizacion_schema=main_module.ensure_hospitalizacion_schema,
        ensure_surgical_schema=main_module.ensure_surgical_schema,
        ensure_surgical_postquirurgica_schema=main_module.ensure_surgical_postquirurgica_schema,
        ensure_modelos_ml_schema=main_module.ensure_modelos_ml_schema,
        ensure_carga_masiva_schema=main_module.ensure_carga_masiva_schema,
        ensure_dim_paciente_geo_schema=main_module.ensure_dim_paciente_geo_schema,
        ensure_form_metadata_schema=main_module.svc_form_metadata_flow.ensure_form_metadata_schema,
        seed_default_form_metadata=main_module.svc_form_metadata_flow.seed_default_form_metadata,
        ensure_outbox_schema=main_module.svc_outbox_flow.ensure_outbox_schema,
        ensure_event_log_schema=main_module.svc_event_log_flow.ensure_event_log_schema,
        ensure_job_registry_schema=main_module.svc_job_registry_flow.ensure_job_registry_schema,
        ensure_hospital_guardia_schema=main_module.svc_ensure_hospital_guardia_schema,
        ensure_patient_files_dir=main_module.ensure_patient_files_dir,
        prewarm_template_file_cache=main_module._prewarm_template_file_cache,
        log_suppressed_exception=main_module._log_suppressed_exception,
        backfill_quirofano_to_surgical=main_module.backfill_quirofano_to_surgical,
        new_surgical_session=main_module._new_surgical_session,
        poblar_dimensiones_catalogo=main_module.poblar_dimensiones_catalogo,
        poblar_dim_fecha=main_module.poblar_dim_fecha,
        actualizar_data_mart=main_module.actualizar_data_mart,
        launch_background_task=main_module.launch_background_task,
        startup_mode=main_module.STARTUP_INTERCONEXION_MODE,
        startup_delay_sec=main_module.STARTUP_INTERCONEXION_DELAY_SEC,
        engine=main_module.engine,
        surgical_engine=main_module.surgical_engine,
    )


def run_startup_ai_agents_bootstrap_wired(main_module: Any) -> None:
    model_paths = [
        main_module.MODELO_RIESGO_PATH,
        main_module.MODELO_RIESGO_V2_PATH,
        os.getenv("MODELO_DURACION_QX_PATH", "modelos/pipeline_duracion_qx.pkl"),
        os.getenv("MODELO_COMPLICACIONES_QX_PATH", "modelos/pipeline_complicaciones_qx.pkl"),
    ]
    main_module.run_startup_ai_agents_bootstrap(
        ensure_consulta_vector_schema=main_module.ensure_consulta_vector_schema,
        engine=main_module.engine,
        logger=main_module.logger,
        log_suppressed_exception=main_module._log_suppressed_exception,
        schedule_model_warmup=main_module.schedule_model_warmup,
        ai_warmup_models=main_module.ai_warmup_models,
        model_paths=model_paths,
        warmup_mode=main_module.AI_WARMUP_MODE,
        warmup_delay_sec=main_module.AI_WARMUP_DELAY_SEC,
    )
