from __future__ import annotations

import base64
import io
import json
from typing import Any, Dict

from app.services import job_registry_flow as svc_job_registry_flow


def configure_default_beat_schedule(celery_app: Any) -> None:
    """Configura schedule base de Celery Beat (aditivo, idempotente)."""
    if celery_app is None:
        return
    try:
        from celery.schedules import crontab

        celery_app.conf.beat_schedule = {
            "actualizar-data-mart-diario": {
                "task": "clinical_ai.async_actualizar_data_mart",
                "schedule": crontab(hour=2, minute=0),
            },
            "entrenar-modelo-riesgo-v2-semanal": {
                "task": "clinical_ai.async_entrenar_modelo_riesgo_v2",
                "schedule": crontab(day_of_week="sun", hour=3, minute=0),
            },
            "fau-bot-central-ciclo-cada-6h": {
                "task": "clinical_ai.async_fau_bot_central_cycle",
                "schedule": crontab(minute=0, hour="*/6"),
            },
            "fau-bot-self-improvement-diario": {
                "task": "clinical_ai.async_fau_bot_self_improvement",
                "schedule": crontab(hour=4, minute=15),
            },
            "fau-bot-quirofano-ciclo-cada-2h": {
                "task": "clinical_ai.async_quirofano_agent_window",
                "schedule": crontab(minute=10, hour="*/2"),
            },
        }
    except Exception:
        # Aditivo: nunca interrumpe arranque.
        return


def run_actualizar_data_mart_sync(main_module: Any, *, incremental: bool = True, task_id: str = "") -> Dict[str, Any]:
    sdb = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb,
            job_name="async_actualizar_data_mart",
            source="celery",
            task_id=task_id or None,
            payload={"incremental": bool(incremental)},
            fn=lambda: main_module._run_data_mart_update_sync(incremental=bool(incremental)),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb.close()


def run_backfill_quirofano(main_module: Any, *, limit: int = 500, task_id: str = "") -> Dict[str, Any]:
    safe_limit = max(1, int(limit or 500))
    sdb = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb,
            job_name="async_backfill_quirofano",
            source="celery",
            task_id=task_id or None,
            payload={"limit": safe_limit},
            fn=lambda: main_module.backfill_quirofano_to_surgical(limit=safe_limit),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb.close()


def run_entrenar_modelo_riesgo_v2(main_module: Any, *, task_id: str = "") -> Dict[str, Any]:
    sdb_registry = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb_registry,
            job_name="async_entrenar_modelo_riesgo_v2",
            source="celery",
            task_id=task_id or None,
            payload={},
            fn=lambda: _train_model_inner(main_module),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb_registry.close()


def _train_model_inner(main_module: Any) -> Dict[str, Any]:
    sdb = main_module._new_surgical_session(enable_dual_write=True)
    try:
        return main_module.entrenar_modelo_riesgo_v2(sdb)
    finally:
        sdb.close()


def run_fau_bot_central_cycle(main_module: Any, *, window_days: int = 30, task_id: str = "") -> Dict[str, Any]:
    sdb_registry = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb_registry,
            job_name="async_fau_bot_central_cycle",
            source="celery",
            task_id=task_id or None,
            payload={"window_days": int(window_days or 30)},
            fn=lambda: _run_fau_bot_central_cycle_inner(main_module, window_days=window_days),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb_registry.close()


def _run_fau_bot_central_cycle_inner(main_module: Any, *, window_days: int = 30) -> Dict[str, Any]:
    from app.services.fau_bot_flow import run_fau_bot_cycle
    from app.services.fau_central_brain import generate_patient_integral_report
    import sys

    db = main_module.SessionLocal()
    sdb = main_module._new_surgical_session(enable_dual_write=True)
    try:
        safe_days = max(1, min(int(window_days or 30), 365))
        summary = run_fau_bot_cycle(
            db,
            sdb,
            window_days=safe_days,
            triggered_by="celery_scheduler",
        )

        activos = (
            db.query(main_module.HospitalizacionDB.consulta_id)
            .filter(main_module.HospitalizacionDB.estatus == "ACTIVO")
            .order_by(main_module.HospitalizacionDB.id.desc())
            .limit(50)
            .all()
        )
        processed = 0
        current_module = sys.modules[main_module.__name__]
        for row in activos:
            try:
                generate_patient_integral_report(db, sdb, current_module, consulta_id=int(row[0]))
                processed += 1
            except Exception:
                db.rollback()

        return {"summary": summary, "profiles_processed": processed}
    finally:
        sdb.close()
        db.close()


def run_fau_bot_self_improvement(main_module: Any, *, task_id: str = "") -> Dict[str, Any]:
    sdb_registry = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb_registry,
            job_name="async_fau_bot_self_improvement",
            source="celery",
            task_id=task_id or None,
            payload={},
            fn=lambda: _run_fau_bot_self_improvement_inner(main_module),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb_registry.close()


def _run_fau_bot_self_improvement_inner(main_module: Any) -> Dict[str, Any]:
    from app.services.fau_central_brain import analyze_runtime_log_and_suggest_pr

    db = main_module.SessionLocal()
    try:
        return analyze_runtime_log_and_suggest_pr(db, source_file="/tmp/rnp_uvicorn.err.log")
    finally:
        db.close()


def run_quirofano_programacion_analizar(*, programacion_id: int, main_module: Any = None, task_id: str = "") -> Dict[str, Any]:
    from app.ai_agents.quirofano_agent import QuirofanoAgentFacade

    if main_module is None:
        agent = QuirofanoAgentFacade()
        return agent.analyze_programacion(max(1, int(programacion_id or 0)), run_id=None)

    sdb_registry = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb_registry,
            job_name="async_quirofano_programacion_analizar",
            source="celery",
            task_id=task_id or None,
            payload={"programacion_id": int(programacion_id or 0)},
            fn=lambda: QuirofanoAgentFacade().analyze_programacion(max(1, int(programacion_id or 0)), run_id=None),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb_registry.close()


def run_quirofano_agent_window(*, window_days: int = 30, limit: int = 700, main_module: Any = None, task_id: str = "") -> Dict[str, Any]:
    from app.ai_agents.quirofano_agent import QuirofanoAgentFacade

    safe_window = max(1, min(int(window_days or 30), 365))
    safe_limit = max(1, min(int(limit or 700), 3000))
    if main_module is None:
        agent = QuirofanoAgentFacade()
        return agent.analyze_window(
            window_days=safe_window,
            run_id=None,
            limit=safe_limit,
        )

    sdb_registry = main_module._new_surgical_session(enable_dual_write=True)
    try:
        tracked = svc_job_registry_flow.run_job(
            sdb_registry,
            job_name="async_quirofano_agent_window",
            source="celery",
            task_id=task_id or None,
            payload={"window_days": safe_window, "limit": safe_limit},
            fn=lambda: QuirofanoAgentFacade().analyze_window(
                window_days=safe_window,
                run_id=None,
                limit=safe_limit,
            ),
        )
        return {"job_run_id": tracked.get("job_run_id"), **(tracked.get("result") or {})}
    finally:
        sdb_registry.close()


def run_carga_masiva_excel(main_module: Any, *, task_ctx: Any, file_content_base64: str, filename: str, usuario: str = "system") -> Dict[str, Any]:
    main_module.ensure_carga_masiva_schema()
    sdb = main_module._new_surgical_session(enable_dual_write=True)
    task_row = None
    try:
        task_id = str(getattr(task_ctx.request, "id", "") or "")
        task_row = main_module.CargaMasivaTask(
            task_id=task_id or None,
            nombre_archivo=filename,
            estado="PROCESANDO",
            creado_por=usuario,
        )
        sdb.add(task_row)
        sdb.commit()
        sdb.refresh(task_row)

        if main_module.pd is None:
            raise RuntimeError("Dependencia pandas no disponible para carga masiva")

        file_bytes = base64.b64decode(file_content_base64)
        df = main_module.pd.read_excel(io.BytesIO(file_bytes))
        status_obj = main_module._process_massive_excel_dataframe(
            df,
            progress_cb=lambda current, total: task_ctx.update_state(
                state="PROGRESS",
                meta={"current": int(current), "total": int(total)},
            ),
        )
        status_obj.task_id = task_id

        task_row.estado = "COMPLETADO"
        task_row.total = status_obj.total
        task_row.exitosos = status_obj.exitosos
        task_row.errores_json = json.dumps(status_obj.errores[:100], ensure_ascii=False)
        task_row.finalizado_en = main_module.utcnow()
        sdb.commit()
        return status_obj.to_dict()
    except Exception as exc:
        if task_row is not None:
            task_row.estado = "ERROR"
            task_row.errores_json = json.dumps([str(exc)], ensure_ascii=False)
            task_row.finalizado_en = main_module.utcnow()
            sdb.commit()
        raise
    finally:
        sdb.close()
