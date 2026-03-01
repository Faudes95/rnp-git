from __future__ import annotations

import base64
import io
import json
from typing import Any, Callable, Dict, Optional

from fastapi import HTTPException, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session


async def carga_masiva_excel_flow(
    *,
    request: Any,
    csrf_token: Optional[str],
    file: UploadFile,
    db: Session,
    validate_csrf_fn: Callable[[Dict[str, Any], Any], None],
    extract_extension_fn: Callable[[str], str],
    allowed_extensions: set[str],
    pd_module: Any,
    celery_app: Any,
    async_carga_masiva_excel_task: Any,
    ensure_carga_masiva_schema_fn: Callable[[], None],
    process_massive_excel_dataframe_fn: Callable[[Any], Any],
) -> JSONResponse:
    if csrf_token:
        validate_csrf_fn({"csrf_token": csrf_token}, request)

    ext = extract_extension_fn(file.filename or "")
    if ext not in allowed_extensions:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos .xlsx o .xls")
    if pd_module is None:
        raise HTTPException(status_code=503, detail="Dependencia pandas no disponible")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="El archivo está vacío")

    usuario = request.headers.get("X-User", "system")
    if celery_app is not None and async_carga_masiva_excel_task is not None:
        try:
            ensure_carga_masiva_schema_fn()
            content_b64 = base64.b64encode(contents).decode("ascii")
            task = async_carga_masiva_excel_task.delay(content_b64, file.filename, usuario=usuario)
            return JSONResponse(
                content={
                    "status": "ok",
                    "message": "Archivo recibido. Procesando en segundo plano.",
                    "task_id": task.id,
                    "check_url": f"/carga_masiva/status/{task.id}",
                }
            )
        except Exception:
            pass

    try:
        df = pd_module.read_excel(io.BytesIO(contents))
        status_obj = process_massive_excel_dataframe_fn(df)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(content={"status": "ok", **status_obj.to_dict()})


def carga_masiva_status_flow(
    *,
    task_id: str,
    sdb: Session,
    ensure_carga_masiva_schema_fn: Callable[[], None],
    carga_masiva_task_model: Any,
    celery_app: Any,
    async_carga_masiva_excel_task: Any,
) -> JSONResponse:
    ensure_carga_masiva_schema_fn()
    task_row = sdb.query(carga_masiva_task_model).filter(carga_masiva_task_model.task_id == task_id).first()
    if task_row:
        errores = []
        if task_row.errores_json:
            try:
                errores = json.loads(task_row.errores_json)
            except Exception:
                errores = [task_row.errores_json]
        return JSONResponse(
            content={
                "task_id": task_row.task_id,
                "nombre_archivo": task_row.nombre_archivo,
                "estado": task_row.estado,
                "total": int(task_row.total or 0),
                "exitosos": int(task_row.exitosos or 0),
                "errores": errores[:20],
                "iniciado_en": task_row.iniciado_en.isoformat() if task_row.iniciado_en else None,
                "finalizado_en": task_row.finalizado_en.isoformat() if task_row.finalizado_en else None,
            }
        )

    if celery_app is not None and async_carga_masiva_excel_task is not None:
        try:
            result = async_carga_masiva_excel_task.AsyncResult(task_id)
            if result.state == "PENDING":
                return JSONResponse(content={"task_id": task_id, "state": "PENDIENTE"})
            if result.state == "PROGRESS":
                return JSONResponse(content={"task_id": task_id, "state": "PROGRESO", "progreso": result.info})
            if result.state == "SUCCESS":
                return JSONResponse(content={"task_id": task_id, "state": "COMPLETADO", "resultado": result.result})
            return JSONResponse(content={"task_id": task_id, "state": "ERROR", "error": str(result.info)})
        except Exception:
            pass

    raise HTTPException(status_code=404, detail="Task ID no encontrado")
