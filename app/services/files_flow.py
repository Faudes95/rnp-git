from __future__ import annotations

import mimetypes
import os
import secrets
from typing import Any, Dict, List, Optional

from fastapi import Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.services.files import format_size


_DYNAMIC_SYMBOLS = [
    "ArchivoPacienteDB",
    "CARGA_ARCHIVOS_TEMPLATE",
    "MAX_PATIENT_FILE_SIZE_MB",
    "ALLOWED_PATIENT_FILE_EXTENSIONS",
    "PATIENT_FILES_DIR",
    "render_template",
    "_resolve_consulta_para_archivo",
    "ensure_patient_files_dir",
    "_safe_filename",
    "_extract_extension",
    "_detect_mime",
    "utcnow",
    "validate_csrf",
    "HTTPException",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing: List[str] = []
    for symbol in _DYNAMIC_SYMBOLS:
        if symbol in module_globals:
            continue
        try:
            module_globals[symbol] = getattr(m, symbol)
        except Exception:
            missing.append(symbol)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def list_archivos_for_template(
    db: Session,
    consulta_id: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    query = db.query(ArchivoPacienteDB).order_by(ArchivoPacienteDB.fecha_subida.desc(), ArchivoPacienteDB.id.desc())
    if consulta_id:
        query = query.filter(ArchivoPacienteDB.consulta_id == consulta_id)
    rows = query.limit(max(1, int(limit))).all()
    return [
        {
            "id": row.id,
            "consulta_id": row.consulta_id,
            "nombre_original": row.nombre_original,
            "extension": row.extension or "",
            "tamano_legible": format_size(row.tamano_bytes),
            "fecha_subida": row.fecha_subida.strftime("%Y-%m-%d %H:%M") if row.fecha_subida else "",
        }
        for row in rows
    ]


async def analisis_cargar_archivos_form_flow(
    request: Request,
    consulta_id: Optional[int],
    curp: Optional[str],
    db: Session,
) -> HTMLResponse:
    _ensure_symbols()
    ensure_patient_files_dir()
    consulta = _resolve_consulta_para_archivo(db, consulta_id, curp)
    resolved_consulta_id = consulta.id if consulta else consulta_id
    archivos = list_archivos_for_template(db, resolved_consulta_id)
    return render_template(
        CARGA_ARCHIVOS_TEMPLATE,
        request=request,
        consulta=consulta,
        consulta_id=resolved_consulta_id,
        curp=curp,
        archivos_paciente=archivos,
        max_size_mb=MAX_PATIENT_FILE_SIZE_MB,
        message=None,
        error=None,
        errores=[],
    )


async def analisis_cargar_archivos_submit_flow(
    request: Request,
    *,
    csrf_token: str,
    consulta_id: Optional[int],
    curp: Optional[str],
    descripcion: Optional[str],
    files: List[UploadFile],
    db: Session,
) -> HTMLResponse:
    _ensure_symbols()
    validate_csrf({"csrf_token": csrf_token}, request)
    ensure_patient_files_dir()
    consulta = _resolve_consulta_para_archivo(db, consulta_id, curp)
    if not consulta:
        return render_template(
            CARGA_ARCHIVOS_TEMPLATE,
            request=request,
            consulta=None,
            consulta_id=consulta_id,
            curp=curp,
            archivos_paciente=list_archivos_for_template(db, None),
            max_size_mb=MAX_PATIENT_FILE_SIZE_MB,
            message=None,
            error="No se encontró paciente con el ID/CURP proporcionado.",
            errores=[],
        )

    errores: List[str] = []
    guardados = 0
    usuario = request.headers.get("X-User", "system")
    max_size_bytes = MAX_PATIENT_FILE_SIZE_MB * 1024 * 1024
    descripcion_clean = (descripcion or "").strip()[:255] if descripcion else None

    for upload in files:
        if upload is None or not upload.filename:
            continue
        safe_original = _safe_filename(upload.filename)
        ext = _extract_extension(safe_original)
        if ext not in ALLOWED_PATIENT_FILE_EXTENSIONS:
            errores.append(f"{safe_original}: extensión no permitida.")
            try:
                await upload.close()
            except Exception:
                pass
            continue

        try:
            content = await upload.read()
            if not content:
                errores.append(f"{safe_original}: archivo vacío.")
                continue
            if len(content) > max_size_bytes:
                errores.append(f"{safe_original}: excede el tamaño máximo de {MAX_PATIENT_FILE_SIZE_MB} MB.")
                continue
            stored_name = f"{consulta.id}_{utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(6)}{ext}"
            target_path = os.path.join(PATIENT_FILES_DIR, stored_name)
            with open(target_path, "wb") as f:
                f.write(content)
            db.add(
                ArchivoPacienteDB(
                    consulta_id=consulta.id,
                    nombre_original=safe_original,
                    nombre_guardado=stored_name,
                    extension=ext,
                    mime_type=_detect_mime(upload, ext),
                    storage_path=target_path,
                    tamano_bytes=len(content),
                    descripcion=descripcion_clean,
                    subido_por=usuario,
                )
            )
            db.commit()
            guardados += 1
        except Exception as exc:
            db.rollback()
            errores.append(f"{safe_original}: {exc}")
        finally:
            try:
                await upload.close()
            except Exception:
                pass

    message = f"Archivos guardados correctamente: {guardados}" if guardados else None
    archivos = list_archivos_for_template(db, consulta.id)
    return render_template(
        CARGA_ARCHIVOS_TEMPLATE,
        request=request,
        consulta=consulta,
        consulta_id=consulta.id,
        curp=consulta.curp,
        archivos_paciente=archivos,
        max_size_mb=MAX_PATIENT_FILE_SIZE_MB,
        message=message,
        error=None if not errores else "Algunos archivos no pudieron guardarse.",
        errores=errores,
    )


async def descargar_archivo_paciente_flow(
    archivo_id: int,
    db: Session,
):
    _ensure_symbols()
    row = db.query(ArchivoPacienteDB).filter(ArchivoPacienteDB.id == archivo_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    if not row.storage_path or not os.path.isfile(row.storage_path):
        raise HTTPException(status_code=404, detail="Archivo no disponible en almacenamiento")

    media_type = row.mime_type or mimetypes.guess_type(row.nombre_original or "")[0] or "application/octet-stream"
    headers = {}
    if not media_type.startswith("image/") and media_type not in {"application/pdf", "application/dicom"}:
        headers["Content-Disposition"] = f'attachment; filename="{row.nombre_original}"'
    return FileResponse(path=row.storage_path, media_type=media_type, filename=row.nombre_original, headers=headers)

