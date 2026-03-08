from __future__ import annotations

import os
import shutil
from collections import Counter
from datetime import date
from typing import Any, Dict, List, Optional

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover - entorno sin PyMuPDF
    fitz = None

from fastapi import Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, joinedload

from app.core.app_context import main_proxy as m
from app.core.time_utils import utcnow
from app.services.quirofano_jefatura_shared import (
    DEFAULT_UNIT_CODE,
    digest_file,
    duration_map_for_import_rows,
    ensure_daily_blocks_for_date,
    ensure_jefatura_quirofano_seed,
    import_storage_dir,
    list_service_lines,
    log_audit,
    normalize_room_code,
    normalize_room_number,
    parse_pdf_date_from_text,
    request_actor,
    safe_int,
    safe_text,
    serialize_import_row,
    service_line_guess,
    shift_for_scheduled_time,
    status_badge,
    template_slot_for_date,
    ui_terms,
)


REQUIRED_IMPORT_FIELDS = [
    "room_code",
    "turno",
    "hora_programada",
    "paciente_nombre",
    "operacion_proyectada",
]


def _parser_version_label() -> str:
    raw = str(getattr(fitz, "__doc__", "") or "").strip()
    parts = raw.split()
    if len(parts) >= 2:
        return parts[1].strip(":")
    return "unknown"


def _import_batch(session: Session, batch_id: int) -> Any:
    return (
        session.query(m.JefaturaQuirofanoImportBatchDB)
        .options(joinedload(m.JefaturaQuirofanoImportBatchDB.rows))
        .filter(m.JefaturaQuirofanoImportBatchDB.id == int(batch_id))
        .first()
    )


def _row_discrepancies(row_data: Dict[str, Any], *, target_date: Optional[date], session: Session) -> Dict[str, Any]:
    issues: Dict[str, Any] = {}
    for field in REQUIRED_IMPORT_FIELDS:
        if not safe_text(row_data.get(field), max_len=255):
            issues[field] = "Campo requerido faltante."
    room_number = normalize_room_number(row_data.get("room_code"))
    if room_number is None:
        issues["room_code"] = "Sala no reconocida."
    turno = safe_text(row_data.get("turno"), max_len=20)
    if turno not in {"MATUTINO", "VESPERTINO"}:
        issues["turno"] = "Turno no válido."
    if target_date is not None and room_number is not None and turno in {"MATUTINO", "VESPERTINO"}:
        slot = template_slot_for_date(session, target_date, room_number=int(room_number), turno=str(turno))
        if slot is None:
            issues["template_slot"] = "La sala/turno no existe en la plantilla activa para esa fecha."
    return issues


def _imports_dashboard(batches: List[Any]) -> Dict[str, Any]:
    status_counts = Counter(str(getattr(batch, "status", "") or "").upper() for batch in batches)
    return {
        "confirmed": status_counts.get("CONFIRMED", 0),
        "review": status_counts.get("REVIEW", 0),
        "unsupported": status_counts.get("UNSUPPORTED", 0),
        "rows": sum(int(getattr(batch, "extracted_rows_count", 0) or 0) for batch in batches),
    }


def _review_dashboard(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    status_counts = Counter(str(row.get("review_status", "") or "").upper() for row in rows)
    discrepancy_rows = [row for row in rows if bool(row.get("discrepancy_flag"))]
    return {
        "total_rows": len(rows),
        "ready": status_counts.get("READY", 0),
        "review": status_counts.get("REVIEW", 0),
        "confirmed": status_counts.get("CONFIRMED", 0),
        "discrepancies": len(discrepancy_rows),
        "next_discrepancy_id": int(discrepancy_rows[0]["id"]) if discrepancy_rows else None,
    }


def parse_imss_operating_pdf(file_path: str, session: Session) -> Dict[str, Any]:
    if fitz is None:
        return {"ok": False, "status": "UNSUPPORTED", "errors": ["PyMuPDF no está disponible en este entorno."], "rows": []}
    doc = fitz.open(file_path)
    try:
        if doc.page_count <= 0:
            return {"ok": False, "status": "UNSUPPORTED", "errors": ["El PDF no contiene páginas."], "rows": []}
        first_page_text = doc.load_page(0).get_text("text")
        file_date = parse_pdf_date_from_text(first_page_text)
        warnings: List[str] = []
        errors: List[str] = []
        rows: List[Dict[str, Any]] = []
        for page_number in range(doc.page_count):
            page = doc.load_page(page_number)
            tables = page.find_tables().tables if hasattr(page, "find_tables") else []
            if not tables:
                errors.append(f"La página {page_number + 1} no contiene una tabla reconocible.")
                continue
            table = tables[0]
            data = table.extract()
            if len(data) < 3:
                errors.append(f"La tabla de la página {page_number + 1} no tiene filas suficientes.")
                continue
            for row_index, row in enumerate(data[2:], start=1):
                if not any(str(cell or "").strip() for cell in row):
                    continue
                payload = {
                    "page_number": page_number + 1,
                    "row_number": row_index,
                    "room_code": normalize_room_code(row[0]),
                    "turno": shift_for_scheduled_time(row[1]),
                    "hora_programada": safe_text(row[1], max_len=10),
                    "cama": safe_text(row[2], max_len=20),
                    "paciente_nombre": safe_text(row[3], max_len=220),
                    "nss": safe_text(row[4], max_len=20),
                    "agregado_medico": safe_text(row[5], max_len=80),
                    "edad": safe_int(row[6]),
                    "diagnostico_preoperatorio": safe_text(row[7], max_len=240),
                    "operacion_proyectada": safe_text(row[8], max_len=240),
                    "cirujano": safe_text(row[9], max_len=180),
                    "anestesiologo": safe_text(row[10], max_len=180),
                    "tipo_anestesia": safe_text(row[11], max_len=120),
                    "enfermera_especialista": safe_text(row[12], max_len=180),
                }
                payload["specialty_guess"] = service_line_guess(
                    payload.get("diagnostico_preoperatorio"),
                    payload.get("operacion_proyectada"),
                )
                payload["discrepancy_json"] = _row_discrepancies(payload, target_date=file_date, session=session)
                payload["discrepancy_flag"] = bool(payload["discrepancy_json"])
                payload["raw_json"] = {"table_row": row}
                payload["normalized_json"] = {k: v for k, v in payload.items() if k not in {"raw_json", "normalized_json"}}
                rows.append(payload)
        if file_date is None:
            errors.append("No fue posible identificar la fecha del programa quirúrgico.")
        status = "UNSUPPORTED" if errors or not rows else "REVIEW"
        return {
            "ok": bool(rows) and not errors,
            "status": status,
            "file_date": file_date,
            "page_count": doc.page_count,
            "warnings": warnings,
            "errors": errors,
            "rows": rows,
        }
    finally:
        doc.close()


async def create_import_batch_from_upload(request: Request, session: Session, upload: UploadFile) -> Dict[str, Any]:
    actor = request_actor(request)
    ensure_jefatura_quirofano_seed(session, actor=actor)
    if upload is None or not str(getattr(upload, "filename", "") or "").lower().endswith(".pdf"):
        return {"ok": False, "error": "Debes subir un archivo PDF."}
    target_dir = import_storage_dir()
    stamp = utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = f"{stamp}_{os.path.basename(str(upload.filename or 'programacion.pdf'))}"
    storage_path = os.path.join(target_dir, safe_name)
    with open(storage_path, "wb") as fh:
        shutil.copyfileobj(upload.file, fh)
    parsed = parse_imss_operating_pdf(storage_path, session)
    batch = m.JefaturaQuirofanoImportBatchDB(
        unidad_code=DEFAULT_UNIT_CODE,
        file_date=parsed.get("file_date"),
        original_filename=str(upload.filename or "programacion.pdf"),
        storage_path=storage_path,
        parser_name="pymupdf_find_tables",
        parser_version=_parser_version_label() if fitz is not None else "na",
        page_count=int(parsed.get("page_count") or 0),
        extracted_rows_count=len(parsed.get("rows") or []),
        status=str(parsed.get("status") or "UNSUPPORTED"),
        warnings_json=parsed.get("warnings") or [],
        errors_json=parsed.get("errors") or [],
        created_by=actor,
    )
    session.add(batch)
    session.flush()
    for row in parsed.get("rows") or []:
        session.add(
            m.JefaturaQuirofanoImportRowDB(
                batch_id=int(batch.id),
                page_number=int(row["page_number"]),
                row_number=int(row["row_number"]),
                review_status="REVIEW" if row["discrepancy_flag"] else "READY",
                room_code=row.get("room_code"),
                turno=row.get("turno"),
                hora_programada=row.get("hora_programada"),
                cama=row.get("cama"),
                paciente_nombre=row.get("paciente_nombre"),
                nss=row.get("nss"),
                agregado_medico=row.get("agregado_medico"),
                edad=row.get("edad"),
                diagnostico_preoperatorio=row.get("diagnostico_preoperatorio"),
                operacion_proyectada=row.get("operacion_proyectada"),
                cirujano=row.get("cirujano"),
                anestesiologo=row.get("anestesiologo"),
                tipo_anestesia=row.get("tipo_anestesia"),
                enfermera_especialista=row.get("enfermera_especialista"),
                specialty_guess=row.get("specialty_guess"),
                discrepancy_flag=bool(row.get("discrepancy_flag")),
                discrepancy_json=row.get("discrepancy_json") or {},
                raw_json=row.get("raw_json") or {},
                normalized_json=row.get("normalized_json") or {},
                edited_json={},
            )
        )
    log_audit(session, actor=actor, action="create_import_batch", entity_type="import_batch", entity_id=int(batch.id), payload={"status": batch.status, "rows": batch.extracted_rows_count, "sha256": digest_file(storage_path)})
    session.commit()
    return {"ok": True, "batch_id": int(batch.id), "status": str(batch.status)}


async def render_jefatura_quirofano_imports_flow(
    request: Request,
    session: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    batches = (
        session.query(m.JefaturaQuirofanoImportBatchDB)
        .order_by(m.JefaturaQuirofanoImportBatchDB.id.desc())
        .limit(25)
        .all()
    )
    return m.render_template(
        "quirofano_jefatura_importaciones.html",
        request=request,
        flash=flash,
        selected_date=date.today(),
        selected_date_label=date.today().strftime("%d/%m/%Y"),
        batches=batches,
        imports_dashboard=_imports_dashboard(list(batches)),
        ui_terms=ui_terms(),
    )


async def render_jefatura_quirofano_import_review_flow(
    request: Request,
    session: Session,
    batch_id: int,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    batch = _import_batch(session, batch_id)
    if batch is None:
        return HTMLResponse(content="<h1>Importación no encontrada</h1>", status_code=404)
    rows = [serialize_import_row(row) for row in sorted(batch.rows, key=lambda item: (item.page_number, item.row_number))]
    return m.render_template(
        "quirofano_jefatura_import_review.html",
        request=request,
        flash=flash,
        batch=batch,
        rows=rows,
        selected_date=batch.file_date,
        selected_date_label=batch.file_date.strftime("%d/%m/%Y") if isinstance(batch.file_date, date) else "Sin fecha",
        review_summary=_review_dashboard(rows),
        service_lines=[{"code": row.code, "nombre": row.nombre} for row in list_service_lines(session) if bool(row.activo)],
        batch_badge=status_badge(batch.status),
        ui_terms=ui_terms(),
    )


def _apply_review_edits(batch: Any, form: Any, session: Session) -> Dict[str, Any]:
    target_date = getattr(batch, "file_date", None)
    unresolved = 0
    for row in getattr(batch, "rows", []) or []:
        row.room_code = normalize_room_code(form.get(f"room_code__{row.id}")) or row.room_code
        row.turno = safe_text(form.get(f"turno__{row.id}"), max_len=20) or row.turno
        row.hora_programada = safe_text(form.get(f"hora_programada__{row.id}"), max_len=10) or row.hora_programada
        row.cama = safe_text(form.get(f"cama__{row.id}"), max_len=20)
        row.paciente_nombre = safe_text(form.get(f"paciente_nombre__{row.id}"), max_len=220)
        row.nss = safe_text(form.get(f"nss__{row.id}"), max_len=20)
        row.agregado_medico = safe_text(form.get(f"agregado_medico__{row.id}"), max_len=80)
        row.edad = safe_int(form.get(f"edad__{row.id}"))
        row.diagnostico_preoperatorio = safe_text(form.get(f"diagnostico_preoperatorio__{row.id}"), max_len=240)
        row.operacion_proyectada = safe_text(form.get(f"operacion_proyectada__{row.id}"), max_len=240)
        row.cirujano = safe_text(form.get(f"cirujano__{row.id}"), max_len=180)
        row.anestesiologo = safe_text(form.get(f"anestesiologo__{row.id}"), max_len=180)
        row.tipo_anestesia = safe_text(form.get(f"tipo_anestesia__{row.id}"), max_len=120)
        row.enfermera_especialista = safe_text(form.get(f"enfermera_especialista__{row.id}"), max_len=180)
        discrepancy = _row_discrepancies(
            {
                "room_code": row.room_code,
                "turno": row.turno,
                "hora_programada": row.hora_programada,
                "cama": row.cama,
                "paciente_nombre": row.paciente_nombre,
                "nss": row.nss,
                "agregado_medico": row.agregado_medico,
                "edad": row.edad,
                "diagnostico_preoperatorio": row.diagnostico_preoperatorio,
                "operacion_proyectada": row.operacion_proyectada,
                "cirujano": row.cirujano,
                "anestesiologo": row.anestesiologo,
                "tipo_anestesia": row.tipo_anestesia,
                "enfermera_especialista": row.enfermera_especialista,
            },
            target_date=target_date,
            session=session,
        )
        row.discrepancy_json = discrepancy
        row.discrepancy_flag = bool(discrepancy)
        row.review_status = "REVIEW" if row.discrepancy_flag else "READY"
        row.edited_json = {
            "room_code": row.room_code,
            "turno": row.turno,
            "hora_programada": row.hora_programada,
            "cama": row.cama,
            "paciente_nombre": row.paciente_nombre,
            "nss": row.nss,
            "agregado_medico": row.agregado_medico,
            "edad": row.edad,
            "diagnostico_preoperatorio": row.diagnostico_preoperatorio,
            "operacion_proyectada": row.operacion_proyectada,
            "cirujano": row.cirujano,
            "anestesiologo": row.anestesiologo,
            "tipo_anestesia": row.tipo_anestesia,
            "enfermera_especialista": row.enfermera_especialista,
        }
        if row.discrepancy_flag:
            unresolved += 1
    batch.status = "REVIEW" if batch.rows else "UNSUPPORTED"
    batch.reviewed_at = utcnow()
    return {"unresolved": unresolved}


async def save_import_review_from_request(request: Request, session: Session, batch_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    batch = _import_batch(session, batch_id)
    if batch is None:
        return {"ok": False, "error": "Importación no encontrada."}
    result = _apply_review_edits(batch, form, session)
    log_audit(session, actor=actor, action="save_import_review", entity_type="import_batch", entity_id=int(batch.id), payload=result)
    session.commit()
    return {"ok": True, **result}


async def confirm_import_batch_from_request(request: Request, session: Session, batch_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    batch = _import_batch(session, batch_id)
    if batch is None:
        return {"ok": False, "error": "Importación no encontrada."}
    if str(batch.status).upper() == "CONFIRMED":
        return {"ok": False, "error": "Esta importación ya fue confirmada."}
    review_result = _apply_review_edits(batch, form, session)
    if review_result["unresolved"] > 0:
        session.commit()
        return {"ok": False, "error": "Existen discrepancias obligatorias pendientes en la conciliación."}
    if not isinstance(batch.file_date, date):
        return {"ok": False, "error": "La importación no tiene una fecha válida."}
    blocks = ensure_daily_blocks_for_date(session, batch.file_date, actor=actor)
    block_map = {(int(item.room_number), str(item.turno)): item for item in blocks}
    durations = duration_map_for_import_rows(batch.rows)
    created = 0
    for row in sorted(batch.rows, key=lambda item: (item.page_number, item.row_number)):
        room_number = normalize_room_number(row.room_code)
        block = block_map.get((int(room_number or 0), str(row.turno or "")))
        if block is None:
            return {"ok": False, "error": f"No existe bloque diario válido para {row.room_code} {row.turno}."}
        case = m.JefaturaQuirofanoCaseProgramacionDB(
            daily_block_id=int(block.id),
            unidad_code=DEFAULT_UNIT_CODE,
            source_type="PDF_IMPORT",
            import_batch_id=int(batch.id),
            import_row_id=int(row.id),
            status="PROGRAMADA",
            scheduled_time=row.hora_programada,
            duracion_estimada_min=durations.get(int(row.id), 60),
            cama=row.cama,
            patient_name=row.paciente_nombre,
            nss=row.nss,
            agregado_medico=row.agregado_medico,
            edad=row.edad,
            diagnostico_preoperatorio=row.diagnostico_preoperatorio,
            operacion_proyectada=row.operacion_proyectada,
            cirujano=row.cirujano,
            anestesiologo=row.anestesiologo,
            enfermera_especialista=row.enfermera_especialista,
            tipo_anestesia=row.tipo_anestesia,
            created_by=actor,
        )
        session.add(case)
        session.flush()
        row.confirmed_case_id = int(case.id)
        row.review_status = "CONFIRMED"
        block.import_batch_id = int(batch.id)
        created += 1
    batch.status = "CONFIRMED"
    batch.confirmed_at = utcnow()
    log_audit(session, actor=actor, action="confirm_import_batch", entity_type="import_batch", entity_id=int(batch.id), payload={"cases_created": created})
    session.commit()
    return {"ok": True, "cases_created": created}
