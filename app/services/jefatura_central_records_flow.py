from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.jefatura_central_shared import (
    CASE_PRIORITY_OPTIONS,
    CASE_STATUS_OPTIONS,
    CENTRAL_MODULE,
    INCIDENCE_SEVERITY_OPTIONS,
    INCIDENCE_STATUS_OPTIONS,
    normalize_text,
    request_actor,
    resident_display,
    resident_selection_groups,
    resolve_assignment_targets,
    safe_date,
    safe_int,
    status_badge,
)


def _format_date_short(value: Optional[date]) -> str:
    if not isinstance(value, date):
        return "Sin fecha"
    return value.strftime("%d/%m/%Y")


def _serialize_case_row(row: Any) -> Dict[str, Any]:
    resident = resident_display(getattr(row, "resident_code", ""))
    badge = status_badge(getattr(row, "estado", "PENDIENTE_CASO"))
    due_on = getattr(row, "fecha_limite", None)
    updated_at = getattr(row, "updated_at", None)
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "resident_code": resident["code"],
        "resident_name": resident["name"],
        "resident_grade": resident["grade"],
        "consulta_id": getattr(row, "consulta_id", None),
        "patient_snapshot": str(getattr(row, "patient_snapshot", "") or "Paciente sin snapshot"),
        "objetivo": str(getattr(row, "objetivo", "") or ""),
        "prioridad": str(getattr(row, "prioridad", "") or "MEDIA"),
        "fecha_limite": due_on,
        "fecha_limite_label": _format_date_short(due_on),
        "estado": str(getattr(row, "estado", "") or "PENDIENTE"),
        "status_label": badge["label"],
        "status_tone": badge["tone"],
        "notas": str(getattr(row, "notas", "") or ""),
        "updated_at_label": str(updated_at or ""),
        "edit_href": f"/jefatura-urologia/central/casos?edit={int(getattr(row, 'id', 0) or 0)}",
    }


def _serialize_incidence_row(row: Any) -> Dict[str, Any]:
    resident = resident_display(getattr(row, "resident_code", ""))
    badge = status_badge(getattr(row, "estado", "ABIERTA"))
    event_date = getattr(row, "fecha_evento", None)
    updated_at = getattr(row, "updated_at", None)
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "resident_code": resident["code"],
        "resident_name": resident["name"],
        "resident_grade": resident["grade"],
        "consulta_id": getattr(row, "consulta_id", None),
        "tipo": str(getattr(row, "tipo", "") or ""),
        "severidad": str(getattr(row, "severidad", "") or "MODERADA"),
        "fecha_evento": event_date,
        "fecha_evento_label": _format_date_short(event_date),
        "estado": str(getattr(row, "estado", "") or "ABIERTA"),
        "status_label": badge["label"],
        "status_tone": badge["tone"],
        "descripcion": str(getattr(row, "descripcion", "") or ""),
        "resolucion": str(getattr(row, "resolucion", "") or ""),
        "updated_at_label": str(updated_at or ""),
        "edit_href": f"/jefatura-urologia/central/incidencias?edit={int(getattr(row, 'id', 0) or 0)}",
    }


def _serialize_case_editor(row: Any) -> Dict[str, Any]:
    serialized = _serialize_case_row(row)
    serialized.update(
        {
            "fecha_limite_value": serialized["fecha_limite"].isoformat() if isinstance(serialized["fecha_limite"], date) else "",
        }
    )
    return serialized


def _serialize_incidence_editor(row: Any) -> Dict[str, Any]:
    serialized = _serialize_incidence_row(row)
    serialized.update(
        {
            "fecha_evento_value": serialized["fecha_evento"].isoformat() if isinstance(serialized["fecha_evento"], date) else "",
        }
    )
    return serialized


def _load_case(db: Session, case_id: int) -> Optional[Any]:
    return db.query(m.ResidentCaseAssignmentDB).filter(m.ResidentCaseAssignmentDB.id == int(case_id)).first()


def _load_incidence(db: Session, incidence_id: int) -> Optional[Any]:
    return db.query(m.ResidentIncidenceDB).filter(m.ResidentIncidenceDB.id == int(incidence_id)).first()


async def render_jefatura_central_cases_flow(
    request: Any,
    db: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
    edit_id_override: Optional[int] = None,
):
    edit_id = edit_id_override if edit_id_override is not None else safe_int(request.query_params.get("edit"))
    editing_row = _load_case(db, edit_id) if edit_id else None
    resolved_flash = flash
    if edit_id and editing_row is None and resolved_flash is None:
        resolved_flash = {"kind": "error", "message": "El caso solicitado para edición no existe."}
    rows = (
        db.query(m.ResidentCaseAssignmentDB)
        .order_by(m.ResidentCaseAssignmentDB.updated_at.desc(), m.ResidentCaseAssignmentDB.id.desc())
        .all()
    )
    return m.render_template(
        "jefatura_central_casos.html",
        request=request,
        module=CENTRAL_MODULE,
        flash=resolved_flash,
        case_rows=[_serialize_case_row(row) for row in rows],
        editing_case=_serialize_case_editor(editing_row) if editing_row is not None else None,
        resident_groups=resident_selection_groups(),
        grades=[group["grade"] for group in resident_selection_groups()],
        priority_options=CASE_PRIORITY_OPTIONS,
        status_options=CASE_STATUS_OPTIONS,
    )


async def create_central_case_from_request(request: Any, db: Session) -> Dict[str, Any]:
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    mode = str(form.get("assignment_mode") or "resident").strip().lower()
    resident_code = normalize_text(form.get("resident_code"), max_len=120)
    resident_grade = normalize_text(form.get("resident_grade"), max_len=10)
    targets = resolve_assignment_targets(mode, resident_code, resident_grade)
    patient_snapshot = normalize_text(form.get("patient_snapshot"), max_len=240)
    objetivo = normalize_text(form.get("objetivo"), max_len=2000)
    prioridad = normalize_text(form.get("prioridad"), max_len=40) or "MEDIA"
    estado = normalize_text(form.get("estado"), max_len=40) or "PENDIENTE"
    fecha_limite = safe_date(form.get("fecha_limite"))
    consulta_id = safe_int(form.get("consulta_id"))
    notas = normalize_text(form.get("notas"), max_len=2000)
    if not targets:
        return {"ok": False, "error": "Selecciona un residente o grado para asignar el caso."}
    if not patient_snapshot or not objetivo:
        return {"ok": False, "error": "El caso asociado requiere paciente resumido y objetivo."}
    try:
        actor = request_actor(request)
        for target in targets:
            db.add(
                m.ResidentCaseAssignmentDB(
                    resident_code=str(target["code"]).upper(),
                    consulta_id=consulta_id,
                    patient_snapshot=patient_snapshot,
                    objetivo=objetivo,
                    prioridad=prioridad,
                    fecha_limite=fecha_limite,
                    estado=estado,
                    notas=notas,
                    assigned_by=actor,
                )
            )
        db.commit()
        return {"ok": True, "message": f"Caso asociado cargado para {len(targets)} residente(s)."}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible registrar el caso asociado."}


async def update_central_case_from_request(request: Any, db: Session, case_id: int) -> Dict[str, Any]:
    row = _load_case(db, case_id)
    if row is None:
        return {"ok": False, "error": "El caso asociado solicitado no existe."}
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    resident_code = normalize_text(form.get("resident_code"), max_len=120)
    patient_snapshot = normalize_text(form.get("patient_snapshot"), max_len=240)
    objetivo = normalize_text(form.get("objetivo"), max_len=2000)
    prioridad = normalize_text(form.get("prioridad"), max_len=40) or "MEDIA"
    estado = normalize_text(form.get("estado"), max_len=40) or "PENDIENTE"
    fecha_limite = safe_date(form.get("fecha_limite"))
    consulta_id = safe_int(form.get("consulta_id"))
    notas = normalize_text(form.get("notas"), max_len=2000)
    if not resident_code:
        return {"ok": False, "error": "Selecciona el residente responsable del caso."}
    if not patient_snapshot or not objetivo:
        return {"ok": False, "error": "El caso asociado requiere paciente resumido y objetivo."}
    try:
        row.resident_code = resident_code.upper()
        row.consulta_id = consulta_id
        row.patient_snapshot = patient_snapshot
        row.objetivo = objetivo
        row.prioridad = prioridad
        row.fecha_limite = fecha_limite
        row.estado = estado
        row.notas = notas
        row.assigned_by = request_actor(request)
        db.commit()
        return {"ok": True}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible actualizar el caso asociado."}


async def render_jefatura_central_incidences_flow(
    request: Any,
    db: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
    edit_id_override: Optional[int] = None,
):
    edit_id = edit_id_override if edit_id_override is not None else safe_int(request.query_params.get("edit"))
    editing_row = _load_incidence(db, edit_id) if edit_id else None
    resolved_flash = flash
    if edit_id and editing_row is None and resolved_flash is None:
        resolved_flash = {"kind": "error", "message": "La incidencia solicitada para edición no existe."}
    rows = (
        db.query(m.ResidentIncidenceDB)
        .order_by(m.ResidentIncidenceDB.updated_at.desc(), m.ResidentIncidenceDB.id.desc())
        .all()
    )
    return m.render_template(
        "jefatura_central_incidencias.html",
        request=request,
        module=CENTRAL_MODULE,
        flash=resolved_flash,
        incidence_rows=[_serialize_incidence_row(row) for row in rows],
        editing_incidence=_serialize_incidence_editor(editing_row) if editing_row is not None else None,
        resident_groups=resident_selection_groups(),
        grades=[group["grade"] for group in resident_selection_groups()],
        severity_options=INCIDENCE_SEVERITY_OPTIONS,
        status_options=INCIDENCE_STATUS_OPTIONS,
    )


async def create_central_incidence_from_request(request: Any, db: Session) -> Dict[str, Any]:
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    mode = str(form.get("assignment_mode") or "resident").strip().lower()
    resident_code = normalize_text(form.get("resident_code"), max_len=120)
    resident_grade = normalize_text(form.get("resident_grade"), max_len=10)
    targets = resolve_assignment_targets(mode, resident_code, resident_grade)
    tipo = normalize_text(form.get("tipo"), max_len=120)
    severidad = normalize_text(form.get("severidad"), max_len=40) or "MODERADA"
    estado = normalize_text(form.get("estado"), max_len=40) or "ABIERTA"
    fecha_evento = safe_date(form.get("fecha_evento")) or date.today()
    consulta_id = safe_int(form.get("consulta_id"))
    descripcion = normalize_text(form.get("descripcion"), max_len=3000)
    resolucion = normalize_text(form.get("resolucion"), max_len=3000)
    if not targets:
        return {"ok": False, "error": "Selecciona un residente o grado para registrar la incidencia."}
    if not tipo or not descripcion:
        return {"ok": False, "error": "La incidencia requiere tipo y descripción."}
    try:
        actor = request_actor(request)
        for target in targets:
            db.add(
                m.ResidentIncidenceDB(
                    resident_code=str(target["code"]).upper(),
                    consulta_id=consulta_id,
                    tipo=tipo,
                    severidad=severidad,
                    fecha_evento=fecha_evento,
                    estado=estado,
                    descripcion=descripcion,
                    resolucion=resolucion,
                    assigned_by=actor,
                )
            )
        db.commit()
        return {"ok": True, "message": f"Incidencia registrada para {len(targets)} residente(s)."}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible registrar la incidencia."}


async def update_central_incidence_from_request(request: Any, db: Session, incidence_id: int) -> Dict[str, Any]:
    row = _load_incidence(db, incidence_id)
    if row is None:
        return {"ok": False, "error": "La incidencia solicitada no existe."}
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    resident_code = normalize_text(form.get("resident_code"), max_len=120)
    tipo = normalize_text(form.get("tipo"), max_len=120)
    severidad = normalize_text(form.get("severidad"), max_len=40) or "MODERADA"
    estado = normalize_text(form.get("estado"), max_len=40) or "ABIERTA"
    fecha_evento = safe_date(form.get("fecha_evento")) or date.today()
    consulta_id = safe_int(form.get("consulta_id"))
    descripcion = normalize_text(form.get("descripcion"), max_len=3000)
    resolucion = normalize_text(form.get("resolucion"), max_len=3000)
    if not resident_code:
        return {"ok": False, "error": "Selecciona el residente responsable de la incidencia."}
    if not tipo or not descripcion:
        return {"ok": False, "error": "La incidencia requiere tipo y descripción."}
    try:
        row.resident_code = resident_code.upper()
        row.consulta_id = consulta_id
        row.tipo = tipo
        row.severidad = severidad
        row.fecha_evento = fecha_evento
        row.estado = estado
        row.descripcion = descripcion
        row.resolucion = resolucion
        row.assigned_by = request_actor(request)
        db.commit()
        return {"ok": True}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible actualizar la incidencia."}


serialize_case_row = _serialize_case_row
serialize_incidence_row = _serialize_incidence_row
