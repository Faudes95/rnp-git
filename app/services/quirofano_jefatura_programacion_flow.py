from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, joinedload

from app.core.app_context import main_proxy as m
from app.core.time_utils import utcnow
from app.services.quirofano_jefatura_shared import (
    BLOCK_STATUS_OPTIONS,
    CASE_EVENT_TYPES,
    CASE_INCIDENT_TYPES,
    CASE_STATUS_OPTIONS,
    DAY_OPTIONS,
    DEFAULT_UNIT_CODE,
    SHIFT_OPTIONS,
    STAFF_ROLE_OPTIONS,
    active_template_version,
    build_day_overview,
    clone_template_version,
    day_label,
    daily_blocks_for_date,
    ensure_jefatura_quirofano_seed,
    format_date,
    format_dt,
    list_service_lines,
    log_audit,
    publication_rows_for_date,
    recent_import_batches,
    request_actor,
    room_code_from_number,
    safe_date,
    safe_int,
    safe_text,
    safe_time,
    serialize_case,
    status_badge,
    template_matrix,
    validate_case_conflicts,
    validate_case_event_sequence,
)


def _service_line_choices(session: Session) -> List[Dict[str, Any]]:
    return [
        {
            "code": str(row.code),
            "nombre": str(row.nombre),
            "line_type": str(row.line_type),
            "activo": bool(row.activo),
        }
        for row in list_service_lines(session)
        if bool(getattr(row, "activo", False))
    ]


def _parse_datetime_local(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None


def build_dashboard_payload(session: Session, target_date: Optional[date] = None) -> Dict[str, Any]:
    actor = "SYSTEM"
    selected_date = target_date or date.today()
    ensure_jefatura_quirofano_seed(session, actor=actor)
    overview = build_day_overview(session, selected_date, actor=actor)
    active_version = active_template_version(session)
    imports = recent_import_batches(session, limit=6)
    return {
        "selected_date": selected_date,
        "selected_date_label": format_date(selected_date),
        "overview": overview,
        "active_template": active_version,
        "recent_imports": imports,
        "template_slots_total": len(getattr(active_version, "slots", []) or []) if active_version is not None else 0,
    }


async def render_jefatura_quirofano_home_flow(
    request: Request,
    session: Session,
    *,
    target_date: Optional[date] = None,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    payload = build_dashboard_payload(session, target_date=target_date)
    return m.render_template(
        "quirofano_jefatura_home.html",
        request=request,
        flash=flash,
        module_slug="quirofano_jefatura",
        selected_date=payload["selected_date"],
        selected_date_label=payload["selected_date_label"],
        overview=payload["overview"],
        active_template=payload["active_template"],
        recent_imports=payload["recent_imports"],
        template_slots_total=payload["template_slots_total"],
    )


async def render_jefatura_quirofano_template_flow(
    request: Request,
    session: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    version = active_template_version(session)
    return m.render_template(
        "quirofano_jefatura_plantillas.html",
        request=request,
        flash=flash,
        current_version=version,
        matrix=template_matrix(session),
        service_lines=[row for row in _service_line_choices(session)],
        day_options=DAY_OPTIONS,
        shift_options=SHIFT_OPTIONS,
    )


async def save_service_lines_from_request(request: Request, session: Session) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    ensure_jefatura_quirofano_seed(session, actor=actor)
    rows = list_service_lines(session)
    row_map = {str(row.code).upper(): row for row in rows}
    touched = 0
    for row in rows:
        code = str(row.code).upper()
        nombre = safe_text(form.get(f"line_name__{code}"), max_len=180)
        line_type = safe_text(form.get(f"line_type__{code}"), max_len=40)
        activo = str(form.get(f"line_active__{code}") or "").lower() == "on"
        if nombre:
            row.nombre = nombre
        if line_type:
            row.line_type = str(line_type).upper()
        row.activo = activo
        touched += 1
    new_code = safe_text(form.get("new_line_code"), max_len=60)
    new_name = safe_text(form.get("new_line_name"), max_len=180)
    new_type = safe_text(form.get("new_line_type"), max_len=40)
    if new_code and new_name:
        code = str(new_code).upper()
        if code in row_map:
            return {"ok": False, "error": f"La línea {code} ya existe."}
        session.add(
            m.JefaturaQuirofanoServiceLineDB(
                unidad_code=DEFAULT_UNIT_CODE,
                code=code,
                nombre=new_name,
                line_type=str(new_type or "CLINICO").upper(),
                activo=True,
                display_order=max([int(getattr(item, "display_order", 0) or 0) for item in rows] + [0]) + 10,
            )
        )
        touched += 1
    log_audit(session, actor=actor, action="update_service_lines", entity_type="service_line_catalog", payload={"rows": touched})
    session.commit()
    return {"ok": True}


async def save_template_version_from_request(request: Request, session: Session) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    service_codes = {str(item["code"]) for item in _service_line_choices(session)}
    slot_specs: List[Dict[str, Any]] = []
    for day in DAY_OPTIONS:
        for shift in SHIFT_OPTIONS:
            for room_number in range(1, 15):
                key = f"slot__{day['value']}__{shift['code']}__{room_number}"
                code = safe_text(form.get(key), max_len=60)
                if not code:
                    continue
                code = str(code).upper()
                if code not in service_codes:
                    return {"ok": False, "error": f"La línea {code} no existe o está inactiva."}
                slot_specs.append(
                    {
                        "day_of_week": day["value"],
                        "turno": shift["code"],
                        "room_number": room_number,
                        "service_line_code": code,
                        "activo": True,
                    }
                )
    if not slot_specs:
        return {"ok": False, "error": "La plantilla no puede quedar vacía."}
    version_label = safe_text(form.get("version_label"), max_len=120) or f"Actualización {utcnow().strftime('%Y-%m-%d %H:%M')}"
    clone_template_version(session, actor=actor, version_label=version_label, slot_specs=slot_specs)
    session.commit()
    return {"ok": True}


async def render_jefatura_quirofano_programacion_index_flow(
    request: Request,
    session: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    today = date.today()
    days = []
    for offset in range(-3, 11):
        target = today + timedelta(days=offset)
        overview = build_day_overview(session, target, actor="SYSTEM")
        days.append(
            {
                "date": target,
                "date_label": format_date(target),
                "weekday": day_label(target.weekday()),
                "programmed_cases": overview["kpis"]["programmed_cases"],
                "blocks_total": overview["kpis"]["blocks_total"],
                "occupancy_pct": overview["kpis"]["occupancy_pct"],
            }
        )
    return m.render_template(
        "quirofano_jefatura_programacion.html",
        request=request,
        flash=flash,
        view_mode="calendar",
        days=days,
        selected_date=today,
        selected_date_label=format_date(today),
    )


async def render_jefatura_quirofano_day_flow(
    request: Request,
    session: Session,
    target_date: date,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    actor = request_actor(request)
    overview = build_day_overview(session, target_date, actor=actor)
    edit_case_id = safe_int(request.query_params.get("edit_case"))
    edit_case = None
    if edit_case_id:
        edit_case = next((row for row in overview["cases"] if int(getattr(row, "id", 0) or 0) == int(edit_case_id)), None)
    return m.render_template(
        "quirofano_jefatura_programacion.html",
        request=request,
        flash=flash,
        view_mode="day",
        selected_date=target_date,
        selected_date_label=format_date(target_date),
        weekday_label=day_label(target_date.weekday()),
        overview=overview,
        service_lines=_service_line_choices(session),
        block_status_options=BLOCK_STATUS_OPTIONS,
        case_status_options=CASE_STATUS_OPTIONS,
        edit_case=serialize_case(edit_case) if edit_case is not None else None,
        import_batches=recent_import_batches(session, limit=10),
        previous_date=target_date - timedelta(days=1),
        next_date=target_date + timedelta(days=1),
    )


async def update_daily_blocks_from_request(request: Request, session: Session, target_date: date) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    blocks = daily_blocks_for_date(session, target_date, actor=actor)
    allowed_lines = {str(item["code"]) for item in _service_line_choices(session)}
    for block in blocks:
        line_code = safe_text(form.get(f"service_line__{block.id}"), max_len=60)
        block_status = safe_text(form.get(f"block_status__{block.id}"), max_len=40)
        notes = safe_text(form.get(f"notes__{block.id}"), max_len=500)
        if line_code and str(line_code).upper() in allowed_lines:
            block.service_line_code = str(line_code).upper()
        if block_status and str(block_status).upper() in BLOCK_STATUS_OPTIONS:
            block.block_status = str(block_status).upper()
        block.notes = notes
        block.confirmed_by = actor
        block.confirmed_at = utcnow()
    log_audit(session, actor=actor, action="update_daily_blocks", entity_type="daily_block", payload={"fecha": target_date.isoformat()})
    session.commit()
    return {"ok": True}


async def upsert_daily_case_from_request(request: Request, session: Session, target_date: date) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    case_id = safe_int(form.get("case_id"))
    daily_block_id = safe_int(form.get("daily_block_id"))
    if not daily_block_id:
        return {"ok": False, "error": "Debes seleccionar una sala del día."}
    payload = {
        "status": safe_text(form.get("status"), max_len=40) or "PROGRAMADA",
        "scheduled_time": safe_text(form.get("scheduled_time"), max_len=10),
        "duracion_estimada_min": safe_int(form.get("duracion_estimada_min")) or 60,
        "cama": safe_text(form.get("cama"), max_len=20),
        "patient_name": safe_text(form.get("patient_name"), max_len=220),
        "nss": safe_text(form.get("nss"), max_len=20),
        "agregado_medico": safe_text(form.get("agregado_medico"), max_len=80),
        "edad": safe_int(form.get("edad")),
        "diagnostico_preoperatorio": safe_text(form.get("diagnostico_preoperatorio"), max_len=240),
        "operacion_proyectada": safe_text(form.get("operacion_proyectada"), max_len=240),
        "cirujano": safe_text(form.get("cirujano"), max_len=180),
        "anestesiologo": safe_text(form.get("anestesiologo"), max_len=180),
        "enfermera_especialista": safe_text(form.get("enfermera_especialista"), max_len=180),
        "tipo_anestesia": safe_text(form.get("tipo_anestesia"), max_len=120),
        "notes": safe_text(form.get("notes"), max_len=800),
    }
    if not payload["patient_name"] or not payload["operacion_proyectada"] or not payload["scheduled_time"]:
        return {"ok": False, "error": "Nombre del paciente, hora y operación proyectada son obligatorios."}
    errors = validate_case_conflicts(
        session,
        daily_block_id=int(daily_block_id),
        scheduled_time=payload["scheduled_time"],
        duration_min=payload["duracion_estimada_min"],
        cirujano=payload["cirujano"],
        anestesiologo=payload["anestesiologo"],
        enfermera_especialista=payload["enfermera_especialista"],
        exclude_case_id=case_id,
    )
    if errors:
        return {"ok": False, "error": " ".join(errors)}
    if case_id:
        row = session.get(m.JefaturaQuirofanoCaseProgramacionDB, int(case_id))
        if row is None:
            return {"ok": False, "error": "El caso que intentas editar ya no existe."}
    else:
        row = m.JefaturaQuirofanoCaseProgramacionDB(
            daily_block_id=int(daily_block_id),
            unidad_code=DEFAULT_UNIT_CODE,
            source_type="MANUAL",
            created_by=actor,
        )
        session.add(row)
    row.daily_block_id = int(daily_block_id)
    row.status = str(payload["status"]).upper()
    row.scheduled_time = payload["scheduled_time"]
    row.duracion_estimada_min = int(payload["duracion_estimada_min"])
    row.cama = payload["cama"]
    row.patient_name = payload["patient_name"]
    row.nss = payload["nss"]
    row.agregado_medico = payload["agregado_medico"]
    row.edad = payload["edad"]
    row.diagnostico_preoperatorio = payload["diagnostico_preoperatorio"]
    row.operacion_proyectada = payload["operacion_proyectada"]
    row.cirujano = payload["cirujano"]
    row.anestesiologo = payload["anestesiologo"]
    row.enfermera_especialista = payload["enfermera_especialista"]
    row.tipo_anestesia = payload["tipo_anestesia"]
    row.notes = payload["notes"]
    session.flush()
    log_audit(
        session,
        actor=actor,
        action="update_case" if case_id else "create_case",
        entity_type="case_programacion",
        entity_id=int(row.id),
        payload={"fecha": target_date.isoformat(), "daily_block_id": int(daily_block_id)},
    )
    session.commit()
    return {"ok": True, "case_id": int(row.id)}


async def render_jefatura_quirofano_case_detail_flow(
    request: Request,
    session: Session,
    case_id: int,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    actor = request_actor(request)
    ensure_jefatura_quirofano_seed(session, actor=actor)
    row = (
        session.query(m.JefaturaQuirofanoCaseProgramacionDB)
        .options(
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.daily_block),
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.staff_assignments),
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.events),
            joinedload(m.JefaturaQuirofanoCaseProgramacionDB.incidencias),
        )
        .filter(m.JefaturaQuirofanoCaseProgramacionDB.id == int(case_id))
        .first()
    )
    if row is None:
        return HTMLResponse(content="<h1>Caso no encontrado</h1>", status_code=404)
    case_payload = serialize_case(row, include_relations=True)
    return m.render_template(
        "quirofano_jefatura_case_detail.html",
        request=request,
        flash=flash,
        case=case_payload,
        case_row=row,
        event_type_options=CASE_EVENT_TYPES,
        incidence_type_options=CASE_INCIDENT_TYPES,
        staff_role_options=STAFF_ROLE_OPTIONS,
        block_badge=status_badge(getattr(getattr(row, "daily_block", None), "block_status", "ACTIVO")),
        target_date=getattr(getattr(row, "daily_block", None), "fecha", None),
        target_date_label=format_date(getattr(getattr(row, "daily_block", None), "fecha", None)),
        now_local=datetime.now().strftime("%Y-%m-%dT%H:%M"),
    )


async def add_case_staff_from_request(request: Request, session: Session, case_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    row = session.get(m.JefaturaQuirofanoCaseProgramacionDB, int(case_id))
    if row is None:
        return {"ok": False, "error": "Caso no encontrado."}
    staff_name = safe_text(form.get("staff_name"), max_len=180)
    staff_role = safe_text(form.get("staff_role"), max_len=60)
    if not staff_name or staff_role not in STAFF_ROLE_OPTIONS:
        return {"ok": False, "error": "Nombre y rol del personal son obligatorios."}
    session.add(
        m.JefaturaQuirofanoCaseStaffDB(
            case_id=int(case_id),
            staff_name=staff_name,
            staff_role=staff_role,
            notes=safe_text(form.get("notes"), max_len=300),
            created_by=actor,
        )
    )
    log_audit(session, actor=actor, action="add_case_staff", entity_type="case_programacion", entity_id=int(case_id), payload={"staff_role": staff_role})
    session.commit()
    return {"ok": True}


async def add_case_event_from_request(request: Request, session: Session, case_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    row = session.get(m.JefaturaQuirofanoCaseProgramacionDB, int(case_id))
    if row is None:
        return {"ok": False, "error": "Caso no encontrado."}
    event_type = safe_text(form.get("event_type"), max_len=60)
    event_at = _parse_datetime_local(form.get("event_at"))
    if event_type not in CASE_EVENT_TYPES or event_at is None:
        return {"ok": False, "error": "Tipo y fecha/hora del evento son obligatorios."}
    errors = validate_case_event_sequence(session, case_id=int(case_id), event_type=str(event_type), event_at=event_at)
    if errors:
        return {"ok": False, "error": " ".join(errors)}
    session.add(
        m.JefaturaQuirofanoCaseEventDB(
            case_id=int(case_id),
            event_type=str(event_type),
            event_at=event_at,
            notes=safe_text(form.get("notes"), max_len=400),
            created_by=actor,
        )
    )
    if str(event_type).upper() == "CANCELADO":
        row.status = "CANCELADA"
    elif str(event_type).upper() == "FIN_CIRUGIA":
        row.status = "REALIZADA"
    elif str(event_type).upper() == "INICIO_CIRUGIA":
        row.status = "EN_CURSO"
    log_audit(session, actor=actor, action="add_case_event", entity_type="case_programacion", entity_id=int(case_id), payload={"event_type": event_type})
    session.commit()
    return {"ok": True}


async def add_case_incidence_from_request(request: Request, session: Session, case_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    actor = request_actor(request)
    row = session.get(m.JefaturaQuirofanoCaseProgramacionDB, int(case_id))
    if row is None:
        return {"ok": False, "error": "Caso no encontrado."}
    incidence_type = safe_text(form.get("incidence_type"), max_len=80)
    event_at = _parse_datetime_local(form.get("event_at")) or utcnow()
    description = safe_text(form.get("description"), max_len=800)
    if incidence_type not in CASE_INCIDENT_TYPES or not description:
        return {"ok": False, "error": "Tipo y descripción de la incidencia son obligatorios."}
    session.add(
        m.JefaturaQuirofanoCaseIncidenciaDB(
            case_id=int(case_id),
            incidence_type=str(incidence_type),
            status="ABIERTA",
            description=description,
            event_at=event_at,
            created_by=actor,
        )
    )
    log_audit(session, actor=actor, action="add_case_incidence", entity_type="case_programacion", entity_id=int(case_id), payload={"incidence_type": incidence_type})
    session.commit()
    return {"ok": True}


async def render_jefatura_quirofano_publication_flow(
    request: Request,
    session: Session,
    target_date: date,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    return m.render_template(
        "quirofano_jefatura_publicacion.html",
        request=request,
        flash=flash,
        selected_date=target_date,
        selected_date_label=format_date(target_date),
        rows=publication_rows_for_date(session, target_date, actor="SYSTEM"),
    )

