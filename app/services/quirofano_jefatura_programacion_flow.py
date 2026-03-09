from __future__ import annotations

from collections import Counter
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
    recent_import_batches_for_dashboard,
    request_actor,
    room_code_from_number,
    safe_date,
    safe_int,
    safe_text,
    safe_time,
    serialize_case,
    status_badge,
    template_matrix,
    ui_terms,
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


def _chart_colors() -> Dict[str, str]:
    return {
        "navy": "#0f3f82",
        "navy_light": "rgba(15, 63, 130, 0.16)",
        "blue": "#2e6ec9",
        "blue_light": "rgba(46, 110, 201, 0.18)",
        "cyan": "#0f6f7d",
        "cyan_light": "rgba(15, 111, 125, 0.16)",
        "rose": "#c82f3f",
        "rose_light": "rgba(200, 47, 63, 0.16)",
        "gold": "#b98a2f",
        "gold_light": "rgba(185, 138, 47, 0.16)",
        "emerald": "#1f8a57",
        "emerald_light": "rgba(31, 138, 87, 0.16)",
        "slate": "#6b7b89",
        "slate_light": "rgba(107, 123, 137, 0.16)",
    }


def _chart_config(
    chart_type: str,
    *,
    labels: List[str],
    datasets: List[Dict[str, Any]],
    stacked: bool = False,
) -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "responsive": True,
        "maintainAspectRatio": False,
        "plugins": {
            "legend": {"display": True, "position": "bottom", "labels": {"boxWidth": 12, "usePointStyle": True}},
            "tooltip": {"mode": "index", "intersect": False},
        },
    }
    if chart_type in {"bar", "line"}:
        options["interaction"] = {"mode": "index", "intersect": False}
        options["scales"] = {
            "x": {"grid": {"display": False}, "stacked": stacked},
            "y": {"beginAtZero": True, "grid": {"color": "rgba(107,123,137,0.14)"}, "stacked": stacked},
        }
    return {"type": chart_type, "data": {"labels": labels, "datasets": datasets}, "options": options}


def _service_line_name_map(session: Session) -> Dict[str, str]:
    return {str(item["code"]): str(item["nombre"]) for item in _service_line_choices(session)}


def _recent_day_trend(session: Session, anchor_date: date, *, window: int = 7) -> List[Dict[str, Any]]:
    trend: List[Dict[str, Any]] = []
    for offset in range(window - 1, -1, -1):
        target = anchor_date - timedelta(days=offset)
        overview = build_day_overview(session, target, actor="SYSTEM")
        trend.append(
            {
                "label": target.strftime("%d/%m"),
                "programmed": overview["kpis"]["programmed_cases"],
                "performed": overview["kpis"]["performed_cases"],
                "occupancy": overview["kpis"]["occupancy_pct"],
            }
        )
    return trend


def _day_chart_payload(session: Session, target_date: date, overview: Dict[str, Any]) -> Dict[str, Any]:
    colors = _chart_colors()
    terms = ui_terms()
    cases = list(overview["cases"])
    blocks = list(overview["blocks"])
    shift_labels = [str(item["label"]) for item in SHIFT_OPTIONS]
    shift_codes = [str(item["code"]) for item in SHIFT_OPTIONS]
    shift_case_counts = [
        sum(1 for row in cases if str(getattr(getattr(row, "daily_block", None), "turno", "") or "").upper() == code)
        for code in shift_codes
    ]
    shift_block_counts = [
        sum(1 for row in blocks if str(getattr(row, "turno", "") or "").upper() == code)
        for code in shift_codes
    ]
    status_order = [
        ("PROGRAMADA", "Programadas", colors["blue"]),
        ("EN_CURSO", "En curso", colors["gold"]),
        ("REALIZADA", "Realizadas", colors["emerald"]),
        ("CANCELADA", "Canceladas", colors["rose"]),
        ("REPROGRAMADA", "Reprogramadas", colors["slate"]),
    ]
    status_counts = Counter(str(getattr(row, "status", "") or "").upper() for row in cases)
    line_name_map = _service_line_name_map(session)
    service_counts = Counter(
        str(getattr(getattr(row, "daily_block", None), "service_line_code", "") or "SIN_LINEA")
        for row in cases
    )
    service_items = sorted(service_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    service_labels = [line_name_map.get(code, code.replace("_", " ")) for code, _ in service_items]
    service_values = [value for _, value in service_items]
    trend = _recent_day_trend(session, target_date)
    return {
        "shift_mix": _chart_config(
            "bar",
            labels=shift_labels,
            datasets=[
                {
                    "label": str(terms["enabled_spaces"]),
                    "data": shift_block_counts,
                    "backgroundColor": colors["navy_light"],
                    "borderColor": colors["navy"],
                    "borderWidth": 1.4,
                    "borderRadius": 10,
                },
                {
                    "label": "Cirugías programadas",
                    "data": shift_case_counts,
                    "backgroundColor": colors["rose_light"],
                    "borderColor": colors["rose"],
                    "borderWidth": 1.4,
                    "borderRadius": 10,
                },
            ],
        ),
        "status_mix": _chart_config(
            "doughnut",
            labels=[label for _, label, _ in status_order],
            datasets=[
                {
                    "data": [status_counts.get(code, 0) for code, _, _ in status_order],
                    "backgroundColor": [color for _, _, color in status_order],
                    "borderWidth": 0,
                }
            ],
        ),
        "service_mix": _chart_config(
            "bar",
            labels=service_labels or ["Sin casos"],
            datasets=[
                {
                    "label": "Cirugías por servicio",
                    "data": service_values or [0],
                    "backgroundColor": colors["cyan_light"],
                    "borderColor": colors["cyan"],
                    "borderWidth": 1.4,
                    "borderRadius": 10,
                }
            ],
        ),
        "trend_mix": _chart_config(
            "line",
            labels=[item["label"] for item in trend],
            datasets=[
                {
                    "label": "Cirugías programadas",
                    "data": [item["programmed"] for item in trend],
                    "borderColor": colors["navy"],
                    "backgroundColor": colors["navy_light"],
                    "fill": True,
                    "tension": 0.34,
                    "pointRadius": 4,
                    "pointHoverRadius": 6,
                },
                {
                    "label": "Realizadas",
                    "data": [item["performed"] for item in trend],
                    "borderColor": colors["emerald"],
                    "backgroundColor": colors["emerald_light"],
                    "fill": False,
                    "tension": 0.34,
                    "pointRadius": 4,
                    "pointHoverRadius": 6,
                },
            ],
        ),
    }


def _template_dashboard(matrix: List[Dict[str, Any]], service_lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    colors = _chart_colors()
    terms = ui_terms()
    active_slots = [slot for slot in matrix if str(slot.get("service_line_code") or "").strip()]
    shift_counts = {
        "MATUTINO": sum(1 for slot in active_slots if str(slot["shift"]).upper() == "MATUTINO"),
        "VESPERTINO": sum(1 for slot in active_slots if str(slot["shift"]).upper() == "VESPERTINO"),
    }
    day_labels = [day["label"] for day in DAY_OPTIONS]
    day_counts = [
        sum(1 for slot in active_slots if int(slot["day_of_week"]) == int(day["value"]))
        for day in DAY_OPTIONS
    ]
    line_name_map = {str(item["code"]): str(item["nombre"]) for item in service_lines}
    line_counter = Counter(str(slot["service_line_code"]) for slot in active_slots)
    line_items = sorted(line_counter.items(), key=lambda item: (-item[1], item[0]))[:10]
    return {
        "kpis": {
            "weekly_blocks": len(active_slots),
            "matutino_blocks": shift_counts["MATUTINO"],
            "vespertino_blocks": shift_counts["VESPERTINO"],
            "active_lines": sum(1 for item in service_lines if bool(item.get("activo"))),
        },
        "charts": {
            "template_capacity": _chart_config(
                "bar",
                labels=day_labels,
                datasets=[
                    {
                        "label": str(terms["weekly_spaces"]),
                        "data": day_counts,
                        "backgroundColor": colors["blue_light"],
                        "borderColor": colors["blue"],
                        "borderWidth": 1.4,
                        "borderRadius": 10,
                    }
                ],
            ),
            "template_lines": _chart_config(
                "doughnut",
                labels=[line_name_map.get(code, code) for code, _ in line_items] or ["Sin líneas"],
                datasets=[
                    {
                        "data": [value for _, value in line_items] or [0],
                        "backgroundColor": [
                            colors["navy"],
                            colors["blue"],
                            colors["cyan"],
                            colors["rose"],
                            colors["gold"],
                            colors["emerald"],
                            "#5d6bd8",
                            "#8771d8",
                            "#d8687d",
                            "#4d9e8d",
                        ][: max(1, len(line_items))],
                        "borderWidth": 0,
                    }
                ],
            ),
        },
    }


def build_dashboard_payload(session: Session, target_date: Optional[date] = None) -> Dict[str, Any]:
    actor = "SYSTEM"
    selected_date = target_date or date.today()
    ensure_jefatura_quirofano_seed(session, actor=actor)
    overview = build_day_overview(session, selected_date, actor=actor)
    active_version = active_template_version(session)
    imports = recent_import_batches_for_dashboard(session, limit=6)
    service_lines = _service_line_choices(session)
    return {
        "selected_date": selected_date,
        "selected_date_label": format_date(selected_date),
        "overview": overview,
        "active_template": active_version,
        "recent_imports": imports,
        "template_slots_total": len(getattr(active_version, "slots", []) or []) if active_version is not None else 0,
        "service_lines_total": len(service_lines),
        "charts": _day_chart_payload(session, selected_date, overview),
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
        service_lines_total=payload["service_lines_total"],
        charts=payload["charts"],
        ui_terms=ui_terms(),
    )


async def render_jefatura_quirofano_template_flow(
    request: Request,
    session: Session,
    *,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    ensure_jefatura_quirofano_seed(session, actor=request_actor(request))
    version = active_template_version(session)
    service_lines = [row for row in _service_line_choices(session)]
    matrix = template_matrix(session)
    template_dashboard = _template_dashboard(matrix, service_lines)
    return m.render_template(
        "quirofano_jefatura_plantillas.html",
        request=request,
        flash=flash,
        current_version=version,
        selected_date=date.today(),
        selected_date_label=format_date(date.today()),
        matrix=matrix,
        service_lines=service_lines,
        template_dashboard=template_dashboard,
        charts=template_dashboard["charts"],
        day_options=DAY_OPTIONS,
        shift_options=SHIFT_OPTIONS,
        ui_terms=ui_terms(),
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
        ui_terms=ui_terms(),
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
        charts=_day_chart_payload(session, target_date, overview),
        service_lines=_service_line_choices(session),
        block_status_options=BLOCK_STATUS_OPTIONS,
        case_status_options=CASE_STATUS_OPTIONS,
        edit_case=serialize_case(edit_case) if edit_case is not None else None,
        import_batches=recent_import_batches(session, limit=10),
        previous_date=target_date - timedelta(days=1),
        next_date=target_date + timedelta(days=1),
        ui_terms=ui_terms(),
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
        ui_terms=ui_terms(),
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
    overview = build_day_overview(session, target_date, actor="SYSTEM")
    return m.render_template(
        "quirofano_jefatura_publicacion.html",
        request=request,
        flash=flash,
        selected_date=target_date,
        selected_date_label=format_date(target_date),
        overview=overview,
        rows=publication_rows_for_date(session, target_date, actor="SYSTEM"),
        ui_terms=ui_terms(),
    )
