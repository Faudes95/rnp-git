from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.jefatura_central_shared import CENTRAL_MODULE, status_badge


def _clean_items(raw_value: Any) -> List[str]:
    text = str(raw_value or "").replace(",", "|")
    return [item.strip() for item in text.split("|") if item.strip()]


def _is_yes(value: Any) -> bool:
    return str(value or "").strip().upper() in {"SI", "SÍ", "YES", "TRUE", "1"}


def _is_cancelled(value: Any) -> bool:
    return "CANCEL" in str(value or "").strip().upper()


def _has_supply_pressure(row: Any) -> bool:
    return bool(
        _clean_items(getattr(row, "insumos_solicitados", None))
        or _is_yes(getattr(row, "requiere_intermed", None))
        or _is_yes(getattr(row, "solicita_hemoderivados", None))
    )


def _serialize_request_row(row: Any, *, source: str, date_attr: str, procedure_attr: str) -> Dict[str, Any]:
    request_date = getattr(row, date_attr, None)
    status_value = str(getattr(row, "estatus", None) or "PENDIENTE")
    badge = status_badge("PENDIENTE_CASO" if not _is_cancelled(status_value) else "CERRADO")
    items = _clean_items(getattr(row, "insumos_solicitados", None))
    return {
        "source": source,
        "patient_name": str(getattr(row, "paciente_nombre", None) or "Paciente sin nombre"),
        "procedure_name": str(getattr(row, procedure_attr, None) or "Procedimiento no especificado"),
        "request_date": request_date,
        "request_date_label": request_date.strftime("%d/%m/%Y") if isinstance(request_date, date) else "Sin fecha",
        "status_label": badge["label"],
        "status_tone": badge["tone"],
        "quirofano": str(getattr(row, "quirofano", None) or source),
        "requiere_intermed": "SI" if _is_yes(getattr(row, "requiere_intermed", None)) else "NO",
        "solicita_hemoderivados": "SI" if _is_yes(getattr(row, "solicita_hemoderivados", None)) else "NO",
        "items": items,
        "items_label": ", ".join(items) if items else "Sin detalle capturado",
    }


def build_central_insumos_overview(db: Session) -> Dict[str, Any]:
    today = date.today()
    recent_cutoff = today - timedelta(days=90)
    urgent_cutoff = today - timedelta(days=21)
    demand_filter_programmed = or_(
        m.SurgicalProgramacionDB.insumos_solicitados.isnot(None),
        m.SurgicalProgramacionDB.requiere_intermed.isnot(None),
        m.SurgicalProgramacionDB.solicita_hemoderivados.isnot(None),
    )
    demand_filter_urgent = or_(
        m.SurgicalUrgenciaProgramacionDB.insumos_solicitados.isnot(None),
        m.SurgicalUrgenciaProgramacionDB.requiere_intermed.isnot(None),
        m.SurgicalUrgenciaProgramacionDB.solicita_hemoderivados.isnot(None),
    )

    programmed_recent_rows = (
        db.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.fecha_programada.isnot(None), m.SurgicalProgramacionDB.fecha_programada >= recent_cutoff, demand_filter_programmed)
        .order_by(m.SurgicalProgramacionDB.fecha_programada.desc(), m.SurgicalProgramacionDB.id.desc())
        .all()
    )
    urgent_recent_rows = (
        db.query(m.SurgicalUrgenciaProgramacionDB)
        .filter(m.SurgicalUrgenciaProgramacionDB.fecha_urgencia.isnot(None), m.SurgicalUrgenciaProgramacionDB.fecha_urgencia >= recent_cutoff, demand_filter_urgent)
        .order_by(m.SurgicalUrgenciaProgramacionDB.fecha_urgencia.desc(), m.SurgicalUrgenciaProgramacionDB.id.desc())
        .all()
    )
    programmed_upcoming_rows = (
        db.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.fecha_programada.isnot(None), m.SurgicalProgramacionDB.fecha_programada >= today, demand_filter_programmed)
        .order_by(m.SurgicalProgramacionDB.fecha_programada.asc(), m.SurgicalProgramacionDB.id.asc())
        .all()
    )
    urgent_active_rows = (
        db.query(m.SurgicalUrgenciaProgramacionDB)
        .filter(m.SurgicalUrgenciaProgramacionDB.fecha_urgencia.isnot(None), m.SurgicalUrgenciaProgramacionDB.fecha_urgencia >= urgent_cutoff, demand_filter_urgent)
        .order_by(m.SurgicalUrgenciaProgramacionDB.fecha_urgencia.desc(), m.SurgicalUrgenciaProgramacionDB.id.desc())
        .all()
    )

    programmed_recent = [_serialize_request_row(row, source="Programada", date_attr="fecha_programada", procedure_attr="procedimiento_programado") for row in programmed_recent_rows if not _is_cancelled(getattr(row, "estatus", None))]
    urgent_recent = [_serialize_request_row(row, source="Urgencia", date_attr="fecha_urgencia", procedure_attr="procedimiento_programado") for row in urgent_recent_rows if not _is_cancelled(getattr(row, "estatus", None))]
    upcoming_programmed = [_serialize_request_row(row, source="Programada", date_attr="fecha_programada", procedure_attr="procedimiento_programado") for row in programmed_upcoming_rows if not _is_cancelled(getattr(row, "estatus", None)) and _has_supply_pressure(row)][:8]
    active_urgent = [_serialize_request_row(row, source="Urgencia", date_attr="fecha_urgencia", procedure_attr="procedimiento_programado") for row in urgent_active_rows if not _is_cancelled(getattr(row, "estatus", None)) and _has_supply_pressure(row)][:8]

    item_counter: Counter[str] = Counter()
    for row in programmed_recent + urgent_recent:
        item_counter.update(row["items"])

    top_items = [
        {"name": item, "count": int(count)}
        for item, count in item_counter.most_common(10)
    ]

    intermed_alerts = sum(1 for row in upcoming_programmed + active_urgent if row["requiere_intermed"] == "SI")
    hemoderivados_alerts = sum(1 for row in upcoming_programmed + active_urgent if row["solicita_hemoderivados"] == "SI")
    demand_90_days = len(programmed_recent) + len(urgent_recent)
    pending_load = len(upcoming_programmed) + len(active_urgent)

    alerts: List[Dict[str, Any]] = []
    if intermed_alerts:
        alerts.append(
            {
                "title": "Intermed requerido",
                "tone": "amber",
                "value": intermed_alerts,
                "copy": "Casos con necesidad de sistema intermed dentro de la carga quirúrgica vigente.",
            }
        )
    if hemoderivados_alerts:
        alerts.append(
            {
                "title": "Hemoderivados solicitados",
                "tone": "red",
                "value": hemoderivados_alerts,
                "copy": "Casos con solicitud explícita de hemoderivados que requieren vigilancia operativa.",
            }
        )
    for item in top_items[:4]:
        if item["count"] < 2:
            continue
        alerts.append(
            {
                "title": item["name"],
                "tone": "blue",
                "value": item["count"],
                "copy": "Apariciones recientes del insumo dentro de solicitudes quirúrgicas capturadas.",
            }
        )

    return {
        "kpis": {
            "demand_90_days": int(demand_90_days),
            "pending_load": int(pending_load),
            "intermed_alerts": int(intermed_alerts),
            "hemoderivados_alerts": int(hemoderivados_alerts),
        },
        "alerts": alerts,
        "top_items": top_items,
        "upcoming_programmed": upcoming_programmed,
        "active_urgent": active_urgent,
    }


async def render_jefatura_central_insumos_flow(request: Any, db: Session, *, flash: Optional[Dict[str, str]] = None):
    return m.render_template(
        "jefatura_central_insumos.html",
        request=request,
        module=CENTRAL_MODULE,
        flash=flash,
        overview=build_central_insumos_overview(db),
    )

