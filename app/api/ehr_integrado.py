"""API y vistas para el Expediente Clínico Integrado (EHR).

Endpoints ADITIVOS — no reemplazan rutas legacy (/expediente, /expediente/fase1, etc.).
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import func, select, or_, and_, desc
from sqlalchemy.orm import Session

from app.models.ehr_models import (
    EHR_DOCUMENTS,
    EHR_TIMELINE_EVENTS,
    EHR_TAGS,
    EHR_DOCUMENT_TAGS,
    EHR_FEATURES_DAILY,
    EHR_PROBLEM_LIST,
    EHR_ALERT_LIFECYCLE,
    ensure_ehr_schema,
)
from app.services.expediente import reindex_last_days, reindex_patient

logger = logging.getLogger("rnp.ehr_integrado")

router = APIRouter(tags=["ehr-integrado"])


def _serialize_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Serializa un dict de fila SQLAlchemy para JSON (convierte dates/datetimes)."""
    out = {}
    for k, v in row_dict.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        elif v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _get_m():
    from app.core.app_context import main_proxy as m
    return m


# ═══════════════════════════════════════════════════════════════════════
#  VISTA PRINCIPAL — Expediente Integrado
# ═══════════════════════════════════════════════════════════════════════

@router.get("/expediente/integrado", response_class=HTMLResponse)
async def expediente_integrado_view(request: Request, patient_uid: str = ""):
    """Renderiza el template del Expediente Clínico Integrado."""
    m = _get_m()
    return m.render_template(
        "expediente_integrado.html",
        request=request,
        patient_uid=patient_uid,
    )


# ═══════════════════════════════════════════════════════════════════════
#  API — Datos del expediente integrado
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/resumen", response_class=JSONResponse)
async def ehr_resumen(
    request: Request,
    patient_uid: str = Query(..., min_length=1),
    db: Session = Depends(_get_db),
):
    """Resumen ejecutivo del paciente con score de completitud."""
    m = _get_m()
    ensure_ehr_schema(db)

    # Autoindex si no hay datos
    doc_count = db.execute(
        select(func.count(EHR_DOCUMENTS.c.id))
        .where(EHR_DOCUMENTS.c.patient_uid == patient_uid)
    ).scalar() or 0

    if doc_count == 0:
        try:
            reindex_patient(db, patient_uid, m=m)
        except Exception as e:
            logger.warning({"event": "ehr_auto_reindex_error", "error": str(e)})

    # Datos del paciente desde consultas legacy
    ultima = db.query(m.ConsultaDB).filter(
        m.ConsultaDB.nss == patient_uid
    ).order_by(m.ConsultaDB.id.desc()).first()

    if not ultima:
        raise HTTPException(status_code=404, detail="Paciente no encontrado")

    # Features
    features = db.execute(
        select(EHR_FEATURES_DAILY)
        .where(EHR_FEATURES_DAILY.c.patient_uid == patient_uid)
        .order_by(EHR_FEATURES_DAILY.c.feature_date.desc())
        .limit(1)
    ).mappings().first()

    # Problem list
    problems = db.execute(
        select(EHR_PROBLEM_LIST)
        .where(EHR_PROBLEM_LIST.c.patient_uid == patient_uid)
        .order_by(EHR_PROBLEM_LIST.c.status, EHR_PROBLEM_LIST.c.problem_name)
    ).mappings().all()

    # Alertas activas
    alertas = db.execute(
        select(EHR_ALERT_LIFECYCLE)
        .where(
            EHR_ALERT_LIFECYCLE.c.patient_uid == patient_uid,
            EHR_ALERT_LIFECYCLE.c.status.in_(["generated", "acknowledged"]),
        )
        .order_by(EHR_ALERT_LIFECYCLE.c.severity.desc(), EHR_ALERT_LIFECYCLE.c.created_at.desc())
        .limit(20)
    ).mappings().all()

    # Antecedentes
    def _parse_json(val):
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                p = json.loads(val)
                return p if isinstance(p, list) else []
            except Exception:
                return []
        return []

    antecedentes = {
        "patologicos": _parse_json(getattr(ultima, "app_patologias_json", None)),
        "quirurgicos": _parse_json(getattr(ultima, "aqx_json", None)),
        "alergias": _parse_json(getattr(ultima, "alergias_json", None)),
        "heredofamiliares": _parse_json(getattr(ultima, "ahf_json", None)),
        "toxicomanias": _parse_json(getattr(ultima, "toxicomanias_json", None)),
    }

    # Último somatometría
    vitals = {
        "peso": getattr(ultima, "peso", None),
        "talla": getattr(ultima, "talla", None),
        "imc": getattr(ultima, "imc", None),
        "ta": str(getattr(ultima, "ta", "") or ""),
        "fc": getattr(ultima, "fc", None),
        "temp": getattr(ultima, "temp", None),
    }

    result = {
        "ok": True,
        "patient": {
            "nss": str(getattr(ultima, "nss", "") or ""),
            "curp": str(getattr(ultima, "curp", "") or ""),
            "nombre": str(getattr(ultima, "nombre", "") or ""),
            "edad": getattr(ultima, "edad", None),
            "sexo": str(getattr(ultima, "sexo", "") or ""),
            "tipo_sangre": str(getattr(ultima, "tipo_sangre", "") or ""),
            "diagnostico_principal": str(getattr(ultima, "diagnostico_principal", "") or ""),
        },
        "vitals": vitals,
        "antecedentes": antecedentes,
        "features": _serialize_row(dict(features)) if features else {},
        "completeness_score": float(features["completeness_score"]) if features and features.get("completeness_score") else 0.0,
        "problems": [_serialize_row(dict(p)) for p in problems],
        "alertas": [_serialize_row(dict(a)) for a in alertas],
        "total_docs": db.execute(
            select(func.count(EHR_DOCUMENTS.c.id))
            .where(EHR_DOCUMENTS.c.patient_uid == patient_uid)
        ).scalar() or 0,
    }

    return JSONResponse(content=result, media_type="application/json")


# ═══════════════════════════════════════════════════════════════════════
#  API — Timeline
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/timeline", response_class=JSONResponse)
async def ehr_timeline(
    request: Request,
    patient_uid: str = Query(...),
    event_type: Optional[str] = Query(None),
    service: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    db: Session = Depends(_get_db),
):
    """Timeline universal con filtros inteligentes."""
    ensure_ehr_schema(db)

    q = select(EHR_TIMELINE_EVENTS).where(
        EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid
    )

    if event_type:
        q = q.where(EHR_TIMELINE_EVENTS.c.event_type == event_type)
    if service:
        q = q.where(EHR_TIMELINE_EVENTS.c.service == service.upper())
    if severity:
        q = q.where(EHR_TIMELINE_EVENTS.c.severity == severity)
    if date_from:
        try:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            q = q.where(EHR_TIMELINE_EVENTS.c.event_ts >= df)
        except Exception:
            pass
    if date_to:
        try:
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            q = q.where(EHR_TIMELINE_EVENTS.c.event_ts <= dt)
        except Exception:
            pass

    total = db.execute(
        select(func.count()).select_from(q.subquery())
    ).scalar() or 0

    rows = db.execute(
        q.order_by(desc(EHR_TIMELINE_EVENTS.c.event_ts))
        .limit(limit).offset(offset)
    ).mappings().all()

    events = []
    for r in rows:
        evt = dict(r)
        # Serializar datetime
        for k in ("event_ts", "created_at"):
            if evt.get(k) and hasattr(evt[k], "isoformat"):
                evt[k] = evt[k].isoformat()
        events.append(evt)

    return JSONResponse(content={
        "ok": True,
        "total": total,
        "events": events,
    })


# ═══════════════════════════════════════════════════════════════════════
#  API — Documentos
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/documentos", response_class=JSONResponse)
async def ehr_documentos(
    request: Request,
    patient_uid: str = Query(...),
    doc_type: Optional[str] = Query(None),
    service: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    db: Session = Depends(_get_db),
):
    """Lista documentos con filtros y búsqueda textual."""
    ensure_ehr_schema(db)

    q = select(
        EHR_DOCUMENTS.c.id,
        EHR_DOCUMENTS.c.doc_type,
        EHR_DOCUMENTS.c.title,
        EHR_DOCUMENTS.c.service,
        EHR_DOCUMENTS.c.author,
        EHR_DOCUMENTS.c.version,
        EHR_DOCUMENTS.c.created_at,
        EHR_DOCUMENTS.c.source_table,
        EHR_DOCUMENTS.c.source_id,
    ).where(EHR_DOCUMENTS.c.patient_uid == patient_uid)

    if doc_type:
        q = q.where(EHR_DOCUMENTS.c.doc_type == doc_type)
    if service:
        q = q.where(EHR_DOCUMENTS.c.service == service.upper())
    if search:
        q = q.where(EHR_DOCUMENTS.c.content_text.contains(search.upper()))

    rows = db.execute(
        q.order_by(desc(EHR_DOCUMENTS.c.created_at)).limit(limit)
    ).mappings().all()

    docs = []
    for r in rows:
        d = dict(r)
        if d.get("created_at") and hasattr(d["created_at"], "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        docs.append(d)

    return JSONResponse(content={"ok": True, "docs": docs})


@router.get("/api/v1/ehr/documento/{doc_id}", response_class=JSONResponse)
async def ehr_documento_detalle(
    doc_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    """Detalle completo de un documento incluyendo tags."""
    ensure_ehr_schema(db)

    doc = db.execute(
        select(EHR_DOCUMENTS).where(EHR_DOCUMENTS.c.id == doc_id)
    ).mappings().first()

    if not doc:
        raise HTTPException(status_code=404, detail="Documento no encontrado")

    doc_dict = dict(doc)
    for k in ("created_at", "updated_at", "signature_ts"):
        if doc_dict.get(k) and hasattr(doc_dict[k], "isoformat"):
            doc_dict[k] = doc_dict[k].isoformat()

    # Parse content_json
    if doc_dict.get("content_json"):
        try:
            doc_dict["content_data"] = json.loads(doc_dict["content_json"]) if isinstance(doc_dict["content_json"], str) else doc_dict["content_json"]
        except Exception:
            doc_dict["content_data"] = {}

    # Tags
    tags = db.execute(
        select(EHR_TAGS.c.tag_name, EHR_TAGS.c.tag_category, EHR_DOCUMENT_TAGS.c.confidence)
        .select_from(EHR_DOCUMENT_TAGS.join(EHR_TAGS, EHR_TAGS.c.id == EHR_DOCUMENT_TAGS.c.tag_id))
        .where(EHR_DOCUMENT_TAGS.c.document_id == doc_id)
    ).mappings().all()
    doc_dict["tags"] = [dict(t) for t in tags]

    return JSONResponse(content={"ok": True, "doc": doc_dict})


# ═══════════════════════════════════════════════════════════════════════
#  API — Alertas (lifecycle)
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/alertas", response_class=JSONResponse)
async def ehr_alertas(
    request: Request,
    patient_uid: str = Query(...),
    status: Optional[str] = Query(None),
    db: Session = Depends(_get_db),
):
    """Obtiene alertas con su ciclo de vida."""
    ensure_ehr_schema(db)

    q = select(EHR_ALERT_LIFECYCLE).where(
        EHR_ALERT_LIFECYCLE.c.patient_uid == patient_uid
    )
    if status:
        q = q.where(EHR_ALERT_LIFECYCLE.c.status == status)

    rows = db.execute(
        q.order_by(
            desc(EHR_ALERT_LIFECYCLE.c.created_at)
        ).limit(50)
    ).mappings().all()

    alertas = []
    for r in rows:
        a = dict(r)
        for k in ("created_at", "updated_at", "acknowledged_at", "resolved_at"):
            if a.get(k) and hasattr(a[k], "isoformat"):
                a[k] = a[k].isoformat()
        alertas.append(a)

    return JSONResponse(content={"ok": True, "alertas": alertas})


@router.post("/api/v1/ehr/alertas/{alert_id}/acknowledge", response_class=JSONResponse)
async def ehr_alerta_acknowledge(
    alert_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    """Marca una alerta como reconocida."""
    ensure_ehr_schema(db)

    row = db.execute(
        select(EHR_ALERT_LIFECYCLE.c.id).where(EHR_ALERT_LIFECYCLE.c.id == alert_id)
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Alerta no encontrada")

    db.execute(
        EHR_ALERT_LIFECYCLE.update()
        .where(EHR_ALERT_LIFECYCLE.c.id == alert_id)
        .values(status="acknowledged", acknowledged_at=datetime.now(), acknowledged_by="user")
    )
    db.commit()
    return JSONResponse(content={"ok": True})


@router.post("/api/v1/ehr/alertas/{alert_id}/resolve", response_class=JSONResponse)
async def ehr_alerta_resolve(
    alert_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    """Resuelve una alerta con acción y outcome."""
    ensure_ehr_schema(db)

    try:
        body = await request.json()
    except Exception:
        body = {}

    db.execute(
        EHR_ALERT_LIFECYCLE.update()
        .where(EHR_ALERT_LIFECYCLE.c.id == alert_id)
        .values(
            status="resolved",
            resolved_at=datetime.now(),
            resolved_by="user",
            action_taken=body.get("action_taken", ""),
            outcome=body.get("outcome", ""),
            outcome_notes=body.get("outcome_notes", ""),
        )
    )
    db.commit()
    return JSONResponse(content={"ok": True})


# ═══════════════════════════════════════════════════════════════════════
#  API — Problem List
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/problemas", response_class=JSONResponse)
async def ehr_problemas(
    request: Request,
    patient_uid: str = Query(...),
    db: Session = Depends(_get_db),
):
    """Lista de problemas del paciente."""
    ensure_ehr_schema(db)

    rows = db.execute(
        select(EHR_PROBLEM_LIST)
        .where(EHR_PROBLEM_LIST.c.patient_uid == patient_uid)
        .order_by(EHR_PROBLEM_LIST.c.status, EHR_PROBLEM_LIST.c.problem_name)
    ).mappings().all()

    problems = []
    for r in rows:
        p = dict(r)
        for k in ("onset_date", "resolution_date", "created_at", "updated_at"):
            if p.get(k) and hasattr(p[k], "isoformat"):
                p[k] = p[k].isoformat()
        problems.append(p)

    return JSONResponse(content={"ok": True, "problems": problems})


# ═══════════════════════════════════════════════════════════════════════
#  API — Búsqueda universal
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/buscar", response_class=JSONResponse)
async def ehr_buscar(
    request: Request,
    q: str = Query(..., min_length=2),
    patient_uid: Optional[str] = Query(None),
    db: Session = Depends(_get_db),
):
    """Búsqueda exact en documentos EHR y pacientes."""
    ensure_ehr_schema(db)
    m = _get_m()

    results: Dict[str, Any] = {"pacientes": [], "documentos": []}
    term = q.strip().upper()

    # Buscar pacientes por NSS o nombre
    pacientes = db.query(m.ConsultaDB).filter(
        or_(
            m.ConsultaDB.nss.contains(term),
            func.upper(m.ConsultaDB.nombre).contains(term),
        )
    ).limit(10).all()

    seen_nss = set()
    for p in pacientes:
        nss = str(getattr(p, "nss", "") or "")
        if nss in seen_nss:
            continue
        seen_nss.add(nss)
        results["pacientes"].append({
            "nss": nss,
            "nombre": str(getattr(p, "nombre", "") or ""),
            "diagnostico": str(getattr(p, "diagnostico_principal", "") or ""),
        })

    # Buscar en documentos
    doc_q = select(
        EHR_DOCUMENTS.c.id,
        EHR_DOCUMENTS.c.title,
        EHR_DOCUMENTS.c.doc_type,
        EHR_DOCUMENTS.c.service,
        EHR_DOCUMENTS.c.patient_uid,
        EHR_DOCUMENTS.c.created_at,
    ).where(EHR_DOCUMENTS.c.content_text.contains(term))

    if patient_uid:
        doc_q = doc_q.where(EHR_DOCUMENTS.c.patient_uid == patient_uid)

    doc_rows = db.execute(doc_q.limit(20)).mappings().all()
    for d in doc_rows:
        dd = dict(d)
        if dd.get("created_at") and hasattr(dd["created_at"], "isoformat"):
            dd["created_at"] = dd["created_at"].isoformat()
        results["documentos"].append(dd)

    return JSONResponse(content={"ok": True, "results": results})


# ═══════════════════════════════════════════════════════════════════════
#  API — Reindex
# ═══════════════════════════════════════════════════════════════════════

@router.post("/api/v1/ehr/reindex", response_class=JSONResponse)
async def ehr_reindex_endpoint(
    request: Request,
    patient_uid: Optional[str] = Query(None),
    last_days: Optional[int] = Query(None),
    db: Session = Depends(_get_db),
):
    """Reindexa un paciente o un rango de días."""
    m = _get_m()

    if patient_uid:
        stats = reindex_patient(db, patient_uid, m=m)
        return JSONResponse(content={"ok": True, "mode": "patient", "stats": stats})

    if last_days:
        stats = reindex_last_days(db, last_days, m=m)
        return JSONResponse(content={"ok": True, "mode": "batch", "stats": stats})

    raise HTTPException(status_code=400, detail="Especifique patient_uid o last_days")


# ═══════════════════════════════════════════════════════════════════════
#  API — Tags
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/v1/ehr/tags", response_class=JSONResponse)
async def ehr_tags_list(
    request: Request,
    patient_uid: str = Query(...),
    db: Session = Depends(_get_db),
):
    """Lista tags asociados a documentos del paciente."""
    ensure_ehr_schema(db)

    rows = db.execute(
        select(
            EHR_TAGS.c.id,
            EHR_TAGS.c.tag_name,
            EHR_TAGS.c.tag_category,
            EHR_TAGS.c.cie10_code,
            func.count(EHR_DOCUMENT_TAGS.c.id).label("doc_count"),
        )
        .select_from(
            EHR_DOCUMENT_TAGS
            .join(EHR_TAGS, EHR_TAGS.c.id == EHR_DOCUMENT_TAGS.c.tag_id)
            .join(EHR_DOCUMENTS, EHR_DOCUMENTS.c.id == EHR_DOCUMENT_TAGS.c.document_id)
        )
        .where(EHR_DOCUMENTS.c.patient_uid == patient_uid)
        .group_by(EHR_TAGS.c.id, EHR_TAGS.c.tag_name, EHR_TAGS.c.tag_category, EHR_TAGS.c.cie10_code)
        .order_by(desc("doc_count"))
    ).mappings().all()

    return JSONResponse(content={"ok": True, "tags": [dict(r) for r in rows]})
