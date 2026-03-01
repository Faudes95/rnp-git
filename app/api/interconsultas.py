"""
API de Interconsultas y Referencias — FASE 4.

ADITIVO: No modifica rutas existentes.
Implementa el flujo de interconsultas (solicitar, responder, cerrar)
y referencias/contrarreferencias IMSS.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, Boolean, desc, func, insert, select, update, and_
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

router = APIRouter(tags=["interconsultas"])


# ---------------------------------------------------------------------------
# Vista HTML
# ---------------------------------------------------------------------------
@router.get("/interconsultas", response_class=HTMLResponse)
async def interconsultas_view(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template("interconsultas.html", request=request)


# ---------------------------------------------------------------------------
# Modelos (tablas nuevas)
# ---------------------------------------------------------------------------
INTERCON_METADATA = MetaData()

INTERCONSULTAS = Table(
    "interconsultas",
    INTERCON_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    # Solicitante
    Column("servicio_solicitante", String(120), nullable=False, default="UROLOGIA"),
    Column("medico_solicitante", String(200), nullable=True),
    Column("matricula_solicitante", String(40), nullable=True),
    # Destino
    Column("servicio_destino", String(120), nullable=False, index=True),
    Column("medico_destino", String(200), nullable=True),
    # Contenido
    Column("motivo", Text, nullable=False),
    Column("diagnostico_presuntivo", String(300), nullable=True),
    Column("urgencia", String(20), nullable=False, default="NORMAL", index=True),  # URGENTE, NORMAL
    Column("antecedentes_relevantes", Text, nullable=True),
    Column("estudios_adjuntos", Text, nullable=True),
    Column("pregunta_clinica", Text, nullable=True),
    # Respuesta
    Column("respuesta_texto", Text, nullable=True),
    Column("diagnostico_interconsultante", String(300), nullable=True),
    Column("plan_sugerido", Text, nullable=True),
    Column("medico_respondio", String(200), nullable=True),
    Column("fecha_respuesta", DateTime, nullable=True),
    # Estado
    Column("estatus", String(30), nullable=False, default="SOLICITADA", index=True),
    # SOLICITADA, EN_REVISION, RESPONDIDA, CERRADA, CANCELADA
    Column("prioridad_score", Integer, nullable=True, default=0),
    Column("notas_seguimiento", Text, nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
)

REFERENCIAS = Table(
    "referencias_contrarreferencias",
    INTERCON_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("tipo", String(20), nullable=False, index=True),  # REFERENCIA, CONTRARREFERENCIA
    # Origen
    Column("unidad_origen", String(200), nullable=False),
    Column("servicio_origen", String(120), nullable=False),
    Column("medico_origen", String(200), nullable=True),
    # Destino
    Column("unidad_destino", String(200), nullable=False),
    Column("servicio_destino", String(120), nullable=True),
    # Contenido
    Column("diagnostico_cie10", String(20), nullable=True, index=True),
    Column("diagnostico_texto", String(300), nullable=True),
    Column("motivo_referencia", Text, nullable=False),
    Column("resumen_clinico", Text, nullable=True),
    Column("tratamiento_actual", Text, nullable=True),
    Column("estudios_realizados", Text, nullable=True),
    # Estado
    Column("estatus", String(30), nullable=False, default="ENVIADA", index=True),
    # ENVIADA, RECIBIDA, ACEPTADA, RECHAZADA, COMPLETADA
    Column("fecha_aceptacion", DateTime, nullable=True),
    Column("notas", Text, nullable=True),
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
)


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        INTERCON_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# INTERCONSULTAS
# ---------------------------------------------------------------------------
@router.post("/api/interconsultas", response_class=JSONResponse)
async def create_interconsulta(request: Request, db: Session = Depends(_get_db)):
    """Solicitar una interconsulta."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    nss = str(body.get("nss", "")).strip()[:10]
    if not nss:
        return JSONResponse(status_code=400, content={"ok": False, "error": "NSS requerido"})
    if not body.get("servicio_destino"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Servicio destino requerido"})
    if not body.get("motivo"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Motivo requerido"})

    now = utcnow()
    stmt = insert(INTERCONSULTAS).values(
        patient_uid=f"nss:{nss}",
        nss=nss,
        consulta_id=body.get("consulta_id"),
        hospitalizacion_id=body.get("hospitalizacion_id"),
        servicio_solicitante=body.get("servicio_solicitante", "UROLOGIA"),
        medico_solicitante=body.get("medico_solicitante", ""),
        matricula_solicitante=body.get("matricula_solicitante", ""),
        servicio_destino=str(body["servicio_destino"]).upper().strip(),
        medico_destino=body.get("medico_destino", ""),
        motivo=body["motivo"],
        diagnostico_presuntivo=body.get("diagnostico_presuntivo", ""),
        urgencia=str(body.get("urgencia", "NORMAL")).upper().strip(),
        antecedentes_relevantes=body.get("antecedentes_relevantes", ""),
        estudios_adjuntos=body.get("estudios_adjuntos", ""),
        pregunta_clinica=body.get("pregunta_clinica", ""),
        estatus="SOLICITADA",
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    ic_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    # Auto-create notification for the destination service
    try:
        from app.api.notificaciones import crear_notificacion
        urgencia = str(body.get("urgencia", "NORMAL")).upper()
        severidad = "CRITICA" if urgencia == "URGENTE" else "NORMAL"
        crear_notificacion(
            db,
            titulo=f"Nueva interconsulta #{ic_id} — {str(body.get('servicio_destino','')).upper()}",
            mensaje=f"NSS: {nss} — {body.get('motivo','')[:200]}",
            tipo="INTERCONSULTA",
            severidad=severidad,
            destinatario_servicio=str(body.get("servicio_destino", "")).upper(),
            origen_modulo="INTERCONSULTAS",
            origen_id=ic_id,
            nss_paciente=nss,
            url_accion="/interconsultas",
        )
    except Exception:
        pass  # Don't break IC creation if notification fails

    return JSONResponse(content={"ok": True, "interconsulta_id": ic_id, "estatus": "SOLICITADA"})


@router.post("/api/interconsultas/{ic_id}/respond", response_class=JSONResponse)
async def respond_interconsulta(ic_id: int, request: Request, db: Session = Depends(_get_db)):
    """Responder una interconsulta."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        body = {}

    now = utcnow()
    stmt = (
        update(INTERCONSULTAS)
        .where(INTERCONSULTAS.c.id == ic_id)
        .values(
            respuesta_texto=body.get("respuesta_texto", ""),
            diagnostico_interconsultante=body.get("diagnostico_interconsultante", ""),
            plan_sugerido=body.get("plan_sugerido", ""),
            medico_respondio=body.get("medico_respondio", ""),
            fecha_respuesta=now,
            estatus="RESPONDIDA",
            actualizado_en=now,
        )
    )
    db.execute(stmt)
    db.commit()
    return JSONResponse(content={"ok": True, "interconsulta_id": ic_id, "estatus": "RESPONDIDA"})


@router.get("/api/interconsultas/patient/{nss}", response_class=JSONResponse)
async def get_patient_interconsultas(nss: str, db: Session = Depends(_get_db)):
    """Listar interconsultas de un paciente."""
    _ensure_tables(db)
    rows = db.execute(
        select(INTERCONSULTAS).where(INTERCONSULTAS.c.nss == nss[:10]).order_by(desc(INTERCONSULTAS.c.id))
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "servicio_destino": r.servicio_destino,
            "motivo": r.motivo[:200] if r.motivo else "",
            "urgencia": r.urgencia, "estatus": r.estatus,
            "medico_solicitante": r.medico_solicitante,
            "respuesta": (r.respuesta_texto or "")[:200],
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "interconsultas": items})


@router.get("/api/interconsultas/pending", response_class=JSONResponse)
async def get_pending_interconsultas(
    servicio: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    """Listar interconsultas pendientes (para el servicio que responde)."""
    _ensure_tables(db)
    q = select(INTERCONSULTAS).where(INTERCONSULTAS.c.estatus.in_(["SOLICITADA", "EN_REVISION"]))
    if servicio:
        q = q.where(INTERCONSULTAS.c.servicio_destino == servicio.upper().strip())
    q = q.order_by(
        INTERCONSULTAS.c.urgencia.desc(),  # URGENTE primero
        INTERCONSULTAS.c.id.asc(),
    )
    rows = db.execute(q).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "nss": r.nss,
            "servicio_solicitante": r.servicio_solicitante,
            "servicio_destino": r.servicio_destino,
            "motivo": r.motivo[:200] if r.motivo else "",
            "urgencia": r.urgencia,
            "estatus": r.estatus,
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "interconsultas": items})


# ---------------------------------------------------------------------------
# REFERENCIAS / CONTRARREFERENCIAS
# ---------------------------------------------------------------------------
@router.post("/api/referencias", response_class=JSONResponse)
async def create_referencia(request: Request, db: Session = Depends(_get_db)):
    """Crear una referencia o contrarreferencia."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    nss = str(body.get("nss", "")).strip()[:10]
    if not nss:
        return JSONResponse(status_code=400, content={"ok": False, "error": "NSS requerido"})

    tipo = str(body.get("tipo", "REFERENCIA")).upper().strip()
    if tipo not in ("REFERENCIA", "CONTRARREFERENCIA"):
        tipo = "REFERENCIA"

    now = utcnow()
    stmt = insert(REFERENCIAS).values(
        patient_uid=f"nss:{nss}",
        nss=nss,
        consulta_id=body.get("consulta_id"),
        tipo=tipo,
        unidad_origen=body.get("unidad_origen", "CMN RAZA - UROLOGIA"),
        servicio_origen=body.get("servicio_origen", "UROLOGIA"),
        medico_origen=body.get("medico_origen", ""),
        unidad_destino=body.get("unidad_destino", ""),
        servicio_destino=body.get("servicio_destino", ""),
        diagnostico_cie10=body.get("diagnostico_cie10", ""),
        diagnostico_texto=body.get("diagnostico_texto", ""),
        motivo_referencia=body.get("motivo_referencia", ""),
        resumen_clinico=body.get("resumen_clinico", ""),
        tratamiento_actual=body.get("tratamiento_actual", ""),
        estudios_realizados=body.get("estudios_realizados", ""),
        estatus="ENVIADA",
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    ref_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
    return JSONResponse(content={"ok": True, "referencia_id": ref_id, "tipo": tipo, "estatus": "ENVIADA"})


@router.get("/api/referencias/patient/{nss}", response_class=JSONResponse)
async def get_patient_referencias(nss: str, db: Session = Depends(_get_db)):
    """Listar referencias/contrarreferencias de un paciente."""
    _ensure_tables(db)
    rows = db.execute(
        select(REFERENCIAS).where(REFERENCIAS.c.nss == nss[:10]).order_by(desc(REFERENCIAS.c.id))
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "tipo": r.tipo,
            "unidad_origen": r.unidad_origen,
            "unidad_destino": r.unidad_destino,
            "diagnostico": r.diagnostico_texto,
            "motivo": (r.motivo_referencia or "")[:200],
            "estatus": r.estatus,
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "referencias": items})
