"""
API de Notificaciones — Item #9.

Sistema de notificaciones para interconsultas, alertas clínicas,
y eventos del sistema. Incluye polling endpoint y UI.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, Boolean, desc, select, insert, update, func, and_
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

router = APIRouter(tags=["notificaciones"])

# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------
NOTIF_METADATA = MetaData()

NOTIFICACIONES = Table(
    "notificaciones",
    NOTIF_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("destinatario_username", String(80), nullable=True, index=True),
    Column("destinatario_rol", String(40), nullable=True, index=True),
    Column("destinatario_servicio", String(120), nullable=True, index=True),
    # Contenido
    Column("titulo", String(300), nullable=False),
    Column("mensaje", Text, nullable=True),
    Column("tipo", String(40), nullable=False, default="INFO", index=True),
    # INTERCONSULTA, ALERTA_CLINICA, REFERENCIA, FIRMA, SISTEMA, INFO, URGENTE
    Column("severidad", String(20), nullable=False, default="NORMAL"),
    # BAJA, NORMAL, ALTA, CRITICA
    Column("categoria", String(40), nullable=True),
    # Origen
    Column("origen_modulo", String(80), nullable=True),
    Column("origen_id", Integer, nullable=True),
    Column("nss_paciente", String(10), nullable=True, index=True),
    Column("url_accion", String(500), nullable=True),  # Link a donde ir
    # Estado
    Column("leida", Boolean, nullable=False, default=False, index=True),
    Column("fecha_lectura", DateTime, nullable=True),
    Column("archivada", Boolean, nullable=False, default=False),
    # Timestamps
    Column("creado_en", DateTime, nullable=False, default=utcnow),
)


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        NOTIF_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Crear notificación (interno, llamado desde otros módulos)
# ---------------------------------------------------------------------------
def crear_notificacion(db: Session, *, titulo: str, mensaje: str = "",
                       tipo: str = "INFO", severidad: str = "NORMAL",
                       destinatario_username: str = None,
                       destinatario_rol: str = None,
                       destinatario_servicio: str = None,
                       origen_modulo: str = "", origen_id: int = None,
                       nss_paciente: str = "", url_accion: str = ""):
    """Helper para crear notificación desde cualquier módulo."""
    _ensure_tables(db)
    now = utcnow()
    stmt = insert(NOTIFICACIONES).values(
        destinatario_username=destinatario_username,
        destinatario_rol=destinatario_rol,
        destinatario_servicio=destinatario_servicio,
        titulo=titulo[:300],
        mensaje=mensaje,
        tipo=tipo,
        severidad=severidad,
        origen_modulo=origen_modulo,
        origen_id=origen_id,
        nss_paciente=nss_paciente[:10] if nss_paciente else None,
        url_accion=url_accion[:500] if url_accion else None,
        leida=False,
        archivada=False,
        creado_en=now,
    )
    db.execute(stmt)
    db.commit()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.post("/api/notificaciones", response_class=JSONResponse)
async def create_notification(request: Request, db: Session = Depends(_get_db)):
    """Crear una notificación manualmente."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    titulo = str(body.get("titulo", "")).strip()
    if not titulo:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Título requerido"})

    crear_notificacion(
        db,
        titulo=titulo,
        mensaje=body.get("mensaje", ""),
        tipo=body.get("tipo", "INFO"),
        severidad=body.get("severidad", "NORMAL"),
        destinatario_username=body.get("destinatario_username"),
        destinatario_rol=body.get("destinatario_rol"),
        destinatario_servicio=body.get("destinatario_servicio"),
        origen_modulo=body.get("origen_modulo", ""),
        origen_id=body.get("origen_id"),
        nss_paciente=body.get("nss_paciente", ""),
        url_accion=body.get("url_accion", ""),
    )

    return JSONResponse(content={"ok": True, "message": "Notificación creada"})


@router.get("/api/notificaciones", response_class=JSONResponse)
async def get_notifications(
    username: Optional[str] = None,
    rol: Optional[str] = None,
    servicio: Optional[str] = None,
    solo_no_leidas: bool = True,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(_get_db),
):
    """Obtener notificaciones (con filtros). Endpoint de polling."""
    _ensure_tables(db)

    q = select(NOTIFICACIONES).where(NOTIFICACIONES.c.archivada == False)

    if solo_no_leidas:
        q = q.where(NOTIFICACIONES.c.leida == False)

    # Filtrar por destinatario (OR: username, rol, servicio, o para todos)
    if username or rol or servicio:
        conditions = []
        if username:
            conditions.append(NOTIFICACIONES.c.destinatario_username == username)
        if rol:
            conditions.append(NOTIFICACIONES.c.destinatario_rol == rol)
        if servicio:
            conditions.append(NOTIFICACIONES.c.destinatario_servicio == servicio.upper())
        # También incluir notificaciones sin destinatario específico (broadcast)
        conditions.append(and_(
            NOTIFICACIONES.c.destinatario_username.is_(None),
            NOTIFICACIONES.c.destinatario_rol.is_(None),
            NOTIFICACIONES.c.destinatario_servicio.is_(None),
        ))
        from sqlalchemy import or_
        q = q.where(or_(*conditions))

    q = q.order_by(desc(NOTIFICACIONES.c.id)).limit(limit)
    rows = db.execute(q).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id,
            "titulo": r.titulo,
            "mensaje": (r.mensaje or "")[:300],
            "tipo": r.tipo,
            "severidad": r.severidad,
            "origen": r.origen_modulo,
            "nss": r.nss_paciente,
            "url_accion": r.url_accion,
            "leida": r.leida,
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })

    # Count total unread
    count_q = select(func.count()).select_from(NOTIFICACIONES).where(
        and_(NOTIFICACIONES.c.leida == False, NOTIFICACIONES.c.archivada == False)
    )
    total_unread = db.execute(count_q).scalar() or 0

    return JSONResponse(content={
        "ok": True,
        "total_unread": total_unread,
        "items": items,
    })


@router.post("/api/notificaciones/{notif_id}/read", response_class=JSONResponse)
async def mark_as_read(notif_id: int, db: Session = Depends(_get_db)):
    """Marcar notificación como leída."""
    _ensure_tables(db)
    db.execute(
        update(NOTIFICACIONES)
        .where(NOTIFICACIONES.c.id == notif_id)
        .values(leida=True, fecha_lectura=utcnow())
    )
    db.commit()
    return JSONResponse(content={"ok": True})


@router.post("/api/notificaciones/read-all", response_class=JSONResponse)
async def mark_all_as_read(request: Request, db: Session = Depends(_get_db)):
    """Marcar todas las notificaciones como leídas."""
    _ensure_tables(db)
    db.execute(
        update(NOTIFICACIONES)
        .where(NOTIFICACIONES.c.leida == False)
        .values(leida=True, fecha_lectura=utcnow())
    )
    db.commit()
    return JSONResponse(content={"ok": True})
