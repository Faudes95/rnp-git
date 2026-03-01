"""
API de Gobernanza Clínica — FASE 2.

ADITIVO: No modifica rutas existentes.
Endpoints para RBAC, Auditoría, Consentimiento y Alertas Clínicas.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import and_, desc, func, insert, select, update
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.core.rbac import require_role, require_module
from app.models.governance_models import (
    GOV_METADATA, GOV_USERS, GOV_SESSIONS, GOV_ACCESS_LOG,
    GOV_CONSENT_FORMS, GOV_CLINICAL_ALERTS,
)

router = APIRouter(tags=["governance"])


# ---------------------------------------------------------------------------
# Vista HTML
# ---------------------------------------------------------------------------
@router.get("/gobernanza", response_class=HTMLResponse)
async def gobernanza_view(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template("gobernanza.html", request=request)


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------
def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_gov_tables(db: Session):
    """Crea tablas de gobernanza si no existen (idempotente)."""
    try:
        GOV_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 1. AUDITORÍA — Log de acceso automático
# ---------------------------------------------------------------------------
@router.get("/api/governance/audit-log", response_class=JSONResponse,
             dependencies=[Depends(require_module("governance_read"))])
async def get_audit_log(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    username: Optional[str] = None,
    modulo: Optional[str] = None,
    nss: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    """Consulta el log de auditoría con filtros."""
    _ensure_gov_tables(db)
    q = select(GOV_ACCESS_LOG).order_by(desc(GOV_ACCESS_LOG.c.id))

    if username:
        q = q.where(GOV_ACCESS_LOG.c.username == username)
    if modulo:
        q = q.where(GOV_ACCESS_LOG.c.modulo == modulo)
    if nss:
        q = q.where(GOV_ACCESS_LOG.c.nss == nss)
    if fecha_desde:
        try:
            fd = datetime.strptime(fecha_desde[:10], "%Y-%m-%d")
            q = q.where(GOV_ACCESS_LOG.c.timestamp >= fd)
        except Exception:
            pass
    if fecha_hasta:
        try:
            fh = datetime.strptime(fecha_hasta[:10], "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            q = q.where(GOV_ACCESS_LOG.c.timestamp <= fh)
        except Exception:
            pass

    total = db.execute(select(func.count()).select_from(q.subquery())).scalar() or 0
    rows = db.execute(q.offset(offset).limit(limit)).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "username": r.username,
            "rol": r.rol,
            "method": r.method,
            "path": r.path,
            "status_code": r.status_code,
            "operacion": r.operacion,
            "tabla_afectada": r.tabla_afectada,
            "patient_uid": r.patient_uid,
            "nss": r.nss,
            "modulo": r.modulo,
            "duracion_ms": r.duracion_ms,
        })

    return JSONResponse(content={"ok": True, "total": total, "items": items})


# ---------------------------------------------------------------------------
# 2. CONSENTIMIENTO INFORMADO
# ---------------------------------------------------------------------------
@router.post("/api/governance/consent", response_class=JSONResponse)
async def create_consent(request: Request, db: Session = Depends(_get_db)):
    """Crear un nuevo consentimiento informado."""
    _ensure_gov_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    required = ["nss", "tipo_consentimiento"]
    for field in required:
        if not body.get(field):
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Campo requerido: {field}"})

    now = utcnow()
    nss = str(body["nss"]).strip()[:10]
    patient_uid = f"nss:{nss}"

    stmt = insert(GOV_CONSENT_FORMS).values(
        patient_uid=patient_uid,
        nss=nss,
        consulta_id=body.get("consulta_id"),
        hospitalizacion_id=body.get("hospitalizacion_id"),
        tipo_consentimiento=str(body["tipo_consentimiento"]).upper().strip(),
        procedimiento_descripcion=body.get("procedimiento_descripcion", ""),
        riesgos_descripcion=body.get("riesgos_descripcion", ""),
        alternativas_descripcion=body.get("alternativas_descripcion", ""),
        medico_responsable=body.get("medico_responsable", ""),
        matricula_medico=body.get("matricula_medico", ""),
        paciente_nombre=body.get("paciente_nombre", ""),
        paciente_o_responsable=body.get("paciente_o_responsable", ""),
        parentesco_responsable=body.get("parentesco_responsable", ""),
        estatus="PENDIENTE",
        notas=body.get("notas", ""),
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    consent_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    return JSONResponse(content={"ok": True, "consent_id": consent_id, "estatus": "PENDIENTE"})


@router.post("/api/governance/consent/{consent_id}/sign", response_class=JSONResponse)
async def sign_consent(consent_id: int, request: Request, db: Session = Depends(_get_db)):
    """Firmar un consentimiento informado."""
    _ensure_gov_tables(db)
    try:
        body = await request.json()
    except Exception:
        body = {}

    now = utcnow()
    firma_paciente = str(body.get("firma_paciente", "ACEPTADO")).strip()
    firma_medico = str(body.get("firma_medico", "FIRMADO")).strip()

    stmt = (
        update(GOV_CONSENT_FORMS)
        .where(GOV_CONSENT_FORMS.c.id == consent_id)
        .values(
            estatus="FIRMADO",
            firma_paciente_hash=hashlib.sha256(firma_paciente.encode()).hexdigest()[:64],
            firma_medico_hash=hashlib.sha256(firma_medico.encode()).hexdigest()[:64],
            firma_testigo1_hash=hashlib.sha256(str(body.get("firma_testigo1", "")).encode()).hexdigest()[:64] if body.get("firma_testigo1") else None,
            firma_testigo2_hash=hashlib.sha256(str(body.get("firma_testigo2", "")).encode()).hexdigest()[:64] if body.get("firma_testigo2") else None,
            fecha_firma=now,
            actualizado_en=now,
        )
    )
    db.execute(stmt)
    db.commit()
    return JSONResponse(content={"ok": True, "consent_id": consent_id, "estatus": "FIRMADO"})


@router.get("/api/governance/consent/patient/{nss}", response_class=JSONResponse)
async def get_patient_consents(nss: str, db: Session = Depends(_get_db)):
    """Listar consentimientos de un paciente."""
    _ensure_gov_tables(db)
    rows = db.execute(
        select(GOV_CONSENT_FORMS)
        .where(GOV_CONSENT_FORMS.c.nss == nss[:10])
        .order_by(desc(GOV_CONSENT_FORMS.c.id))
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "tipo": r.tipo_consentimiento,
            "procedimiento": r.procedimiento_descripcion,
            "medico": r.medico_responsable,
            "estatus": r.estatus,
            "fecha_firma": r.fecha_firma.isoformat() if r.fecha_firma else None,
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "consents": items})


# ---------------------------------------------------------------------------
# 3. ALERTAS CLÍNICAS CROSS-MODULE
# ---------------------------------------------------------------------------
@router.post("/api/governance/alerts", response_class=JSONResponse)
async def create_clinical_alert(request: Request, db: Session = Depends(_get_db)):
    """Crear una alerta clínica cross-module."""
    _ensure_gov_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    nss = str(body.get("nss", "")).strip()[:10]
    if not nss:
        return JSONResponse(status_code=400, content={"ok": False, "error": "NSS requerido"})

    patient_uid = f"nss:{nss}"
    titulo = str(body.get("titulo", "")).strip()
    tipo = str(body.get("tipo_alerta", "ALERGIA")).upper().strip()

    # Idempotente: no duplicar alertas iguales
    existing = db.execute(
        select(GOV_CLINICAL_ALERTS.c.id)
        .where(and_(
            GOV_CLINICAL_ALERTS.c.patient_uid == patient_uid,
            GOV_CLINICAL_ALERTS.c.tipo_alerta == tipo,
            GOV_CLINICAL_ALERTS.c.titulo == titulo,
        ))
    ).first()

    if existing:
        return JSONResponse(content={"ok": True, "alert_id": existing.id, "action": "already_exists"})

    now = utcnow()
    stmt = insert(GOV_CLINICAL_ALERTS).values(
        patient_uid=patient_uid,
        nss=nss,
        tipo_alerta=tipo,
        severidad=str(body.get("severidad", "MEDIA")).upper().strip(),
        titulo=titulo,
        descripcion=body.get("descripcion", ""),
        origen_modulo=body.get("origen_modulo", ""),
        origen_tabla=body.get("origen_tabla", ""),
        origen_id=body.get("origen_id"),
        activa=True,
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    alert_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
    return JSONResponse(content={"ok": True, "alert_id": alert_id, "action": "created"})


@router.get("/api/governance/alerts/patient/{nss}", response_class=JSONResponse)
async def get_patient_alerts(nss: str, db: Session = Depends(_get_db)):
    """Listar alertas activas de un paciente (para mostrar en quirófano, hospitalización, etc.)."""
    _ensure_gov_tables(db)
    rows = db.execute(
        select(GOV_CLINICAL_ALERTS)
        .where(and_(
            GOV_CLINICAL_ALERTS.c.nss == nss[:10],
            GOV_CLINICAL_ALERTS.c.activa == True,
        ))
        .order_by(
            # CRITICA primero, luego ALTA, MEDIA, BAJA
            GOV_CLINICAL_ALERTS.c.severidad.desc(),
            desc(GOV_CLINICAL_ALERTS.c.id),
        )
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "tipo": r.tipo_alerta,
            "severidad": r.severidad, "titulo": r.titulo,
            "descripcion": r.descripcion,
            "origen": r.origen_modulo,
            "creado_en": r.creado_en.isoformat() if r.creado_en else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "alerts": items})


# ---------------------------------------------------------------------------
# 4. RBAC — Gestión de usuarios (básico)
# ---------------------------------------------------------------------------
@router.get("/api/governance/users", response_class=JSONResponse)
async def list_users(db: Session = Depends(_get_db)):
    """Listar usuarios del sistema."""
    _ensure_gov_tables(db)
    rows = db.execute(
        select(GOV_USERS).where(GOV_USERS.c.activo == True).order_by(GOV_USERS.c.nombre_completo)
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id, "username": r.username,
            "nombre": r.nombre_completo, "rol": r.rol,
            "servicio": r.servicio, "matricula": r.matricula,
            "ultimo_login": r.ultimo_login.isoformat() if r.ultimo_login else None,
        })
    return JSONResponse(content={"ok": True, "total": len(items), "users": items})


@router.post("/api/governance/users", response_class=JSONResponse,
              dependencies=[Depends(require_module("gobernanza"))])
async def create_user(request: Request, db: Session = Depends(_get_db)):
    """Crear un nuevo usuario."""
    _ensure_gov_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    username = str(body.get("username", "")).strip().lower()
    password = str(body.get("password", "")).strip()
    if not username or not password:
        return JSONResponse(status_code=400, content={"ok": False, "error": "username y password requeridos"})

    VALID_ROLES = {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria", "capturista", "readonly"}
    rol = str(body.get("rol", "residente")).strip().lower()
    if rol not in VALID_ROLES:
        rol = "residente"

    now = utcnow()
    password_hash = hashlib.sha256(password.encode()).hexdigest()

    # Check duplicate
    existing = db.execute(select(GOV_USERS.c.id).where(GOV_USERS.c.username == username)).first()
    if existing:
        return JSONResponse(status_code=409, content={"ok": False, "error": "Usuario ya existe"})

    stmt = insert(GOV_USERS).values(
        username=username,
        password_hash=password_hash,
        nombre_completo=str(body.get("nombre_completo", username)).strip().upper(),
        matricula=body.get("matricula", ""),
        cedula_profesional=body.get("cedula_profesional", ""),
        rol=rol,
        servicio=body.get("servicio", "UROLOGIA"),
        email=body.get("email", ""),
        activo=True,
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    user_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
    return JSONResponse(content={"ok": True, "user_id": user_id, "username": username, "rol": rol})


# ---------------------------------------------------------------------------
# 5. MIDDLEWARE DE AUDITORÍA AUTOMÁTICA
# ---------------------------------------------------------------------------
def log_access(db: Session, *, method: str, path: str, status_code: int = 200,
               username: str = "system", rol: str = "", ip: str = "",
               tabla: str = "", registro_id: int = 0, operacion: str = "",
               patient_uid: str = "", nss: str = "", modulo: str = "",
               datos_anteriores: Any = None, datos_nuevos: Any = None,
               duracion_ms: int = 0):
    """Helper para registrar acceso en el log de auditoría."""
    try:
        _ensure_gov_tables(db)
        stmt = insert(GOV_ACCESS_LOG).values(
            timestamp=utcnow(),
            username=username, rol=rol, ip_address=ip,
            method=method, path=path[:500], status_code=status_code,
            tabla_afectada=tabla[:120] if tabla else None,
            registro_id=registro_id or None,
            operacion=operacion[:40] if operacion else None,
            patient_uid=patient_uid[:64] if patient_uid else None,
            nss=nss[:10] if nss else None,
            modulo=modulo[:80] if modulo else None,
            datos_anteriores_json=json.dumps(datos_anteriores, default=str) if datos_anteriores else None,
            datos_nuevos_json=json.dumps(datos_nuevos, default=str) if datos_nuevos else None,
            duracion_ms=duracion_ms,
        )
        db.execute(stmt)
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 6. DASHBOARD DE GOBERNANZA
# ---------------------------------------------------------------------------
@router.get("/api/governance/dashboard", response_class=JSONResponse)
async def governance_dashboard(db: Session = Depends(_get_db)):
    """Dashboard resumen de gobernanza: métricas clave."""
    _ensure_gov_tables(db)

    try:
        total_users = db.execute(select(func.count()).select_from(GOV_USERS).where(GOV_USERS.c.activo == True)).scalar() or 0
    except Exception:
        total_users = 0

    try:
        total_logs_today = db.execute(
            select(func.count()).select_from(GOV_ACCESS_LOG)
            .where(GOV_ACCESS_LOG.c.timestamp >= datetime.utcnow().replace(hour=0, minute=0, second=0))
        ).scalar() or 0
    except Exception:
        total_logs_today = 0

    try:
        total_consents = db.execute(select(func.count()).select_from(GOV_CONSENT_FORMS)).scalar() or 0
        pending_consents = db.execute(
            select(func.count()).select_from(GOV_CONSENT_FORMS)
            .where(GOV_CONSENT_FORMS.c.estatus == "PENDIENTE")
        ).scalar() or 0
    except Exception:
        total_consents = 0
        pending_consents = 0

    try:
        active_alerts = db.execute(
            select(func.count()).select_from(GOV_CLINICAL_ALERTS)
            .where(GOV_CLINICAL_ALERTS.c.activa == True)
        ).scalar() or 0
        critical_alerts = db.execute(
            select(func.count()).select_from(GOV_CLINICAL_ALERTS)
            .where(and_(GOV_CLINICAL_ALERTS.c.activa == True, GOV_CLINICAL_ALERTS.c.severidad == "CRITICA"))
        ).scalar() or 0
    except Exception:
        active_alerts = 0
        critical_alerts = 0

    return JSONResponse(content={
        "ok": True,
        "metrics": {
            "users_activos": total_users,
            "accesos_hoy": total_logs_today,
            "consentimientos_total": total_consents,
            "consentimientos_pendientes": pending_consents,
            "alertas_activas": active_alerts,
            "alertas_criticas": critical_alerts,
        }
    })
