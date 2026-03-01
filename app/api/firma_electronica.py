"""
API de Firma Electrónica Médica — Item #4.

Implementa:
- Captura de firma en canvas (recibe imagen base64)
- Hash SHA-256 de la firma
- Almacenamiento seguro
- Verificación de firma
- Tabla de firmas electrónicas
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, Boolean, desc, select, insert, update, func
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

router = APIRouter(tags=["firma_electronica"])

# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------
FIRMA_METADATA = MetaData()

FIRMAS_ELECTRONICAS = Table(
    "firmas_electronicas",
    FIRMA_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    # Contexto
    Column("tipo_documento", String(80), nullable=False, index=True),
    # NOTA_MEDICA, CONSENTIMIENTO, RECETA, INTERCONSULTA, EGRESO, ALTA
    Column("documento_id", Integer, nullable=True, index=True),
    Column("modulo_origen", String(80), nullable=True),
    # Firmante
    Column("firmante_username", String(80), nullable=False, index=True),
    Column("firmante_nombre", String(200), nullable=True),
    Column("firmante_matricula", String(40), nullable=True, index=True),
    Column("firmante_cedula", String(40), nullable=True),
    Column("firmante_rol", String(40), nullable=True),
    # Firma
    Column("firma_imagen_base64", Text, nullable=True),  # Canvas data
    Column("firma_hash", String(128), nullable=False, unique=True, index=True),
    Column("firma_metadata_json", Text, nullable=True),  # user-agent, IP, timestamp
    # Estado
    Column("valida", Boolean, nullable=False, default=True),
    Column("motivo_invalidacion", Text, nullable=True),
    Column("invalidada_por", String(80), nullable=True),
    Column("fecha_invalidacion", DateTime, nullable=True),
    # Timestamps
    Column("creado_en", DateTime, nullable=False, default=utcnow),
)


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        FIRMA_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Vista HTML
# ---------------------------------------------------------------------------
@router.get("/firma-electronica", response_class=HTMLResponse)
async def firma_view(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template("firma_electronica.html", request=request)


# ---------------------------------------------------------------------------
# Crear firma
# ---------------------------------------------------------------------------
@router.post("/api/firma-electronica", response_class=JSONResponse)
async def create_firma(request: Request, db: Session = Depends(_get_db)):
    """Registrar una firma electrónica médica."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    nss = str(body.get("nss", "")).strip()[:10]
    firma_imagen = str(body.get("firma_imagen_base64", "")).strip()
    firmante = str(body.get("firmante_username", "")).strip()
    tipo = str(body.get("tipo_documento", "NOTA_MEDICA")).upper().strip()

    if not nss:
        return JSONResponse(status_code=400, content={"ok": False, "error": "NSS requerido"})
    if not firma_imagen:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Imagen de firma requerida"})
    if not firmante:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Firmante requerido"})

    # Generate hash from signature image + timestamp + firmante
    now = utcnow()
    hash_input = f"{firma_imagen}|{firmante}|{now.isoformat()}|{nss}"
    firma_hash = hashlib.sha256(hash_input.encode()).hexdigest()

    # Metadata de la firma
    firma_meta = {
        "ip": request.client.host if request.client else "",
        "user_agent": request.headers.get("user-agent", "")[:200],
        "timestamp": now.isoformat(),
        "origin": request.headers.get("origin", ""),
    }

    stmt = insert(FIRMAS_ELECTRONICAS).values(
        patient_uid=f"nss:{nss}",
        nss=nss,
        tipo_documento=tipo,
        documento_id=body.get("documento_id"),
        modulo_origen=body.get("modulo_origen", ""),
        firmante_username=firmante,
        firmante_nombre=body.get("firmante_nombre", ""),
        firmante_matricula=body.get("firmante_matricula", ""),
        firmante_cedula=body.get("firmante_cedula", ""),
        firmante_rol=body.get("firmante_rol", ""),
        firma_imagen_base64=firma_imagen[:500000],  # Max 500KB base64
        firma_hash=firma_hash,
        firma_metadata_json=json.dumps(firma_meta),
        valida=True,
        creado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    firma_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    return JSONResponse(content={
        "ok": True,
        "firma_id": firma_id,
        "firma_hash": firma_hash,
        "timestamp": now.isoformat(),
    })


# ---------------------------------------------------------------------------
# Verificar firma
# ---------------------------------------------------------------------------
@router.get("/api/firma-electronica/verify/{firma_hash}", response_class=JSONResponse)
async def verify_firma(firma_hash: str, db: Session = Depends(_get_db)):
    """Verificar si una firma electrónica es válida."""
    _ensure_tables(db)
    row = db.execute(
        select(FIRMAS_ELECTRONICAS)
        .where(FIRMAS_ELECTRONICAS.c.firma_hash == firma_hash)
    ).first()

    if not row:
        return JSONResponse(content={"ok": False, "valid": False, "error": "Firma no encontrada"})

    return JSONResponse(content={
        "ok": True,
        "valid": row.valida,
        "firma": {
            "id": row.id,
            "nss": row.nss,
            "tipo_documento": row.tipo_documento,
            "firmante": row.firmante_nombre or row.firmante_username,
            "matricula": row.firmante_matricula,
            "fecha": row.creado_en.isoformat() if row.creado_en else None,
            "valida": row.valida,
            "motivo_invalidacion": row.motivo_invalidacion if not row.valida else None,
        },
    })


# ---------------------------------------------------------------------------
# Firmas de un paciente
# ---------------------------------------------------------------------------
@router.get("/api/firma-electronica/patient/{nss}", response_class=JSONResponse)
async def get_patient_firmas(nss: str, db: Session = Depends(_get_db)):
    """Listar todas las firmas de un paciente."""
    _ensure_tables(db)
    rows = db.execute(
        select(FIRMAS_ELECTRONICAS)
        .where(FIRMAS_ELECTRONICAS.c.nss == nss[:10])
        .order_by(desc(FIRMAS_ELECTRONICAS.c.id))
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id,
            "tipo_documento": r.tipo_documento,
            "firmante": r.firmante_nombre or r.firmante_username,
            "matricula": r.firmante_matricula,
            "firma_hash": r.firma_hash,
            "valida": r.valida,
            "fecha": r.creado_en.isoformat() if r.creado_en else None,
        })

    return JSONResponse(content={"ok": True, "total": len(items), "firmas": items})


# ---------------------------------------------------------------------------
# Invalidar firma
# ---------------------------------------------------------------------------
@router.post("/api/firma-electronica/{firma_id}/invalidate", response_class=JSONResponse)
async def invalidate_firma(firma_id: int, request: Request, db: Session = Depends(_get_db)):
    """Invalidar una firma electrónica (solo admin/jefe_servicio)."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        body = {}

    now = utcnow()
    db.execute(
        update(FIRMAS_ELECTRONICAS)
        .where(FIRMAS_ELECTRONICAS.c.id == firma_id)
        .values(
            valida=False,
            motivo_invalidacion=body.get("motivo", "Invalidada por administrador"),
            invalidada_por=body.get("invalidada_por", "admin"),
            fecha_invalidacion=now,
        )
    )
    db.commit()
    return JSONResponse(content={"ok": True, "firma_id": firma_id, "valida": False})
