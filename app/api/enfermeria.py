"""
API de Hoja de Enfermería — Item #5.

Implementa:
- Registro de signos vitales por turno
- Control de líquidos (ingresos/egresos)
- Medicamentos administrados
- Notas de enfermería
- Balance hídrico
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, Text, Boolean, desc, select, insert, update, func, and_
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

router = APIRouter(tags=["enfermeria"])

# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
ENF_METADATA = MetaData()

HOJA_ENFERMERIA = Table(
    "hoja_enfermeria",
    ENF_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("patient_uid", String(64), nullable=False, index=True),
    Column("nss", String(10), nullable=False, index=True),
    Column("hospitalizacion_id", Integer, nullable=True, index=True),
    Column("cama", String(20), nullable=True),
    Column("fecha", DateTime, nullable=False, index=True),
    Column("turno", String(20), nullable=False, index=True),  # MATUTINO, VESPERTINO, NOCTURNO, JORNADA_ACUMULADA
    # Signos vitales
    Column("ta_sistolica", Integer, nullable=True),
    Column("ta_diastolica", Integer, nullable=True),
    Column("fc", Integer, nullable=True),
    Column("fr", Integer, nullable=True),
    Column("temp", Float, nullable=True),
    Column("spo2", Integer, nullable=True),
    Column("dolor_eva", Integer, nullable=True),  # Escala 0-10
    Column("glasgow", Integer, nullable=True),
    # Somatometría
    Column("peso_kg", Float, nullable=True),
    Column("talla_m", Float, nullable=True),
    # Control de líquidos - Ingresos
    Column("liquidos_iv_ml", Float, nullable=True),
    Column("liquidos_oral_ml", Float, nullable=True),
    Column("hemoderivados_ml", Float, nullable=True),
    Column("medicamentos_iv_ml", Float, nullable=True),
    Column("otros_ingresos_ml", Float, nullable=True),
    # Control de líquidos - Egresos
    Column("diuresis_ml", Float, nullable=True),
    Column("evacuaciones_ml", Float, nullable=True),
    Column("vomito_ml", Float, nullable=True),
    Column("sangrado_ml", Float, nullable=True),
    Column("drenajes_ml", Float, nullable=True),
    Column("otros_egresos_ml", Float, nullable=True),
    # Balance
    Column("balance_hidrico_ml", Float, nullable=True),
    # Medicamentos
    Column("medicamentos_json", Text, nullable=True),  # [{nombre, dosis, via, hora, observaciones}]
    # Notas
    Column("nota_enfermeria", Text, nullable=True),
    Column("intervenciones", Text, nullable=True),
    Column("plan_cuidados", Text, nullable=True),
    # Estado del paciente
    Column("estado_general", String(40), nullable=True),  # ESTABLE, DELICADO, GRAVE, CRITICO
    Column("nivel_consciencia", String(40), nullable=True),
    Column("tipo_dieta", String(80), nullable=True),
    Column("movilidad", String(40), nullable=True),
    Column("riesgo_caidas", String(20), nullable=True),
    Column("riesgo_ulceras", String(20), nullable=True),
    # Catéteres y dispositivos
    Column("cateter_venoso", String(80), nullable=True),
    Column("sonda_foley", Boolean, nullable=True),
    Column("sonda_nasogastrica", Boolean, nullable=True),
    Column("drenaje_tipo", String(80), nullable=True),
    Column("oxigeno_tipo", String(80), nullable=True),
    Column("oxigeno_litros", Float, nullable=True),
    # Enfermera
    Column("enfermera_nombre", String(200), nullable=True),
    Column("enfermera_matricula", String(40), nullable=True, index=True),
    Column("enfermera_username", String(80), nullable=True),
    # Timestamps
    Column("creado_en", DateTime, nullable=False, default=utcnow),
    Column("actualizado_en", DateTime, nullable=False, default=utcnow),
)


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        ENF_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Vista HTML
# ---------------------------------------------------------------------------
@router.get("/enfermeria", response_class=HTMLResponse)
async def enfermeria_view(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template("enfermeria.html", request=request)


# ---------------------------------------------------------------------------
# Crear registro
# ---------------------------------------------------------------------------
@router.post("/api/enfermeria/registro", response_class=JSONResponse)
async def create_registro(request: Request, db: Session = Depends(_get_db)):
    """Crear un nuevo registro en hoja de enfermería."""
    _ensure_tables(db)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    nss = str(body.get("nss", "")).strip()[:10]
    if not nss:
        return JSONResponse(status_code=400, content={"ok": False, "error": "NSS requerido"})

    turno = str(body.get("turno", "MATUTINO")).upper().strip()
    if turno not in ("MATUTINO", "VESPERTINO", "NOCTURNO", "JORNADA_ACUMULADA"):
        turno = "MATUTINO"

    now = utcnow()

    # Calcular balance hídrico
    ingresos = sum(filter(None, [
        body.get("liquidos_iv_ml"),
        body.get("liquidos_oral_ml"),
        body.get("hemoderivados_ml"),
        body.get("medicamentos_iv_ml"),
        body.get("otros_ingresos_ml"),
    ]))
    egresos = sum(filter(None, [
        body.get("diuresis_ml"),
        body.get("evacuaciones_ml"),
        body.get("vomito_ml"),
        body.get("sangrado_ml"),
        body.get("drenajes_ml"),
        body.get("otros_egresos_ml"),
    ]))
    balance = ingresos - egresos

    # Medicamentos como JSON
    meds = body.get("medicamentos", [])
    meds_json = json.dumps(meds, ensure_ascii=False) if meds else None

    stmt = insert(HOJA_ENFERMERIA).values(
        patient_uid=f"nss:{nss}",
        nss=nss,
        hospitalizacion_id=body.get("hospitalizacion_id"),
        cama=body.get("cama", ""),
        fecha=now,
        turno=turno,
        ta_sistolica=body.get("ta_sistolica"),
        ta_diastolica=body.get("ta_diastolica"),
        fc=body.get("fc"),
        fr=body.get("fr"),
        temp=body.get("temp"),
        spo2=body.get("spo2"),
        dolor_eva=body.get("dolor_eva"),
        glasgow=body.get("glasgow"),
        peso_kg=body.get("peso_kg"),
        talla_m=body.get("talla_m"),
        liquidos_iv_ml=body.get("liquidos_iv_ml"),
        liquidos_oral_ml=body.get("liquidos_oral_ml"),
        hemoderivados_ml=body.get("hemoderivados_ml"),
        medicamentos_iv_ml=body.get("medicamentos_iv_ml"),
        otros_ingresos_ml=body.get("otros_ingresos_ml"),
        diuresis_ml=body.get("diuresis_ml"),
        evacuaciones_ml=body.get("evacuaciones_ml"),
        vomito_ml=body.get("vomito_ml"),
        sangrado_ml=body.get("sangrado_ml"),
        drenajes_ml=body.get("drenajes_ml"),
        otros_egresos_ml=body.get("otros_egresos_ml"),
        balance_hidrico_ml=balance,
        medicamentos_json=meds_json,
        nota_enfermeria=body.get("nota_enfermeria", ""),
        intervenciones=body.get("intervenciones", ""),
        plan_cuidados=body.get("plan_cuidados", ""),
        estado_general=body.get("estado_general", ""),
        nivel_consciencia=body.get("nivel_consciencia", ""),
        tipo_dieta=body.get("tipo_dieta", ""),
        movilidad=body.get("movilidad", ""),
        riesgo_caidas=body.get("riesgo_caidas", ""),
        riesgo_ulceras=body.get("riesgo_ulceras", ""),
        cateter_venoso=body.get("cateter_venoso", ""),
        sonda_foley=body.get("sonda_foley"),
        sonda_nasogastrica=body.get("sonda_nasogastrica"),
        drenaje_tipo=body.get("drenaje_tipo", ""),
        oxigeno_tipo=body.get("oxigeno_tipo", ""),
        oxigeno_litros=body.get("oxigeno_litros"),
        enfermera_nombre=body.get("enfermera_nombre", ""),
        enfermera_matricula=body.get("enfermera_matricula", ""),
        enfermera_username=body.get("enfermera_username", ""),
        creado_en=now,
        actualizado_en=now,
    )
    result = db.execute(stmt)
    db.commit()
    reg_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    return JSONResponse(content={
        "ok": True,
        "registro_id": reg_id,
        "balance_hidrico_ml": balance,
        "turno": turno,
    })


# ---------------------------------------------------------------------------
# Listar registros de un paciente
# ---------------------------------------------------------------------------
@router.get("/api/enfermeria/patient/{nss}", response_class=JSONResponse)
async def get_patient_enfermeria(
    nss: str,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(_get_db),
):
    """Listar registros de enfermería de un paciente."""
    _ensure_tables(db)
    rows = db.execute(
        select(HOJA_ENFERMERIA)
        .where(HOJA_ENFERMERIA.c.nss == nss[:10])
        .order_by(desc(HOJA_ENFERMERIA.c.id))
        .limit(limit)
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r.id,
            "fecha": r.fecha.isoformat() if r.fecha else None,
            "turno": r.turno,
            "cama": r.cama,
            "ta": f"{r.ta_sistolica}/{r.ta_diastolica}" if r.ta_sistolica else None,
            "fc": r.fc,
            "fr": r.fr,
            "temp": r.temp,
            "spo2": r.spo2,
            "dolor_eva": r.dolor_eva,
            "glasgow": r.glasgow,
            "balance_hidrico_ml": r.balance_hidrico_ml,
            "estado_general": r.estado_general,
            "nota": (r.nota_enfermeria or "")[:200],
            "enfermera": r.enfermera_nombre,
            "medicamentos": json.loads(r.medicamentos_json) if r.medicamentos_json else [],
        })

    return JSONResponse(content={"ok": True, "total": len(items), "registros": items})


# ---------------------------------------------------------------------------
# Balance hídrico acumulado (24h)
# ---------------------------------------------------------------------------
@router.get("/api/enfermeria/balance/{nss}", response_class=JSONResponse)
async def get_balance_hidrico(nss: str, db: Session = Depends(_get_db)):
    """Calcular balance hídrico de las últimas 24 horas."""
    _ensure_tables(db)
    from datetime import timedelta
    now = utcnow()
    hace_24h = now - timedelta(hours=24)

    rows = db.execute(
        select(HOJA_ENFERMERIA)
        .where(and_(
            HOJA_ENFERMERIA.c.nss == nss[:10],
            HOJA_ENFERMERIA.c.fecha >= hace_24h,
        ))
        .order_by(HOJA_ENFERMERIA.c.fecha)
    ).fetchall()

    total_ingresos = 0
    total_egresos = 0
    turnos = []

    for r in rows:
        ing = sum(filter(None, [
            r.liquidos_iv_ml, r.liquidos_oral_ml, r.hemoderivados_ml,
            r.medicamentos_iv_ml, r.otros_ingresos_ml,
        ]))
        egr = sum(filter(None, [
            r.diuresis_ml, r.evacuaciones_ml, r.vomito_ml,
            r.sangrado_ml, r.drenajes_ml, r.otros_egresos_ml,
        ]))
        total_ingresos += ing
        total_egresos += egr
        turnos.append({
            "turno": r.turno,
            "fecha": r.fecha.isoformat() if r.fecha else None,
            "ingresos_ml": ing,
            "egresos_ml": egr,
            "balance_ml": ing - egr,
        })

    return JSONResponse(content={
        "ok": True,
        "nss": nss[:10],
        "periodo": "24h",
        "total_ingresos_ml": total_ingresos,
        "total_egresos_ml": total_egresos,
        "balance_total_ml": total_ingresos - total_egresos,
        "turnos": turnos,
    })
