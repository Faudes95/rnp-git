"""API y vistas para el Perfil Clínico del Paciente.

Proporciona búsqueda de pacientes y vista integral de perfil clínico
para consultas subsecuentes de CONSULTA EXTERNA, LEOCH y UROENDOSCOPIA.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.services.consulta_domain import (
    CONSULTA_EXTERNA_ATENCIONES,
    ensure_consulta_externa_schema,
)

router = APIRouter(tags=["perfil-clinico"])


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


# ── Vistas de entrada por servicio ──

@router.get("/perfil-clinico/consulta-externa", response_class=HTMLResponse)
async def perfil_clinico_consulta_externa(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template(
        "perfil_clinico.html",
        request=request,
        servicio="CONSULTA EXTERNA",
        servicio_label="Consulta Externa",
        atencion_url="/consulta/metadata",
    )


@router.get("/perfil-clinico/leoch", response_class=HTMLResponse)
async def perfil_clinico_leoch(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template(
        "perfil_clinico.html",
        request=request,
        servicio="LEOCH",
        servicio_label="LEOCH",
        atencion_url="/consulta_externa/leoch",
    )


@router.get("/perfil-clinico/uroendoscopia", response_class=HTMLResponse)
async def perfil_clinico_uroendoscopia(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template(
        "perfil_clinico.html",
        request=request,
        servicio="UROENDOSCOPIA",
        servicio_label="Uroendoscopia",
        atencion_url="/consulta_externa/uroendoscopia",
    )


# ── API de búsqueda ──

@router.get("/api/perfil-clinico/buscar", response_class=JSONResponse)
async def buscar_paciente_perfil(request: Request, db: Session = Depends(_get_db)):
    """Busca pacientes por NSS y/o nombre en el expediente."""
    from app.core.app_context import main_proxy as m

    nss = str(request.query_params.get("nss") or "").strip()
    nombre = str(request.query_params.get("nombre") or "").strip().upper()

    if not nss and not nombre:
        raise HTTPException(status_code=400, detail="Ingrese NSS o nombre del paciente")

    q = db.query(m.ConsultaDB)
    if nss:
        q = q.filter(m.ConsultaDB.nss.contains(nss))
    if nombre:
        q = q.filter(func.upper(m.ConsultaDB.nombre).contains(nombre))

    # Obtener los pacientes únicos (agrupados por NSS)
    consultas = q.order_by(m.ConsultaDB.id.desc()).limit(50).all()

    # Deduplicar por NSS
    seen_nss = set()
    pacientes: List[Dict[str, Any]] = []
    for c in consultas:
        key = str(getattr(c, "nss", "") or "").strip()
        if not key:
            key = str(getattr(c, "nombre", "") or "").strip().upper()
        if key in seen_nss:
            continue
        seen_nss.add(key)

        # Contar consultas del paciente
        count_q = db.query(func.count(m.ConsultaDB.id)).filter(m.ConsultaDB.nss == str(getattr(c, "nss", "") or ""))
        total = count_q.scalar() or 0

        pacientes.append({
            "nss": str(getattr(c, "nss", "") or ""),
            "curp": str(getattr(c, "curp", "") or ""),
            "nombre": str(getattr(c, "nombre", "") or ""),
            "edad": getattr(c, "edad", None),
            "sexo": str(getattr(c, "sexo", "") or ""),
            "tipo_sangre": str(getattr(c, "tipo_sangre", "") or ""),
            "total_consultas": total,
        })

    return JSONResponse(content={"ok": True, "pacientes": pacientes})


# ── API de datos del perfil ──

@router.get("/api/perfil-clinico/datos", response_class=JSONResponse)
async def datos_perfil_clinico(request: Request, db: Session = Depends(_get_db)):
    """Obtiene datos integrales del perfil clínico de un paciente."""
    from app.core.app_context import main_proxy as m

    nss = str(request.query_params.get("nss") or "").strip()
    nombre = str(request.query_params.get("nombre") or "").strip().upper()

    if not nss and not nombre:
        raise HTTPException(status_code=400, detail="Se requiere NSS o nombre")

    # Buscar consultas del paciente
    q = db.query(m.ConsultaDB)
    if nss:
        q = q.filter(m.ConsultaDB.nss == nss)
    if nombre and not nss:
        q = q.filter(func.upper(m.ConsultaDB.nombre) == nombre)

    consultas = q.order_by(m.ConsultaDB.id.desc()).all()
    if not consultas:
        raise HTTPException(status_code=404, detail="Paciente no encontrado")

    ultima = consultas[0]

    # Extraer antecedentes de la consulta más reciente
    def parse_json_field(val: Any) -> list:
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    antecedentes = {
        "patologicos": parse_json_field(getattr(ultima, "app_patologias_json", None)),
        "quirurgicos": parse_json_field(getattr(ultima, "aqx_json", None)),
        "alergias": parse_json_field(getattr(ultima, "alergias_json", None)),
        "heredofamiliares": parse_json_field(getattr(ultima, "ahf_json", None)),
        "toxicomanias": parse_json_field(getattr(ultima, "toxicomanias_json", None)),
    }

    # Hospitalizaciones previas
    hosp_previas = parse_json_field(getattr(ultima, "hosp_previas_json", None))

    # Buscar alertas/inconsistencias
    alertas: List[str] = []
    for c in consultas[:5]:
        inc = str(getattr(c, "inconsistencias", "") or "").strip()
        if inc:
            for a in inc.split(";"):
                a = a.strip()
                if a and a not in alertas:
                    alertas.append(a)

    # Buscar hospitalizaciones en la tabla de hospitalizaciones
    hospitalizaciones_db: List[Dict[str, Any]] = []
    try:
        hosps = db.query(m.HospitalizacionDB).filter(
            or_(
                m.HospitalizacionDB.nss == nss,
                func.upper(m.HospitalizacionDB.nombre_paciente) == nombre,
            )
        ).order_by(m.HospitalizacionDB.id.desc()).limit(10).all()
        for h in hosps:
            hospitalizaciones_db.append({
                "fecha": str(getattr(h, "fecha_ingreso", "") or ""),
                "motivo": str(getattr(h, "motivo_ingreso", "") or getattr(h, "diagnostico", "") or ""),
                "dias": getattr(h, "dias_estancia", None),
            })
    except Exception:
        pass

    # Combinar hospitalizaciones previas (JSON + DB)
    all_hosp = []
    for hp in hosp_previas:
        all_hosp.append({
            "fecha": hp.get("fecha", ""),
            "motivo": hp.get("motivo", ""),
            "dias": hp.get("dias_estancia", hp.get("dias", "")),
            "uci": hp.get("ingreso_uci", "").upper() == "SI" if isinstance(hp.get("ingreso_uci"), str) else False,
            "dias_uci": hp.get("dias_uci", ""),
        })
    for hdb in hospitalizaciones_db:
        all_hosp.append({
            "fecha": hdb["fecha"],
            "motivo": hdb["motivo"],
            "dias": hdb.get("dias", ""),
            "uci": False,
            "dias_uci": "",
        })

    # Buscar atenciones de consulta externa
    atenciones: List[Dict[str, Any]] = []
    try:
        ensure_consulta_externa_schema(db)
        from sqlalchemy import select
        rows = db.execute(
            select(CONSULTA_EXTERNA_ATENCIONES)
            .where(CONSULTA_EXTERNA_ATENCIONES.c.nss == nss)
            .order_by(CONSULTA_EXTERNA_ATENCIONES.c.fecha_atencion.desc())
            .limit(30)
        ).mappings().all()
        for r in rows:
            atenciones.append({
                "servicio": str(r.get("servicio") or "CONSULTA EXTERNA"),
                "fecha": str(r.get("fecha_atencion") or ""),
                "diagnostico": str(r.get("diagnostico_principal") or ""),
                "resumen": str(r.get("nota_resumen") or ""),
                "medico": str(r.get("medico_responsable") or ""),
            })
    except Exception:
        pass

    # Generar notas combinadas (consultas + atenciones externas)
    notas: List[Dict[str, Any]] = []
    for c in consultas:
        notas.append({
            "servicio": "CONSULTA EXTERNA",
            "fecha": str(getattr(c, "fecha_registro", "") or ""),
            "diagnostico": str(getattr(c, "diagnostico_principal", "") or ""),
            "resumen": str(getattr(c, "padecimiento_actual", "") or "")[:200],
            "medico": "",
        })
    for a in atenciones:
        notas.append(a)

    # Ordenar notas por fecha (más recientes primero)
    def parse_date_str(s: str) -> str:
        return s if s else "0000-00-00"
    notas.sort(key=lambda n: parse_date_str(n.get("fecha", "")), reverse=True)

    result = {
        "ok": True,
        "nss": nss,
        "nombre": str(getattr(ultima, "nombre", "") or ""),
        "diagnostico_principal": str(getattr(ultima, "diagnostico_principal", "") or ""),
        "protocolo": str(getattr(ultima, "protocolo", "") or ""),
        "plan_tratamiento": str(getattr(ultima, "plan_tratamiento", "") or getattr(ultima, "subsecuente_plan", "") or ""),
        "total_consultas": len(consultas),
        "total_atenciones": len(atenciones),
        "ultima_atencion": notas[0].get("fecha", "") if notas else "",
        "antecedentes": antecedentes,
        "hospitalizaciones_previas": all_hosp,
        "alertas": alertas[:10],
        "notas": notas[:50],
    }

    return JSONResponse(content=result)
