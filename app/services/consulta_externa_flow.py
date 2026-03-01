from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table, Text, func, insert, select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.core.terminology import normalize_diagnostico, normalize_procedimiento
from app.services.event_log_flow import emit_event
from app.services.common import classify_age_group


CONSULTA_EXTERNA_METADATA = MetaData()

CONSULTA_EXTERNA_ATENCIONES = Table(
    "consulta_externa_atenciones",
    CONSULTA_EXTERNA_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("fecha_atencion", Date, nullable=False, index=True),
    Column("servicio", String(40), nullable=False, index=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("nss", String(32), nullable=True, index=True),
    Column("nombre", String(255), nullable=True, index=True),
    Column("edad", Integer, nullable=True, index=True),
    Column("sexo", String(30), nullable=True, index=True),
    Column("diagnostico_principal", String(255), nullable=True, index=True),
    Column("cie10_codigo", String(30), nullable=True, index=True),
    Column("medico_responsable", String(120), nullable=True, index=True),
    Column("turno", String(30), nullable=True, index=True),
    Column("hgz", String(120), nullable=True, index=True),
    Column("nota_resumen", Text, nullable=True),
    Column("payload_json", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    Column("actualizado_en", DateTime, default=utcnow, nullable=False, index=True),
)


SERVICIOS_CONSULTA_EXTERNA: List[str] = [
    "CONSULTA EXTERNA",
    "UROENDOSCOPIA",
    "LEOCH",
    "RECETAS",
]


DIAGNOSTICOS_BASE: List[Tuple[str, str]] = [
    ("N20.0", "CÁLCULO DEL RIÑÓN"),
    ("N20.1", "CÁLCULO DEL URÉTER"),
    ("N21.0", "CÁLCULO DE VEJIGA"),
    ("N13.2", "HIDRONEFROSIS CON OBSTRUCCIÓN"),
    ("C61", "CÁNCER DE PRÓSTATA"),
    ("C67.9", "CÁNCER DE VEJIGA"),
    ("C64", "CÁNCER RENAL"),
    ("C62.9", "CÁNCER DE TESTÍCULO"),
    ("N40", "HIPERPLASIA PROSTÁTICA"),
    ("R31", "HEMATURIA"),
    ("R33", "RETENCIÓN URINARIA"),
    ("N39.0", "INFECCIÓN DE VÍAS URINARIAS"),
]


PROCEDIMIENTOS_UROENDOSCOPIA: List[str] = [
    "CISTOSCOPIA DIAGNÓSTICA",
    "URETEROSCOPIA",
    "URETERORENOSCOPIA FLEXIBLE",
    "RESECCIÓN TRANSURETRAL DE VEJIGA",
    "RESECCIÓN TRANSURETRAL DE PRÓSTATA",
    "CAMBIO/RETIRO DE CATÉTER JJ",
    "OTRO",
]


def ensure_consulta_externa_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    CONSULTA_EXTERNA_METADATA.create_all(bind=bind, checkfirst=True)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_return_to(value: Any, default: str = "/consulta_externa") -> str:
    candidate = _safe_text(value)
    if not candidate:
        return default
    if not candidate.startswith("/") or candidate.startswith("//"):
        return default
    if "javascript:" in candidate.lower():
        return default
    return candidate


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except Exception:
        return None


def _norm_name(value: Any) -> str:
    return _safe_text(value).upper()


def _norm_nss(value: Any, m: Any) -> str:
    v = _safe_text(value)
    try:
        return m.normalize_nss(v)
    except Exception:
        return v


def _resolve_consulta(db: Session, m: Any, *, consulta_id: Optional[int], nss: str, nombre: str):
    consulta = None
    if consulta_id:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(consulta_id)).first()
    if consulta is None and nss:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == nss).order_by(m.ConsultaDB.id.desc()).first()
    if consulta is None and nombre:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(func.upper(m.ConsultaDB.nombre).contains(nombre))
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    return consulta


def _build_note_text(servicio: str, payload: Dict[str, Any]) -> str:
    return (
        f"Atención registrada en {servicio}. "
        f"Dx: {_safe_text(payload.get('diagnostico_principal')) or 'NO REGISTRADO'}. "
        f"CIE10: {_safe_text(payload.get('cie10_codigo')) or 'N/E'}. "
        f"Médico: {_safe_text(payload.get('medico_responsable')) or 'NO REGISTRADO'}. "
        f"Detalle: {_safe_text(payload.get('nota_resumen')) or _safe_text(payload.get('hallazgos')) or 'Sin detalle adicional.'}"
    )


def register_service_attention(
    db: Session,
    m: Any,
    *,
    service_name: str,
    payload: Dict[str, Any],
    request_user: str = "system",
    attach_to_expediente: bool = True,
) -> Dict[str, Any]:
    ensure_consulta_externa_schema(db)

    service_norm = _safe_text(service_name).upper() or "CONSULTA EXTERNA"
    consulta_id = _safe_int(payload.get("consulta_id"))
    nss = _norm_nss(payload.get("nss"), m)
    nombre = _norm_name(payload.get("nombre"))

    consulta = _resolve_consulta(
        db,
        m,
        consulta_id=consulta_id,
        nss=nss,
        nombre=nombre,
    )
    if consulta is None:
        raise ValueError("No se encontró perfil del paciente. Registre primero una consulta base o indique consulta_id válido.")

    resolved_consulta_id = int(getattr(consulta, "id"))
    resolved_nss = nss or _norm_nss(getattr(consulta, "nss", ""), m)
    resolved_nombre = nombre or _norm_name(getattr(consulta, "nombre", ""))
    resolved_edad = _safe_int(payload.get("edad"))
    if resolved_edad is None:
        resolved_edad = _safe_int(getattr(consulta, "edad", None))
    resolved_sexo = _safe_text(payload.get("sexo")).upper() or _safe_text(getattr(consulta, "sexo", "")).upper()
    resolved_dx = _safe_text(payload.get("diagnostico_principal")).upper() or _safe_text(
        getattr(consulta, "diagnostico_principal", "")
    ).upper()
    resolved_cie10 = _safe_text(payload.get("cie10_codigo")).upper()
    dx_norm = normalize_diagnostico(resolved_dx, cie10_codigo=resolved_cie10)
    resolved_dx = _safe_text(dx_norm.get("normalized") or resolved_dx).upper()
    resolved_cie10 = _safe_text(dx_norm.get("cie10_codigo") or resolved_cie10).upper()
    proc_norm = normalize_procedimiento(payload.get("procedimiento") or payload.get("procedimiento_realizado"))
    nota_resumen = _safe_text(payload.get("nota_resumen")) or _build_note_text(service_norm, payload)
    fecha_atencion_raw = _safe_text(payload.get("fecha_atencion"))
    try:
        fecha_atencion = date.fromisoformat(fecha_atencion_raw) if fecha_atencion_raw else date.today()
    except Exception:
        fecha_atencion = date.today()

    ins = insert(CONSULTA_EXTERNA_ATENCIONES).values(
        fecha_atencion=fecha_atencion,
        servicio=service_norm,
        consulta_id=resolved_consulta_id,
        nss=resolved_nss,
        nombre=resolved_nombre,
        edad=resolved_edad,
        sexo=resolved_sexo,
        diagnostico_principal=resolved_dx,
        cie10_codigo=resolved_cie10,
        medico_responsable=_safe_text(payload.get("medico_responsable")).upper() or _safe_text(getattr(consulta, "agregado_medico", "")).upper(),
        turno=_safe_text(payload.get("turno")).upper(),
        hgz=_safe_text(payload.get("hgz")).upper(),
        nota_resumen=nota_resumen,
        payload_json=json.dumps(
            {
                **payload,
                "terminologia": {
                    "diagnostico": dx_norm,
                    "procedimiento": proc_norm,
                },
            },
            ensure_ascii=False,
        ),
        creado_en=utcnow(),
        actualizado_en=utcnow(),
    )
    result = db.execute(ins)
    attention_id = int(result.inserted_primary_key[0])

    nota_id = None
    if attach_to_expediente:
        from app.services.expediente_nota_medica_flow import save_nota_medica_diaria

        saved_nota = save_nota_medica_diaria(
            db,
            m,
            raw_form={
                "consulta_id": resolved_consulta_id,
                "nss": resolved_nss,
                "nombre": resolved_nombre,
                "fecha_nota": fecha_atencion.isoformat(),
                "servicio_nota": service_norm,
                "cie10_codigo": resolved_cie10,
                "diagnostico_cie10": resolved_dx,
                "nota_texto": nota_resumen,
            },
            request_user=request_user,
        )
        nota_id = saved_nota.get("nota_id")
    else:
        db.commit()

    try:
        from app.services.master_identity_flow import upsert_master_identity

        upsert_master_identity(
            db,
            nss=resolved_nss,
            curp=getattr(consulta, "curp", None),
            nombre=resolved_nombre,
            sexo=resolved_sexo,
            consulta_id=resolved_consulta_id,
            source_table="consulta_externa_atenciones",
            source_pk=attention_id,
            module=service_norm,
            fecha_evento=fecha_atencion,
            payload={
                "cie10_codigo": resolved_cie10,
                "diagnostico_principal": resolved_dx,
                "medico_responsable": _safe_text(payload.get("medico_responsable")).upper(),
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    try:
        emit_event(
            db,
            module="consulta_externa",
            event_type="ATENCION_REGISTRADA",
            entity="consulta_externa_atenciones",
            entity_id=str(attention_id),
            consulta_id=resolved_consulta_id,
            actor=request_user,
            source_route=f"/consulta_externa/{service_norm.lower().replace(' ', '_')}",
            payload={
                "servicio": service_norm,
                "nss": resolved_nss,
                "nombre": resolved_nombre,
                "diagnostico_principal": resolved_dx,
                "cie10_codigo": resolved_cie10,
                "cie11_codigo": dx_norm.get("cie11_codigo"),
                "nota_id": nota_id,
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    return {
        "attention_id": attention_id,
        "nota_id": nota_id,
        "consulta_id": resolved_consulta_id,
        "nss": resolved_nss,
        "nombre": resolved_nombre,
        "servicio": service_norm,
        "diagnostico_principal": resolved_dx,
        "edad": resolved_edad,
        "sexo": resolved_sexo,
    }


def register_consulta_attention(db: Session, m: Any, *, consulta: Any) -> Dict[str, Any]:
    payload = {
        "consulta_id": getattr(consulta, "id", None),
        "nss": getattr(consulta, "nss", None),
        "nombre": getattr(consulta, "nombre", None),
        "edad": getattr(consulta, "edad", None),
        "sexo": getattr(consulta, "sexo", None),
        "diagnostico_principal": getattr(consulta, "diagnostico_principal", None),
        "cie10_codigo": getattr(consulta, "cie10", None) or "",
        "medico_responsable": getattr(consulta, "agregado_medico", None),
        "turno": "",
        "hgz": "",
        "fecha_atencion": getattr(consulta, "fecha_registro", None).isoformat() if getattr(consulta, "fecha_registro", None) else date.today().isoformat(),
        "nota_resumen": (
            f"Consulta externa registrada. Estatus de protocolo: {_safe_text(getattr(consulta, 'estatus_protocolo', 'N/E')).upper()}"
        ),
    }
    return register_service_attention(
        db,
        m,
        service_name="CONSULTA EXTERNA",
        payload=payload,
        request_user="consulta_form",
        attach_to_expediente=True,
    )


def get_consulta_externa_recommendations() -> Dict[str, List[str]]:
    return {
        "CONSULTA EXTERNA": [
            "Motivo de consulta y CIE-10 principal/secundario.",
            "Decisión clínica: manejo ambulatorio, referencia, programación quirúrgica o seguimiento.",
            "Fecha objetivo de control y criterio de alarma documentado.",
        ],
        "UROENDOSCOPIA": [
            "Indicación del procedimiento y tipo exacto de intervención.",
            "Hallazgos estructurados, toma de biopsia y eventos adversos inmediatos.",
            "Dispositivos colocados/retirados (catéter JJ, sonda) y plan postprocedimiento.",
        ],
        "LEOCH": [
            "Localización, tamaño (mm), densidad (UH) y lateralidad del lito.",
            "Parámetros de sesión (energía/disparos) y número de sesión.",
            "Resultado inmediato (fragmentación, JJ, complicaciones) y plan de seguimiento.",
        ],
    }


def get_consulta_externa_stats(db: Session) -> Dict[str, Any]:
    ensure_consulta_externa_schema(db)

    rows = db.execute(
        select(
            CONSULTA_EXTERNA_ATENCIONES.c.servicio,
            CONSULTA_EXTERNA_ATENCIONES.c.sexo,
            CONSULTA_EXTERNA_ATENCIONES.c.edad,
            CONSULTA_EXTERNA_ATENCIONES.c.diagnostico_principal,
            func.count().label("total"),
        ).group_by(
            CONSULTA_EXTERNA_ATENCIONES.c.servicio,
            CONSULTA_EXTERNA_ATENCIONES.c.sexo,
            CONSULTA_EXTERNA_ATENCIONES.c.edad,
            CONSULTA_EXTERNA_ATENCIONES.c.diagnostico_principal,
        )
    ).mappings().all()

    por_servicio: Dict[str, int] = {s: 0 for s in SERVICIOS_CONSULTA_EXTERNA}
    por_servicio_sexo: Dict[Tuple[str, str], int] = {}
    por_servicio_edad: Dict[Tuple[str, str], int] = {}
    por_servicio_dx: Dict[Tuple[str, str], int] = {}
    detalle: List[Dict[str, Any]] = []

    for row in rows:
        servicio = _safe_text(row.get("servicio")).upper() or "NO_REGISTRADO"
        sexo = _safe_text(row.get("sexo")).upper() or "NO_REGISTRADO"
        edad = _safe_int(row.get("edad"))
        edad_grupo = classify_age_group(edad)
        dx = _safe_text(row.get("diagnostico_principal")).upper() or "NO_REGISTRADO"
        total = int(row.get("total") or 0)

        por_servicio[servicio] = por_servicio.get(servicio, 0) + total
        por_servicio_sexo[(servicio, sexo)] = por_servicio_sexo.get((servicio, sexo), 0) + total
        por_servicio_edad[(servicio, edad_grupo)] = por_servicio_edad.get((servicio, edad_grupo), 0) + total
        por_servicio_dx[(servicio, dx)] = por_servicio_dx.get((servicio, dx), 0) + total
        detalle.append(
            {
                "servicio": servicio,
                "sexo": sexo,
                "edad_grupo": edad_grupo,
                "diagnostico": dx,
                "total": total,
            }
        )

    ordered_servicios = [s for s in SERVICIOS_CONSULTA_EXTERNA]
    extras = sorted([s for s in por_servicio.keys() if s not in ordered_servicios])
    ordered_servicios.extend(extras)

    por_servicio_rows = [(s, int(por_servicio.get(s, 0))) for s in ordered_servicios]
    por_servicio_sexo_rows = sorted(
        [{"servicio": k[0], "sexo": k[1], "total": v} for k, v in por_servicio_sexo.items()],
        key=lambda r: (ordered_servicios.index(r["servicio"]) if r["servicio"] in ordered_servicios else 999, -r["total"], r["sexo"]),
    )
    por_servicio_edad_rows = sorted(
        [{"servicio": k[0], "edad_grupo": k[1], "total": v} for k, v in por_servicio_edad.items()],
        key=lambda r: (ordered_servicios.index(r["servicio"]) if r["servicio"] in ordered_servicios else 999, -r["total"], r["edad_grupo"]),
    )
    por_servicio_dx_rows = sorted(
        [{"servicio": k[0], "diagnostico": k[1], "total": v} for k, v in por_servicio_dx.items()],
        key=lambda r: (ordered_servicios.index(r["servicio"]) if r["servicio"] in ordered_servicios else 999, -r["total"], r["diagnostico"]),
    )
    detalle_rows = sorted(
        detalle,
        key=lambda r: (ordered_servicios.index(r["servicio"]) if r["servicio"] in ordered_servicios else 999, -r["total"], r["sexo"], r["edad_grupo"], r["diagnostico"]),
    )

    return {
        "total_atenciones": int(sum(v for _, v in por_servicio_rows)),
        "por_servicio": por_servicio_rows,
        "por_servicio_sexo": por_servicio_sexo_rows,
        "por_servicio_edad": por_servicio_edad_rows,
        "por_servicio_diagnostico": por_servicio_dx_rows,
        "detalle": detalle_rows,
        "recommendations": get_consulta_externa_recommendations(),
    }


def consulta_externa_home_flow(request: Request) -> HTMLResponse:
    from app.core.app_context import main_proxy as m

    return m.render_template("consulta_externa_home.html", request=request)


def consulta_externa_uroendoscopia_form_flow(request: Request) -> HTMLResponse:
    from app.core.app_context import main_proxy as m
    return_to = _safe_return_to(request.query_params.get("return_to"), "/consulta_externa")

    return m.render_template(
        "consulta_externa_uroendoscopia.html",
        request=request,
        diagnosticos=DIAGNOSTICOS_BASE,
        procedimientos=PROCEDIMIENTOS_UROENDOSCOPIA,
        return_to=return_to,
    )


def consulta_externa_leoch_form_flow(request: Request) -> HTMLResponse:
    from app.core.app_context import main_proxy as m
    return_to = _safe_return_to(request.query_params.get("return_to"), "/consulta_externa")

    return m.render_template(
        "consulta_externa_leoch.html",
        request=request,
        diagnosticos=DIAGNOSTICOS_BASE,
        return_to=return_to,
    )


def consulta_externa_recetas_placeholder_flow(request: Request) -> HTMLResponse:
    from app.core.app_context import main_proxy as m

    return m.render_template(
        "consulta_externa_recetas.html",
        request=request,
        return_to=_safe_return_to(request.query_params.get("return_to"), "/consulta_externa"),
    )


async def consulta_externa_uroendoscopia_guardar_flow(request: Request, db: Session) -> HTMLResponse:
    from app.core.app_context import main_proxy as m

    form_data = await request.form()
    raw = {k: v for k, v in form_data.items()}
    m.validate_csrf(raw, request)
    return_to = _safe_return_to(raw.get("return_to"), "/consulta_externa")

    try:
        saved = register_service_attention(
            db,
            m,
            service_name="UROENDOSCOPIA",
            payload=raw,
            request_user="uroendoscopia_form",
            attach_to_expediente=True,
        )
        m.push_module_feedback(
            consulta_id=int(saved["consulta_id"]),
            modulo="consulta_externa_uroendoscopia",
            referencia_id=f"consulta_externa_atencion:{saved['attention_id']}",
            payload={
                "servicio": "UROENDOSCOPIA",
                "diagnostico_principal": saved.get("diagnostico_principal"),
            },
        )
    except Exception as exc:
        db.rollback()
        return HTMLResponse(
            f"<h1>Error al guardar Uroendoscopia</h1><p>{exc}</p><a href='/consulta_externa/uroendoscopia'>Volver</a>",
            status_code=400,
        )

    expediente_href = f"/expediente?consulta_id={saved['consulta_id']}"
    return HTMLResponse(
        "<h1>Atención Uroendoscopia guardada</h1>"
        f"<p>ID atención: {saved['attention_id']}</p>"
        f"<p><a href='{expediente_href}'>Ver expediente clínico único</a></p>"
        f"<p><a href='/consulta_externa/uroendoscopia?return_to={quote(return_to, safe='/_-')}'>Capturar otra atención</a></p>"
        f"<p><a href='{return_to}'>Volver</a></p>"
    )


async def consulta_externa_leoch_guardar_flow(request: Request, db: Session) -> HTMLResponse:
    from app.core.app_context import main_proxy as m

    form_data = await request.form()
    raw = {k: v for k, v in form_data.items()}
    m.validate_csrf(raw, request)
    return_to = _safe_return_to(raw.get("return_to"), "/consulta_externa")

    try:
        saved = register_service_attention(
            db,
            m,
            service_name="LEOCH",
            payload=raw,
            request_user="leoch_form",
            attach_to_expediente=True,
        )
        m.push_module_feedback(
            consulta_id=int(saved["consulta_id"]),
            modulo="consulta_externa_leoch",
            referencia_id=f"consulta_externa_atencion:{saved['attention_id']}",
            payload={
                "servicio": "LEOCH",
                "diagnostico_principal": saved.get("diagnostico_principal"),
            },
        )
    except Exception as exc:
        db.rollback()
        return HTMLResponse(
            f"<h1>Error al guardar LEOCH</h1><p>{exc}</p><a href='/consulta_externa/leoch'>Volver</a>",
            status_code=400,
        )

    expediente_href = f"/expediente?consulta_id={saved['consulta_id']}"
    return HTMLResponse(
        "<h1>Atención LEOCH guardada</h1>"
        f"<p>ID atención: {saved['attention_id']}</p>"
        f"<p><a href='{expediente_href}'>Ver expediente clínico único</a></p>"
        f"<p><a href='/consulta_externa/leoch?return_to={quote(return_to, safe='/_-')}'>Capturar otra atención</a></p>"
        f"<p><a href='{return_to}'>Volver</a></p>"
    )


def api_consulta_externa_servicios_stats_flow(db: Session) -> JSONResponse:
    return JSONResponse(content=get_consulta_externa_stats(db))


async def api_consulta_externa_recetas_ingest_flow(request: Request, db: Session) -> JSONResponse:
    from app.core.app_context import main_proxy as m

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "message": "Payload JSON inválido"})

    if not isinstance(payload, dict):
        return JSONResponse(status_code=400, content={"ok": False, "message": "Se esperaba objeto JSON"})

    try:
        saved = register_service_attention(
            db,
            m,
            service_name="RECETAS",
            payload=payload,
            request_user="recetas_external_api",
            attach_to_expediente=True,
        )
        m.push_module_feedback(
            consulta_id=int(saved["consulta_id"]),
            modulo="consulta_externa_recetas",
            referencia_id=f"consulta_externa_atencion:{saved['attention_id']}",
            payload={"servicio": "RECETAS", "diagnostico_principal": saved.get("diagnostico_principal")},
        )
        return JSONResponse(content={"ok": True, "saved": saved})
    except Exception as exc:
        db.rollback()
        return JSONResponse(status_code=400, content={"ok": False, "message": str(exc)})
