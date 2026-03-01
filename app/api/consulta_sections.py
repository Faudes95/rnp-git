from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.services.consulta_secciones_flow import (
    SECTION_LABELS,
    attach_draft_to_consulta,
    finalize_draft_endpoint,
    get_draft_identity_payload,
    get_draft_resumen,
    get_draft_section_payload,
    save_consulta_seccion,
    validate_consulta_seccion,
)
from app.services.consulta_aditivos_flow import (
    process_consulta_study_uploads,
    process_consulta_study_uploads_draft,
)
from app.services.event_log_flow import emit_event

router = APIRouter(prefix="/api/consulta/seccion", tags=["consulta", "captura-secciones"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


@router.post("/guardar", response_class=JSONResponse)
async def guardar_seccion(request: Request, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    body: Dict[str, Any] = await request.json()
    csrf_token = str(body.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    seccion_codigo = str(body.get("seccion_codigo") or "").strip()
    if seccion_codigo not in SECTION_LABELS:
        raise HTTPException(status_code=400, detail="Sección inválida")

    payload = body.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    validation = validate_consulta_seccion(
        db,
        draft_id=body.get("draft_id"),
        seccion_codigo=seccion_codigo,
        payload=payload,
    )
    if not validation.get("valid"):
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "detail": "Sección inválida",
                "validation": validation,
            },
        )

    saved = save_consulta_seccion(
        db,
        draft_id=validation.get("draft_id") or body.get("draft_id"),
        seccion_codigo=seccion_codigo,
        seccion_nombre=body.get("seccion_nombre"),
        payload=validation.get("normalized_payload") or payload,
        usuario=request.headers.get("X-User", "system"),
    )
    try:
        emit_event(
            db,
            module="consulta_secciones",
            event_type="SECCION_GUARDADA",
            entity="consulta_draft",
            entity_id=str(saved.get("draft_id") or ""),
            actor=request.headers.get("X-User", "system"),
            source_route=request.url.path,
            payload={
                "seccion_codigo": seccion_codigo,
                "seccion_nombre": saved.get("seccion_nombre"),
                "version": saved.get("version"),
                "quality_score": validation.get("quality_score"),
            },
            commit=True,
        )
    except Exception:
        db.rollback()
    resumen = get_draft_resumen(db, draft_id=saved["draft_id"])
    return JSONResponse(content={"ok": True, "saved": saved, "resumen": resumen})


@router.post("/guardar-con-archivos", response_class=JSONResponse)
async def guardar_seccion_con_archivos(request: Request, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    csrf_token = str(form.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    seccion_codigo = str(form.get("seccion_codigo") or "").strip()
    if seccion_codigo not in SECTION_LABELS:
        raise HTTPException(status_code=400, detail="Sección inválida")

    payload_raw = form.get("payload_json")
    payload: Dict[str, Any] = {}
    if payload_raw:
        try:
            import json as _json

            obj = _json.loads(str(payload_raw))
            if isinstance(obj, dict):
                payload = obj
        except Exception:
            payload = {}

    draft_id = str(form.get("draft_id") or "").strip() or None
    validation = validate_consulta_seccion(
        db,
        draft_id=draft_id,
        seccion_codigo=seccion_codigo,
        payload=payload,
    )
    if not validation.get("valid"):
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": "Sección inválida", "validation": validation},
        )

    saved = save_consulta_seccion(
        db,
        draft_id=validation.get("draft_id") or draft_id,
        seccion_codigo=seccion_codigo,
        seccion_nombre=form.get("seccion_nombre"),
        payload=validation.get("normalized_payload") or payload,
        usuario=request.headers.get("X-User", "system"),
    )

    file_items = []
    try:
        if hasattr(form, "getlist"):
            file_items = [f for f in (form.getlist("estudios_files") or []) if getattr(f, "filename", None)]
    except Exception:
        file_items = []

    upload_result: Dict[str, Any] = {"saved_count": 0, "warnings": []}
    if seccion_codigo == "8" and file_items:
        consulta_id = None
        did = str(saved.get("draft_id") or "")
        identity = get_draft_identity_payload(db, draft_id=did) if did else {}
        nss = str(identity.get("nss") or "").strip()
        nombre = str(identity.get("nombre") or "").strip().upper()
        if nss:
            q = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == nss)
            if nombre:
                q = q.filter(func.upper(m.ConsultaDB.nombre) == nombre)
            row = q.order_by(m.ConsultaDB.id.desc()).first()
            if row is not None:
                try:
                    consulta_id = int(row.id)
                except Exception:
                    consulta_id = None
        if consulta_id is not None:
            upload_result = await process_consulta_study_uploads(
                db,
                m,
                consulta_id=int(consulta_id),
                uploads=file_items,
                usuario=request.headers.get("X-User", "system"),
            )
        else:
            upload_result = await process_consulta_study_uploads_draft(
                db,
                m,
                draft_id=did,
                uploads=file_items,
                usuario=request.headers.get("X-User", "system"),
            )
            upload_result.setdefault("warnings", []).append(
                "Archivos guardados en borrador metadata; se vincularán al expediente al finalizar y asociar la consulta."
            )

    try:
        emit_event(
            db,
            module="consulta_secciones",
            event_type="SECCION_GUARDADA",
            entity="consulta_draft",
            entity_id=str(saved.get("draft_id") or ""),
            actor=request.headers.get("X-User", "system"),
            source_route=request.url.path,
            payload={
                "seccion_codigo": seccion_codigo,
                "seccion_nombre": saved.get("seccion_nombre"),
                "version": saved.get("version"),
                "quality_score": validation.get("quality_score"),
                "files_saved": int(upload_result.get("saved_count") or 0),
            },
            commit=True,
        )
    except Exception:
        db.rollback()
    resumen = get_draft_resumen(db, draft_id=saved["draft_id"])
    return JSONResponse(content={"ok": True, "saved": saved, "resumen": resumen, "upload": upload_result})


@router.post("/validar", response_class=JSONResponse)
async def validar_seccion(request: Request, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    body: Dict[str, Any] = await request.json()
    csrf_token = str(body.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    seccion_codigo = str(body.get("seccion_codigo") or "").strip()
    if seccion_codigo not in SECTION_LABELS:
        raise HTTPException(status_code=400, detail="Sección inválida")

    payload = body.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    result = validate_consulta_seccion(
        db,
        draft_id=body.get("draft_id"),
        seccion_codigo=seccion_codigo,
        payload=payload,
    )
    return JSONResponse(content={"ok": True, "validation": result})


@router.get("/resumen/{draft_id}", response_class=JSONResponse)
def seccion_resumen(draft_id: str, db: Session = Depends(_get_db)):
    try:
        resumen = get_draft_resumen(db, draft_id=draft_id)
        return JSONResponse(content=resumen)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/cargar/{draft_id}/{seccion_codigo}", response_class=JSONResponse)
def seccion_cargar(draft_id: str, seccion_codigo: str, db: Session = Depends(_get_db)):
    try:
        data = get_draft_section_payload(db, draft_id=draft_id, seccion_codigo=seccion_codigo)
        return JSONResponse(content=data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/finalizar", response_class=JSONResponse)
async def seccion_finalizar(request: Request, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    body: Dict[str, Any] = await request.json()
    csrf_token = str(body.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    draft_id = str(body.get("draft_id") or "").strip()
    consulta_id = body.get("consulta_id")
    if not draft_id or consulta_id is None:
        raise HTTPException(status_code=400, detail="draft_id y consulta_id requeridos")
    try:
        consulta_id_int = int(consulta_id)
    except Exception:
        raise HTTPException(status_code=400, detail="consulta_id inválido")

    data = finalize_draft_endpoint(db, draft_id=draft_id, consulta_id=consulta_id_int)
    try:
        emit_event(
            db,
            module="consulta_secciones",
            event_type="DRAFT_FINALIZADO",
            entity="consulta",
            entity_id=str(consulta_id_int),
            consulta_id=consulta_id_int,
            actor=request.headers.get("X-User", "system"),
            source_route=request.url.path,
            payload={"draft_id": draft_id},
            commit=True,
        )
    except Exception:
        db.rollback()
    return JSONResponse(content={"ok": True, "result": data})


# helper aditivo para uso interno cuando se requiera link desde otros flujos

def attach_draft(db: Session, *, draft_id: str, consulta_id: int) -> Dict[str, Any]:
    return attach_draft_to_consulta(db, draft_id=draft_id, consulta_id=consulta_id)


# ── Nuevos endpoints: Guardar en Expediente Clínico Único e Imprimir ──

metadata_router = APIRouter(prefix="/api/consulta/metadata", tags=["consulta", "metadata-expediente"])


def _get_db_meta():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


@metadata_router.post("/guardar-expediente", response_class=JSONResponse)
async def guardar_metadata_en_expediente(request: Request, db: Session = Depends(_get_db_meta)):
    """
    Consolida todas las secciones del draft metadata y crea/actualiza
    la consulta en el Expediente Clínico Único.
    """
    from app.core.app_context import main_proxy as m

    body: Dict[str, Any] = await request.json()
    csrf_token = str(body.get("csrf_token") or "")
    m.validate_csrf({"csrf_token": csrf_token}, request)

    draft_id_raw = str(body.get("draft_id") or "").strip()
    if not draft_id_raw:
        raise HTTPException(status_code=400, detail="draft_id requerido")

    # Obtener resumen del draft
    resumen = get_draft_resumen(db, draft_id=draft_id_raw)
    secciones = resumen.get("secciones") or []
    if not secciones:
        raise HTTPException(status_code=400, detail="No hay secciones guardadas en este borrador")

    # Consolidar payloads de todas las secciones
    consolidated: Dict[str, Any] = {}
    for sec in secciones:
        code = str(sec.get("seccion_codigo") or "")
        section_data = get_draft_section_payload(db, draft_id=draft_id_raw, seccion_codigo=code)
        payload = section_data.get("payload") or {}
        consolidated.update(payload)

    # Verificar datos mínimos de identidad (sección 1)
    nss = str(consolidated.get("nss") or "").strip()
    nombre = str(consolidated.get("nombre") or "").strip().upper()
    if not nss and not nombre:
        raise HTTPException(status_code=400, detail="Se requiere al menos NSS o nombre del paciente")

    # Buscar consulta existente o crear nueva
    consulta_id = None
    existing = None
    if nss:
        q = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == nss)
        if nombre:
            q = q.filter(func.upper(m.ConsultaDB.nombre) == nombre)
        existing = q.order_by(m.ConsultaDB.id.desc()).first()

    if existing:
        # Actualizar consulta existente con nuevos datos
        consulta_id = int(existing.id)
        for key, val in consolidated.items():
            if val is not None and hasattr(existing, key):
                try:
                    setattr(existing, key, val)
                except Exception:
                    pass
        try:
            db.commit()
            db.refresh(existing)
        except Exception:
            db.rollback()
            raise HTTPException(status_code=500, detail="Error al actualizar expediente")
    else:
        # Crear nueva consulta
        safe_fields = {}
        consulta_fields = set()
        try:
            consulta_fields = set(m.ConsultaDB.__table__.columns.keys())
        except Exception:
            consulta_fields = set()

        for key, val in consolidated.items():
            if key in consulta_fields and val is not None:
                safe_fields[key] = val

        safe_fields.setdefault("fecha_registro", date.today())
        try:
            new_consulta = m.ConsultaDB(**safe_fields)
            db.add(new_consulta)
            db.commit()
            db.refresh(new_consulta)
            consulta_id = int(new_consulta.id)
        except Exception as exc:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Error al crear consulta: {str(exc)[:200]}")

    # Vincular draft con la consulta
    if consulta_id:
        attach_draft_to_consulta(db, draft_id=draft_id_raw, consulta_id=consulta_id)

    try:
        emit_event(
            db,
            module="consulta_metadata",
            event_type="EXPEDIENTE_GUARDADO",
            entity="consulta",
            entity_id=str(consulta_id),
            actor=request.headers.get("X-User", "system"),
            source_route=request.url.path,
            payload={"draft_id": draft_id_raw, "consulta_id": consulta_id, "updated": existing is not None},
            commit=True,
        )
    except Exception:
        db.rollback()

    return JSONResponse(content={
        "ok": True,
        "consulta_id": consulta_id,
        "draft_id": draft_id_raw,
        "updated_existing": existing is not None,
        "message": f"Consulta {'actualizada' if existing else 'creada'} exitosamente en expediente #{consulta_id}"
    })


@metadata_router.get("/imprimir", response_class=HTMLResponse)
async def imprimir_nota_metadata(request: Request, db: Session = Depends(_get_db_meta)):
    """
    Genera una vista de impresión con formato institucional IMSS
    de la consulta guardada desde metadata.
    """
    from app.core.app_context import main_proxy as m

    consulta_id_raw = request.query_params.get("consulta_id")
    draft_id_raw = request.query_params.get("draft_id", "").strip()

    data: Dict[str, Any] = {}
    consulta_id = None

    if consulta_id_raw:
        try:
            consulta_id = int(consulta_id_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="consulta_id inválido")
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == consulta_id).first()
        if not consulta:
            raise HTTPException(status_code=404, detail="Consulta no encontrada")
        # Extraer todos los campos de la consulta
        for col in m.ConsultaDB.__table__.columns:
            val = getattr(consulta, col.name, None)
            if val is not None:
                data[col.name] = val
    elif draft_id_raw:
        resumen = get_draft_resumen(db, draft_id=draft_id_raw)
        secciones = resumen.get("secciones") or []
        for sec in secciones:
            code = str(sec.get("seccion_codigo") or "")
            section_data = get_draft_section_payload(db, draft_id=draft_id_raw, seccion_codigo=code)
            payload = section_data.get("payload") or {}
            data.update(payload)
    else:
        raise HTTPException(status_code=400, detail="Se requiere consulta_id o draft_id")

    # Generar HTML de impresión institucional
    html = _generar_html_impresion(data, consulta_id)
    return HTMLResponse(content=html)


def _generar_html_impresion(data: Dict[str, Any], consulta_id: int | None) -> str:
    """Genera HTML con formato institucional IMSS para impresión."""
    from datetime import datetime

    def v(key: str, default: str = "N/E") -> str:
        val = data.get(key)
        if val is None or str(val).strip() == "":
            return default
        return str(val).strip()

    def json_list(key: str) -> list:
        raw = data.get(key)
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    fecha_impresion = datetime.now().strftime("%d/%m/%Y %H:%M")
    consulta_label = f"Consulta #{consulta_id}" if consulta_id else "Borrador"

    # Antecedentes heredofamiliares
    ahf_rows = json_list("ahf_json")
    ahf_html = ""
    if ahf_rows:
        ahf_html = "<ul>" + "".join(
            f"<li><strong>{r.get('linea','')}</strong>: {r.get('padecimiento','')} — {r.get('estatus','')}</li>"
            for r in ahf_rows
        ) + "</ul>"
    else:
        ahf_html = f"<p>{v('ahf_status', 'No referidos')}</p>"

    # APP
    app_rows = json_list("app_patologias_json")
    app_html = ""
    if app_rows:
        app_html = "<ul>" + "".join(
            f"<li><strong>{r.get('patologia','')}</strong> — Evolución: {r.get('evolucion','')} — Tx: {r.get('tratamiento','')}</li>"
            for r in app_rows
        ) + "</ul>"
    else:
        app_html = f"<p>{v('app_status', 'Negados')}</p>"

    # AQx
    aqx_rows = json_list("aqx_json")
    aqx_html = ""
    if aqx_rows:
        aqx_html = "<ul>" + "".join(
            f"<li>{r.get('fecha','')} — <strong>{r.get('procedimiento','')}</strong> — {r.get('hallazgos','')}</li>"
            for r in aqx_rows
        ) + "</ul>"
    else:
        aqx_html = f"<p>{v('aqx_status', 'Negados')}</p>"

    # Alergias
    alergias_rows = json_list("alergias_json")
    alergias_html = ""
    if alergias_rows:
        alergias_html = "<ul>" + "".join(
            f"<li><strong>{r.get('alergeno','')}</strong> — Reacción: {r.get('reaccion','')}</li>"
            for r in alergias_rows
        ) + "</ul>"
    else:
        alergias_html = f"<p>{v('alergias_status', 'Negadas')}</p>"

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Nota Médica — {consulta_label}</title>
    <style>
        @page {{ size: letter; margin: 1.5cm 2cm; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Arial', sans-serif; font-size: 11pt; color: #1a1a1a; line-height: 1.45; }}
        .header {{ display: flex; align-items: center; justify-content: space-between; border-bottom: 3px solid #13322B; padding-bottom: 10px; margin-bottom: 14px; }}
        .header-logo {{ display: flex; align-items: center; gap: 10px; }}
        .logo-imss {{ width: 60px; height: 60px; background: #13322B; border-radius: 8px; display: flex; align-items: center; justify-content: center; color: #fff; font-weight: 900; font-size: 14px; }}
        .header-text h1 {{ font-size: 13pt; color: #13322B; margin-bottom: 2px; }}
        .header-text p {{ font-size: 9pt; color: #555; }}
        .header-right {{ text-align: right; font-size: 9pt; color: #666; }}
        .section {{ margin-bottom: 12px; page-break-inside: avoid; }}
        .section-title {{ background: #13322B; color: #fff; padding: 5px 10px; font-size: 10pt; font-weight: 700; border-radius: 4px 4px 0 0; text-transform: uppercase; letter-spacing: 0.5px; }}
        .section-body {{ border: 1px solid #d0d0d0; border-top: none; padding: 8px 10px; border-radius: 0 0 4px 4px; }}
        .row {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 4px; }}
        .field {{ flex: 1; min-width: 140px; }}
        .field-label {{ font-size: 8pt; color: #666; text-transform: uppercase; font-weight: 700; }}
        .field-value {{ font-size: 10pt; font-weight: 600; color: #1a1a1a; }}
        .section-body ul {{ margin-left: 18px; margin-top: 4px; }}
        .section-body li {{ margin-bottom: 3px; font-size: 10pt; }}
        .signature-area {{ margin-top: 40px; display: flex; justify-content: space-between; }}
        .signature-line {{ width: 40%; text-align: center; border-top: 1px solid #333; padding-top: 6px; font-size: 9pt; }}
        .footer {{ margin-top: 20px; border-top: 2px solid #B38E5D; padding-top: 8px; font-size: 8pt; color: #888; text-align: center; }}
        @media print {{
            body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
            .no-print {{ display: none !important; }}
        }}
        .print-toolbar {{ background: #f0f0f0; padding: 10px 20px; text-align: center; border-bottom: 1px solid #ddd; }}
        .print-toolbar button {{ background: #13322B; color: #fff; border: none; padding: 10px 24px; border-radius: 6px; font-weight: 700; cursor: pointer; font-size: 13px; }}
    </style>
</head>
<body>
    <div class="print-toolbar no-print">
        <button onclick="window.print()">Imprimir Nota Médica</button>
        <button onclick="window.close()" style="background:#666;margin-left:10px;">Cerrar</button>
    </div>

    <div style="max-width:720px;margin:auto;padding:20px;">
        <div class="header">
            <div class="header-logo">
                <div class="logo-imss">IMSS</div>
                <div class="header-text">
                    <h1>Instituto Mexicano del Seguro Social</h1>
                    <p>CMN Raza — Servicio de Urología</p>
                    <p>Nota Médica de Consulta Externa</p>
                </div>
            </div>
            <div class="header-right">
                <div><strong>{consulta_label}</strong></div>
                <div>Fecha: {v('fecha_registro', fecha_impresion)}</div>
                <div>Impreso: {fecha_impresion}</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Ficha de Identificación</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">NSS</div><div class="field-value">{v('nss')}</div></div>
                    <div class="field"><div class="field-label">CURP</div><div class="field-value">{v('curp')}</div></div>
                    <div class="field"><div class="field-label">Nombre</div><div class="field-value">{v('nombre')}</div></div>
                </div>
                <div class="row">
                    <div class="field"><div class="field-label">Edad</div><div class="field-value">{v('edad')}</div></div>
                    <div class="field"><div class="field-label">Sexo</div><div class="field-value">{v('sexo')}</div></div>
                    <div class="field"><div class="field-label">Fecha Nacimiento</div><div class="field-value">{v('fecha_nacimiento')}</div></div>
                    <div class="field"><div class="field-label">Tipo Sangre</div><div class="field-value">{v('tipo_sangre')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Somatometría y Signos Vitales</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">Peso</div><div class="field-value">{v('peso')} kg</div></div>
                    <div class="field"><div class="field-label">Talla</div><div class="field-value">{v('talla')} m</div></div>
                    <div class="field"><div class="field-label">IMC</div><div class="field-value">{v('imc')}</div></div>
                    <div class="field"><div class="field-label">T/A</div><div class="field-value">{v('ta')}</div></div>
                    <div class="field"><div class="field-label">FC</div><div class="field-value">{v('fc')}</div></div>
                    <div class="field"><div class="field-label">Temp</div><div class="field-value">{v('temp')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Antecedentes Heredofamiliares</div>
            <div class="section-body">{ahf_html}</div>
        </div>

        <div class="section">
            <div class="section-title">Antecedentes Personales Patológicos</div>
            <div class="section-body">{app_html}</div>
        </div>

        <div class="section">
            <div class="section-title">Antecedentes Quirúrgicos</div>
            <div class="section-body">{aqx_html}</div>
        </div>

        <div class="section">
            <div class="section-title">Alergias</div>
            <div class="section-body">{alergias_html}</div>
        </div>

        <div class="section">
            <div class="section-title">Padecimiento Actual</div>
            <div class="section-body"><p>{v('padecimiento_actual', 'Sin datos')}</p></div>
        </div>

        <div class="section">
            <div class="section-title">Exploración Física</div>
            <div class="section-body"><p>{v('exploracion_fisica', 'Sin datos')}</p></div>
        </div>

        <div class="section">
            <div class="section-title">Diagnóstico</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">Diagnóstico Principal</div><div class="field-value">{v('diagnostico_principal')}</div></div>
                    <div class="field"><div class="field-label">CIE-11</div><div class="field-value">{v('cie11_codigo')}</div></div>
                </div>
                <div class="row">
                    <div class="field"><div class="field-label">Protocolo</div><div class="field-value">{v('protocolo')}</div></div>
                    <div class="field"><div class="field-label">Subprotocolo</div><div class="field-value">{v('subprotocolo')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Plan de Tratamiento</div>
            <div class="section-body"><p>{v('plan_tratamiento', 'Sin datos')}</p></div>
        </div>

        <div class="signature-area">
            <div class="signature-line">Nombre y firma del médico</div>
            <div class="signature-line">Matrícula / Cédula profesional</div>
        </div>

        <div class="footer">
            IMSS — CMN Raza — Servicio de Urología — Registro Nacional de Pacientes (RNP) — Documento generado automáticamente
        </div>
    </div>
</body>
</html>"""
