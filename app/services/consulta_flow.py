"""Flujos legacy de consulta clínica extraídos de main.py (refactor aditivo)."""

from __future__ import annotations

import json
from datetime import date

from fastapi import Request
from fastapi.responses import HTMLResponse
from starlette.datastructures import UploadFile as StarletteUploadFile
from sqlalchemy.orm import Session

from app.ai_agents.vector_store import sync_consulta_embedding_vector
from app.services.consulta_aditivos_flow import (
    evaluate_consulta_clinical_alerts,
    persist_consulta_multivalor,
    process_consulta_study_uploads,
)
from app.services.consulta_secciones_flow import attach_draft_to_consulta


def _safe_return_to(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value).strip()
    if not v.startswith("/") or v.startswith("//"):
        return None
    if "javascript:" in v.lower():
        return None
    return v


async def guardar_consulta_completa_flow(request: Request, db: Session):
    """Mantiene comportamiento legacy de /guardar_consulta_completa."""
    from app.core.app_context import main_proxy as m

    form_data = await request.form()
    raw_dict = {}
    estudios_uploads = []
    for key, value in form_data.multi_items():
        if isinstance(value, StarletteUploadFile):
            if key == "estudios_files":
                estudios_uploads.append(value)
            continue
        raw_dict[key] = value
    data_dict = m.svc_preparar_payload_consulta(
        raw_form=raw_dict,
        validate_csrf_fn=m.validate_csrf,
        request=request,
        normalize_form_data_fn=m.normalize_form_data,
        apply_aliases_fn=m.apply_aliases,
        aplicar_derivaciones_fn=m.aplicar_derivaciones,
        expected_fields=m.ConsultaCreate.__fields__.keys(),
    )

    try:
        consulta_validada = m.ConsultaCreate(**data_dict)
        m.enforce_required_fields(consulta_validada)
    except m.ValidationError as e:
        errores = "<br>".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
        return HTMLResponse(
            content=f"<h1>Error de validación</h1><p>{errores}</p><a href='/consulta'>Regresar al formulario</a>",
            status_code=400,
        )
    except ValueError as e:
        return HTMLResponse(
            content=f"<h1>Error de validación</h1><p>{e}</p><a href='/consulta'>Regresar al formulario</a>",
            status_code=400,
        )

    protocolo_detalles = m.extraer_protocolo_detalles(data_dict)
    inconsistencias = m.detectar_inconsistencias(data_dict)
    alertas_aditivas = evaluate_consulta_clinical_alerts(raw_form=raw_dict, normalized_payload=data_dict)
    if alertas_aditivas:
        inconsistencias.extend(alertas_aditivas)
        if isinstance(protocolo_detalles, dict):
            protocolo_detalles["alertas_aditivas"] = alertas_aditivas
    nota_soap = m.generar_nota_soap(data_dict)
    note_text = m.build_embedding_text(data_dict)
    embedding = None if m.ASYNC_EMBEDDINGS else m.compute_embedding(note_text)

    db_consulta = m.ConsultaDB(
        fecha_registro=date.today(),
        **consulta_validada.dict(exclude_unset=True),
        protocolo_detalles=protocolo_detalles,
        embedding_diagnostico=embedding,
        nota_soap_auto=json.dumps(nota_soap, ensure_ascii=False),
        inconsistencias="; ".join(inconsistencias) if inconsistencias else None,
    )
    try:
        db.add(db_consulta)
        db.commit()
        db.refresh(db_consulta)
    except Exception:
        db.rollback()
        return HTMLResponse(
            content="<h1>Error al guardar el expediente</h1><p>Intente nuevamente.</p><a href='/consulta'>Regresar al formulario</a>",
            status_code=500,
        )

    try:
        from app.services.master_identity_flow import upsert_master_identity

        upsert_master_identity(
            db,
            nss=db_consulta.nss,
            curp=db_consulta.curp,
            nombre=db_consulta.nombre,
            sexo=db_consulta.sexo,
            consulta_id=int(db_consulta.id),
            source_table="consultas",
            source_pk=int(db_consulta.id),
            module="consulta_externa",
            fecha_evento=db_consulta.fecha_registro,
            payload={
                "diagnostico_principal": db_consulta.diagnostico_principal,
                "estatus_protocolo": db_consulta.estatus_protocolo,
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    warnings_aditivas = []
    try:
        persist_consulta_multivalor(
            db,
            consulta_id=int(db_consulta.id),
            raw_form=raw_dict,
            imc_value=getattr(db_consulta, "imc", None),
        )
    except Exception as exc:
        db.rollback()
        warnings_aditivas.append(f"No se pudo guardar el bloque multivalor aditivo: {exc}")

    if estudios_uploads:
        try:
            parsed_payload = await process_consulta_study_uploads(
                db,
                m,
                consulta_id=int(db_consulta.id),
                uploads=estudios_uploads,
                usuario=request.headers.get("X-User", "system"),
            )
            parsed_text = (parsed_payload or {}).get("parsed_text") or ""
            if parsed_text:
                current_text = (db_consulta.estudios_hallazgos or "").strip()
                append_block = f"[AUTO-PARSEO ESTUDIOS]\n{parsed_text}".strip()
                db_consulta.estudios_hallazgos = f"{current_text}\n\n{append_block}".strip() if current_text else append_block
                db.commit()
            warnings_aditivas.extend((parsed_payload or {}).get("warnings") or [])
        except Exception as exc:
            db.rollback()
            warnings_aditivas.append(f"No se pudieron procesar estudios adjuntos: {exc}")

    if embedding:
        # Sincroniza columna vectorial pgvector sin interferir con flujo clínico base.
        sync_consulta_embedding_vector(
            db,
            consulta_id=int(db_consulta.id),
            embedding=embedding,
            commit=True,
        )

    try:
        from app.services.expediente_plus_flow import upsert_enriched_record

        upsert_enriched_record(
            db,
            consulta=db_consulta,
            raw_form=raw_dict,
            source="consulta_form",
        )
    except Exception:
        # Flujo aditivo: nunca bloquea el guardado clínico base.
        db.rollback()

    return_to = _safe_return_to(raw_dict.get("return_to"))
    nombre = consulta_validada.nombre.upper() if consulta_validada.nombre else "PACIENTE"
    diag = consulta_validada.diagnostico_principal.upper() if consulta_validada.diagnostico_principal else ""
    estatus = consulta_validada.estatus_protocolo or ""
    msg = m.svc_mensaje_estatus_consulta(estatus)

    if m.ASYNC_EMBEDDINGS:
        m.enqueue_embedding(db_consulta.id, note_text)

    m.push_module_feedback(
        consulta_id=db_consulta.id,
        modulo="consulta_externa",
        referencia_id=f"consulta:{db_consulta.id}",
        payload={
            "diagnostico_principal": db_consulta.diagnostico_principal,
            "estatus_protocolo": db_consulta.estatus_protocolo,
            "fecha_registro": str(db_consulta.fecha_registro) if db_consulta.fecha_registro else None,
        },
    )
    try:
        from app.services.consulta_externa_flow import register_consulta_attention

        register_consulta_attention(db, m, consulta=db_consulta)
    except Exception:
        # Mantener guardado clínico base intacto si falla el enriquecimiento.
        db.rollback()

    if (db_consulta.estatus_protocolo or "").lower() == "completo":
        m.registrar_evento_flujo_quirurgico(
            consulta_id=db_consulta.id,
            evento="PENDIENTE_PROGRAMAR",
            estatus="PENDIENTE",
            edad=db_consulta.edad,
            sexo=db_consulta.sexo,
            nss=db_consulta.nss,
            diagnostico=db_consulta.diagnostico_principal,
            metadata_json={
                "origen": "consulta_externa",
                "estatus_protocolo": db_consulta.estatus_protocolo,
            },
        )

    # El draft_id es opaco; no forzamos formato de ruta.
    raw_draft_id = str(raw_dict.get("consulta_draft_id") or "").strip()
    if raw_draft_id:
        try:
            attach_draft_to_consulta(db, draft_id=raw_draft_id, consulta_id=int(db_consulta.id))
        except Exception:
            db.rollback()

    return m.render_template(
        m.CONFIRMACION_TEMPLATE,
        request=request,
        nombre=nombre,
        diag=diag,
        msg_estatus=msg,
        inconsistencias=inconsistencias + warnings_aditivas,
        return_to=return_to,
    )
