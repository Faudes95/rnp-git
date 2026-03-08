from __future__ import annotations

import json
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.busqueda_flow import busqueda_flow, busqueda_semantica_flow
from app.services.expediente_flow import ver_expediente_flow
from app.services.fhir_flow import (
    fhir_condition_search_flow,
    fhir_encounter_search_flow,
    fhir_expediente_flow,
    fhir_export_cohort_flow,
    fhir_observation_search_flow,
    fhir_patient_by_curp_flow,
    fhir_procedure_search_flow,
)
from app.services.interconexion_flow import interconexion_consulta_flow
from app.services.quirofano_flow import (
    cancelar_programacion_flow,
    guardar_postquirurgica_flow,
    guardar_quirofano_flow,
    listar_quirofanos_flow,
    render_postquirurgica_flow,
)
from app.services.quirofano_waitlist_flow import (
    render_waitlist_ingreso_flow,
    render_waitlist_lista_flow,
    save_waitlist_ingreso_flow,
)

router = APIRouter(tags=["legacy-web"])


def _get_db():
    yield from m.get_db()


def _get_surgical_db():
    yield from m.get_surgical_db()


@router.get("/quirofano", response_class=HTMLResponse)
async def quirofano_home(request: Request):
    return m.render_template(m.QUIROFANO_HOME_TEMPLATE, request=request)


@router.get("/quirofano/programada", response_class=HTMLResponse)
async def quirofano_programada_home(request: Request):
    return m.render_template(m.QUIROFANO_PROGRAMADA_TEMPLATE, request=request)


@router.get("/quirofano/programada/lista", response_class=HTMLResponse)
@router.get("/quirofano/lista", response_class=HTMLResponse)
async def listar_quirofanos(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
    campo: Optional[str] = None,
    q: Optional[str] = None,
):
    return await listar_quirofanos_flow(request, db, sdb, campo=campo, q=q)


@router.get("/quirofano/lista-espera-programacion", response_class=HTMLResponse)
async def listar_espera_programacion_quirurgica(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
    q: Optional[str] = None,
    mostrar_todos: Optional[int] = 0,
):
    _ = mostrar_todos  # compat: parámetro legado no requerido por el nuevo renderer.
    return await render_waitlist_lista_flow(
        request,
        sdb,
        q=q,
    )


@router.get("/quirofano/lista-espera")
async def quirofano_lista_espera_legacy_alias():
    # Compatibilidad con enlaces legacy: mantener navegación estable.
    return RedirectResponse(url="/quirofano/lista-espera-programacion", status_code=307)


@router.get("/quirofano/lista-espera/ingresar", response_class=HTMLResponse)
async def quirofano_lista_espera_ingresar(
    request: Request,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    saved: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_waitlist_ingreso_flow(
        request,
        db,
        sdb,
        consulta_id=consulta_id,
        nss=nss,
        saved=saved or "",
        error=error or "",
    )


@router.post("/quirofano/lista-espera/ingresar", response_class=HTMLResponse)
async def guardar_quirofano_lista_espera_ingresar(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await save_waitlist_ingreso_flow(request, db, sdb)


@router.get("/quirofano/programada/postquirurgica", response_class=HTMLResponse)
async def quirofano_postquirurgica(
    request: Request,
    surgical_programacion_id: Optional[int] = None,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_postquirurgica_flow(
        request,
        db,
        sdb,
        selected_programacion_id=surgical_programacion_id,
    )


@router.get("/quirofano/programada/{surgical_programacion_id}/postquirurgica", response_class=HTMLResponse)
async def quirofano_postquirurgica_direct(
    request: Request,
    surgical_programacion_id: int,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_postquirurgica_flow(
        request,
        db,
        sdb,
        selected_programacion_id=surgical_programacion_id,
    )


@router.post("/quirofano/programada/postquirurgica", response_class=HTMLResponse)
async def guardar_postquirurgica(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await guardar_postquirurgica_flow(request, db, sdb)


@router.post("/quirofano/programada/cancelar", response_class=HTMLResponse)
async def cancelar_quirofano_programada(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await cancelar_programacion_flow(
        request,
        db,
        sdb,
        urgencias_only=False,
        back_href="/quirofano/programada/lista",
    )


@router.get("/quirofano/nuevo", response_class=HTMLResponse)
async def nuevo_quirofano_form(request: Request):
    return m.render_template(
        m.QUIROFANO_NUEVO_TEMPLATE,
        request=request,
        sexo_options=m.QUIROFANO_SEXOS,
        patologia_options=m.QUIROFANO_PATOLOGIAS,
        patologia_options_json=json.dumps(m.QUIROFANO_PATOLOGIAS, ensure_ascii=False),
        patologia_onco_json=json.dumps(sorted(m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS), ensure_ascii=False),
        patologia_litiasis_json=json.dumps(sorted(m.QUIROFANO_PATOLOGIAS_LITIASIS), ensure_ascii=False),
        procedimiento_options=m.QUIROFANO_PROCEDIMIENTOS,
        procedimientos_abordaje=sorted(m.QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE),
        procedimientos_abiertos=sorted(m.QUIROFANO_PROCEDIMIENTOS_ABIERTOS),
        insumo_options=m.QUIROFANO_INSUMOS,
        patologias_onco=sorted(m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS),
        patologias_litiasis=sorted(m.QUIROFANO_PATOLOGIAS_LITIASIS),
    )


@router.post("/quirofano/nuevo", response_class=HTMLResponse)
async def guardar_quirofano(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await guardar_quirofano_flow(request, db)


@router.get("/expediente", response_class=HTMLResponse)
async def ver_expediente(
    request: Request,
    consulta_id: Optional[int] = None,
    curp: Optional[str] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await ver_expediente_flow(
        request,
        consulta_id,
        curp,
        db,
        nss=nss,
        nombre=nombre,
        q=q,
    )


@router.get("/fhir/expediente", response_class=JSONResponse)
async def fhir_expediente(
    consulta_id: Optional[int] = None,
    curp: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await fhir_expediente_flow(consulta_id, curp, db)


@router.get("/fhir/Patient/{curp}", response_class=JSONResponse)
async def fhir_patient_by_curp(curp: str, db: Session = Depends(_get_db)):
    return await fhir_patient_by_curp_flow(curp, db)


@router.get("/fhir/Condition", response_class=JSONResponse)
async def fhir_condition_search(subject: str, db: Session = Depends(_get_db)):
    return await fhir_condition_search_flow(subject, db)


@router.get("/fhir/export/cohort", response_class=JSONResponse)
async def fhir_export_cohort(
    diagnostico: Optional[str] = None,
    fecha_desde: Optional[date] = None,
    fecha_hasta: Optional[date] = None,
    db: Session = Depends(_get_db),
):
    return await fhir_export_cohort_flow(diagnostico, fecha_desde, fecha_hasta, db)


@router.get("/fhir/Procedure", response_class=JSONResponse)
async def fhir_procedure_search(
    subject: Optional[str] = None,
    date: Optional[str] = None,
    code: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await fhir_procedure_search_flow(subject, date, code, status, db, sdb)


@router.get("/fhir/Observation", response_class=JSONResponse)
async def fhir_observation_search(
    subject: Optional[str] = None,
    code: Optional[str] = None,
    date: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await fhir_observation_search_flow(subject, code, date, db)


@router.get("/fhir/Encounter", response_class=JSONResponse)
async def fhir_encounter_search(
    subject: Optional[str] = None,
    date: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await fhir_encounter_search_flow(subject, date, db)


@router.get("/api/interconexion/consulta/{consulta_id}", response_class=JSONResponse)
async def interconexion_consulta(
    consulta_id: int,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return await interconexion_consulta_flow(consulta_id, db, sdb)


@router.get("/busqueda", response_class=HTMLResponse)
async def busqueda(
    request: Request,
    q: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await busqueda_flow(request, q, db)


@router.get("/busqueda_semantica", response_class=HTMLResponse)
async def busqueda_semantica(
    request: Request,
    q: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await busqueda_semantica_flow(request, q, db)
