from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.jefaturas.central import (
    assign_central_exam_from_request,
    create_central_exam_template_from_request,
    create_central_case_from_request,
    create_central_incidence_from_request,
    render_jefatura_central_cases_flow,
    render_jefatura_central_exam_assignment_flow,
    render_jefatura_central_exams_flow,
    render_jefatura_central_home_flow,
    render_jefatura_central_incidences_flow,
    render_jefatura_central_insumos_flow,
    render_resident_exam_flow,
    submit_resident_exam_response,
    update_central_case_from_request,
    update_central_incidence_from_request,
)
from app.services.jefaturas.urologia import (
    resident_profile_photo_response,
    render_jefatura_urologia_home_flow,
    render_jefatura_urologia_module_flow,
    render_jefatura_urologia_programa_submodule_flow,
    render_jefatura_urologia_residente_profile_flow,
    update_resident_profile_from_request,
)

router = APIRouter(tags=["jefatura-web"])


def _get_surgical_db():
    yield from m.get_surgical_db()


@router.get("/jefatura-urologia", response_class=HTMLResponse)
async def jefatura_urologia_home(request: Request):
    return await render_jefatura_urologia_home_flow(request)


@router.get("/jefatura-urologia/central", response_class=HTMLResponse)
async def jefatura_urologia_central(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_central_home_flow(request, sdb)


@router.get("/jefatura-urologia/reporte-jefatura")
async def jefatura_urologia_central_legacy_alias():
    return RedirectResponse(url="/jefatura-urologia/central", status_code=307)


@router.get("/jefatura-urologia/insumos")
async def jefatura_urologia_central_insumos_alias():
    return RedirectResponse(url="/jefatura-urologia/central/insumos", status_code=307)


@router.get("/jefatura-urologia/{slug}", response_class=HTMLResponse)
async def jefatura_urologia_modulo(
    request: Request,
    slug: str,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_urologia_module_flow(request, slug, sdb)


@router.get("/jefatura-urologia/programa-academico/{section_slug}", response_class=HTMLResponse)
async def jefatura_urologia_programa_submodulo(
    request: Request,
    section_slug: str,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_urologia_programa_submodule_flow(request, section_slug, sdb)


@router.get("/jefatura-urologia/programa-academico/residentes/{resident_code}", response_class=HTMLResponse)
async def jefatura_urologia_programa_residente_perfil(
    request: Request,
    resident_code: str,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_urologia_residente_profile_flow(request, sdb, resident_code)


@router.get("/jefatura-urologia/programa-academico/residentes/{resident_code}/examenes/{assignment_id}", response_class=HTMLResponse)
async def jefatura_urologia_programa_residente_examen(
    request: Request,
    resident_code: str,
    assignment_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_resident_exam_flow(request, sdb, resident_code, assignment_id)


@router.post("/jefatura-urologia/programa-academico/residentes/{resident_code}/examenes/{assignment_id}/responder", response_class=HTMLResponse)
async def jefatura_urologia_programa_residente_examen_responder(
    request: Request,
    resident_code: str,
    assignment_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await submit_resident_exam_response(request, sdb, resident_code, assignment_id)
    if result.get("ok"):
        return RedirectResponse(
            url=f"/jefatura-urologia/programa-academico/residentes/{resident_code}/examenes/{assignment_id}?saved=1",
            status_code=303,
        )
    return await render_resident_exam_flow(
        request,
        sdb,
        resident_code,
        assignment_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible enviar el examen.")},
    )


@router.get("/jefatura-urologia/programa-academico/residentes/{resident_code}/foto")
async def jefatura_urologia_programa_residente_foto(
    resident_code: str,
    sdb: Session = Depends(_get_surgical_db),
):
    return resident_profile_photo_response(sdb, resident_code)


@router.post("/jefatura-urologia/programa-academico/residentes/{resident_code}/perfil", response_class=HTMLResponse)
async def jefatura_urologia_programa_residente_actualizar(
    request: Request,
    resident_code: str,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await update_resident_profile_from_request(request, sdb, resident_code)
    if result.get("ok"):
        return RedirectResponse(
            url=f"/jefatura-urologia/programa-academico/residentes/{resident_code}?saved=1",
            status_code=303,
        )
    return await render_jefatura_urologia_residente_profile_flow(
        request,
        sdb,
        resident_code,
        flash={
            "kind": "error",
            "message": str(result.get("error") or "No fue posible guardar el perfil."),
        },
        drawer_open=True,
    )


@router.get("/jefatura-urologia/central/examenes", response_class=HTMLResponse)
async def jefatura_urologia_central_examenes(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = None
    if str(request.query_params.get("saved") or "") == "1":
        flash = {"kind": "success", "message": "Examen creado correctamente."}
    return await render_jefatura_central_exams_flow(request, sdb, flash=flash)


@router.get("/jefatura-urologia/central/examenes/nuevo", response_class=HTMLResponse)
async def jefatura_urologia_central_examenes_nuevo(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_central_exams_flow(request, sdb, create_open=True)


@router.post("/jefatura-urologia/central/examenes", response_class=HTMLResponse)
async def jefatura_urologia_central_examenes_crear(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await create_central_exam_template_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(
            url=f"/jefatura-urologia/central/examenes/{int(result['exam_id'])}/asignar?created=1",
            status_code=303,
        )
    return await render_jefatura_central_exams_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible crear el examen.")},
        create_open=True,
    )


@router.get("/jefatura-urologia/central/examenes/{exam_id}/asignar", response_class=HTMLResponse)
async def jefatura_urologia_central_examenes_asignar(
    request: Request,
    exam_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = None
    if str(request.query_params.get("created") or "") == "1":
        flash = {"kind": "success", "message": "Examen creado. Ahora puedes asignarlo a residentes o generaciones."}
    elif str(request.query_params.get("saved") or "") == "1":
        flash = {"kind": "success", "message": "Asignación de examen completada."}
    return await render_jefatura_central_exam_assignment_flow(request, sdb, exam_id, flash=flash)


@router.post("/jefatura-urologia/central/examenes/{exam_id}/asignar", response_class=HTMLResponse)
async def jefatura_urologia_central_examenes_asignar_post(
    request: Request,
    exam_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await assign_central_exam_from_request(request, sdb, exam_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/jefatura-urologia/central/examenes/{exam_id}/asignar?saved=1", status_code=303)
    return await render_jefatura_central_exam_assignment_flow(
        request,
        sdb,
        exam_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible asignar el examen.")},
    )


@router.get("/jefatura-urologia/central/casos", response_class=HTMLResponse)
async def jefatura_urologia_central_casos(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = None
    if str(request.query_params.get("saved") or "") == "1":
        flash = {"kind": "success", "message": "Caso asociado registrado correctamente."}
    elif str(request.query_params.get("updated") or "") == "1":
        flash = {"kind": "success", "message": "Caso asociado actualizado correctamente."}
    return await render_jefatura_central_cases_flow(request, sdb, flash=flash)


@router.post("/jefatura-urologia/central/casos", response_class=HTMLResponse)
async def jefatura_urologia_central_casos_post(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await create_central_case_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/jefatura-urologia/central/casos?saved=1", status_code=303)
    return await render_jefatura_central_cases_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar el caso asociado.")},
    )


@router.get("/jefatura-urologia/central/incidencias", response_class=HTMLResponse)
async def jefatura_urologia_central_incidencias(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = None
    if str(request.query_params.get("saved") or "") == "1":
        flash = {"kind": "success", "message": "Incidencia registrada correctamente."}
    elif str(request.query_params.get("updated") or "") == "1":
        flash = {"kind": "success", "message": "Incidencia actualizada correctamente."}
    return await render_jefatura_central_incidences_flow(request, sdb, flash=flash)


@router.post("/jefatura-urologia/central/incidencias", response_class=HTMLResponse)
async def jefatura_urologia_central_incidencias_post(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await create_central_incidence_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/jefatura-urologia/central/incidencias?saved=1", status_code=303)
    return await render_jefatura_central_incidences_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar la incidencia.")},
    )


@router.post("/jefatura-urologia/central/casos/{case_id}", response_class=HTMLResponse)
async def jefatura_urologia_central_casos_update(
    request: Request,
    case_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await update_central_case_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url="/jefatura-urologia/central/casos?updated=1", status_code=303)
    return await render_jefatura_central_cases_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar el caso asociado.")},
        edit_id_override=case_id,
    )


@router.post("/jefatura-urologia/central/incidencias/{incidence_id}", response_class=HTMLResponse)
async def jefatura_urologia_central_incidencias_update(
    request: Request,
    incidence_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await update_central_incidence_from_request(request, sdb, incidence_id)
    if result.get("ok"):
        return RedirectResponse(url="/jefatura-urologia/central/incidencias?updated=1", status_code=303)
    return await render_jefatura_central_incidences_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar la incidencia.")},
        edit_id_override=incidence_id,
    )


@router.get("/jefatura-urologia/central/insumos", response_class=HTMLResponse)
async def jefatura_urologia_central_insumos(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_central_insumos_flow(request, sdb)
