from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.quirofano.jefatura import (
    add_case_event_from_request,
    add_case_incidence_from_request,
    add_case_staff_from_request,
    build_dashboard_payload,
    build_day_overview,
    confirm_import_batch_from_request,
    create_import_batch_from_upload,
    render_jefatura_quirofano_import_review_flow,
    render_jefatura_quirofano_imports_flow,
    render_jefatura_quirofano_waiting_flow,
    save_import_review_from_request,
    render_jefatura_quirofano_case_detail_flow,
    render_jefatura_quirofano_day_flow,
    render_jefatura_quirofano_programacion_index_flow,
    render_jefatura_quirofano_publication_flow,
    render_jefatura_quirofano_template_flow,
    save_service_lines_from_request,
    save_template_version_from_request,
    serialize_case,
    serialize_daily_block,
    update_daily_blocks_from_request,
    upsert_daily_case_from_request,
)


router = APIRouter(tags=["quirofano-jefatura-web"])


def _get_surgical_db():
    yield from m.get_surgical_db()


def _date_flash(request: Request, success_map: dict[str, str]) -> Optional[dict[str, str]]:
    for key, message in success_map.items():
        if str(request.query_params.get(key) or "") == "1":
            return {"kind": "success", "message": message}
    return None


@router.get("/quirofano/jefatura", response_class=HTMLResponse)
async def jefatura_quirofano_home(
    request: Request,
    target_date: Optional[date] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_quirofano_waiting_flow(request, sdb, target_date=target_date)


@router.get("/quirofano/jefatura/plantillas", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = _date_flash(
        request,
        {
            "saved": "Plantilla semanal versionada correctamente.",
            "catalog_saved": "Catálogo de líneas de servicio actualizado.",
        },
    )
    return await render_jefatura_quirofano_template_flow(request, sdb, flash=flash)


@router.post("/quirofano/jefatura/plantillas", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas_post(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await save_template_version_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/quirofano/jefatura/plantillas?saved=1", status_code=303)
    return await render_jefatura_quirofano_template_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar la plantilla.")},
    )


@router.post("/quirofano/jefatura/plantillas/catalogo", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas_catalogo_post(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await save_service_lines_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/quirofano/jefatura/plantillas?catalog_saved=1", status_code=303)
    return await render_jefatura_quirofano_template_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar el catálogo.")},
    )


@router.get("/quirofano/jefatura/programacion", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_index(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_quirofano_programacion_index_flow(request, sdb)


@router.get("/quirofano/jefatura/programacion/{target_date}", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_dia(
    request: Request,
    target_date: date,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = _date_flash(
        request,
        {
            "blocks_saved": "Distribución de salas actualizada.",
            "case_saved": "Caso del día guardado correctamente.",
        },
    )
    return await render_jefatura_quirofano_day_flow(request, sdb, target_date, flash=flash)


@router.post("/quirofano/jefatura/programacion/{target_date}/bloques", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_bloques_post(
    request: Request,
    target_date: date,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await update_daily_blocks_from_request(request, sdb, target_date)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/programacion/{target_date.isoformat()}?blocks_saved=1", status_code=303)
    return await render_jefatura_quirofano_day_flow(
        request,
        sdb,
        target_date,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar la distribución.")},
    )


@router.post("/quirofano/jefatura/programacion/{target_date}/casos", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_casos_post(
    request: Request,
    target_date: date,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await upsert_daily_case_from_request(request, sdb, target_date)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/programacion/{target_date.isoformat()}?case_saved=1", status_code=303)
    return await render_jefatura_quirofano_day_flow(
        request,
        sdb,
        target_date,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar el caso.")},
    )


@router.get("/quirofano/jefatura/importaciones", response_class=HTMLResponse)
async def jefatura_quirofano_importaciones(
    request: Request,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = _date_flash(
        request,
        {
            "saved": "Importación cargada para revisión.",
            "review_saved": "Conciliación guardada.",
            "confirmed": "Importación confirmada y convertida a programación diaria.",
        },
    )
    return await render_jefatura_quirofano_imports_flow(request, sdb, flash=flash)


@router.post("/quirofano/jefatura/importaciones", response_class=HTMLResponse)
async def jefatura_quirofano_importaciones_post(
    request: Request,
    pdf_file: UploadFile = File(...),
    sdb: Session = Depends(_get_surgical_db),
):
    result = await create_import_batch_from_upload(request, sdb, pdf_file)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{int(result['batch_id'])}", status_code=303)
    return await render_jefatura_quirofano_imports_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible procesar el PDF.")},
    )


@router.get("/quirofano/jefatura/importaciones/{batch_id}", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review(
    request: Request,
    batch_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = _date_flash(
        request,
        {
            "review_saved": "Cambios de conciliación guardados.",
            "confirmed": "Importación confirmada y aplicada al día.",
        },
    )
    return await render_jefatura_quirofano_import_review_flow(request, sdb, batch_id, flash=flash)


@router.post("/quirofano/jefatura/importaciones/{batch_id}/guardar", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review_guardar(
    request: Request,
    batch_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await save_import_review_from_request(request, sdb, batch_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{batch_id}?review_saved=1", status_code=303)
    return await render_jefatura_quirofano_import_review_flow(
        request,
        sdb,
        batch_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar la conciliación.")},
    )


@router.post("/quirofano/jefatura/importaciones/{batch_id}/confirmar", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review_confirmar(
    request: Request,
    batch_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await confirm_import_batch_from_request(request, sdb, batch_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{batch_id}?confirmed=1", status_code=303)
    return await render_jefatura_quirofano_import_review_flow(
        request,
        sdb,
        batch_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible confirmar la importación.")},
    )


@router.get("/quirofano/jefatura/casos/{case_id}", response_class=HTMLResponse)
async def jefatura_quirofano_case_detail(
    request: Request,
    case_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    flash = _date_flash(
        request,
        {
            "staff_saved": "Personal agregado al caso.",
            "event_saved": "Evento registrado en la línea de tiempo.",
            "incidence_saved": "Incidencia registrada para el caso.",
        },
    )
    return await render_jefatura_quirofano_case_detail_flow(request, sdb, case_id, flash=flash)


@router.post("/quirofano/jefatura/casos/{case_id}/staff", response_class=HTMLResponse)
async def jefatura_quirofano_case_staff_post(
    request: Request,
    case_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await add_case_staff_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?staff_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible agregar personal.")},
    )


@router.post("/quirofano/jefatura/casos/{case_id}/eventos", response_class=HTMLResponse)
async def jefatura_quirofano_case_event_post(
    request: Request,
    case_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await add_case_event_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?event_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar el evento.")},
    )


@router.post("/quirofano/jefatura/casos/{case_id}/incidencias", response_class=HTMLResponse)
async def jefatura_quirofano_case_incidence_post(
    request: Request,
    case_id: int,
    sdb: Session = Depends(_get_surgical_db),
):
    result = await add_case_incidence_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?incidence_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar la incidencia.")},
    )


@router.get("/quirofano/jefatura/publicacion/{target_date}", response_class=HTMLResponse)
async def jefatura_quirofano_publicacion(
    request: Request,
    target_date: date,
    sdb: Session = Depends(_get_surgical_db),
):
    return await render_jefatura_quirofano_publication_flow(request, sdb, target_date)


@router.get("/api/quirofano/jefatura/dashboard", response_class=JSONResponse)
async def api_quirofano_jefatura_dashboard(
    target_date: Optional[date] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    payload = build_dashboard_payload(sdb, target_date=target_date)
    return JSONResponse(
        content={
            "date": payload["selected_date"].isoformat(),
            "kpis": payload["overview"]["kpis"],
            "recent_imports": [
                {
                    "id": int(row.id),
                    "file_date": row.file_date.isoformat() if row.file_date else None,
                    "filename": row.original_filename,
                    "status": row.status,
                    "rows": int(row.extracted_rows_count or 0),
                }
                for row in payload["recent_imports"]
            ],
        }
    )


@router.get("/api/quirofano/jefatura/programacion/{target_date}", response_class=JSONResponse)
async def api_quirofano_jefatura_programacion(
    target_date: date,
    sdb: Session = Depends(_get_surgical_db),
):
    overview = build_day_overview(sdb, target_date, actor="API")
    return JSONResponse(
        content={
            "date": target_date.isoformat(),
            "kpis": overview["kpis"],
            "blocks": [serialize_daily_block(row) for row in overview["blocks"]],
            "cases": [serialize_case(row) for row in overview["cases"]],
        }
    )
