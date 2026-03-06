from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy import desc

from app.core.app_context import main_proxy as m
from app.services.connectivity_flow import build_connectivity_payload

try:
    from fastapi_cache.decorator import cache
except Exception:
    def cache(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator


router = APIRouter(tags=["legacy-core"])
GATEWAY_COOKIE_NAME = "uromed_inicio_ok"


def _get_db():
    yield from m.get_db()


def _get_surgical_db():
    yield from m.get_surgical_db()


def _stats_response_common() -> Dict[str, Any]:
    return {
        "stats_oncology_payload_fn": m.stats_oncology_payload_core,
        "stats_lithiasis_payload_fn": m.stats_lithiasis_payload_core,
        "stats_surgery_payload_fn": m.stats_surgery_payload_core,
        "hecho_programacion_cls": m.HechoProgramacionQuirurgica,
        "dim_diagnostico_cls": m.DimDiagnostico,
        "dim_paciente_cls": m.DimPaciente,
        "dim_procedimiento_cls": m.DimProcedimiento,
        "surgical_programacion_cls": m.SurgicalProgramacionDB,
        "safe_pct_fn": m._safe_pct,
        "calc_percentile_fn": m._calc_percentile,
    }


def _trends_response_common() -> Dict[str, Any]:
    return {
        "trends_diagnosticos_payload_fn": m.trends_diagnosticos_payload_core,
        "trends_procedimientos_payload_fn": m.trends_procedimientos_payload_core,
        "trends_lista_espera_payload_fn": m.trends_lista_espera_payload_core,
        "dim_fecha_cls": m.DimFecha,
        "dim_diagnostico_cls": m.DimDiagnostico,
        "dim_procedimiento_cls": m.DimProcedimiento,
        "hecho_programacion_cls": m.HechoProgramacionQuirurgica,
        "sql_func": func,
    }


async def _render_menu_principal(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    kpis = _compute_menu_kpis(db, sdb)

    return m.render_template(
        m.MENU_TEMPLATE,
        request=request,
        imss_logo_url=m._resolve_menu_asset(
            m.MENU_IMSS_LOGO_URL, m.MENU_IMSS_LOGO_PATH, m.MENU_IMSS_LOGO_FALLBACK_PATH
        ),
        imss_pattern_url=m._resolve_menu_asset(
            m.MENU_IMSS_PATTERN_URL, m.MENU_IMSS_PATTERN_PATH, m.MENU_IMSS_PATTERN_FALLBACK_PATH
        ),
        urologia_logo_url=m._resolve_menu_asset(
            m.MENU_UROLOGIA_LOGO_URL, m.MENU_UROLOGIA_LOGO_PATH, m.MENU_UROLOGIA_LOGO_FALLBACK_PATH
        ),
        hospital_bg_url=m._resolve_menu_asset(
            m.MENU_HOSPITAL_BG_URL, m.MENU_HOSPITAL_BG_PATH, m.MENU_HOSPITAL_BG_FALLBACK_PATH
        ),
        kpi_camas_ocupadas=kpis["kpi_camas_ocupadas"],
        kpi_camas_libres=kpis["kpi_camas_libres"],
        kpi_pacientes_graves=kpis["kpi_pacientes_graves"],
        kpi_cirugias_pendientes=kpis["kpi_cirugias_pendientes"],
    )


def _compute_menu_kpis(db: Session, sdb: Session) -> Dict[str, int]:
    total_camas_config = 40
    kpi_camas_ocupadas = 0
    kpi_camas_libres = 0
    kpi_pacientes_graves = 0
    kpi_cirugias_pendientes = 0

    try:
        total_camas_config = int(str(getattr(m, "os").getenv("HOSPITAL_TOTAL_CAMAS", "40")).strip() or "40")
        if total_camas_config <= 0:
            total_camas_config = 40
    except Exception:
        total_camas_config = 40

    try:
        normalizer = getattr(m, "normalize_nss", None)
        active_rows = (
            db.query(m.HospitalizacionDB)
            .filter(m.HospitalizacionDB.estatus == "ACTIVO")
            .all()
        )
        active_keys = set()
        for row in active_rows:
            raw_nss = getattr(row, "nss", "") or ""
            nss = normalizer(raw_nss) if callable(normalizer) else str(raw_nss).strip()
            key = nss or f"HOSP_{getattr(row, 'id', '')}"
            active_keys.add(str(key))
        kpi_camas_ocupadas = len(active_keys)
        kpi_camas_libres = max(total_camas_config - kpi_camas_ocupadas, 0)
    except Exception:
        kpi_camas_ocupadas = 0
        kpi_camas_libres = max(total_camas_config, 0)

    try:
        latest_censo = (
            db.query(m.HospitalCensoDiarioDB)
            .order_by(desc(m.HospitalCensoDiarioDB.fecha), desc(m.HospitalCensoDiarioDB.id))
            .first()
        )
        graves_keys = set()
        pacientes = []
        if latest_censo is not None:
            raw = getattr(latest_censo, "pacientes_json", None)
            if isinstance(raw, list):
                pacientes = raw
        for item in pacientes:
            if not isinstance(item, dict):
                continue
            estado = str(
                item.get("estado_clinico")
                or item.get("estatus_detalle")
                or item.get("estado")
                or ""
            ).upper()
            if "GRAVE" not in estado:
                continue
            key = (
                str(item.get("nss") or "").strip()
                or str(item.get("consulta_id") or "").strip()
                or f"{str(item.get('nombre') or '').strip()}::{str(item.get('cama') or '').strip()}"
            )
            if key:
                graves_keys.add(key)
        if graves_keys:
            kpi_pacientes_graves = len(graves_keys)
        else:
            # Fallback aditivo: si no hay censo del día, inferir desde hospitalización activa.
            fallback_graves = (
                db.query(m.HospitalizacionDB)
                .filter(m.HospitalizacionDB.estatus == "ACTIVO")
                .all()
            )
            for row in fallback_graves:
                estado = str(getattr(row, "estado_clinico", "") or "").upper()
                if "GRAVE" not in estado:
                    continue
                key = str(getattr(row, "nss", "") or "").strip() or f"HOSP_{getattr(row, 'id', '')}"
                graves_keys.add(key)
            kpi_pacientes_graves = len(graves_keys)
    except Exception:
        kpi_pacientes_graves = 0

    try:
        qx_rows = (
            sdb.query(m.SurgicalProgramacionDB)
            .filter(
                m.SurgicalProgramacionDB.estatus.notin_(["REALIZADA", "CANCELADA"])
            )
            .all()
        )
        waiting_ids = set()
        for row in qx_rows:
            origen = str(getattr(row, "modulo_origen", "") or "").upper()
            if origen == "QUIROFANO_URGENCIA":
                continue
            waiting_ids.add(int(getattr(row, "id", 0) or 0))
        kpi_cirugias_pendientes = len([x for x in waiting_ids if x > 0])
    except Exception:
        kpi_cirugias_pendientes = 0

    return {
        "kpi_camas_ocupadas": kpi_camas_ocupadas,
        "kpi_camas_libres": kpi_camas_libres,
        "kpi_pacientes_graves": kpi_pacientes_graves,
        "kpi_cirugias_pendientes": kpi_cirugias_pendientes,
        "total_camas_config": total_camas_config,
    }


@router.get("/", response_class=HTMLResponse)
async def inicio_plataforma(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    if str(request.cookies.get(GATEWAY_COOKIE_NAME) or "").strip() == "1":
        return await _render_menu_principal(request, db, sdb)
    return m.render_template(
        "inicio_uromed.html",
        request=request,
        error_message="",
        username_hint=os.getenv("IMSS_USER", "Faudes"),
    )


@router.post("/inicio/ingresar", response_class=HTMLResponse)
async def inicio_plataforma_ingresar(request: Request):
    form = await request.form()
    username = str(form.get("username") or "").strip()
    password = str(form.get("password") or "")
    remember_me = str(form.get("remember_me") or "").lower() in {"1", "true", "on", "si", "sí", "yes"}

    expected_user = (os.getenv("IMSS_USER", "Faudes") or "").strip()
    expected_pass = (os.getenv("IMSS_PASS", "") or "").strip()

    valid_user = (not expected_user) or (username.lower() == expected_user.lower())
    valid_pass = (not expected_pass) or (password == expected_pass)

    if not valid_user or not valid_pass:
        return m.render_template(
            "inicio_uromed.html",
            request=request,
            error_message="Usuario o contraseña inválidos.",
            username_hint=expected_user or "Usuario",
        )

    response = RedirectResponse(url="/menu-principal", status_code=303)
    response.set_cookie(
        key=GATEWAY_COOKIE_NAME,
        value="1",
        max_age=(30 * 24 * 3600) if remember_me else None,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


@router.get("/menu-principal", response_class=HTMLResponse)
async def menu_principal(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    response = await _render_menu_principal(request, db, sdb)
    if str(request.cookies.get(GATEWAY_COOKIE_NAME) or "").strip() != "1":
        response.set_cookie(
            key=GATEWAY_COOKIE_NAME,
            value="1",
            max_age=24 * 3600,
            httponly=True,
            samesite="lax",
            path="/",
        )
    return response


@router.get("/api/menu/kpis", response_class=JSONResponse)
async def menu_kpis(
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    payload = _compute_menu_kpis(db, sdb)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    return JSONResponse(payload)


@router.get("/consulta")
async def formulario_consulta(request: Request, return_to: Optional[str] = None, draft_id: Optional[str] = None):
    """Redirige a la consulta por secciones (metadata-driven). La consulta clásica fue reemplazada."""
    from fastapi.responses import RedirectResponse
    target = "/consulta/metadata"
    if draft_id:
        target += f"?draft_id={str(draft_id).strip()[:64]}"
    return RedirectResponse(url=target, status_code=307)


@router.get("/consulta/metadata", response_class=HTMLResponse)
async def formulario_consulta_metadata(request: Request):
    return m.render_template(
        "form_metadata_pilot.html",
        request=request,
        page_title="Consulta Externa - Metadata Integrada por Secciones",
        form_code="consulta_externa",
        classic_url="/consulta/metadata",
        pilot_url="/consulta/metadata",
        section_save_url="/api/consulta/seccion/guardar",
    )


@router.get("/consulta/metadata/pilot", response_class=HTMLResponse)
async def formulario_consulta_metadata_pilot(request: Request):
    return m.render_template(
        "form_metadata_pilot.html",
        request=request,
        page_title="Consulta Externa - Metadata Integrada por Secciones",
        form_code="consulta_externa",
        classic_url="/consulta/metadata",
        pilot_url="/consulta/metadata",
        section_save_url="/api/consulta/seccion/guardar",
    )


@router.get("/hospitalizacion/metadata", response_class=HTMLResponse)
async def formulario_hospitalizacion_metadata(request: Request):
    return m.render_template(
        "form_metadata_pilot.html",
        request=request,
        page_title="Hospitalización - Piloto Metadata-Driven",
        form_code="hospitalizacion_ingreso",
        classic_url="/hospitalizacion/ingresar",
        pilot_url="/hospitalizacion/metadata",
        section_save_url="/api/consulta/seccion/guardar",
    )


@router.get("/quirofano/urgencias/metadata", response_class=HTMLResponse)
async def formulario_urgencias_metadata(request: Request):
    return m.render_template(
        "form_metadata_pilot.html",
        request=request,
        page_title="Urgencias Quirúrgicas - Piloto Metadata-Driven",
        form_code="urgencias_solicitud_qx",
        classic_url="/quirofano/urgencias/nuevo",
        pilot_url="/quirofano/urgencias/metadata",
        section_save_url="/api/consulta/seccion/guardar",
    )


@router.post("/guardar_consulta_completa")
async def guardar_consulta_completa(request: Request, db: Session = Depends(_get_db)):
    """Redirige a la consulta por secciones. La captura clásica fue reemplazada."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/consulta/metadata", status_code=307)


@router.get("/reporte", response_class=HTMLResponse)
def reporte(request: Request, db: Session = Depends(_get_db)):
    return m.svc_render_reporte_html(request, db)


@router.get("/reporte_cached", response_class=HTMLResponse)
@cache(expire=300)
async def reporte_cached(request: Request, db: Session = Depends(_get_db)):
    return m.svc_render_reporte_html(request, db)


@router.get("/qx/catalogos", response_class=JSONResponse)
async def qx_catalogos(_db: Session = Depends(_get_db)):
    return m.svc_render_qx_catalogos_json()


@router.get("/qx/catalogos_cached", response_class=JSONResponse)
@cache(expire=86400)
async def qx_catalogos_cached(_db: Session = Depends(_get_db)):
    return m.svc_render_qx_catalogos_json()


@router.get("/admin/actualizar_data_mart", response_class=JSONResponse)
def admin_actualizar_data_mart(sdb: Session = Depends(_get_surgical_db)):
    status_code, payload = m.svc_admin_ml_flow.actualizar_data_mart_payload(
        sdb=sdb,
        actualizar_data_mart_fn=m.actualizar_data_mart,
    )
    if status_code != 200:
        return JSONResponse(status_code=status_code, content=payload)
    return payload


@router.get("/admin/actualizar_data_mart_async", response_class=JSONResponse)
def admin_actualizar_data_mart_async():
    if m.celery_app is not None:
        try:
            m.async_actualizar_data_mart_task.delay()
            return {"status": "ok", "message": "Tarea encolada"}
        except Exception:
            pass
    result = m._run_data_mart_update_sync(incremental=True)
    return {"status": "ok", "message": "Ejecución síncrona", "result": result}


@router.get("/admin/calidad_datos", response_class=JSONResponse)
def admin_calidad_datos(sdb: Session = Depends(_get_surgical_db)):
    status_code, payload = m.svc_admin_ml_flow.calidad_datos_payload(
        sdb=sdb,
        check_data_quality_fn=m.check_data_quality,
    )
    if status_code != 200:
        return JSONResponse(status_code=status_code, content=payload)
    return payload


@router.post("/admin/entrenar_modelo_riesgo", response_class=JSONResponse)
def admin_entrenar_modelo_riesgo(sdb: Session = Depends(_get_surgical_db)):
    status_code, payload = m.svc_admin_ml_flow.entrenar_modelo_payload(
        sdb=sdb,
        entrenar_fn=m.entrenar_modelo_riesgo,
    )
    return JSONResponse(status_code=status_code, content=payload)


@router.post("/admin/entrenar_modelo_riesgo_v2", response_class=JSONResponse)
def admin_entrenar_modelo_riesgo_v2(sdb: Session = Depends(_get_surgical_db)):
    status_code, payload = m.svc_admin_ml_flow.entrenar_modelo_payload(
        sdb=sdb,
        entrenar_fn=m.entrenar_modelo_riesgo_v2,
    )
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/admin/modelos_ml", response_class=JSONResponse)
def admin_listar_modelos_ml(sdb: Session = Depends(_get_surgical_db)):
    payload = m.svc_admin_ml_flow.listar_modelos_ml_payload(
        sdb=sdb,
        ensure_modelos_ml_schema_fn=m.ensure_modelos_ml_schema,
        modelo_ml_cls=m.ModeloML,
    )
    return JSONResponse(content=payload)


@router.get("/admin/connectivity", response_class=JSONResponse)
def admin_connectivity():
    return build_connectivity_payload(
        app_static_dir=m.APP_STATIC_DIR,
        menu_imss_logo_fallback_path=m.MENU_IMSS_LOGO_FALLBACK_PATH,
        menu_imss_pattern_fallback_path=m.MENU_IMSS_PATTERN_FALLBACK_PATH,
        menu_urologia_logo_fallback_path=m.MENU_UROLOGIA_LOGO_FALLBACK_PATH,
        menu_hospital_bg_fallback_path=m.MENU_HOSPITAL_BG_FALLBACK_PATH,
        connectivity_mode=m.CONNECTIVITY_MODE,
        offline_strict_mode=m.OFFLINE_STRICT_MODE,
        geocoder_available=bool(m.requests is not None),
    )


@router.get("/api/predict/risk", response_class=JSONResponse)
def api_predict_risk(
    edad: int,
    sexo: str,
    ecog: Optional[str] = None,
    charlson: Optional[str] = None,
):
    status_code, payload = m.svc_analytics_dashboard_api_flow.predict_risk_response(
        edad=edad,
        sexo=sexo,
        ecog=ecog,
        charlson=charlson,
        predict_risk_payload_fn=m.predict_risk_payload_core,
        cargar_modelo_riesgo_fn=m.cargar_modelo_riesgo,
        normalize_upper_fn=m.normalize_upper,
        parse_int_from_text_fn=m._parse_int_from_text,
    )
    if status_code != 200:
        return JSONResponse(status_code=status_code, content=payload)
    return payload


@router.get("/api/stats/oncology", response_class=JSONResponse)
def api_stats_oncology(sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.stats_response_by_kind(
            kind="oncology",
            sdb=sdb,
            **_stats_response_common(),
        )
    )


@router.get("/api/stats/lithiasis", response_class=JSONResponse)
def api_stats_lithiasis(sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.stats_response_by_kind(
            kind="lithiasis",
            sdb=sdb,
            **_stats_response_common(),
        )
    )


@router.get("/api/stats/surgery", response_class=JSONResponse)
def api_stats_surgery(sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.stats_response_by_kind(
            kind="surgery",
            sdb=sdb,
            **_stats_response_common(),
        )
    )


@router.get("/api/trends/diagnosticos", response_class=JSONResponse)
def api_trends_diagnosticos(
    sdb: Session = Depends(_get_surgical_db),
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.trends_response_by_kind(
            kind="diagnosticos",
            sdb=sdb,
            **_trends_response_common(),
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            procedimiento=None,
        )
    )


@router.get("/api/trends/procedimientos", response_class=JSONResponse)
def api_trends_procedimientos(
    sdb: Session = Depends(_get_surgical_db),
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    procedimiento: Optional[str] = None,
):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.trends_response_by_kind(
            kind="procedimientos",
            sdb=sdb,
            **_trends_response_common(),
            anio=anio,
            mes=mes,
            diagnostico=None,
            procedimiento=procedimiento,
        )
    )


@router.get("/api/trends/lista_espera", response_class=JSONResponse)
def api_trends_lista_espera(sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(
        content=m.svc_analytics_stats_api_flow.trends_response_by_kind(
            kind="lista_espera",
            sdb=sdb,
            **_trends_response_common(),
            anio=None,
            mes=None,
            diagnostico=None,
            procedimiento=None,
        )
    )


@router.get("/api/cie11/search", response_class=JSONResponse)
def api_cie11_search(q: str, sdb: Session = Depends(_get_surgical_db)):
    payload = m.svc_analytics_stats_api_flow.cie11_search_response(
        q=q,
        sdb=sdb,
        cie11_search_payload_fn=m.cie11_search_payload_core,
        catalogo_cie11_cls=m.CatalogoCIE11,
        surgical_cie11_map=m.SURGICAL_CIE11_MAP,
    )
    return JSONResponse(content=payload)


@router.get("/api/survival/km/{diagnostico}", response_class=JSONResponse)
def api_survival_km(diagnostico: str, db: Session = Depends(_get_db)):
    payload = m.svc_analytics_stats_api_flow.survival_km_response(
        diagnostico=diagnostico,
        db=db,
        consulta_model=m.ConsultaDB,
        survival_km_payload_fn=m.survival_km_payload_core,
        resolve_survival_event_fn=m.resolve_survival_event,
        kaplan_meier_fn=m.kaplan_meier,
        kaplan_meier_fitter_cls=m.KaplanMeierFitter,
    )
    return JSONResponse(content=payload)


@router.get("/api/survival/logrank", response_class=JSONResponse)
def api_survival_logrank(diagnostico1: str, diagnostico2: str, db: Session = Depends(_get_db)):
    status_code, payload = m.svc_analytics_dashboard_api_flow.survival_logrank_response(
        diagnostico1=diagnostico1,
        diagnostico2=diagnostico2,
        db=db,
        consulta_model=m.ConsultaDB,
        resolve_survival_event_fn=m.resolve_survival_event,
        survival_logrank_payload_fn=m.survival_logrank_payload_core,
        logrank_test_fn=m.logrank_test,
    )
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/forecast/surgery", response_class=JSONResponse)
def api_forecast_surgery(dias: int = 30, sdb: Session = Depends(_get_surgical_db)):
    status_code, payload = m.svc_forecast_geo_extracted.forecast_surgery_payload(dias, sdb)
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/research/export/csv")
def api_research_export_csv(sdb: Session = Depends(_get_surgical_db)):
    csv_data = m.svc_analytics_dashboard_api_flow.research_export_csv_content(
        sdb=sdb,
        hecho_programacion_cls=m.HechoProgramacionQuirurgica,
        dim_paciente_cls=m.DimPaciente,
        build_research_records_fn=m.build_research_records_core,
        records_to_csv_fn=m.records_to_csv_core,
        pd_module=m.pd,
    )
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=investigacion_anonima.csv"},
    )


@router.get("/api/geostats/hgz", response_class=JSONResponse)
def api_geostats_hgz(sdb: Session = Depends(_get_surgical_db)):
    return JSONResponse(
        content=m.svc_analytics_dashboard_api_flow.geostats_hgz_payload(
            sdb=sdb,
            hecho_programacion_cls=m.HechoProgramacionQuirurgica,
            sql_func=func,
        )
    )


@router.get("/admin/geocodificar", response_class=JSONResponse)
def admin_geocodificar(
    limite: int = 100,
    sleep_seconds: float = 1.0,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    return m.svc_admin_geocodificar_flow(
        limite=limite,
        sleep_seconds=sleep_seconds,
        db=db,
        sdb=sdb,
        geocodificar_pacientes_pendientes_fn=m.svc_forecast_geo_extracted.geocodificar_pacientes_pendientes,
    )


@router.get("/api/geostats/pacientes", response_class=JSONResponse)
def api_geostats_pacientes(limit: int = 2000, sdb: Session = Depends(_get_surgical_db)):
    return m.svc_api_geostats_pacientes_flow(
        limit=limit,
        sdb=sdb,
        build_geojson_pacientes_programados_fn=m.svc_forecast_geo_extracted.build_geojson_pacientes_programados,
    )


@router.get("/mapa_epidemiologico_geojson", response_class=HTMLResponse)
def mapa_epidemiologico_geojson(_request: Request, sdb: Session = Depends(_get_surgical_db)):
    return m.svc_mapa_epidemiologico_geojson_flow(
        sdb=sdb,
        folium_module=m.folium,
        build_geojson_pacientes_programados_fn=m.svc_forecast_geo_extracted.build_geojson_pacientes_programados,
    )


@router.get("/mapa_epidemiologico", response_class=HTMLResponse)
def mapa_epidemiologico(_request: Request, sdb: Session = Depends(_get_surgical_db)):
    return m.svc_mapa_epidemiologico_flow(
        sdb=sdb,
        folium_module=m.folium,
        marker_cluster_cls=m.MarkerCluster,
        hecho_programacion_cls=m.HechoProgramacionQuirurgica,
        sql_func=func,
    )


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_epidemiologico(request: Request):
    return m.render_template(m.DASHBOARD_TEMPLATE, request=request)


@router.get("/analisis/cargar-archivos", response_class=HTMLResponse)
async def analisis_cargar_archivos_form(
    request: Request,
    consulta_id: Optional[int] = None,
    curp: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await m.svc_files_flow.analisis_cargar_archivos_form_flow(request, consulta_id, curp, db)


@router.post("/analisis/cargar-archivos", response_class=HTMLResponse)
async def analisis_cargar_archivos_submit(
    request: Request,
    csrf_token: str = Form(...),
    consulta_id: Optional[int] = Form(None),
    curp: Optional[str] = Form(None),
    descripcion: Optional[str] = Form(None),
    files: List[UploadFile] = File(...),
    db: Session = Depends(_get_db),
):
    return await m.svc_files_flow.analisis_cargar_archivos_submit_flow(
        request,
        csrf_token=csrf_token,
        consulta_id=consulta_id,
        curp=curp,
        descripcion=descripcion,
        files=files,
        db=db,
    )


@router.get("/archivos_paciente/{archivo_id}")
async def descargar_archivo_paciente(archivo_id: int, db: Session = Depends(_get_db)):
    return await m.svc_files_flow.descargar_archivo_paciente_flow(archivo_id, db)


@router.post("/carga_masiva_excel", response_class=JSONResponse)
async def carga_masiva_excel(
    request: Request,
    csrf_token: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(_get_db),
):
    return await m.svc_carga_masiva_excel_flow(
        request=request,
        csrf_token=csrf_token,
        file=file,
        db=db,
        validate_csrf_fn=m.validate_csrf,
        extract_extension_fn=m._extract_extension,
        allowed_extensions=m.ALLOWED_MASS_UPLOAD_EXTENSIONS,
        pd_module=m.pd,
        celery_app=m.celery_app,
        async_carga_masiva_excel_task=m.async_carga_masiva_excel_task,
        ensure_carga_masiva_schema_fn=m.ensure_carga_masiva_schema,
        process_massive_excel_dataframe_fn=m._process_massive_excel_dataframe,
    )


@router.get("/carga_masiva/status/{task_id}", response_class=JSONResponse)
def carga_masiva_status(task_id: str, sdb: Session = Depends(_get_surgical_db)):
    return m.svc_carga_masiva_status_flow(
        task_id=task_id,
        sdb=sdb,
        ensure_carga_masiva_schema_fn=m.ensure_carga_masiva_schema,
        carga_masiva_task_model=m.CargaMasivaTask,
        celery_app=m.celery_app,
        async_carga_masiva_excel_task=m.async_carga_masiva_excel_task,
    )
