from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from app.services.hospitalizacion.clinical import (
    api_ingresos_hospitalizacion_flow,
    buscar_paciente_hospitalizacion_flow,
    cerrar_hospitalizacion_activa_flow,
    guardar_censo_cambios_flow,
    guardar_guardia_flow,
    guardar_hospitalizacion_flow,
    hospitalizacion_incapacidades_flow,
    hospitalizacion_ingresar_entry_flow,
    hospitalizacion_ingreso_preop_imprimir_docx_flow,
    hospitalizacion_urgencias_finalizar_draft_flow,
    imprimir_censo_excel_flow,
    listar_hospitalizaciones_flow,
    nuevo_hospitalizacion_form_flow,
    precheck_hospitalizacion_ingreso_flow,
    reporte_estadistico_hospitalizacion_flow,
    ver_censo_diario_flow,
)
from app.services.hospitalizacion.egreso import (
    api_hospitalizacion_egresos_flow,
    hospitalizacion_alta_form_flow,
    hospitalizacion_alta_guardar_flow,
    hospitalizacion_alta_imprimir_docx_flow,
    hospitalizacion_egresos_reporte_flow,
)
from app.services.hospitalizacion.guardia import (
    DATASET_SPECS,
    eliminar_hospitalizacion_guardia_dataset_flow,
    hospitalizacion_guardia_dataset_export_docx_flow,
    guardar_hospitalizacion_guardia_dataset_flow,
    hospitalizacion_guardia_dataset_flow,
    hospitalizacion_guardia_exportar_flow,
    hospitalizacion_guardia_home_flow,
    hospitalizacion_guardia_importar_form_flow,
    hospitalizacion_guardia_importar_submit_flow,
    hospitalizacion_guardia_reporte_flow,
    hospitalizacion_guardia_reporte_json_flow,
    get_guardia_template,
    list_guardia_templates,
    upsert_guardia_template,
)

router = APIRouter(tags=["hospitalizacion"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _base_guardia_specs():
    return DATASET_SPECS


@router.get("/hospitalizacion", response_class=HTMLResponse)
async def listar_hospitalizaciones(request: Request, db: Session = Depends(_get_db)):
    return await listar_hospitalizaciones_flow(request, db)


@router.get("/hospitalizacion/nuevo", response_class=HTMLResponse)
async def nuevo_hospitalizacion_form(request: Request, db: Session = Depends(_get_db)):
    return await nuevo_hospitalizacion_form_flow(request, db)


@router.get("/hospitalizacion/ingresar", response_class=HTMLResponse)
async def ingresar_hospitalizacion_form(
    request: Request,
    tipo: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_ingresar_entry_flow(request, db, tipo=tipo)


@router.post("/hospitalizacion/buscar", response_class=HTMLResponse)
async def buscar_paciente_hospitalizacion(request: Request, db: Session = Depends(_get_db)):
    return await buscar_paciente_hospitalizacion_flow(request, db)


@router.post("/hospitalizacion/nuevo", response_class=HTMLResponse)
async def guardar_hospitalizacion(request: Request, db: Session = Depends(_get_db)):
    return await guardar_hospitalizacion_flow(request, db)


@router.post("/api/hospitalizacion/urgencias/finalizar", response_class=JSONResponse)
async def hospitalizacion_urgencias_finalizar(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_urgencias_finalizar_draft_flow(request, db)


@router.get("/api/hospitalizacion/precheck-ingreso", response_class=JSONResponse)
async def hospitalizacion_precheck_ingreso(
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return precheck_hospitalizacion_ingreso_flow(
        db,
        consulta_id=consulta_id,
        nss=nss or "",
        nombre=nombre or "",
    )


@router.post("/api/hospitalizacion/cerrar-activa", response_class=JSONResponse)
async def hospitalizacion_cerrar_activa(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await cerrar_hospitalizacion_activa_flow(request, db)


@router.get("/hospitalizacion/ingreso/docx/{hospitalizacion_id}")
async def hospitalizacion_ingreso_docx(
    hospitalizacion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_ingreso_preop_imprimir_docx_flow(
        request,
        db,
        hospitalizacion_id=hospitalizacion_id,
    )


@router.get("/api/hospitalizacion/imprimir-ingreso", response_class=HTMLResponse)
async def hospitalizacion_imprimir_ingreso(request: Request):
    """Genera formato de impresión institucional del ingreso hospitalario."""
    from datetime import datetime

    params = dict(request.query_params)

    def v(key: str, default: str = "N/E") -> str:
        val = params.get(key, "").strip()
        return val if val else default

    fecha_impresion = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Ingreso Hospitalario — {v('nombre_completo', 'Paciente')}</title>
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
        .section-title {{ background: #13322B; color: #fff; padding: 5px 10px; font-size: 10pt; font-weight: 700; border-radius: 4px 4px 0 0; text-transform: uppercase; }}
        .section-body {{ border: 1px solid #d0d0d0; border-top: none; padding: 8px 10px; border-radius: 0 0 4px 4px; }}
        .row {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 4px; }}
        .field {{ flex: 1; min-width: 120px; }}
        .field-label {{ font-size: 8pt; color: #666; text-transform: uppercase; font-weight: 700; }}
        .field-value {{ font-size: 10pt; font-weight: 600; }}
        .signature-area {{ margin-top: 40px; display: flex; justify-content: space-between; }}
        .signature-line {{ width: 40%; text-align: center; border-top: 1px solid #333; padding-top: 6px; font-size: 9pt; }}
        .footer {{ margin-top: 20px; border-top: 2px solid #B38E5D; padding-top: 8px; font-size: 8pt; color: #888; text-align: center; }}
        @media print {{ .no-print {{ display: none !important; }} body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }} }}
        .print-toolbar {{ background: #f0f0f0; padding: 10px 20px; text-align: center; border-bottom: 1px solid #ddd; }}
        .print-toolbar button {{ background: #13322B; color: #fff; border: none; padding: 10px 24px; border-radius: 6px; font-weight: 700; cursor: pointer; }}
    </style>
</head>
<body>
    <div class="print-toolbar no-print">
        <button onclick="window.print()">Imprimir Formato de Ingreso</button>
        <button onclick="window.close()" style="background:#666;margin-left:10px;">Cerrar</button>
    </div>
    <div style="max-width:720px;margin:auto;padding:20px;">
        <div class="header">
            <div class="header-logo">
                <div class="logo-imss">IMSS</div>
                <div class="header-text">
                    <h1>Instituto Mexicano del Seguro Social</h1>
                    <p>CMN Raza — Servicio de Urologia</p>
                    <p>Nota de Ingreso Hospitalario</p>
                </div>
            </div>
            <div class="header-right">
                <div>Fecha Ingreso: {v('fecha_ingreso')}</div>
                <div>Impreso: {fecha_impresion}</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Datos del Paciente</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">Nombre</div><div class="field-value">{v('nombre_completo')}</div></div>
                    <div class="field"><div class="field-label">NSS</div><div class="field-value">{v('nss')}</div></div>
                    <div class="field"><div class="field-label">Edad</div><div class="field-value">{v('edad')}</div></div>
                    <div class="field"><div class="field-label">Sexo</div><div class="field-value">{v('sexo')}</div></div>
                </div>
                <div class="row">
                    <div class="field"><div class="field-label">Cama</div><div class="field-value">{v('cama')}</div></div>
                    <div class="field"><div class="field-label">Diagnostico</div><div class="field-value">{v('diagnostico')}</div></div>
                    <div class="field"><div class="field-label">HGZ de envio</div><div class="field-value">{v('hgz_envio')}</div></div>
                </div>
                <div class="row">
                    <div class="field"><div class="field-label">Medico a cargo</div><div class="field-value">{v('medico_a_cargo')}</div></div>
                    <div class="field"><div class="field-label">Agregado medico</div><div class="field-value">{v('agregado_medico')}</div></div>
                    <div class="field"><div class="field-label">Estatus</div><div class="field-value">{v('estatus_detalle')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Tipo de Ingreso</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">Programado</div><div class="field-value">{v('programado')}</div></div>
                    <div class="field"><div class="field-label">Urgencia</div><div class="field-value">{v('urgencia')}</div></div>
                    <div class="field"><div class="field-label">UCI</div><div class="field-value">{v('uci')}</div></div>
                    <div class="field"><div class="field-label">Tipo Urgencia</div><div class="field-value">{v('urgencia_tipo')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Antecedentes y Nota de Ingreso</div>
            <div class="section-body">
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">AHF</div><div class="field-value">{v('preop__ahf_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">APP</div><div class="field-value">{v('preop__app_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">APNP</div><div class="field-value">{v('preop__apnp_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Alergias</div><div class="field-value">{v('preop__alergias_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">AQx</div><div class="field-value">{v('preop__aqx_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Medicamentos cronicos</div><div class="field-value">{v('preop__meds_cronicos_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Padecimiento actual</div><div class="field-value">{v('preop__padecimiento_actual_text')}</div></div></div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Signos Vitales</div>
            <div class="section-body">
                <div class="row">
                    <div class="field"><div class="field-label">TA</div><div class="field-value">{v('preop__ta_sis')}/{v('preop__ta_dia')}</div></div>
                    <div class="field"><div class="field-label">FC</div><div class="field-value">{v('preop__fc')}</div></div>
                    <div class="field"><div class="field-label">FR</div><div class="field-value">{v('preop__fr')}</div></div>
                    <div class="field"><div class="field-label">Temp</div><div class="field-value">{v('preop__temp_c')} C</div></div>
                    <div class="field"><div class="field-label">SpO2</div><div class="field-value">{v('preop__spo2')}%</div></div>
                    <div class="field"><div class="field-label">Peso</div><div class="field-value">{v('preop__peso_kg')} kg</div></div>
                    <div class="field"><div class="field-label">Talla</div><div class="field-value">{v('preop__talla_m')} m</div></div>
                    <div class="field"><div class="field-label">IMC</div><div class="field-value">{v('preop__imc')}</div></div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Exploracion Fisica y Estudios</div>
            <div class="section-body">
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Exploracion fisica</div><div class="field-value">{v('preop__exploracion_fisica_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Laboratorios</div><div class="field-value">{v('preop__labs_text')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Imagenologia</div><div class="field-value">{v('preop__imagenologia_text')}</div></div></div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Diagnostico y Plan Quirurgico</div>
            <div class="section-body">
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Diagnostico preoperatorio</div><div class="field-value">{v('preop__diagnostico_preop')}</div></div></div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Procedimiento</div><div class="field-value">{v('preop__procedimiento_text')}</div></div></div>
                <div class="row">
                    <div class="field"><div class="field-label">Fecha cirugia</div><div class="field-value">{v('preop__fecha_cirugia')}</div></div>
                    <div class="field"><div class="field-label">Cirujano</div><div class="field-value">{v('preop__cirujano_text')}</div></div>
                    <div class="field"><div class="field-label">ASA</div><div class="field-value">{v('preop__asa')}</div></div>
                </div>
                <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Indicaciones preoperatorias</div><div class="field-value">{v('preop__indicaciones_preop_text')}</div></div></div>
            </div>
        </div>

        <div class="row"><div class="field" style="min-width:100%;"><div class="field-label">Observaciones</div><div class="field-value">{v('observaciones')}</div></div></div>

        <div class="signature-area">
            <div class="signature-line">Nombre y firma del medico</div>
            <div class="signature-line">Matricula / Cedula profesional</div>
        </div>

        <div class="footer">
            IMSS — CMN Raza — Servicio de Urologia — Registro Nacional de Pacientes (RNP) — Documento generado automaticamente
        </div>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/hospitalizacion/censo", response_class=HTMLResponse)
async def hospitalizacion_censo_diario(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await ver_censo_diario_flow(request, db, fecha=fecha)


@router.post("/hospitalizacion/censo/guardar", response_class=HTMLResponse)
async def hospitalizacion_censo_guardar(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await guardar_censo_cambios_flow(request, db)


@router.get("/hospitalizacion/censo/imprimir")
async def hospitalizacion_censo_imprimir(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await imprimir_censo_excel_flow(request, db, fecha=fecha)


@router.post("/hospitalizacion/guardia/guardar", response_class=HTMLResponse)
async def hospitalizacion_guardia_guardar(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await guardar_guardia_flow(request, db)


@router.get("/hospitalizacion/tablero", response_class=HTMLResponse)
async def hospitalizacion_tablero(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_home_flow(request, db, fecha=fecha)


@router.get("/hospitalizacion/guardia", response_class=HTMLResponse)
async def hospitalizacion_guardia(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_home_flow(request, db, fecha=fecha)


@router.get("/hospitalizacion/importar", response_class=HTMLResponse)
async def hospitalizacion_guardia_importar_form(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_importar_form_flow(request, db, fecha=fecha)


@router.post("/hospitalizacion/importar", response_class=HTMLResponse)
async def hospitalizacion_guardia_importar_submit(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_importar_submit_flow(request, db)


@router.get("/hospitalizacion/exportar")
async def hospitalizacion_guardia_exportar(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_exportar_flow(request, db, fecha=fecha)


@router.get("/hospitalizacion/ingresos", response_class=HTMLResponse)
async def hospitalizacion_guardia_ingresos(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="ingresos", fecha=fecha)


@router.get("/hospitalizacion/operados", response_class=HTMLResponse)
async def hospitalizacion_guardia_operados(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="operados", fecha=fecha)


@router.get("/hospitalizacion/labs", response_class=HTMLResponse)
async def hospitalizacion_guardia_labs(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="laboratorios", fecha=fecha)


@router.get("/hospitalizacion/valoraciones", response_class=HTMLResponse)
async def hospitalizacion_guardia_valoraciones(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="valoraciones", fecha=fecha)


@router.get("/hospitalizacion/sala13", response_class=HTMLResponse)
async def hospitalizacion_guardia_sala13(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="sala13", fecha=fecha)


@router.get("/hospitalizacion/productividad-ce", response_class=HTMLResponse)
async def hospitalizacion_guardia_productividad_ce(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="productividad_ce", fecha=fecha)


@router.get("/hospitalizacion/rendicion-division", response_class=HTMLResponse)
async def hospitalizacion_guardia_rendicion_division(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="rendicion_division", fecha=fecha)


@router.get("/hospitalizacion/gestion-camas", response_class=HTMLResponse)
async def hospitalizacion_guardia_gestion_camas(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="gestion_camas", fecha=fecha)


@router.get("/hospitalizacion/estancias-prolongadas", response_class=HTMLResponse)
async def hospitalizacion_guardia_estancias_prolongadas(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="estancias_prolongadas", fecha=fecha)


@router.get("/hospitalizacion/estancias-prolongadas/estrategias", response_class=HTMLResponse)
async def hospitalizacion_guardia_estancias_estrategias(
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_flow(request, db, dataset="estancias_estrategias", fecha=fecha)


@router.post("/hospitalizacion/guardia/{dataset}/guardar", response_class=HTMLResponse)
async def hospitalizacion_guardia_dataset_guardar(
    dataset: str,
    request: Request,
    db: Session = Depends(_get_db),
):
    return await guardar_hospitalizacion_guardia_dataset_flow(request, db, dataset=dataset)


@router.get("/hospitalizacion/guardia/{dataset}/exportar")
async def hospitalizacion_guardia_dataset_exportar(
    dataset: str,
    request: Request,
    fecha: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_dataset_export_docx_flow(
        request,
        db,
        dataset=dataset,
        fecha=fecha,
    )


@router.post("/hospitalizacion/guardia/{dataset}/eliminar/{record_id}", response_class=HTMLResponse)
async def hospitalizacion_guardia_dataset_eliminar(
    dataset: str,
    record_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    return await eliminar_hospitalizacion_guardia_dataset_flow(request, db, dataset=dataset, record_id=record_id)


@router.get("/hospitalizacion/reporte/guardia", response_class=HTMLResponse)
async def hospitalizacion_reporte_guardia(
    request: Request,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_reporte_flow(
        request,
        db,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )


@router.get("/api/hospitalizacion/guardia/reporte", response_class=JSONResponse)
async def api_hospitalizacion_reporte_guardia(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_guardia_reporte_json_flow(
        db,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )


@router.get("/api/hospitalizacion/guardia/templates", response_class=JSONResponse)
def api_hospitalizacion_guardia_templates(db: Session = Depends(_get_db)):
    templates = list_guardia_templates(db, base_specs=_base_guardia_specs())
    return JSONResponse(content={"total": len(templates), "templates": templates})


@router.get("/api/hospitalizacion/guardia/templates/{dataset}", response_class=JSONResponse)
def api_hospitalizacion_guardia_template(dataset: str, db: Session = Depends(_get_db)):
    tmpl = get_guardia_template(db, dataset=dataset, base_specs=_base_guardia_specs())
    if not tmpl:
        return JSONResponse(status_code=404, content={"detail": "dataset no encontrado"})
    return JSONResponse(content=tmpl)


@router.post("/api/hospitalizacion/guardia/templates/{dataset}", response_class=JSONResponse)
async def api_hospitalizacion_guardia_template_upsert(dataset: str, request: Request, db: Session = Depends(_get_db)):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    schema = payload.get("schema") if isinstance(payload, dict) else {}
    version = str(payload.get("version") or "v1") if isinstance(payload, dict) else "v1"
    activo = bool(payload.get("activo", True)) if isinstance(payload, dict) else True
    saved = upsert_guardia_template(db, dataset=dataset, schema_payload=(schema or {}), version=version, activo=activo)
    return JSONResponse(content={"status": "ok", "template": saved})


@router.get("/hospitalizacion/reporte", response_class=HTMLResponse)
async def hospitalizacion_reporte(
    request: Request,
    scope: Optional[str] = None,
    periodo: Optional[str] = None,
    ingreso_tipo: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await reporte_estadistico_hospitalizacion_flow(
        request,
        db,
        scope=scope,
        periodo=periodo,
        ingreso_tipo=ingreso_tipo,
    )


@router.get("/api/hospitalizacion/ingresos", response_class=JSONResponse)
async def api_hospitalizacion_ingresos(
    scope: Optional[str] = None,
    periodo: Optional[str] = None,
    ingreso_tipo: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await api_ingresos_hospitalizacion_flow(
        db,
        scope=scope,
        periodo=periodo,
        ingreso_tipo=ingreso_tipo,
    )


@router.get("/hospitalizacion/incapacidades", response_class=HTMLResponse)
async def hospitalizacion_incapacidades(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_incapacidades_flow(request, db)


@router.get("/hospitalizacion/alta", response_class=HTMLResponse)
async def hospitalizacion_alta_form(
    request: Request,
    hospitalizacion_id: Optional[int] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_alta_form_flow(
        request,
        db,
        hospitalizacion_id=hospitalizacion_id,
    )


@router.post("/hospitalizacion/alta/guardar", response_class=HTMLResponse)
async def hospitalizacion_alta_guardar(
    request: Request,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_alta_guardar_flow(request, db)


@router.get("/hospitalizacion/alta/{egreso_id}/imprimir")
async def hospitalizacion_alta_imprimir(
    request: Request,
    egreso_id: int,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_alta_imprimir_docx_flow(
        request,
        db,
        egreso_id=egreso_id,
    )


@router.get("/hospitalizacion/reporte/egresos", response_class=HTMLResponse)
async def hospitalizacion_reporte_egresos(
    request: Request,
    periodo: Optional[str] = None,
    q: Optional[str] = None,
    medico: Optional[str] = None,
    procedimiento: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_egresos_reporte_flow(
        request,
        db,
        periodo=periodo,
        q=q,
        medico=medico,
        procedimiento=procedimiento,
    )


@router.get("/hospitalizacion/egresos", response_class=HTMLResponse)
async def hospitalizacion_egresos(
    request: Request,
    periodo: Optional[str] = None,
    q: Optional[str] = None,
    medico: Optional[str] = None,
    procedimiento: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await hospitalizacion_egresos_reporte_flow(
        request,
        db,
        periodo=periodo,
        q=q,
        medico=medico,
        procedimiento=procedimiento,
    )


@router.get("/api/hospitalizacion/egresos", response_class=JSONResponse)
async def api_hospitalizacion_egresos(
    periodo: Optional[str] = None,
    q: Optional[str] = None,
    medico: Optional[str] = None,
    procedimiento: Optional[str] = None,
    db: Session = Depends(_get_db),
):
    return await api_hospitalizacion_egresos_flow(
        db,
        periodo=periodo,
        q=q,
        medico=medico,
        procedimiento=procedimiento,
    )
