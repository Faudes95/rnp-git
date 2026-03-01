from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services import analytics_dashboard_api_flow, analytics_stats_api_flow, dashboard_extracted

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/health", response_class=JSONResponse)
def dashboard_health():
    return JSONResponse(content={"status": "ok", "module": "dashboard"})


@router.get("/resumen-lite", response_class=JSONResponse)
def dashboard_resumen():
    sdb = m.SurgicalSessionLocal()
    try:
        stats_kw = {
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
        onco = analytics_stats_api_flow.stats_response_by_kind(kind="oncology", sdb=sdb, **stats_kw)
        lit = analytics_stats_api_flow.stats_response_by_kind(kind="lithiasis", sdb=sdb, **stats_kw)
        surg = analytics_stats_api_flow.stats_response_by_kind(kind="surgery", sdb=sdb, **stats_kw)
        return JSONResponse(
            content={
                "total_procedimientos": surg.get("total_procedimientos", 0),
                "total_oncologicos": onco.get("total_oncologicos_programados", 0),
                "total_litiasis": lit.get("total_litiasis_programados", 0),
                "onco_ecog": onco.get("por_ecog", {}),
                "litiasis_uh": lit.get("por_uh", {}),
            }
        )
    finally:
        sdb.close()


def _get_surgical_db():
    yield from m.get_surgical_db()


def _dashboard_json_response(
    *,
    section: str,
    sdb: Session,
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    limit: int = 300,
) -> JSONResponse:
    return JSONResponse(
        content=analytics_dashboard_api_flow.dashboard_payload(
            section=section,
            sdb=sdb,
            dashboard_service=dashboard_extracted,
            anio=anio,
            mes=mes,
            diagnostico=diagnostico,
            hgz=hgz,
            limit=limit,
        )
    )


@router.get("/resumen", response_class=JSONResponse)
def api_dashboard_resumen(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="resumen",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
    )


@router.get("/tendencia", response_class=JSONResponse)
def api_dashboard_tendencia(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="tendencia",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
    )


@router.get("/diagnosticos", response_class=JSONResponse)
def api_dashboard_diagnosticos(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="diagnosticos",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
    )


@router.get("/sexo", response_class=JSONResponse)
def api_dashboard_sexo(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="sexo",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
    )


@router.get("/procedimientos_top", response_class=JSONResponse)
def api_dashboard_procedimientos_top(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="procedimientos_top",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
    )


@router.get("/detalle", response_class=JSONResponse)
def api_dashboard_detalle(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    hgz: Optional[str] = None,
    limit: int = 300,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="detalle",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
        hgz=hgz,
        limit=limit,
    )


@router.get("/hgz", response_class=JSONResponse)
def api_dashboard_hgz(
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    diagnostico: Optional[str] = None,
    sdb: Session = Depends(_get_surgical_db),
):
    return _dashboard_json_response(
        section="hgz",
        sdb=sdb,
        anio=anio,
        mes=mes,
        diagnostico=diagnostico,
    )
