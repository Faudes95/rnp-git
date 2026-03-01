from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.common import normalize_curp, normalize_nss
from app.services.consulta import mensaje_estatus_consulta

router = APIRouter(prefix="/api/consulta", tags=["consulta"])


@router.get("/health", response_class=JSONResponse)
def consulta_health():
    """Endpoint aditivo para validación operativa del dominio consulta."""
    return JSONResponse(content={"status": "ok", "module": "consulta"})


@router.get("/normalizar-identificadores", response_class=JSONResponse)
def consulta_normalizar_identificadores(
    curp: Optional[str] = None,
    nss: Optional[str] = None,
):
    return JSONResponse(
        content={
            "curp_original": curp,
            "curp_normalizada": normalize_curp(curp or ""),
            "nss_original": nss,
            "nss_normalizado": normalize_nss(nss or ""),
        }
    )


@router.get("/mensaje-estatus", response_class=JSONResponse)
def consulta_mensaje_estatus(estatus: Optional[str] = None):
    return JSONResponse(
        content={
            "estatus": estatus or "",
            "mensaje": mensaje_estatus_consulta((estatus or "").strip().lower()),
        }
    )
