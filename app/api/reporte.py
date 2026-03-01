from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.reporte import agregar_timestamp, resumen_serializable_reporte

router = APIRouter(prefix="/api/reporte", tags=["reporte"])


@router.get("/health", response_class=JSONResponse)
def reporte_health():
    return JSONResponse(content={"status": "ok", "module": "reporte"})


@router.get("/resumen", response_class=JSONResponse)
def reporte_resumen():
    """Resumen JSON aditivo del BI existente sin alterar rutas legacy."""
    # Import local para evitar dependencia circular al cargar módulos.
    from app.core.app_context import main_proxy as main_module

    db = main_module.SessionLocal()
    try:
        context = main_module.generar_reporte_bi(db)
        context = agregar_timestamp(context, now=datetime.now())
        return JSONResponse(content=resumen_serializable_reporte(context))
    finally:
        db.close()
