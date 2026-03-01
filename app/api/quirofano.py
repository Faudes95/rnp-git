from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.surgical import resumen_programaciones

router = APIRouter(prefix="/api/quirofano", tags=["quirofano"])


@router.get("/health", response_class=JSONResponse)
def quirofano_health():
    return JSONResponse(content={"status": "ok", "module": "quirofano"})


@router.get("/catalogos", response_class=JSONResponse)
def quirofano_catalogos():
    from app.core.app_context import main_proxy as main_module

    return JSONResponse(content=main_module.qx_catalogos_payload())


@router.get("/resumen-programadas", response_class=JSONResponse)
def quirofano_resumen_programadas(limit: Optional[int] = 500):
    from app.core.app_context import main_proxy as main_module

    max_rows = max(1, min(int(limit or 500), 5000))
    sdb = main_module.SurgicalSessionLocal()
    try:
        rows = (
            sdb.query(main_module.SurgicalProgramacionDB)
            .filter(main_module.SurgicalProgramacionDB.estatus == "PROGRAMADA")
            .filter(
                (main_module.SurgicalProgramacionDB.modulo_origen.is_(None))
                | (main_module.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA")
            )
            .order_by(main_module.SurgicalProgramacionDB.id.desc())
            .limit(max_rows)
            .all()
        )
        payload = resumen_programaciones(rows)
        payload["limit"] = max_rows
        return JSONResponse(content=payload)
    finally:
        sdb.close()
