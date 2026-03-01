from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.services.catalog_registry import get_catalog, list_catalog_names, validate_catalog_value


router = APIRouter(tags=["catalogs"])


class CatalogValidatePayload(BaseModel):
    key: Optional[str] = None
    code: Optional[str] = None


@router.get("/api/catalogs", response_class=JSONResponse)
def api_list_catalogs():
    names = list_catalog_names()
    return JSONResponse(content={"total": len(names), "items": names})


@router.get("/api/catalogs/{catalog_name}", response_class=JSONResponse)
def api_get_catalog(catalog_name: str, refresh: int = Query(default=0, ge=0, le=1)):
    try:
        payload = get_catalog(catalog_name, force_refresh=bool(refresh))
        return JSONResponse(content=payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No fue posible cargar catálogo: {exc}") from exc


@router.post("/api/catalogs/{catalog_name}/validate", response_class=JSONResponse)
def api_validate_catalog_value(catalog_name: str, payload: CatalogValidatePayload):
    try:
        result = validate_catalog_value(catalog_name, key=payload.key, code=payload.code)
        return JSONResponse(content=result)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No fue posible validar catálogo: {exc}") from exc

