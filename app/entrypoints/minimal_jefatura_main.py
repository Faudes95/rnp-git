from __future__ import annotations

import logging
import os
import runpy
import secrets
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "__all__")
os.environ.setdefault("RNP_APP_CONTEXT_MODULE", "app.entrypoints.minimal_jefatura_main")

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import JSON, event, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker


APP_BOOT_PROFILE = "minimal_jefatura"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = str(PROJECT_ROOT)
APP_STATIC_DIR = os.path.join(BASE_DIR, "app", "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "app", "templates")
PATIENT_FILES_DIR = os.path.join(BASE_DIR, "patient_files")
define_jefatura_quirofano_models = runpy.run_path(
    str(PROJECT_ROOT / "app" / "core" / "jefatura_quirofano_models.py")
)["define_jefatura_quirofano_models"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rnp.minimal_jefatura")

AUTH_PUBLIC_PATHS = {"/status"}
AUTH_PUBLIC_PREFIXES = ("/static/",)
CSRF_COOKIE_NAME = "csrf_token"
SECURE_COOKIES = os.getenv("SECURE_COOKIES", "false").lower() in ("1", "true", "yes")
FORCE_HTTPS = os.getenv("FORCE_HTTPS", "false").lower() in ("1", "true", "yes")
ENABLE_HSTS = os.getenv("ENABLE_HSTS", "false").lower() in ("1", "true", "yes")
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() in ("1", "true", "yes")
AUTH_USER = (os.getenv("IMSS_USER") or os.getenv("AUTH_USER") or "Faudes").strip()
AUTH_PASS = (os.getenv("IMSS_PASS") or os.getenv("AUTH_PASS") or "1995").strip()
try:
    HSTS_MAX_AGE = max(0, int(os.getenv("HSTS_MAX_AGE", "31536000") or "31536000"))
except Exception:
    HSTS_MAX_AGE = 31536000

security = HTTPBasic(auto_error=False)

SURGICAL_DATABASE_URL = os.getenv("SURGICAL_DATABASE_URL", "sqlite:///./urologia_quirurgico.db")
SURGICAL_IS_SQLITE = SURGICAL_DATABASE_URL.startswith("sqlite")
surgical_connect_args = {"check_same_thread": False} if SURGICAL_IS_SQLITE else {}
surgical_engine = create_engine(SURGICAL_DATABASE_URL, connect_args=surgical_connect_args)

if SURGICAL_IS_SQLITE:
    @event.listens_for(surgical_engine, "connect")
    def _set_surgical_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

SurgicalSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=surgical_engine)
SurgicalBase = declarative_base()
SURGICAL_JSON_TYPE = JSONB if (not SURGICAL_IS_SQLITE and SURGICAL_DATABASE_URL.startswith("postgres")) else JSON

_jefatura_quirofano_models = define_jefatura_quirofano_models(
    base=SurgicalBase,
    json_type=SURGICAL_JSON_TYPE,
    utcnow_fn=datetime.utcnow,
)
JefaturaQuirofanoServiceLineDB = _jefatura_quirofano_models.JefaturaQuirofanoServiceLineDB
JefaturaQuirofanoTemplateVersionDB = _jefatura_quirofano_models.JefaturaQuirofanoTemplateVersionDB
JefaturaQuirofanoTemplateSlotDB = _jefatura_quirofano_models.JefaturaQuirofanoTemplateSlotDB
JefaturaQuirofanoImportBatchDB = _jefatura_quirofano_models.JefaturaQuirofanoImportBatchDB
JefaturaQuirofanoDailyBlockDB = _jefatura_quirofano_models.JefaturaQuirofanoDailyBlockDB
JefaturaQuirofanoImportRowDB = _jefatura_quirofano_models.JefaturaQuirofanoImportRowDB
JefaturaQuirofanoCaseProgramacionDB = _jefatura_quirofano_models.JefaturaQuirofanoCaseProgramacionDB
JefaturaQuirofanoCaseStaffDB = _jefatura_quirofano_models.JefaturaQuirofanoCaseStaffDB
JefaturaQuirofanoCaseEventDB = _jefatura_quirofano_models.JefaturaQuirofanoCaseEventDB
JefaturaQuirofanoCaseIncidenciaDB = _jefatura_quirofano_models.JefaturaQuirofanoCaseIncidenciaDB
JefaturaQuirofanoAuditLogDB = _jefatura_quirofano_models.JefaturaQuirofanoAuditLogDB

jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(default=True, enabled_extensions=("html", "xml")),
)


def _log_suppressed_exception(event_name: str, exc: Exception, **extra: Any) -> None:
    payload: Dict[str, Any] = {"event": event_name, "error": str(exc)}
    payload.update(extra)
    logger.warning(payload)


def render_template(template_string: str, request: Optional[Request] = None, **context):
    token = ""
    if request is not None:
        token = str(request.cookies.get(CSRF_COOKIE_NAME) or "").strip() or secrets.token_urlsafe(24)
        context.setdefault("request", request)
        context.setdefault("csrf_token", token)
    html = jinja_env.get_template(template_string).render(**context)
    response = HTMLResponse(content=html)
    if request is not None and str(request.cookies.get(CSRF_COOKIE_NAME) or "").strip() != token:
        response.set_cookie(
            CSRF_COOKIE_NAME,
            token,
            secure=SECURE_COOKIES,
            httponly=False,
            samesite="lax",
        )
    return response


def validate_csrf(form_data: Dict[str, Any], request: Request):
    form_token = str(form_data.get("csrf_token") or form_data.get(CSRF_COOKIE_NAME) or "").strip()
    cookie_token = str(request.cookies.get(CSRF_COOKIE_NAME) or "").strip()
    if not form_token or not cookie_token or not secrets.compare_digest(form_token, cookie_token):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token CSRF inválido",
        )


def get_surgical_db():
    db = SurgicalSessionLocal()
    try:
        yield db
    finally:
        db.close()


def _date_flash(request: Request, success_map: Dict[str, str]) -> Optional[Dict[str, str]]:
    for key, message in success_map.items():
        if str(request.query_params.get(key) or "") == "1":
            return {"kind": "success", "message": message}
    return None


def require_auth(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(security),
):
    normalized = (request.url.path or "/").split("?", 1)[0] or "/"
    if normalized in AUTH_PUBLIC_PATHS or any(normalized.startswith(prefix) for prefix in AUTH_PUBLIC_PREFIXES):
        return
    if not AUTH_ENABLED:
        return
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales requeridas",
            headers={"WWW-Authenticate": "Basic"},
        )
    valid_user = secrets.compare_digest(str(credentials.username or ""), AUTH_USER)
    valid_pass = secrets.compare_digest(str(credentials.password or ""), AUTH_PASS)
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Basic"},
        )


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    yield


app = FastAPI(
    title="RNP Minimal Jefatura Quirófano",
    dependencies=[Depends(require_auth)],
    lifespan=_lifespan,
)

if os.path.isdir(APP_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=APP_STATIC_DIR), name="static")


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    forwarded_proto = str(request.headers.get("x-forwarded-proto") or "").strip().lower()
    is_https = request.url.scheme == "https" or forwarded_proto == "https"
    if FORCE_HTTPS and not is_https:
        redirect_url = str(request.url.replace(scheme="https"))
        return RedirectResponse(url=redirect_url, status_code=307)
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    if ENABLE_HSTS and is_https:
        response.headers.setdefault(
            "Strict-Transport-Security",
            f"max-age={int(HSTS_MAX_AGE)}; includeSubDomains",
        )
    return response


@app.get("/", include_in_schema=False)
async def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/quirofano/jefatura", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@app.get("/status", include_in_schema=False)
async def status_page() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "app_boot_profile": APP_BOOT_PROFILE,
            "auth_enabled": AUTH_ENABLED,
            "surgical_database_url": SURGICAL_DATABASE_URL,
        }
    )


@app.get("/quirofano/jefatura", response_class=HTMLResponse)
async def jefatura_quirofano_home(
    request: Request,
    target_date: Optional[date] = None,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_flow import render_jefatura_quirofano_waiting_flow

    return await render_jefatura_quirofano_waiting_flow(request, sdb, target_date=target_date)


@app.get("/quirofano/jefatura/plantillas", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas(
    request: Request,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_template_flow

    flash = _date_flash(
        request,
        {
            "saved": "Plantilla semanal versionada correctamente.",
            "catalog_saved": "Catálogo de líneas de servicio actualizado.",
        },
    )
    return await render_jefatura_quirofano_template_flow(request, sdb, flash=flash)


@app.post("/quirofano/jefatura/plantillas", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas_post(
    request: Request,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        render_jefatura_quirofano_template_flow,
        save_template_version_from_request,
    )

    result = await save_template_version_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/quirofano/jefatura/plantillas?saved=1", status_code=303)
    return await render_jefatura_quirofano_template_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar la plantilla.")},
    )


@app.post("/quirofano/jefatura/plantillas/catalogo", response_class=HTMLResponse)
async def jefatura_quirofano_plantillas_catalogo_post(
    request: Request,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        render_jefatura_quirofano_template_flow,
        save_service_lines_from_request,
    )

    result = await save_service_lines_from_request(request, sdb)
    if result.get("ok"):
        return RedirectResponse(url="/quirofano/jefatura/plantillas?catalog_saved=1", status_code=303)
    return await render_jefatura_quirofano_template_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar el catálogo.")},
    )


@app.get("/quirofano/jefatura/programacion", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_index(
    request: Request,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_programacion_index_flow

    return await render_jefatura_quirofano_programacion_index_flow(request, sdb)


@app.get("/quirofano/jefatura/programacion/{target_date}", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_dia(
    request: Request,
    target_date: date,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_day_flow

    flash = _date_flash(
        request,
        {
            "blocks_saved": "Distribución de salas actualizada.",
            "case_saved": "Caso del día guardado correctamente.",
        },
    )
    return await render_jefatura_quirofano_day_flow(request, sdb, target_date, flash=flash)


@app.post("/quirofano/jefatura/programacion/{target_date}/bloques", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_bloques_post(
    request: Request,
    target_date: date,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        render_jefatura_quirofano_day_flow,
        update_daily_blocks_from_request,
    )

    result = await update_daily_blocks_from_request(request, sdb, target_date)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/programacion/{target_date.isoformat()}?blocks_saved=1", status_code=303)
    return await render_jefatura_quirofano_day_flow(
        request,
        sdb,
        target_date,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible actualizar la distribución.")},
    )


@app.post("/quirofano/jefatura/programacion/{target_date}/casos", response_class=HTMLResponse)
async def jefatura_quirofano_programacion_casos_post(
    request: Request,
    target_date: date,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        render_jefatura_quirofano_day_flow,
        upsert_daily_case_from_request,
    )

    result = await upsert_daily_case_from_request(request, sdb, target_date)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/programacion/{target_date.isoformat()}?case_saved=1", status_code=303)
    return await render_jefatura_quirofano_day_flow(
        request,
        sdb,
        target_date,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar el caso.")},
    )


@app.get("/quirofano/jefatura/importaciones", response_class=HTMLResponse)
async def jefatura_quirofano_importaciones(
    request: Request,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_import_flow import render_jefatura_quirofano_imports_flow

    flash = _date_flash(
        request,
        {
            "saved": "Importación cargada para revisión.",
            "review_saved": "Conciliación guardada.",
            "confirmed": "Importación confirmada y convertida a programación diaria.",
        },
    )
    return await render_jefatura_quirofano_imports_flow(request, sdb, flash=flash)


@app.post("/quirofano/jefatura/importaciones", response_class=HTMLResponse)
async def jefatura_quirofano_importaciones_post(
    request: Request,
    pdf_file: UploadFile = File(...),
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_import_flow import (
        create_import_batch_from_upload,
        render_jefatura_quirofano_imports_flow,
    )

    result = await create_import_batch_from_upload(request, sdb, pdf_file)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{int(result['batch_id'])}", status_code=303)
    return await render_jefatura_quirofano_imports_flow(
        request,
        sdb,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible procesar el PDF.")},
    )


@app.get("/quirofano/jefatura/importaciones/{batch_id}", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review(
    request: Request,
    batch_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_import_flow import render_jefatura_quirofano_import_review_flow

    flash = _date_flash(
        request,
        {
            "review_saved": "Cambios de conciliación guardados.",
            "confirmed": "Importación confirmada y aplicada al día.",
        },
    )
    return await render_jefatura_quirofano_import_review_flow(request, sdb, batch_id, flash=flash)


@app.post("/quirofano/jefatura/importaciones/{batch_id}/guardar", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review_guardar(
    request: Request,
    batch_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_import_flow import (
        render_jefatura_quirofano_import_review_flow,
        save_import_review_from_request,
    )

    result = await save_import_review_from_request(request, sdb, batch_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{batch_id}?review_saved=1", status_code=303)
    return await render_jefatura_quirofano_import_review_flow(
        request,
        sdb,
        batch_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible guardar la conciliación.")},
    )


@app.post("/quirofano/jefatura/importaciones/{batch_id}/confirmar", response_class=HTMLResponse)
async def jefatura_quirofano_importacion_review_confirmar(
    request: Request,
    batch_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_import_flow import (
        confirm_import_batch_from_request,
        render_jefatura_quirofano_import_review_flow,
    )

    result = await confirm_import_batch_from_request(request, sdb, batch_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/importaciones/{batch_id}?confirmed=1", status_code=303)
    return await render_jefatura_quirofano_import_review_flow(
        request,
        sdb,
        batch_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible confirmar la importación.")},
    )


@app.get("/quirofano/jefatura/casos/{case_id}", response_class=HTMLResponse)
async def jefatura_quirofano_case_detail(
    request: Request,
    case_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_case_detail_flow

    flash = _date_flash(
        request,
        {
            "staff_saved": "Personal agregado al caso.",
            "event_saved": "Evento registrado en la línea de tiempo.",
            "incidence_saved": "Incidencia registrada para el caso.",
        },
    )
    return await render_jefatura_quirofano_case_detail_flow(request, sdb, case_id, flash=flash)


@app.post("/quirofano/jefatura/casos/{case_id}/staff", response_class=HTMLResponse)
async def jefatura_quirofano_case_staff_post(
    request: Request,
    case_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        add_case_staff_from_request,
        render_jefatura_quirofano_case_detail_flow,
    )

    result = await add_case_staff_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?staff_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible agregar personal.")},
    )


@app.post("/quirofano/jefatura/casos/{case_id}/eventos", response_class=HTMLResponse)
async def jefatura_quirofano_case_event_post(
    request: Request,
    case_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        add_case_event_from_request,
        render_jefatura_quirofano_case_detail_flow,
    )

    result = await add_case_event_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?event_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar el evento.")},
    )


@app.post("/quirofano/jefatura/casos/{case_id}/incidencias", response_class=HTMLResponse)
async def jefatura_quirofano_case_incidence_post(
    request: Request,
    case_id: int,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import (
        add_case_incidence_from_request,
        render_jefatura_quirofano_case_detail_flow,
    )

    result = await add_case_incidence_from_request(request, sdb, case_id)
    if result.get("ok"):
        return RedirectResponse(url=f"/quirofano/jefatura/casos/{case_id}?incidence_saved=1", status_code=303)
    return await render_jefatura_quirofano_case_detail_flow(
        request,
        sdb,
        case_id,
        flash={"kind": "error", "message": str(result.get("error") or "No fue posible registrar la incidencia.")},
    )


@app.get("/quirofano/jefatura/publicacion/{target_date}", response_class=HTMLResponse)
async def jefatura_quirofano_publicacion(
    request: Request,
    target_date: date,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_publication_flow

    return await render_jefatura_quirofano_publication_flow(request, sdb, target_date)


@app.get("/api/quirofano/jefatura/dashboard", response_class=JSONResponse)
async def api_quirofano_jefatura_dashboard(
    target_date: Optional[date] = None,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import build_dashboard_payload

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


@app.get("/api/quirofano/jefatura/programacion/{target_date}", response_class=JSONResponse)
async def api_quirofano_jefatura_programacion(
    target_date: date,
    sdb=Depends(get_surgical_db),
):
    from app.services.quirofano_jefatura_programacion_flow import build_day_overview
    from app.services.quirofano_jefatura_shared import serialize_case, serialize_daily_block

    overview = build_day_overview(sdb, target_date, actor="API")
    return JSONResponse(
        content={
            "date": target_date.isoformat(),
            "kpis": overview["kpis"],
            "blocks": [serialize_daily_block(row) for row in overview["blocks"]],
            "cases": [serialize_case(row) for row in overview["cases"]],
        }
    )


__all__ = [
    "APP_BOOT_PROFILE",
    "SURGICAL_JSON_TYPE",
    "SurgicalBase",
    "SurgicalSessionLocal",
    "JefaturaQuirofanoServiceLineDB",
    "JefaturaQuirofanoTemplateVersionDB",
    "JefaturaQuirofanoTemplateSlotDB",
    "JefaturaQuirofanoImportBatchDB",
    "JefaturaQuirofanoDailyBlockDB",
    "JefaturaQuirofanoImportRowDB",
    "JefaturaQuirofanoCaseProgramacionDB",
    "JefaturaQuirofanoCaseStaffDB",
    "JefaturaQuirofanoCaseEventDB",
    "JefaturaQuirofanoCaseIncidenciaDB",
    "JefaturaQuirofanoAuditLogDB",
    "app",
    "get_surgical_db",
    "render_template",
    "validate_csrf",
]
