"""
Middleware de Auditoría Automática — Item #1.

Intercepta TODAS las requests HTTP y las registra en gov_access_log.
Se integra como middleware Starlette en bootstrap.py.
"""
from __future__ import annotations

import json
import time
import logging
from typing import Any, Callable, Awaitable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("rnp.audit")

# Paths a excluir del log (estáticos, healthcheck)
EXCLUDED_PREFIXES = (
    "/static/",
    "/favicon.ico",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/healthz",
    "/_debug",
)


def _extract_nss_from_path(path: str) -> str:
    """Intenta extraer NSS de la ruta."""
    import re
    m = re.search(r"/(?:patient|nss|paciente)/(\d{10})", path)
    return m.group(1) if m else ""


def _detect_module(path: str) -> str:
    """Detecta módulo desde el path."""
    p = path.lower()
    if "/consulta" in p:
        return "CONSULTA"
    if "/hospitalizacion" in p or "/inpatient" in p:
        return "HOSPITALIZACION"
    if "/quirofano" in p:
        return "QUIROFANO"
    if "/urgencia" in p:
        return "URGENCIAS"
    if "/governance" in p or "/gobernanza" in p:
        return "GOBERNANZA"
    if "/interconsulta" in p or "/referencia" in p:
        return "INTERCONSULTAS"
    if "/expediente" in p or "/ehr" in p:
        return "EXPEDIENTE"
    if "/ward" in p or "/command" in p:
        return "WARD"
    if "/perfil" in p:
        return "PERFIL_CLINICO"
    if "/api/" in p:
        return "API"
    return "GENERAL"


def _detect_operation(method: str, path: str) -> str:
    """Detecta tipo de operación."""
    m = method.upper()
    if m == "GET":
        return "READ"
    if m == "POST":
        if "guardar" in path.lower() or "save" in path.lower() or "crear" in path.lower():
            return "CREATE"
        return "CREATE"
    if m == "PUT" or m == "PATCH":
        return "UPDATE"
    if m == "DELETE":
        return "DELETE"
    return "READ"


class AuditMiddleware(BaseHTTPMiddleware):
    """Middleware que registra cada request en gov_access_log."""

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        path = request.url.path

        # Skip static/docs
        if any(path.startswith(p) for p in EXCLUDED_PREFIXES):
            return await call_next(request)

        start = time.perf_counter()

        # Extract user info from Basic Auth
        username = "anonymous"
        try:
            from base64 import b64decode
            auth_header = request.headers.get("authorization", "")
            if auth_header.lower().startswith("basic "):
                decoded = b64decode(auth_header[6:]).decode("utf-8", errors="replace")
                username = decoded.split(":")[0] if ":" in decoded else decoded
        except Exception:
            pass

        # Execute request
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as exc:
            status_code = 500
            raise
        finally:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            # Log asynchronously to not block response
            try:
                self._log_to_db(
                    method=request.method,
                    path=path,
                    status_code=status_code,
                    username=username,
                    ip=request.client.host if request.client else "",
                    modulo=_detect_module(path),
                    operacion=_detect_operation(request.method, path),
                    nss=_extract_nss_from_path(path),
                    duracion_ms=elapsed_ms,
                )
            except Exception as e:
                logger.debug(f"Audit log failed: {e}")

    def _log_to_db(self, *, method: str, path: str, status_code: int,
                   username: str, ip: str, modulo: str, operacion: str,
                   nss: str, duracion_ms: int):
        """Escribe al log de auditoría en la BD."""
        try:
            from app.core.app_context import main_proxy as m
            db_gen = m.get_db()
            db = next(db_gen)
            try:
                from app.api.governance import log_access
                log_access(
                    db,
                    method=method,
                    path=path,
                    status_code=status_code,
                    username=username,
                    ip=ip,
                    modulo=modulo,
                    operacion=operacion,
                    nss=nss,
                    duracion_ms=duracion_ms,
                )
            finally:
                try:
                    next(db_gen, None)
                except StopIteration:
                    pass
        except Exception as e:
            logger.debug(f"Audit DB write failed: {e}")
