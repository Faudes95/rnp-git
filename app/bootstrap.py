from __future__ import annotations

from contextlib import asynccontextmanager
import time
from typing import Any, Awaitable, Callable, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from app.core.observability import observe_request
from app.core.errors import AppDomainError
from app.core.request_context import generate_correlation_id, get_correlation_id, reset_correlation_id, set_correlation_id


def register_audit_middleware(app: FastAPI) -> None:
    """Registra el middleware de auditoría que loguea TODAS las requests."""
    try:
        from app.core.audit_middleware import AuditMiddleware
        app.add_middleware(AuditMiddleware)
    except Exception:
        pass  # Si falla, no rompe el server


def register_security_middlewares(
    app: FastAPI,
    *,
    request_is_https: Callable[[Request], bool],
    force_https: bool,
    enable_hsts: bool,
    hsts_max_age: int,
    logger: Any,
) -> None:
    """Registra middlewares de seguridad/errores sin cambiar contratos HTTP."""

    @app.middleware("http")
    async def https_and_security_headers(request: Request, call_next):
        is_https = request_is_https(request)
        if force_https and not is_https:
            redirect_url = str(request.url.replace(scheme="https"))
            return RedirectResponse(url=redirect_url, status_code=307)

        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        if enable_hsts and is_https:
            response.headers.setdefault(
                "Strict-Transport-Security",
                f"max-age={int(hsts_max_age)}; includeSubDomains",
            )
        return response

    @app.middleware("http")
    async def correlation_id_middleware(request: Request, call_next):
        incoming = (
            request.headers.get("X-Correlation-Id")
            or request.headers.get("X-Request-Id")
            or ""
        )
        cid = str(incoming or "").strip() or generate_correlation_id()
        token = set_correlation_id(cid)
        request.state.correlation_id = cid
        try:
            response = await call_next(request)
            response.headers.setdefault("X-Correlation-Id", cid)
            return response
        finally:
            reset_correlation_id(token)

    @app.middleware("http")
    async def log_unhandled_errors(request: Request, call_next):
        start = time.perf_counter()
        cid = get_correlation_id(
            default=(
                request.headers.get("X-Correlation-Id")
                or request.headers.get("X-Request-Id")
                or ""
            )
        ) or str(getattr(request.state, "correlation_id", "") or "")
        try:
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            observe_request(
                method=request.method,
                path=request.url.path,
                status_code=int(response.status_code or 0),
                latency_ms=elapsed_ms,
                error=int(response.status_code or 0) >= 500,
            )
            return response
        except AppDomainError as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            observe_request(
                method=request.method,
                path=request.url.path,
                status_code=int(exc.status_code or 500),
                latency_ms=elapsed_ms,
                error=int(exc.status_code or 500) >= 500,
            )
            logger.warning(
                {
                    "event": "domain_error",
                    "correlation_id": cid,
                    "path": request.url.path,
                    "method": request.method,
                    "code": exc.code,
                    "message": str(exc),
                    "details": exc.details,
                }
            )
            return JSONResponse(
                status_code=int(exc.status_code or 500),
                content={
                    "status": "error",
                    "code": exc.code,
                    "message": str(exc),
                    "details": exc.details,
                },
            )
        except HTTPException as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            observe_request(
                method=request.method,
                path=request.url.path,
                status_code=int(exc.status_code or 500),
                latency_ms=elapsed_ms,
                error=int(exc.status_code or 500) >= 500,
            )
            raise
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            observe_request(
                method=request.method,
                path=request.url.path,
                status_code=500,
                latency_ms=elapsed_ms,
                error=True,
            )
            logger.exception(
                {
                    "event": "unhandled_exception",
                    "correlation_id": cid,
                    "path": request.url.path,
                    "method": request.method,
                    "query": dict(request.query_params),
                }
            )
            raise


def build_main_lifespan(
    *,
    startup_interconexion: Callable[[], Any],
    startup_ai_agents_bootstrap: Callable[[], Any],
    startup_redis_cache: Callable[[], Awaitable[Any]],
    shutdown_redis_cache: Callable[[], Awaitable[Any]],
):
    """Crea lifespan equivalente al flujo startup/shutdown legacy."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        startup_interconexion()
        startup_ai_agents_bootstrap()
        await startup_redis_cache()
        try:
            yield
        finally:
            await shutdown_redis_cache()

    return lifespan
