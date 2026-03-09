from __future__ import annotations

import os
from typing import Any, Callable, Iterable

from fastapi import Depends, FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles


def create_app_instance(
    *,
    title: str,
    require_auth: Callable[..., Any],
    app_static_dir: str,
    routers: Iterable[Any],
    register_security_middlewares: Callable[..., Any],
    request_is_https: Callable[..., bool],
    force_https: bool,
    enable_hsts: bool,
    hsts_max_age: int,
    logger: Any,
) -> FastAPI:
    """Composition root Fase 1: creación/wiring del app FastAPI."""
    app = FastAPI(
        title=title,
        dependencies=[Depends(require_auth)],
    )
    if os.path.isdir(app_static_dir):
        app.mount("/static", StaticFiles(directory=app_static_dir), name="static")
        favicon_target = os.path.join(app_static_dir, "img", "logooficial_brand.png")
        if os.path.isfile(favicon_target):
            @app.get("/favicon.ico", include_in_schema=False)
            async def favicon() -> RedirectResponse:
                return RedirectResponse(url="/static/img/logooficial_brand.png", status_code=307)
    for router in routers:
        app.include_router(router)
    register_security_middlewares(
        app,
        request_is_https=request_is_https,
        force_https=force_https,
        enable_hsts=enable_hsts,
        hsts_max_age=hsts_max_age,
        logger=logger,
    )
    # Registrar middleware de auditoría automática
    try:
        from app.bootstrap import register_audit_middleware
        register_audit_middleware(app)
    except Exception:
        pass
    return app


def attach_lifespan(
    app: FastAPI,
    *,
    build_main_lifespan: Callable[..., Any],
    startup_interconexion: Callable[..., Any],
    startup_ai_agents_bootstrap: Callable[..., Any],
    startup_redis_cache: Callable[..., Any],
    shutdown_redis_cache: Callable[..., Any],
) -> None:
    """Adjunta lifespan sin alterar contratos existentes."""
    app.router.lifespan_context = build_main_lifespan(
        startup_interconexion=startup_interconexion,
        startup_ai_agents_bootstrap=startup_ai_agents_bootstrap,
        startup_redis_cache=startup_redis_cache,
        shutdown_redis_cache=shutdown_redis_cache,
    )
