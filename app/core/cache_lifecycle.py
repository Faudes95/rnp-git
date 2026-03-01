from __future__ import annotations

from typing import Any, Awaitable, Callable


async def startup_redis_cache_lifecycle(
    *,
    redis_async: Any,
    fastapi_cache_cls: Any,
    redis_url: str,
    set_client: Callable[[Any], None],
    log_suppressed_exception: Callable[[str, Exception], Any],
) -> None:
    if redis_async is None or fastapi_cache_cls is None:
        return
    try:
        client = redis_async.from_url(redis_url, encoding="utf8", decode_responses=True)
        fastapi_cache_cls.init(client, prefix="rnp-cache")
        set_client(client)
    except Exception as exc:
        log_suppressed_exception("startup_redis_cache_failed", exc)
        set_client(None)


async def shutdown_redis_cache_lifecycle(
    *,
    get_client: Callable[[], Any],
    set_client: Callable[[Any], None],
    log_suppressed_exception: Callable[[str, Exception], Any],
) -> None:
    client = get_client()
    if client is None:
        return
    try:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            maybe = close_fn()
            if hasattr(maybe, "__await__"):
                await maybe
    except Exception as exc:
        log_suppressed_exception("shutdown_redis_cache_failed", exc)
    set_client(None)
