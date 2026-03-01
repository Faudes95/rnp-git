from __future__ import annotations

import base64
import errno
import os
import secrets
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from fastapi import Request
from fastapi.responses import HTMLResponse


def template_file_for_ref(
    template_ref: str,
    *,
    template_literal_to_file_cache: Dict[str, str],
    template_name_by_const: Dict[str, str],
    global_values: Dict[str, Any],
) -> Optional[str]:
    if template_ref in template_literal_to_file_cache:
        return template_literal_to_file_cache.get(template_ref)
    for const_name, file_name in template_name_by_const.items():
        literal = global_values.get(const_name)
        if isinstance(literal, str):
            template_literal_to_file_cache.setdefault(literal, file_name)
    return template_literal_to_file_cache.get(template_ref)


def load_template_file_source(
    template_file: str,
    *,
    templates_dir: str,
    template_source_cache: Dict[str, Tuple[float, str]],
    sleep_fn: Callable[[float], None],
    log_suppressed_exception: Callable[[str, Exception, Any], None],
) -> Optional[Tuple[str, str]]:
    path = os.path.join(templates_dir, template_file)
    if not os.path.isfile(path):
        return None

    cache_key = f"file:{template_file}"
    cached = template_source_cache.get(cache_key)
    mtime = None
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        pass

    if cached is not None and mtime is not None and cached[0] == mtime:
        return f"{cache_key}@{cached[0]}", cached[1]

    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                source = fh.read()
            template_source_cache[cache_key] = (mtime or 0.0, source)
            return f"{cache_key}@{template_source_cache[cache_key][0]}", source
        except OSError as exc:
            last_exc = exc
            if getattr(exc, "errno", None) == errno.EDEADLK and attempt < 2:
                sleep_fn(0.03 * (attempt + 1))
                continue
            break
        except Exception as exc:
            last_exc = exc
            break

    if last_exc is not None:
        log_suppressed_exception("template_file_load_failed", last_exc, template=template_file, attempts=3)
    if cached is not None:
        return f"{cache_key}@{cached[0]}", cached[1]
    return None


def prewarm_template_file_cache(
    *,
    template_files: Iterable[str],
    load_template_file_source_fn: Callable[[str], Optional[Tuple[str, str]]],
) -> None:
    for template_file in sorted(set(template_files)):
        load_template_file_source_fn(template_file)


def resolve_template_source(
    template_ref: str,
    *,
    template_file_for_ref_fn: Callable[[str], Optional[str]],
    load_template_file_source_fn: Callable[[str], Optional[Tuple[str, str]]],
) -> Tuple[str, str]:
    template_file = template_file_for_ref_fn(template_ref)
    if template_file:
        loaded = load_template_file_source_fn(template_file)
        if loaded is not None:
            return loaded

    if isinstance(template_ref, str) and template_ref.endswith(".html") and "<" not in template_ref:
        loaded = load_template_file_source_fn(template_ref)
        if loaded is not None:
            return loaded

    return f"inline:{hash(template_ref)}", template_ref


def render_template_response(
    template_string: str,
    *,
    request: Optional[Request],
    context: Dict[str, Any],
    resolve_template_source_fn: Callable[[str], Tuple[str, str]],
    template_cache: Dict[str, Any],
    jinja_env: Any,
    csrf_cookie_name: str,
    secure_cookies: bool,
    force_https: bool,
    inject_ui_shell_fn: Optional[Callable[[str, Request, Dict[str, Any]], str]],
    log_suppressed_exception: Callable[[str, Exception, Any], None],
) -> HTMLResponse:
    template_key, template_source = resolve_template_source_fn(template_string)
    template = template_cache.get(template_key)
    if not template:
        template = jinja_env.from_string(template_source)
        template_cache[template_key] = template

    rendered_context = dict(context or {})
    set_cookie = False
    if request is not None:
        token = request.cookies.get(csrf_cookie_name)
        if not token:
            token = secrets.token_urlsafe(32)
            set_cookie = True
        rendered_context["csrf_token"] = token

    html = template.render(**rendered_context)
    if request is not None and inject_ui_shell_fn is not None:
        try:
            html = inject_ui_shell_fn(html, request, rendered_context)
        except Exception as exc:
            log_suppressed_exception(
                "ui_shell_injection_failed",
                exc,
                path=str(getattr(request.url, "path", "")),
            )

    response = HTMLResponse(content=html)
    if request is not None and set_cookie:
        response.set_cookie(
            csrf_cookie_name,
            rendered_context["csrf_token"],
            httponly=True,
            samesite="lax",
            secure=secure_cookies or force_https,
        )
    return response


def image_file_to_data_url(
    file_path: str,
    *,
    log_suppressed_exception: Callable[[str, Exception, Any], None],
) -> Optional[str]:
    if not file_path:
        return None
    if not os.path.isfile(file_path):
        return None

    ext = os.path.splitext(file_path)[1].lower()
    mime_map = {
        ".png": "image/png",
        ".pgn": "image/jpeg",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
    }
    mime_type = mime_map.get(ext)
    if mime_type is None:
        return None

    try:
        with open(file_path, "rb") as fh:
            encoded = base64.b64encode(fh.read()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"
    except Exception as exc:
        log_suppressed_exception("image_to_data_url_failed", exc, path=file_path)
        return None


def resolve_menu_asset(
    default_url: str,
    local_path: str,
    packaged_fallback_path: str = "",
    *,
    image_file_to_data_url_fn: Callable[[str], Optional[str]],
    offline_strict_mode: bool,
) -> str:
    for candidate in (local_path, packaged_fallback_path):
        data_url = image_file_to_data_url_fn(candidate)
        if data_url:
            return data_url
    if offline_strict_mode:
        return ""
    return default_url
