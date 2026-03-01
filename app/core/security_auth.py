from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import HTTPException, status
from fastapi.security import HTTPBasicCredentials


@dataclass(frozen=True)
class AuthSettings:
    enabled: bool
    allow_insecure_default_credentials: bool
    user: str
    password: str


def load_auth_settings(logger: Any) -> AuthSettings:
    enabled = os.getenv("AUTH_ENABLED", "true").lower() not in ("0", "false", "no")
    allow_insecure = os.getenv("ALLOW_INSECURE_DEFAULT_CREDENTIALS", "false").lower() in ("1", "true", "yes")
    user = (os.getenv("IMSS_USER", "") or "").strip()
    password = os.getenv("IMSS_PASS")

    if enabled and (not user or not password):
        if allow_insecure:
            user = user or "admin"
            password = password or "admin"
            logger.warning(
                {
                    "event": "insecure_default_credentials_enabled",
                    "detail": "ALLOW_INSECURE_DEFAULT_CREDENTIALS=true habilita credenciales débiles; usar solo en desarrollo local.",
                }
            )
        else:
            user = user or "__disabled__"
            password = password or secrets.token_urlsafe(32)
            logger.warning(
                {
                    "event": "auth_credentials_missing",
                    "detail": "IMSS_USER/IMSS_PASS no definidos; autenticación bloqueada hasta configurar credenciales seguras.",
                }
            )

    return AuthSettings(
        enabled=enabled,
        allow_insecure_default_credentials=allow_insecure,
        user=user,
        password=password or "",
    )


def require_auth_basic(credentials: Optional[HTTPBasicCredentials], settings: AuthSettings) -> None:
    if not settings.enabled:
        return
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales requeridas",
            headers={"WWW-Authenticate": "Basic"},
        )
    valid_user = secrets.compare_digest(str(credentials.username or ""), str(settings.user or ""))
    valid_pass = secrets.compare_digest(str(credentials.password or ""), str(settings.password or ""))
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Basic"},
        )
