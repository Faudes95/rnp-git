from __future__ import annotations

from typing import Any, Dict, Optional


class AppDomainError(Exception):
    """Error de dominio para exponer fallas tipadas sin romper contratos existentes."""

    status_code: int = 400
    code: str = "app_domain_error"

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        if status_code is not None:
            self.status_code = int(status_code)
        if code:
            self.code = code
        self.details = details or {}


class ValidationDomainError(AppDomainError):
    status_code = 422
    code = "validation_error"


class NotFoundDomainError(AppDomainError):
    status_code = 404
    code = "not_found"


class InfrastructureDomainError(AppDomainError):
    status_code = 503
    code = "infrastructure_error"

