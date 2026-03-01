"""
RBAC (Role-Based Access Control) — Item #2.

Decorador y dependencia FastAPI para proteger endpoints según rol.
Roles: admin, jefe_servicio, medico_adscrito, residente, enfermeria, capturista, readonly
"""
from __future__ import annotations

import hashlib
import functools
from typing import List, Optional, Set, Tuple

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.governance_models import GOV_USERS, GOV_METADATA

# Jerarquía de permisos (mayor nivel tiene más permisos)
ROLE_HIERARCHY = {
    "admin": 100,
    "jefe_servicio": 80,
    "medico_adscrito": 60,
    "residente": 40,
    "enfermeria": 30,
    "capturista": 20,
    "readonly": 10,
}

# Permisos por módulo
MODULE_PERMISSIONS = {
    "gobernanza": {"admin", "jefe_servicio"},
    "governance_read": {"admin", "jefe_servicio", "medico_adscrito"},
    "interconsultas_write": {"admin", "jefe_servicio", "medico_adscrito", "residente"},
    "interconsultas_read": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria"},
    "consulta_write": {"admin", "jefe_servicio", "medico_adscrito", "residente"},
    "consulta_read": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria", "capturista"},
    "quirofano_write": {"admin", "jefe_servicio", "medico_adscrito", "residente"},
    "quirofano_read": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria"},
    "hospitalizacion_write": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria"},
    "hospitalizacion_read": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria", "capturista"},
    "enfermeria_write": {"admin", "jefe_servicio", "enfermeria"},
    "enfermeria_read": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria"},
    "firma_write": {"admin", "jefe_servicio", "medico_adscrito", "residente"},
    "admin_only": {"admin"},
    "all": {"admin", "jefe_servicio", "medico_adscrito", "residente", "enfermeria", "capturista", "readonly"},
}


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        GOV_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


def get_current_user_role(request: Request) -> Tuple[str, str]:
    """Extrae username y rol del request actual.

    Primero busca en gov_users por username. Si no existe,
    retorna rol por defecto basado en las credenciales.
    """
    username = "anonymous"
    try:
        from base64 import b64decode
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("basic "):
            decoded = b64decode(auth_header[6:]).decode("utf-8", errors="replace")
            username = decoded.split(":")[0] if ":" in decoded else decoded
    except Exception:
        pass

    # Si es el admin por defecto, darle rol admin
    if username == "admin":
        return username, "admin"

    # Buscar en gov_users
    try:
        from app.core.app_context import main_proxy as m
        db_gen = m.get_db()
        db = next(db_gen)
        try:
            _ensure_tables(db)
            row = db.execute(
                select(GOV_USERS.c.rol)
                .where(GOV_USERS.c.username == username)
                .where(GOV_USERS.c.activo == True)
            ).first()
            if row:
                return username, row.rol
        finally:
            try:
                next(db_gen, None)
            except StopIteration:
                pass
    except Exception:
        pass

    # Default: medico_adscrito para usuarios autenticados
    return username, "medico_adscrito" if username != "anonymous" else "readonly"


def require_role(*allowed_roles: str):
    """Dependencia FastAPI que verifica que el usuario tenga el rol requerido.

    Uso:
        @router.post("/admin/action", dependencies=[Depends(require_role("admin", "jefe_servicio"))])
        async def admin_action():
            ...
    """
    allowed = set(allowed_roles)

    async def _check_role(request: Request):
        username, role = get_current_user_role(request)
        if role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Acceso denegado. Rol '{role}' no tiene permiso. Roles requeridos: {', '.join(sorted(allowed))}",
            )
        # Attach user info to request state for downstream use
        request.state.username = username
        request.state.user_role = role
        return {"username": username, "role": role}

    return _check_role


def require_module(module_name: str):
    """Dependencia FastAPI que verifica permisos por módulo.

    Uso:
        @router.post("/api/governance/users", dependencies=[Depends(require_module("gobernanza"))])
    """
    allowed = MODULE_PERMISSIONS.get(module_name, set())
    if not allowed:
        allowed = MODULE_PERMISSIONS.get("all", set())

    async def _check_module(request: Request):
        username, role = get_current_user_role(request)
        if role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Acceso denegado al módulo '{module_name}'. Rol '{role}' no autorizado.",
            )
        request.state.username = username
        request.state.user_role = role
        return {"username": username, "role": role}

    return _check_module


def get_user_info(request: Request) -> dict:
    """Helper para obtener info del usuario actual sin bloquear."""
    username, role = get_current_user_role(request)
    return {
        "username": username,
        "role": role,
        "role_level": ROLE_HIERARCHY.get(role, 0),
        "permissions": {
            mod: role in roles
            for mod, roles in MODULE_PERMISSIONS.items()
        },
    }
