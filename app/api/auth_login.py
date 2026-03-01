"""
API de Login y Sesiones — Item #6.

Endpoints para autenticación con roles, gestión de sesiones,
y login UI. Integra con gov_users y gov_sessions.
"""
from __future__ import annotations

import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import and_, select, update, insert
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.governance_models import (
    GOV_METADATA, GOV_USERS, GOV_SESSIONS,
)

router = APIRouter(tags=["auth"])

SESSION_DURATION_HOURS = 12
SESSION_COOKIE_NAME = "rnp_session"


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _ensure_tables(db: Session):
    try:
        GOV_METADATA.create_all(bind=db.get_bind(), checkfirst=True)
    except Exception:
        pass


def _ensure_default_admin(db: Session):
    """Asegura que exista al menos un usuario admin."""
    _ensure_tables(db)
    row = db.execute(
        select(GOV_USERS.c.id).where(GOV_USERS.c.username == "admin")
    ).first()
    if not row:
        now = utcnow()
        db.execute(insert(GOV_USERS).values(
            username="admin",
            password_hash=hashlib.sha256("admin".encode()).hexdigest(),
            nombre_completo="ADMINISTRADOR DEL SISTEMA",
            matricula="000000",
            cedula_profesional="",
            rol="admin",
            servicio="UROLOGIA",
            email="admin@imss.gob.mx",
            activo=True,
            creado_en=now,
            actualizado_en=now,
        ))
        db.commit()


# ---------------------------------------------------------------------------
# Login Page (HTML)
# ---------------------------------------------------------------------------
@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    from app.core.app_context import main_proxy as m
    return m.render_template("login.html", request=request)


# ---------------------------------------------------------------------------
# Login API
# ---------------------------------------------------------------------------
@router.post("/api/auth/login", response_class=JSONResponse)
async def do_login(request: Request, db: Session = Depends(_get_db)):
    """Autenticar usuario y crear sesión."""
    _ensure_tables(db)
    _ensure_default_admin(db)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    username = str(body.get("username", "")).strip().lower()
    password = str(body.get("password", "")).strip()

    if not username or not password:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Usuario y contraseña requeridos"})

    password_hash = hashlib.sha256(password.encode()).hexdigest()

    row = db.execute(
        select(GOV_USERS)
        .where(and_(
            GOV_USERS.c.username == username,
            GOV_USERS.c.password_hash == password_hash,
            GOV_USERS.c.activo == True,
        ))
    ).first()

    if not row:
        return JSONResponse(status_code=401, content={"ok": False, "error": "Credenciales inválidas"})

    # Create session
    now = utcnow()
    token = secrets.token_urlsafe(48)
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    db.execute(insert(GOV_SESSIONS).values(
        user_id=row.id,
        token_hash=token_hash,
        ip_address=request.client.host if request.client else "",
        user_agent=request.headers.get("user-agent", "")[:300],
        creado_en=now,
        expira_en=now + timedelta(hours=SESSION_DURATION_HOURS),
        activo=True,
    ))

    # Update last login
    db.execute(
        update(GOV_USERS)
        .where(GOV_USERS.c.id == row.id)
        .values(ultimo_login=now, actualizado_en=now)
    )
    db.commit()

    response = JSONResponse(content={
        "ok": True,
        "token": token,
        "user": {
            "id": row.id,
            "username": row.username,
            "nombre": row.nombre_completo,
            "rol": row.rol,
            "servicio": row.servicio,
            "matricula": row.matricula,
        },
        "expires_in": SESSION_DURATION_HOURS * 3600,
    })

    # Set session cookie
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_DURATION_HOURS * 3600,
        httponly=True,
        samesite="lax",
        path="/",
    )

    return response


# ---------------------------------------------------------------------------
# Logout
# ---------------------------------------------------------------------------
@router.post("/api/auth/logout", response_class=JSONResponse)
async def do_logout(request: Request, db: Session = Depends(_get_db)):
    """Cerrar sesión activa."""
    _ensure_tables(db)

    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    if not token:
        # Try from Authorization header or body
        try:
            body = await request.json()
            token = body.get("token", "")
        except Exception:
            pass

    if token:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        db.execute(
            update(GOV_SESSIONS)
            .where(GOV_SESSIONS.c.token_hash == token_hash)
            .values(activo=False)
        )
        db.commit()

    response = JSONResponse(content={"ok": True, "message": "Sesión cerrada"})
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return response


# ---------------------------------------------------------------------------
# Session Info
# ---------------------------------------------------------------------------
@router.get("/api/auth/me", response_class=JSONResponse)
async def get_current_session(request: Request, db: Session = Depends(_get_db)):
    """Obtener info del usuario logueado."""
    _ensure_tables(db)

    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    if not token:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]

    if not token:
        return JSONResponse(content={"ok": False, "authenticated": False})

    token_hash = hashlib.sha256(token.encode()).hexdigest()
    now = utcnow()

    session = db.execute(
        select(GOV_SESSIONS)
        .where(and_(
            GOV_SESSIONS.c.token_hash == token_hash,
            GOV_SESSIONS.c.activo == True,
            GOV_SESSIONS.c.expira_en > now,
        ))
    ).first()

    if not session:
        return JSONResponse(content={"ok": False, "authenticated": False})

    user = db.execute(
        select(GOV_USERS)
        .where(GOV_USERS.c.id == session.user_id)
    ).first()

    if not user:
        return JSONResponse(content={"ok": False, "authenticated": False})

    return JSONResponse(content={
        "ok": True,
        "authenticated": True,
        "user": {
            "id": user.id,
            "username": user.username,
            "nombre": user.nombre_completo,
            "rol": user.rol,
            "servicio": user.servicio,
            "matricula": user.matricula,
            "cedula": user.cedula_profesional,
            "email": user.email,
        },
        "session": {
            "created": session.creado_en.isoformat() if session.creado_en else None,
            "expires": session.expira_en.isoformat() if session.expira_en else None,
        },
    })


# ---------------------------------------------------------------------------
# Change Password
# ---------------------------------------------------------------------------
@router.post("/api/auth/change-password", response_class=JSONResponse)
async def change_password(request: Request, db: Session = Depends(_get_db)):
    """Cambiar contraseña del usuario actual."""
    _ensure_tables(db)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "JSON inválido"})

    username = str(body.get("username", "")).strip().lower()
    current_pw = str(body.get("current_password", "")).strip()
    new_pw = str(body.get("new_password", "")).strip()

    if not username or not current_pw or not new_pw:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Todos los campos son requeridos"})

    if len(new_pw) < 6:
        return JSONResponse(status_code=400, content={"ok": False, "error": "La contraseña debe tener al menos 6 caracteres"})

    current_hash = hashlib.sha256(current_pw.encode()).hexdigest()
    row = db.execute(
        select(GOV_USERS.c.id)
        .where(and_(
            GOV_USERS.c.username == username,
            GOV_USERS.c.password_hash == current_hash,
            GOV_USERS.c.activo == True,
        ))
    ).first()

    if not row:
        return JSONResponse(status_code=401, content={"ok": False, "error": "Credenciales actuales inválidas"})

    new_hash = hashlib.sha256(new_pw.encode()).hexdigest()
    db.execute(
        update(GOV_USERS)
        .where(GOV_USERS.c.id == row.id)
        .values(password_hash=new_hash, actualizado_en=utcnow())
    )
    db.commit()

    return JSONResponse(content={"ok": True, "message": "Contraseña actualizada"})
