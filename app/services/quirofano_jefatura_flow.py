from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.services.quirofano_jefatura_programacion_flow import render_jefatura_quirofano_home_flow


async def render_jefatura_quirofano_waiting_flow(
    request: Request,
    session: Session,
    *,
    target_date: Optional[date] = None,
    flash: Optional[Dict[str, str]] = None,
) -> HTMLResponse:
    return await render_jefatura_quirofano_home_flow(request, session, target_date=target_date, flash=flash)

