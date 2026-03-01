from __future__ import annotations

from typing import Any, Dict

from app.db.sessions import clinical_session, surgical_session
from app.services.fau_bot_flow import run_fau_bot_cycle


class FauBotCentralFacade:
    """Entidad IA central desacoplada para orquestación multi-agente."""

    def run_cycle(self, *, window_days: int = 30, triggered_by: str = "manual") -> Dict[str, Any]:
        with clinical_session() as db, surgical_session() as sdb:
            return run_fau_bot_cycle(
                db,
                sdb,
                window_days=max(1, min(int(window_days or 30), 365)),
                triggered_by=str(triggered_by or "manual"),
            )

