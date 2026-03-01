from __future__ import annotations

from typing import Any, Dict, Optional

from app.db.sessions import clinical_session, surgical_session
from app.services.fau_quirofano_agent import (
    analyze_quirofano_programacion,
    analyze_quirofano_programaciones,
    list_quirofano_predictions,
    list_recent_quirofano_alerts,
    summarize_quirofano_alerts,
)


class QuirofanoAgentFacade:
    """Entidad IA independiente del módulo Quirófano.

    Consume el servicio operativo existente sin alterar rutas ni lógica clínica.
    """

    def analyze_programacion(self, programacion_id: int, *, run_id: Optional[int] = None) -> Dict[str, Any]:
        from app.core.app_context import main_proxy as m

        with clinical_session() as db, surgical_session() as sdb:
            return analyze_quirofano_programacion(
                db,
                sdb,
                m,
                programacion_id=int(programacion_id),
                run_id=run_id,
            )

    def analyze_window(
        self,
        *,
        window_days: int = 30,
        run_id: Optional[int] = None,
        limit: int = 700,
    ) -> Dict[str, Any]:
        from app.core.app_context import main_proxy as m

        with clinical_session() as db, surgical_session() as sdb:
            return analyze_quirofano_programaciones(
                db,
                sdb,
                m,
                window_days=window_days,
                run_id=run_id,
                limit=limit,
            )

    def latest_alerts(
        self,
        *,
        limit: int = 200,
        days: int = 30,
        level: Optional[str] = None,
        only_open: bool = False,
    ) -> Dict[str, Any]:
        with surgical_session() as sdb:
            rows = list_recent_quirofano_alerts(
                sdb,
                limit=limit,
                days=days,
                level=level,
                only_open=only_open,
            )
            return {"resumen": summarize_quirofano_alerts(rows), "alertas": rows}

    def latest_predictions(
        self,
        *,
        programacion_id: Optional[int] = None,
        consulta_id: Optional[int] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        with surgical_session() as sdb:
            rows = list_quirofano_predictions(
                sdb,
                programacion_id=programacion_id,
                consulta_id=consulta_id,
                limit=limit,
            )
            return {"total": len(rows), "predicciones": rows}

