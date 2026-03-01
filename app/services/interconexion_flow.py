from typing import Any, Dict

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session


async def interconexion_consulta_flow(consulta_id: int, db: Session, sdb: Session) -> Any:
    from app.core.app_context import main_proxy as m

    consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == consulta_id).first()
    if not consulta:
        raise HTTPException(status_code=404, detail="Consulta no encontrada")

    hospitalizaciones = db.query(m.HospitalizacionDB).filter(m.HospitalizacionDB.consulta_id == consulta_id).all()
    cirugias_core = db.query(m.QuirofanoDB).filter(m.QuirofanoDB.consulta_id == consulta_id).all()
    cirugias_surgical = (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.consulta_id == consulta_id)
        .all()
    )
    feedback = (
        sdb.query(m.SurgicalFeedbackDB)
        .filter(m.SurgicalFeedbackDB.consulta_id == consulta_id)
        .order_by(m.SurgicalFeedbackDB.creado_en.desc())
        .limit(40)
        .all()
    )

    master_identity = None
    try:
        from app.services.master_identity_flow import (
            get_master_identity_journey,
            get_master_identity_snapshot,
        )

        snapshot = get_master_identity_snapshot(db, nss=consulta.nss, include_links=False)
        if snapshot.get("ok"):
            master = snapshot.get("master") or {}
            journey = get_master_identity_journey(db, nss=consulta.nss, limit=200)
            master_identity = {
                "patient_uid": master.get("patient_uid"),
                "nss_canonico": master.get("nss_canonico"),
                "ultima_consulta_id": master.get("ultima_consulta_id"),
                "source_count": master.get("source_count"),
                "conflicto_identidad": master.get("conflicto_identidad"),
                "timeline_total": journey.get("timeline_total") if isinstance(journey, dict) else None,
                "timeline_span_days": journey.get("timeline_span_days") if isinstance(journey, dict) else None,
            }
    except Exception:
        master_identity = None

    return JSONResponse(
        content={
            "consulta_id": consulta_id,
            "paciente": {
                "curp": consulta.curp,
                "nss": consulta.nss,
                "nombre": consulta.nombre,
                "diagnostico_principal": consulta.diagnostico_principal,
                "estatus_protocolo": consulta.estatus_protocolo,
            },
            "master_identity": master_identity,
            "modulos": {
                "consulta_externa": {"registros": 1},
                "hospitalizacion": {
                    "registros": len(hospitalizaciones),
                    "items": [
                        {
                            "id": h.id,
                            "motivo": h.motivo,
                            "servicio": h.servicio,
                            "estatus": h.estatus,
                            "fecha_ingreso": str(h.fecha_ingreso) if h.fecha_ingreso else None,
                            "fecha_egreso": str(h.fecha_egreso) if h.fecha_egreso else None,
                        }
                        for h in hospitalizaciones
                    ],
                },
                "quirofano_core": {
                    "registros": len(cirugias_core),
                    "items": [
                        {
                            "id": q.id,
                            "procedimiento": q.procedimiento,
                            "estatus": q.estatus,
                            "fecha_programada": str(q.fecha_programada) if q.fecha_programada else None,
                        }
                        for q in cirugias_core
                    ],
                },
                "quirofano_bd_dedicada": {
                    "registros": len(cirugias_surgical),
                    "items": [
                        {
                            "id": q.id,
                            "quirofano_id": q.quirofano_id,
                            "procedimiento": q.procedimiento,
                            "estatus": q.estatus,
                            "fecha_programada": str(q.fecha_programada) if q.fecha_programada else None,
                            "actualizado_en": q.actualizado_en.isoformat() if q.actualizado_en else None,
                        }
                        for q in cirugias_surgical
                    ],
                },
                "expediente": {"habilitado": True},
                "busqueda": {"habilitado": True},
                "reporte": {"habilitado": True},
            },
            "feedback": [
                {
                    "id": f.id,
                    "modulo": f.modulo,
                    "referencia_id": f.referencia_id,
                    "payload": f.payload,
                    "creado_en": f.creado_en.isoformat() if f.creado_en else None,
                }
                for f in feedback
            ],
        }
    )
