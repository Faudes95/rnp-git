"""
Ward Round Dashboard Flow — Pase de Visita Digital.

ADITIVO: No modifica ninguna lógica existente.
Proporciona una vista unificada de todos los pacientes hospitalizados
con nota SOAP inline y auto-cálculo de días.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import and_, desc, func, or_, select, text as sa_text
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val: Any, default: str = "") -> str:
    return str(val).strip() if val else default


def _auto_calc_days(
    fecha_ingreso: Any,
    fecha_cirugia: Any = None,
    ref_date: Optional[date] = None,
) -> Dict[str, int]:
    """Auto-calcula días de hospitalización y postquirúrgicos."""
    hoy = ref_date or date.today()
    dias_hosp = 0
    dias_postqx = 0

    if fecha_ingreso:
        try:
            if isinstance(fecha_ingreso, str):
                fecha_ingreso = datetime.strptime(fecha_ingreso[:10], "%Y-%m-%d").date()
            elif isinstance(fecha_ingreso, datetime):
                fecha_ingreso = fecha_ingreso.date()
            dias_hosp = max(0, (hoy - fecha_ingreso).days)
        except Exception:
            pass

    if fecha_cirugia:
        try:
            if isinstance(fecha_cirugia, str):
                fecha_cirugia = datetime.strptime(fecha_cirugia[:10], "%Y-%m-%d").date()
            elif isinstance(fecha_cirugia, datetime):
                fecha_cirugia = fecha_cirugia.date()
            dias_postqx = max(0, (hoy - fecha_cirugia).days)
        except Exception:
            pass

    return {"dias_hospitalizacion": dias_hosp, "dias_postquirurgicos": dias_postqx}


def _fetch_latest_vitals(db: Session, consulta_id: int) -> Dict[str, Any]:
    """Obtiene los signos vitales más recientes del paciente."""
    try:
        from app.models.inpatient_ai_models import VITALS_TS
        row = db.execute(
            select(VITALS_TS)
            .where(VITALS_TS.c.consulta_id == consulta_id)
            .order_by(desc(VITALS_TS.c.recorded_at))
            .limit(1)
        ).mappings().first()
        if row:
            return dict(row)
    except Exception:
        pass
    return {}


def _fetch_latest_labs(db: Session, consulta_id: int) -> List[Dict[str, Any]]:
    """Obtiene los laboratorios más recientes."""
    try:
        from app.models.inpatient_ai_models import LAB_RESULTS
        rows = db.execute(
            select(LAB_RESULTS)
            .where(LAB_RESULTS.c.consulta_id == consulta_id)
            .order_by(desc(LAB_RESULTS.c.collected_at))
            .limit(10)
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        pass
    return []


def _fetch_active_devices(db: Session, consulta_id: int) -> List[Dict[str, Any]]:
    """Obtiene dispositivos activos del paciente."""
    try:
        from app.models.inpatient_ai_models import UROLOGY_DEVICES
        rows = db.execute(
            select(UROLOGY_DEVICES)
            .where(
                and_(
                    UROLOGY_DEVICES.c.consulta_id == consulta_id,
                    UROLOGY_DEVICES.c.removed_at.is_(None),
                )
            )
            .order_by(desc(UROLOGY_DEVICES.c.placed_at))
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        pass
    return []


def _check_note_today(db: Session, consulta_id: int) -> bool:
    """Verifica si ya existe nota del día de hoy."""
    hoy = date.today()
    try:
        from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
        row = db.execute(
            select(func.count())
            .select_from(INPATIENT_DAILY_NOTES)
            .where(
                and_(
                    INPATIENT_DAILY_NOTES.c.consulta_id == consulta_id,
                    INPATIENT_DAILY_NOTES.c.note_date == hoy,
                )
            )
        ).scalar()
        return (row or 0) > 0
    except Exception:
        pass
    # Fallback: check legacy notes table
    try:
        from app.services.expediente_nota_medica_flow import EXPEDIENTE_NOTAS_DIARIAS
        row = db.execute(
            select(func.count())
            .select_from(EXPEDIENTE_NOTAS_DIARIAS)
            .where(
                and_(
                    EXPEDIENTE_NOTAS_DIARIAS.c.consulta_id == consulta_id,
                    EXPEDIENTE_NOTAS_DIARIAS.c.fecha_nota == hoy,
                )
            )
        ).scalar()
        return (row or 0) > 0
    except Exception:
        return False


def _lab_alert(labs: List[Dict]) -> Dict[str, Any]:
    """Determina alertas de laboratorio."""
    alerts = []
    lab_summary = {}
    for lab in labs:
        name = _safe(lab.get("test_name", "")).lower()
        val = lab.get("value_num")
        if val is None:
            continue
        if "creatinina" in name or "creat" in name:
            lab_summary["cr"] = val
            if val >= 2.0:
                alerts.append(f"Cr {val} ↑ AKI")
        elif "hemoglobin" in name or "hb" in name or "hemoglob" in name:
            lab_summary["hb"] = val
            if val < 8.0:
                alerts.append(f"Hb {val} ↓↓")
        elif "leucocit" in name or "leuc" in name or "wbc" in name:
            lab_summary["leuc"] = val
            if val > 10000:
                alerts.append(f"Leuc {val:,.0f} ↑")
        elif "sodio" in name or "na" == name:
            lab_summary["na"] = val
            if val < 135:
                alerts.append(f"Na {val} ↓")
        elif "potasio" in name or "k" == name:
            lab_summary["k"] = val
        elif "plaqueta" in name or "plt" in name:
            lab_summary["plaq"] = val
    return {"alerts": alerts, "summary": lab_summary}


def _device_abbreviation(device_type: str) -> str:
    """Abreviación estándar del dispositivo."""
    abbrevs = {
        "SONDA FOLEY": "SF",
        "CATETER JJ": "JJ",
        "CATETER URETERAL": "CU",
        "PENROSE": "PEN",
        "SARATOGA": "SAR",
        "JACKSON": "JP",
        "NEFROSTOMIA": "NF",
        "CONDUCTO ILEAL": "CI",
        "DRENAJE PELVICO": "DP",
    }
    for key, abbr in abbrevs.items():
        if key in (device_type or "").upper():
            return abbr
    return (device_type or "")[:3].upper()


def _fetch_surgical_date(db: Session, consulta_id: int) -> Optional[date]:
    """Busca la fecha de cirugía más reciente para el paciente."""
    try:
        row = db.execute(
            sa_text(
                "SELECT fecha_programacion FROM surgical_programaciones "
                "WHERE consulta_id = :cid AND estatus IN ('REALIZADA','COMPLETADA') "
                "ORDER BY fecha_programacion DESC LIMIT 1"
            ),
            {"cid": consulta_id},
        ).first()
        if row and row[0]:
            v = row[0]
            if isinstance(v, str):
                return datetime.strptime(v[:10], "%Y-%m-%d").date()
            if isinstance(v, datetime):
                return v.date()
            return v
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Main flows
# ---------------------------------------------------------------------------

async def ward_round_dashboard_flow(
    request: Request,
    db: Session,
    fecha: Optional[str] = None,
) -> HTMLResponse:
    """Renderiza el dashboard de pase de visita."""
    from app.core.app_context import main_proxy as m

    patients: List[Dict[str, Any]] = []
    totals = {
        "total": 0, "delicados": 0, "graves": 0,
        "ocupacion": 0, "promedio_estancia": 0, "egresos_hoy": 0,
    }

    try:
        rows = db.execute(
            sa_text(
                "SELECT * FROM hospitalizaciones "
                "WHERE estatus = 'ACTIVO' "
                "ORDER BY cama ASC"
            )
        ).mappings().all()

        estancia_days = []
        for h in rows:
            h = dict(h)
            cid = h.get("consulta_id") or 0
            nss = _safe(h.get("nss"))

            # Auto-calculate days
            fecha_qx = _fetch_surgical_date(db, cid)
            days = _auto_calc_days(h.get("fecha_ingreso"), fecha_qx)
            h["dias_hosp_auto"] = days["dias_hospitalizacion"]
            h["dias_postqx_auto"] = days["dias_postquirurgicos"]
            estancia_days.append(days["dias_hospitalizacion"])

            # Vitals
            h["vitals"] = _fetch_latest_vitals(db, cid)

            # Labs
            labs_raw = _fetch_latest_labs(db, cid)
            lab_info = _lab_alert(labs_raw)
            h["lab_alerts"] = lab_info["alerts"]
            h["lab_summary"] = lab_info["summary"]

            # Devices
            devices = _fetch_active_devices(db, cid)
            h["devices"] = devices
            h["devices_abbr"] = [
                {
                    "abbr": _device_abbreviation(d.get("device_type", "")),
                    "side": _safe(d.get("side", ""))[:1],
                    "fr": d.get("size_fr", ""),
                }
                for d in devices
            ]

            # Note status
            h["nota_hoy"] = _check_note_today(db, cid)

            # Estado clínico
            estado = _safe(h.get("estado_clinico", "ESTABLE")).upper()
            h["estado_clinico"] = estado
            if estado == "DELICADO":
                totals["delicados"] += 1
            elif estado == "GRAVE":
                totals["graves"] += 1

            patients.append(h)

        totals["total"] = len(patients)
        if estancia_days:
            totals["promedio_estancia"] = round(sum(estancia_days) / len(estancia_days), 1)
        # 36 camas totales como referencia IMSS urología
        totals["ocupacion"] = round(len(patients) / 36 * 100) if patients else 0

    except Exception as exc:
        logger.warning("ward_round_dashboard_flow error: %s", exc, exc_info=True)

    # Guardia actual (intentar leer de la tabla)
    guardia = {"r5": "", "r4": "", "r3": "", "r2": ""}
    try:
        g = db.execute(
            sa_text(
                "SELECT r5, r4, r3, r2 FROM hospital_guardias "
                "ORDER BY fecha DESC LIMIT 1"
            )
        ).mappings().first()
        if g:
            guardia = dict(g)
    except Exception:
        pass

    return m.render_template(
        "ward_round_dashboard.html",
        request,
        patients=patients,
        totals=totals,
        guardia=guardia,
        fecha_hoy=date.today().isoformat(),
    )


async def ward_round_save_inline_note_flow(
    request: Request,
    db: Session,
) -> JSONResponse:
    """Guarda nota SOAP inline desde el pase de visita."""
    try:
        form = await request.form()
        consulta_id = int(form.get("consulta_id", 0))
        hospitalizacion_id = int(form.get("hospitalizacion_id", 0))
        nss = _safe(form.get("nss"))
        nombre = _safe(form.get("nombre"))
        cama = _safe(form.get("cama"))
        soap_s = _safe(form.get("soap_s"))
        soap_o = _safe(form.get("soap_o"))
        soap_a = _safe(form.get("soap_a"))
        soap_p = _safe(form.get("soap_p"))

        nota_text = f"S: {soap_s}\nO: {soap_o}\nA: {soap_a}\nP: {soap_p}"
        hoy = date.today()

        # Save to legacy EXPEDIENTE_NOTAS_DIARIAS for backward compat
        try:
            from app.services.expediente_nota_medica_flow import (
                EXPEDIENTE_NOTAS_DIARIAS,
                ensure_expediente_nota_schema,
            )
            ensure_expediente_nota_schema(db)
            db.execute(
                EXPEDIENTE_NOTAS_DIARIAS.insert().values(
                    consulta_id=consulta_id,
                    nss=nss,
                    nombre=nombre,
                    fecha_nota=hoy,
                    cama=cama,
                    servicio="UROLOGIA",
                    nota_texto=nota_text,
                    creado_en=utcnow(),
                )
            )
        except Exception as e:
            logger.warning("ward_round legacy note save: %s", e)

        # Save to INPATIENT_DAILY_NOTES (structured)
        try:
            from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
            db.execute(
                INPATIENT_DAILY_NOTES.insert().values(
                    consulta_id=consulta_id,
                    patient_id=nss,
                    note_date=hoy,
                    note_type="EVOLUCION",
                    free_text=nota_text,
                    status="FINAL",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
            )
        except Exception as e:
            logger.warning("ward_round structured note save: %s", e)

        db.commit()
        return JSONResponse({"ok": True, "message": "Nota guardada exitosamente"})

    except Exception as exc:
        logger.error("ward_round_save_inline_note error: %s", exc, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


async def ward_round_autofill_vitals_flow(
    request: Request,
    db: Session,
) -> JSONResponse:
    """Devuelve los últimos vitales para auto-llenado."""
    try:
        consulta_id = int(request.query_params.get("consulta_id", 0))
        vitals = _fetch_latest_vitals(db, consulta_id)
        return JSONResponse({"ok": True, "vitals": vitals})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
