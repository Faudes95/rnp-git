from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import desc, or_, select
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.models.hospitalization_episode import HOSPITALIZATION_EPISODES, ensure_hospitalization_notes_schema
from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES
from app.services.consulta_secciones_flow import attach_draft_to_consulta, get_draft_section_payload
from app.services.event_log_flow import emit_event
from app.services.master_identity_flow import (
    PATIENT_MASTER_IDENTITY,
    ensure_master_identity_schema,
    get_master_identity_snapshot,
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_name(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_text(value).upper())


def _nss10(value: Any, m: Any | None = None) -> str:
    raw = _safe_text(value)
    if m is not None:
        try:
            return _safe_text(m.normalize_nss(raw))
        except Exception:
            pass
    return re.sub(r"\D", "", raw)[:10]


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(str(value).strip().replace(",", ""))
    except Exception:
        return None


def _safe_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    txt = _safe_text(value)
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(txt[:10], fmt).date()
        except Exception:
            continue
    return None


def _load_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        obj = json.loads(str(value))
        if isinstance(default, list) and isinstance(obj, list):
            return obj
        if isinstance(default, dict) and isinstance(obj, dict):
            return obj
    except Exception:
        return default
    return default


def _pick_first(payload: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        if key in payload:
            val = _safe_text(payload.get(key))
            if val:
                return val
    return ""


def _consulta_ids_for_nss(db: Session, m: Any, nss: str, limit: int = 200) -> List[int]:
    if not nss:
        return []
    ids: List[int] = []
    for row in (
        db.query(m.ConsultaDB)
        .filter(m.ConsultaDB.nss == nss)
        .order_by(m.ConsultaDB.id.desc())
        .limit(max(1, min(int(limit), 5000)))
        .all()
    ):
        try:
            ids.append(int(row.id))
        except Exception:
            continue
    return ids


def _resolve_nss_by_master_identity(db: Session, *, curp: str, nombre: str = "") -> str:
    c = _safe_text(curp).upper()
    if not c:
        return ""
    try:
        ensure_master_identity_schema(db)
        q = select(PATIENT_MASTER_IDENTITY).where(PATIENT_MASTER_IDENTITY.c.curp_canonico == c).limit(1)
        row = db.execute(q).mappings().first()
        if row and _safe_text(row.get("nss_canonico")):
            return _safe_text(row.get("nss_canonico"))
    except Exception:
        pass
    if nombre:
        try:
            qn = (
                select(PATIENT_MASTER_IDENTITY)
                .where(PATIENT_MASTER_IDENTITY.c.nombre_canonico == _norm_name(nombre))
                .order_by(PATIENT_MASTER_IDENTITY.c.actualizado_en.desc())
                .limit(1)
            )
            rown = db.execute(qn).mappings().first()
            if rown and _safe_text(rown.get("nss_canonico")):
                return _safe_text(rown.get("nss_canonico"))
        except Exception:
            pass
    return ""


def _build_recent_labs_summary(rows: List[Any], limit: int = 12) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows[: max(1, int(limit))]:
        out.append(
            {
                "timestamp": getattr(row, "timestamp", None).isoformat() if getattr(row, "timestamp", None) else None,
                "test_code": _safe_text(getattr(row, "test_code", "")),
                "test_name": _safe_text(getattr(row, "test_name", "")),
                "value": _safe_text(getattr(row, "value", "")),
                "unit": _safe_text(getattr(row, "unit", "")),
                "source": _safe_text(getattr(row, "source", "")),
            }
        )
    return out


def build_patient_context(
    db: Session,
    m: Any,
    *,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    curp: Optional[str] = None,
    hospitalizacion_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Contexto clínico unificado para prefill/autocompletar en hospitalización.

    Agrega datos de consulta, hospitalización, notas diarias, vitales/labs, quirófano y archivos.
    """
    try:
        ensure_master_identity_schema(db)
        ensure_hospitalization_notes_schema(db)
        ensure_hosp_schema = getattr(m, "ensure_hospitalizacion_schema", None)
        if callable(ensure_hosp_schema):
            ensure_hosp_schema()
    except Exception:
        # Aditivo: nunca bloquear el prefill por aseguramiento de esquema.
        pass

    target_consulta_id = _safe_int(consulta_id)
    target_hosp_id = _safe_int(hospitalizacion_id)
    target_nss = _nss10(nss, m)
    target_curp = _safe_text(curp).upper()
    if not target_nss and target_curp:
        target_nss = _nss10(_resolve_nss_by_master_identity(db, curp=target_curp), m)

    hosp_row = None
    if target_hosp_id is not None:
        hosp_row = db.query(m.HospitalizacionDB).filter(m.HospitalizacionDB.id == int(target_hosp_id)).first()
        if hosp_row is not None and target_consulta_id is None:
            target_consulta_id = _safe_int(getattr(hosp_row, "consulta_id", None))
        if hosp_row is not None and not target_nss:
            target_nss = _nss10(getattr(hosp_row, "nss", ""), m)

    consulta = None
    if target_consulta_id is not None:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(target_consulta_id)).first()
    if consulta is None and target_nss:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nss == target_nss)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    if consulta is None and target_curp:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.curp == target_curp)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )

    if consulta is not None:
        if target_consulta_id is None:
            target_consulta_id = _safe_int(getattr(consulta, "id", None))
        if not target_nss:
            target_nss = _nss10(getattr(consulta, "nss", ""), m)
        if not target_curp:
            target_curp = _safe_text(getattr(consulta, "curp", "")).upper()

    consulta_ids: List[int] = []
    if target_nss:
        consulta_ids = _consulta_ids_for_nss(db, m, target_nss, limit=400)
    elif target_consulta_id is not None:
        consulta_ids = [int(target_consulta_id)]

    if target_consulta_id is not None and int(target_consulta_id) not in consulta_ids:
        consulta_ids.insert(0, int(target_consulta_id))

    active_hosp = None
    closed_hosp = None
    hosp_q = db.query(m.HospitalizacionDB)
    if target_hosp_id is not None:
        active_hosp = db.query(m.HospitalizacionDB).filter(m.HospitalizacionDB.id == int(target_hosp_id)).first()
    if active_hosp is None:
        if consulta_ids:
            active_hosp = (
                hosp_q.filter(m.HospitalizacionDB.consulta_id.in_(consulta_ids))
                .filter(m.HospitalizacionDB.estatus == "ACTIVO")
                .order_by(m.HospitalizacionDB.id.desc())
                .first()
            )
        elif target_nss:
            active_hosp = (
                hosp_q.filter(m.HospitalizacionDB.nss == target_nss)
                .filter(m.HospitalizacionDB.estatus == "ACTIVO")
                .order_by(m.HospitalizacionDB.id.desc())
                .first()
            )

    if consulta_ids:
        closed_hosp = (
            hosp_q.filter(m.HospitalizacionDB.consulta_id.in_(consulta_ids))
            .filter(m.HospitalizacionDB.estatus != "ACTIVO")
            .order_by(m.HospitalizacionDB.fecha_ingreso.desc(), m.HospitalizacionDB.id.desc())
            .first()
        )
    elif target_nss:
        closed_hosp = (
            hosp_q.filter(m.HospitalizacionDB.nss == target_nss)
            .filter(m.HospitalizacionDB.estatus != "ACTIVO")
            .order_by(m.HospitalizacionDB.fecha_ingreso.desc(), m.HospitalizacionDB.id.desc())
            .first()
        )

    vital_latest = None
    labs_latest_rows: List[Any] = []
    qv = db.query(m.VitalDB)
    ql = db.query(m.LabDB)
    if consulta_ids:
        qv = qv.filter(m.VitalDB.consulta_id.in_(consulta_ids))
        ql = ql.filter(m.LabDB.consulta_id.in_(consulta_ids))
    elif target_nss:
        qv = qv.filter(m.VitalDB.patient_id == target_nss)
        ql = ql.filter(m.LabDB.patient_id == target_nss)
    vital_latest = qv.order_by(m.VitalDB.timestamp.desc(), m.VitalDB.id.desc()).first()
    labs_latest_rows = ql.order_by(m.LabDB.timestamp.desc(), m.LabDB.id.desc()).limit(30).all()

    notes_latest: Optional[Dict[str, Any]] = None
    episodes_latest: Optional[Dict[str, Any]] = None
    try:
        ensure_hospitalization_notes_schema(db)
        q_notes = select(INPATIENT_DAILY_NOTES)
        q_eps = select(HOSPITALIZATION_EPISODES)
        cond_notes = []
        cond_eps = []
        if target_hosp_id is not None:
            cond_notes.append(INPATIENT_DAILY_NOTES.c.hospitalizacion_id == int(target_hosp_id))
            cond_eps.append(HOSPITALIZATION_EPISODES.c.hospitalizacion_id == int(target_hosp_id))
        if consulta_ids:
            cond_notes.append(INPATIENT_DAILY_NOTES.c.consulta_id.in_(consulta_ids))
            cond_eps.append(HOSPITALIZATION_EPISODES.c.consulta_id.in_(consulta_ids))
        if target_nss:
            cond_notes.append(INPATIENT_DAILY_NOTES.c.patient_id == target_nss)
            cond_eps.append(HOSPITALIZATION_EPISODES.c.patient_id == target_nss)
        if cond_notes:
            q_notes = q_notes.where(or_(*cond_notes))
        if cond_eps:
            q_eps = q_eps.where(or_(*cond_eps))
        notes_row = db.execute(
            q_notes.order_by(INPATIENT_DAILY_NOTES.c.note_date.desc(), INPATIENT_DAILY_NOTES.c.id.desc()).limit(1)
        ).mappings().first()
        ep_row = db.execute(
            q_eps.order_by(HOSPITALIZATION_EPISODES.c.started_on.desc(), HOSPITALIZATION_EPISODES.c.id.desc()).limit(1)
        ).mappings().first()
        if notes_row:
            notes_latest = {
                "note_date": notes_row.get("note_date").isoformat() if notes_row.get("note_date") else None,
                "diagnostico": _safe_text(notes_row.get("diagnostico")),
                "cie10_codigo": _safe_text(notes_row.get("cie10_codigo")),
                "service": _safe_text(notes_row.get("service")),
                "location": _safe_text(notes_row.get("location")),
                "vitals": _load_json(notes_row.get("vitals_json"), {}),
                "labs": _load_json(notes_row.get("labs_json"), {}),
                "devices": _load_json(notes_row.get("devices_json"), {}),
                "events": _load_json(notes_row.get("events_json"), {}),
                "free_text": _safe_text(notes_row.get("free_text") or notes_row.get("note_text")),
            }
        if ep_row:
            episodes_latest = {
                "id": ep_row.get("id"),
                "status": _safe_text(ep_row.get("status")),
                "service": _safe_text(ep_row.get("service")),
                "location": _safe_text(ep_row.get("location")),
                "started_on": ep_row.get("started_on").isoformat() if ep_row.get("started_on") else None,
                "ended_on": ep_row.get("ended_on").isoformat() if ep_row.get("ended_on") else None,
            }
    except Exception:
        notes_latest = None
        episodes_latest = None

    files_rows: List[Any] = []
    if consulta_ids:
        files_rows = (
            db.query(m.ArchivoPacienteDB)
            .filter(m.ArchivoPacienteDB.consulta_id.in_(consulta_ids))
            .order_by(m.ArchivoPacienteDB.fecha_subida.desc(), m.ArchivoPacienteDB.id.desc())
            .limit(25)
            .all()
        )

    surgical_programacion = None
    surgical_postop = None
    legacy_qx = None
    if consulta_ids:
        legacy_qx = (
            db.query(m.QuirofanoDB)
            .filter(m.QuirofanoDB.consulta_id.in_(consulta_ids))
            .order_by(m.QuirofanoDB.id.desc())
            .first()
        )

    sdb = None
    try:
        sdb_factory = getattr(m, "_new_surgical_session", None)
        if callable(sdb_factory):
            sdb = sdb_factory(enable_dual_write=True)
        if sdb is not None:
            q_prog = sdb.query(m.SurgicalProgramacionDB)
            q_post = sdb.query(m.SurgicalPostquirurgicaDB)
            if consulta_ids:
                q_prog = q_prog.filter(m.SurgicalProgramacionDB.consulta_id.in_(consulta_ids))
                q_post = q_post.filter(m.SurgicalPostquirurgicaDB.consulta_id.in_(consulta_ids))
            elif target_nss:
                q_prog = q_prog.filter(m.SurgicalProgramacionDB.nss == target_nss)
            surgical_programacion = q_prog.order_by(m.SurgicalProgramacionDB.id.desc()).first()
            surgical_postop = q_post.order_by(m.SurgicalPostquirurgicaDB.id.desc()).first()
    except Exception:
        surgical_programacion = None
        surgical_postop = None
    finally:
        try:
            if sdb is not None:
                sdb.close()
        except Exception:
            pass

    nombre = _norm_name(
        getattr(consulta, "nombre", "")
        or getattr(active_hosp, "nombre_completo", "")
        or getattr(closed_hosp, "nombre_completo", "")
    )
    diagnostico = _safe_text(
        getattr(consulta, "diagnostico_principal", "")
        or getattr(active_hosp, "diagnostico", "")
        or getattr(closed_hosp, "diagnostico", "")
        or (notes_latest or {}).get("diagnostico")
    ).upper()
    medico_base = _safe_text(
        getattr(active_hosp, "medico_a_cargo", "")
        or getattr(consulta, "agregado_medico", "")
        or getattr(closed_hosp, "medico_a_cargo", "")
    ).upper()

    prefill = {
        "consulta_id": int(getattr(consulta, "id", target_consulta_id or 0) or 0) or "",
        "nss": target_nss,
        "nombre_completo": nombre,
        "edad": _safe_int(getattr(consulta, "edad", None) or getattr(active_hosp, "edad", None) or getattr(closed_hosp, "edad", None)) or "",
        "sexo": _safe_text(getattr(consulta, "sexo", "") or getattr(active_hosp, "sexo", "") or "MASCULINO").upper(),
        "diagnostico": diagnostico,
        "agregado_medico": _safe_text(getattr(consulta, "agregado_medico", "") or getattr(active_hosp, "agregado_medico", "") or "").upper(),
        "medico_a_cargo": medico_base,
        "hgz_envio": _safe_text(getattr(active_hosp, "hgz_envio", "") or getattr(closed_hosp, "hgz_envio", "")).upper(),
        "origen_flujo": _safe_text(getattr(active_hosp, "origen_flujo", "")),
    }

    suggestion_proc = _safe_text(
        getattr(surgical_programacion, "procedimiento_programado", "")
        or getattr(surgical_postop, "procedimiento_realizado", "")
        or getattr(legacy_qx, "procedimiento", "")
    )
    suggestion_cirujano = _safe_text(
        getattr(surgical_programacion, "cirujano", "")
        or getattr(surgical_postop, "cirujano", "")
        or getattr(legacy_qx, "cirujano", "")
    )
    suggestion_fecha = (
        _safe_date(getattr(surgical_programacion, "fecha_programada", None))
        or _safe_date(getattr(surgical_postop, "fecha_realizacion", None))
        or _safe_date(getattr(legacy_qx, "fecha_programada", None))
    )

    latest_vitals = {
        "timestamp": getattr(vital_latest, "timestamp", None).isoformat() if vital_latest and getattr(vital_latest, "timestamp", None) else None,
        "ta": _safe_text(getattr(vital_latest, "sbp", "")) + ("/" + _safe_text(getattr(vital_latest, "dbp", "")) if vital_latest and getattr(vital_latest, "dbp", None) is not None else ""),
        "fc": _safe_int(getattr(vital_latest, "hr", None)),
        "temp": _safe_float(getattr(vital_latest, "temp", None)),
        "peso": _safe_float(getattr(vital_latest, "peso", None)),
        "talla": _safe_float(getattr(vital_latest, "talla", None)),
        "imc": _safe_float(getattr(vital_latest, "imc", None)),
        "source": _safe_text(getattr(vital_latest, "source", "")),
    }

    master_identity = {}
    if target_nss:
        try:
            master_identity = get_master_identity_snapshot(db, nss=target_nss, include_links=False)
        except Exception:
            master_identity = {}

    contexto = {
        "ok": True,
        "resolved": {
            "consulta_id": prefill.get("consulta_id") or None,
            "hospitalizacion_id": int(getattr(active_hosp, "id", target_hosp_id or 0) or 0) or None,
            "nss": target_nss,
            "curp": target_curp,
            "nombre": nombre,
            "consulta_ids": consulta_ids,
            "patient_uid": _safe_text((master_identity or {}).get("master", {}).get("patient_uid", "")),
        },
        "master_identity": master_identity or {},
        "prefill": prefill,
        "suggestions": {
            "procedimiento_text": suggestion_proc,
            "cirujano_text": suggestion_cirujano,
            "fecha_cirugia": suggestion_fecha.isoformat() if suggestion_fecha else "",
            "diagnostico_preop": diagnostico,
        },
        "last_known_values": {
            "vitals": latest_vitals,
            "labs": _build_recent_labs_summary(labs_latest_rows, limit=15),
            "daily_note": notes_latest or {},
            "episode": episodes_latest or {},
        },
        "hospitalizacion": {
            "activa": {
                "id": int(getattr(active_hosp, "id", 0) or 0) if active_hosp is not None else None,
                "cama": _safe_text(getattr(active_hosp, "cama", "")),
                "ingreso_tipo": _safe_text(getattr(active_hosp, "ingreso_tipo", "")),
                "fecha_ingreso": getattr(active_hosp, "fecha_ingreso", None).isoformat() if active_hosp is not None and getattr(active_hosp, "fecha_ingreso", None) else None,
            } if active_hosp is not None else None,
            "previa_cerrada": {
                "id": int(getattr(closed_hosp, "id", 0) or 0),
                "cama": _safe_text(getattr(closed_hosp, "cama", "")),
                "diagnostico": _safe_text(getattr(closed_hosp, "diagnostico", "")),
                "fecha_ingreso": getattr(closed_hosp, "fecha_ingreso", None).isoformat() if getattr(closed_hosp, "fecha_ingreso", None) else None,
                "fecha_egreso": getattr(closed_hosp, "fecha_egreso", None).isoformat() if getattr(closed_hosp, "fecha_egreso", None) else None,
            } if closed_hosp is not None else None,
        },
        "quirofano": {
            "programacion": {
                "id": int(getattr(surgical_programacion, "id", 0) or 0) if surgical_programacion is not None else None,
                "procedimiento": _safe_text(getattr(surgical_programacion, "procedimiento_programado", "")),
                "cirujano": _safe_text(getattr(surgical_programacion, "cirujano", "")),
                "fecha_programada": getattr(surgical_programacion, "fecha_programada", None).isoformat() if surgical_programacion is not None and getattr(surgical_programacion, "fecha_programada", None) else None,
                "estatus": _safe_text(getattr(surgical_programacion, "estatus", "")),
            },
            "postquirurgica": {
                "id": int(getattr(surgical_postop, "id", 0) or 0) if surgical_postop is not None else None,
                "procedimiento_realizado": _safe_text(getattr(surgical_postop, "procedimiento_realizado", "")),
                "fecha_realizacion": getattr(surgical_postop, "fecha_realizacion", None).isoformat() if surgical_postop is not None and getattr(surgical_postop, "fecha_realizacion", None) else None,
                "cirujano": _safe_text(getattr(surgical_postop, "cirujano", "")),
                "sangrado_ml": _safe_float(getattr(surgical_postop, "sangrado_ml", None)),
            },
            "legacy": {
                "id": int(getattr(legacy_qx, "id", 0) or 0) if legacy_qx is not None else None,
                "procedimiento": _safe_text(getattr(legacy_qx, "procedimiento", "")),
                "cirujano": _safe_text(getattr(legacy_qx, "cirujano", "")),
                "fecha_programada": getattr(legacy_qx, "fecha_programada", None).isoformat() if legacy_qx is not None and getattr(legacy_qx, "fecha_programada", None) else None,
                "estatus": _safe_text(getattr(legacy_qx, "estatus", "")),
            },
        },
        "archivos": [
            {
                "id": int(getattr(x, "id", 0) or 0),
                "consulta_id": int(getattr(x, "consulta_id", 0) or 0),
                "nombre_original": _safe_text(getattr(x, "nombre_original", "")),
                "extension": _safe_text(getattr(x, "extension", "")),
                "fecha_subida": getattr(x, "fecha_subida", None).isoformat() if getattr(x, "fecha_subida", None) else None,
            }
            for x in files_rows
        ],
        "generated_at": utcnow().isoformat() + "Z",
    }
    return contexto


def persist_hospitalizacion_context_snapshot(
    db: Session,
    m: Any,
    *,
    hospitalizacion_id: Optional[int],
    context: Dict[str, Any],
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    curp: Optional[str] = None,
    source: str = "hospitalizacion_nuevo",
) -> Optional[int]:
    hid = _safe_int(hospitalizacion_id)
    if hid is None:
        return None
    row = m.HospitalizacionContextSnapshotDB(
        hospitalizacion_id=int(hid),
        consulta_id=_safe_int(consulta_id),
        nss=_nss10(nss, m),
        curp=_safe_text(curp).upper() or None,
        source=_safe_text(source) or "hospitalizacion_nuevo",
        context_json=context or {},
        created_at=utcnow(),
    )
    db.add(row)
    db.flush()
    return int(getattr(row, "id", 0) or 0) or None


def create_consulta_from_metadata_draft_for_urgencias(
    db: Session,
    m: Any,
    *,
    draft_id: str,
    actor: str = "system",
    source_route: str = "/api/hospitalizacion/urgencias/finalizar",
) -> Dict[str, Any]:
    did = _safe_text(draft_id)
    if not did:
        raise ValueError("draft_id requerido")

    sections: Dict[str, Dict[str, Any]] = {}
    for code in ("1", "2", "3", "4", "5", "6", "7", "8", "9"):
        sec = get_draft_section_payload(db, draft_id=did, seccion_codigo=code)
        sections[code] = sec.get("payload") if isinstance(sec, dict) else {}
        if not isinstance(sections[code], dict):
            sections[code] = {}

    s1 = sections.get("1") or {}
    s2 = sections.get("2") or {}
    s6 = sections.get("6") or {}
    s7 = sections.get("7") or {}
    s8 = sections.get("8") or {}
    s9 = sections.get("9") or {}

    nss = _nss10(s1.get("nss"), m)
    nombre = _norm_name(s1.get("nombre"))
    if not nss or len(nss) != 10:
        raise ValueError("La sección 1 del draft no tiene NSS válido de 10 dígitos.")
    if not nombre:
        raise ValueError("La sección 1 del draft no tiene nombre de paciente.")

    sexo = _safe_text(s1.get("sexo") or "MASCULINO").upper()
    if sexo not in {"MASCULINO", "FEMENINO"}:
        sexo = "MASCULINO"

    diagnostico = _pick_first(
        s7,
        [
            "diagnostico_principal",
            "diagnostico_cie10",
            "diagnostico",
            "cie10_descripcion",
        ],
    ).upper()
    cie10 = _pick_first(s7, ["cie10_codigo", "diagnostico_cie10_codigo", "cie10"]).upper()

    consulta_row = m.ConsultaDB(
        fecha_registro=date.today(),
        curp=_safe_text(s1.get("curp")).upper() or None,
        nss=nss,
        agregado_medico=_safe_text(s1.get("agregado_medico")).upper() or None,
        nombre=nombre,
        fecha_nacimiento=_safe_date(s1.get("fecha_nacimiento")),
        edad=_safe_int(s1.get("edad")),
        sexo=sexo,
        tipo_sangre=_safe_text(s1.get("tipo_sangre")) or None,
        ocupacion=_safe_text(s1.get("ocupacion")) or None,
        nombre_empresa=_safe_text(s1.get("nombre_empresa")) or None,
        escolaridad=_safe_text(s1.get("escolaridad")) or None,
        cp=_safe_text(s1.get("cp")) or None,
        alcaldia=_safe_text(s1.get("alcaldia")) or None,
        colonia=_safe_text(s1.get("colonia")) or None,
        estado_foraneo=_safe_text(s1.get("estado_foraneo")) or None,
        calle=_safe_text(s1.get("calle")) or None,
        no_ext=_safe_text(s1.get("no_ext")) or None,
        no_int=_safe_text(s1.get("no_int")) or None,
        telefono=_safe_text(s1.get("telefono")) or None,
        email=_safe_text(s1.get("email")).lower() or None,
        peso=_safe_float(s2.get("peso")),
        talla=_safe_float(s2.get("talla")),
        imc=_safe_float(s2.get("imc")),
        ta=_safe_text(s2.get("ta")) or None,
        fc=_safe_int(s2.get("fc")),
        temp=_safe_float(s2.get("temp")),
        padecimiento_actual=_pick_first(s6, ["padecimiento_actual", "resumen_ingreso", "motivo_consulta"]),
        exploracion_fisica=_pick_first(s6, ["exploracion_fisica", "exploracion", "ef"]),
        diagnostico_principal=diagnostico or None,
        estudios_hallazgos=_pick_first(s8, ["estudios_hallazgos", "imagenologia_text", "resumen_estudios"]),
        estatus_protocolo=_safe_text(s9.get("estatus_protocolo") or "incompleto").lower(),
        plan_especifico=_pick_first(s9, ["plan_especifico", "plan", "conducta"]) or "INGRESO URGENCIAS",
        evento_clinico=_pick_first(s9, ["evento_clinico", "evento"]),
        fecha_evento=_safe_date(s9.get("fecha_evento")),
        protocolo_detalles={
            "metadata_draft_id": did,
            "captura_origen": "hospitalizacion_ingreso_urgencias",
            "diagnostico_cie10_codigo": cie10,
            "secciones": sections,
        },
        nota_soap_auto=json.dumps(
            {
                "subjetivo": _pick_first(s6, ["padecimiento_actual", "motivo_consulta"]),
                "objetivo": f"TA {_safe_text(s2.get('ta'))} FC {_safe_text(s2.get('fc'))} TEMP {_safe_text(s2.get('temp'))}",
                "analisis": diagnostico or "INGRESO POR URGENCIAS",
                "plan": _pick_first(s9, ["plan_especifico", "plan", "conducta"]) or "HOSPITALIZACION",
            },
            ensure_ascii=False,
        ),
    )

    db.add(consulta_row)
    db.flush()

    consulta_id = int(getattr(consulta_row, "id", 0) or 0)
    if not consulta_id:
        raise ValueError("No se pudo crear la consulta de urgencias.")

    try:
        attach_draft_to_consulta(db, draft_id=did, consulta_id=consulta_id)
    except Exception:
        # No bloquear guardado principal de consulta si falla el enlace aditivo.
        pass

    try:
        from app.services.consulta_externa_flow import register_consulta_attention

        register_consulta_attention(db, m, consulta=consulta_row)
    except Exception:
        db.rollback()

    try:
        from app.services.master_identity_flow import upsert_master_identity

        upsert_master_identity(
            db,
            nss=consulta_row.nss,
            curp=consulta_row.curp,
            nombre=consulta_row.nombre,
            sexo=consulta_row.sexo,
            consulta_id=consulta_id,
            source_table="consultas",
            source_pk=consulta_id,
            module="consulta_externa_urgencias",
            fecha_evento=consulta_row.fecha_registro,
            payload={
                "diagnostico_principal": consulta_row.diagnostico_principal,
                "estatus_protocolo": consulta_row.estatus_protocolo,
                "metadata_draft_id": did,
            },
            commit=False,
        )
    except Exception:
        pass

    try:
        emit_event(
            db,
            module="hospitalizacion",
            event_type="URG_INGRESO_CONSULTA_CREATED",
            entity="consultas",
            entity_id=str(consulta_id),
            consulta_id=consulta_id,
            actor=actor,
            source_route=source_route,
            payload={
                "draft_id": did,
                "nss": nss,
                "nombre": nombre,
                "diagnostico": diagnostico,
            },
            commit=False,
        )
    except Exception:
        pass

    db.commit()
    db.refresh(consulta_row)

    return {
        "ok": True,
        "consulta_id": consulta_id,
        "nss": nss,
        "nombre": nombre,
        "diagnostico": diagnostico,
        "draft_id": did,
    }
