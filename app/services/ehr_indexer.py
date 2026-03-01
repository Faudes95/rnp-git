"""EHR Indexer — Indexa datos legacy en las tablas EHR aditivas.

Principio: NUNCA modifica tablas legacy. Lee de ellas y escribe en ehr_*.
Soporta reindex por paciente o por ventana de tiempo (last_days).
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session

from app.models.ehr_models import (
    EHR_DOCUMENTS,
    EHR_TIMELINE_EVENTS,
    EHR_TAGS,
    EHR_DOCUMENT_TAGS,
    EHR_FEATURES_DAILY,
    EHR_PROBLEM_LIST,
    EHR_ALERT_LIFECYCLE,
    ensure_ehr_schema,
)

logger = logging.getLogger("rnp.ehr_indexer")


# ═══════════════════════════════════════════════════════════════════════
#  UTILIDADES
# ═══════════════════════════════════════════════════════════════════════

def _safe_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _safe_json(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    try:
        return json.dumps(val, ensure_ascii=False, default=str)
    except Exception:
        return None


def _parse_json_field(val: Any) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _to_datetime(val: Any) -> Optional[datetime]:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    if isinstance(val, str) and val.strip():
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(val.strip(), fmt)
            except Exception:
                continue
    return None


# ═══════════════════════════════════════════════════════════════════════
#  GET OR CREATE TAG
# ═══════════════════════════════════════════════════════════════════════

def _get_or_create_tag(db: Session, tag_name: str, category: str = "diagnostico", cie10: str = "") -> int:
    """Retorna tag_id; crea si no existe."""
    tag_name = tag_name.strip().upper()[:120]
    if not tag_name:
        return 0

    row = db.execute(
        select(EHR_TAGS.c.id).where(EHR_TAGS.c.tag_name == tag_name)
    ).first()
    if row:
        return row[0]

    result = db.execute(
        EHR_TAGS.insert().values(
            tag_name=tag_name,
            tag_category=category,
            cie10_code=cie10 or None,
        )
    )
    db.flush()
    return result.inserted_primary_key[0] if result.inserted_primary_key else 0


# ═══════════════════════════════════════════════════════════════════════
#  INDEX CONSULTA
# ═══════════════════════════════════════════════════════════════════════

def _index_consulta(db: Session, consulta: Any, patient_uid: str) -> Optional[int]:
    """Indexa una consulta como documento + evento. Retorna document_id."""
    source_id = getattr(consulta, "id", 0)

    # Verificar si ya existe
    existing = db.execute(
        select(EHR_DOCUMENTS.c.id).where(
            EHR_DOCUMENTS.c.source_table == "consultas",
            EHR_DOCUMENTS.c.source_id == source_id,
        )
    ).first()
    if existing:
        return existing[0]

    fecha = _to_datetime(getattr(consulta, "fecha_registro", None)) or datetime.now()
    dx = _safe_str(getattr(consulta, "diagnostico_principal", ""))
    padecimiento = _safe_str(getattr(consulta, "padecimiento_actual", ""))
    ef = _safe_str(getattr(consulta, "exploracion_fisica", ""))
    plan = _safe_str(getattr(consulta, "plan_tratamiento", "") or getattr(consulta, "subsecuente_plan", ""))
    protocolo = _safe_str(getattr(consulta, "protocolo", ""))

    content_text = f"Dx: {dx}. {padecimiento} {ef} {plan}".strip()
    content_json = _safe_json({
        "diagnostico_principal": dx,
        "padecimiento_actual": padecimiento,
        "exploracion_fisica": ef,
        "plan": plan,
        "protocolo": protocolo,
        "peso": getattr(consulta, "peso", None),
        "talla": getattr(consulta, "talla", None),
        "imc": getattr(consulta, "imc", None),
        "ta": _safe_str(getattr(consulta, "ta", "")),
        "fc": getattr(consulta, "fc", None),
        "nota_soap": _safe_str(getattr(consulta, "nota_soap_auto", "")),
    })

    result = db.execute(
        EHR_DOCUMENTS.insert().values(
            patient_uid=patient_uid,
            source_table="consultas",
            source_id=source_id,
            doc_type="nota_consulta",
            title=f"Consulta — {dx}" if dx else "Consulta Externa",
            service="CONSULTA EXTERNA",
            content_json=content_json,
            content_text=content_text[:5000],
            version=1,
        )
    )
    db.flush()
    doc_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    # Timeline event
    db.execute(
        EHR_TIMELINE_EVENTS.insert().values(
            patient_uid=patient_uid,
            event_type="consulta",
            event_ts=fecha,
            document_id=doc_id,
            source_table="consultas",
            source_id=source_id,
            title=f"Consulta — {dx}" if dx else "Consulta Externa",
            summary=padecimiento[:300] if padecimiento else None,
            service="CONSULTA EXTERNA",
            severity="info",
        )
    )

    # Tag por diagnóstico
    if dx:
        tag_id = _get_or_create_tag(db, dx, "diagnostico")
        if tag_id and doc_id:
            db.execute(
                EHR_DOCUMENT_TAGS.insert().values(
                    document_id=doc_id, tag_id=tag_id, source="auto"
                )
            )

    return doc_id


# ═══════════════════════════════════════════════════════════════════════
#  INDEX HOSPITALIZACION
# ═══════════════════════════════════════════════════════════════════════

def _index_hospitalizacion(db: Session, hosp: Any, patient_uid: str) -> Optional[int]:
    """Indexa una hospitalización como documento + evento."""
    source_id = getattr(hosp, "id", 0)

    existing = db.execute(
        select(EHR_DOCUMENTS.c.id).where(
            EHR_DOCUMENTS.c.source_table == "hospitalizaciones",
            EHR_DOCUMENTS.c.source_id == source_id,
        )
    ).first()
    if existing:
        return existing[0]

    fecha = _to_datetime(getattr(hosp, "fecha_ingreso", None)) or datetime.now()
    motivo = _safe_str(getattr(hosp, "motivo", "") or getattr(hosp, "motivo_ingreso", ""))
    dx = _safe_str(getattr(hosp, "diagnostico", ""))
    servicio = _safe_str(getattr(hosp, "servicio", "HOSPITALIZACIÓN"))

    content_text = f"Hospitalización: {motivo or dx}. Servicio: {servicio}"
    content_json = _safe_json({
        "motivo": motivo,
        "diagnostico": dx,
        "servicio": servicio,
        "cama": _safe_str(getattr(hosp, "cama", "")),
        "fecha_ingreso": str(getattr(hosp, "fecha_ingreso", "")),
        "fecha_egreso": str(getattr(hosp, "fecha_egreso", "")),
        "dias_estancia": getattr(hosp, "dias_estancia", None),
    })

    result = db.execute(
        EHR_DOCUMENTS.insert().values(
            patient_uid=patient_uid,
            source_table="hospitalizaciones",
            source_id=source_id,
            doc_type="nota_ingreso",
            title=f"Ingreso — {motivo or dx}" if (motivo or dx) else "Hospitalización",
            service=servicio.upper() if servicio else "HOSPITALIZACIÓN",
            content_json=content_json,
            content_text=content_text[:5000],
            version=1,
        )
    )
    db.flush()
    doc_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    db.execute(
        EHR_TIMELINE_EVENTS.insert().values(
            patient_uid=patient_uid,
            event_type="hospitalizacion",
            event_ts=fecha,
            document_id=doc_id,
            source_table="hospitalizaciones",
            source_id=source_id,
            title=f"Ingreso — {motivo or dx}" if (motivo or dx) else "Hospitalización",
            summary=content_text[:300],
            service=servicio.upper() if servicio else "HOSPITALIZACIÓN",
            severity="warning",
        )
    )

    if dx:
        tag_id = _get_or_create_tag(db, dx, "diagnostico")
        if tag_id and doc_id:
            db.execute(
                EHR_DOCUMENT_TAGS.insert().values(
                    document_id=doc_id, tag_id=tag_id, source="auto"
                )
            )

    return doc_id


# ═══════════════════════════════════════════════════════════════════════
#  INDEX ATENCION (consulta_externa_atenciones)
# ═══════════════════════════════════════════════════════════════════════

def _index_atencion(db: Session, row: Dict[str, Any], patient_uid: str) -> Optional[int]:
    """Indexa una atención de consulta externa (LEOCH/UROENDOSCOPIA/etc)."""
    source_id = row.get("id", 0)
    if not source_id:
        return None

    existing = db.execute(
        select(EHR_DOCUMENTS.c.id).where(
            EHR_DOCUMENTS.c.source_table == "consulta_externa_atenciones",
            EHR_DOCUMENTS.c.source_id == source_id,
        )
    ).first()
    if existing:
        return existing[0]

    servicio = str(row.get("servicio") or "CONSULTA EXTERNA").upper()
    fecha = _to_datetime(row.get("fecha_atencion")) or datetime.now()
    dx = str(row.get("diagnostico_principal") or "")
    resumen = str(row.get("nota_resumen") or "")
    medico = str(row.get("medico_responsable") or "")

    doc_type = {
        "LEOCH": "nota_leoch",
        "UROENDOSCOPIA": "nota_uroendoscopia",
    }.get(servicio, "nota_consulta")

    content_text = f"{servicio}: {dx}. {resumen}".strip()

    result = db.execute(
        EHR_DOCUMENTS.insert().values(
            patient_uid=patient_uid,
            source_table="consulta_externa_atenciones",
            source_id=source_id,
            doc_type=doc_type,
            title=f"{servicio} — {dx}" if dx else servicio,
            author=medico,
            service=servicio,
            content_json=_safe_json(dict(row)),
            content_text=content_text[:5000],
            version=1,
        )
    )
    db.flush()
    doc_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    db.execute(
        EHR_TIMELINE_EVENTS.insert().values(
            patient_uid=patient_uid,
            event_type="procedimiento" if servicio in ("LEOCH", "UROENDOSCOPIA") else "consulta",
            event_ts=fecha,
            document_id=doc_id,
            source_table="consulta_externa_atenciones",
            source_id=source_id,
            title=f"{servicio} — {dx}" if dx else servicio,
            summary=resumen[:300] if resumen else None,
            service=servicio,
            author=medico,
            severity="info",
        )
    )

    if dx:
        tag_id = _get_or_create_tag(db, dx, "diagnostico")
        if tag_id and doc_id:
            db.execute(
                EHR_DOCUMENT_TAGS.insert().values(
                    document_id=doc_id, tag_id=tag_id, source="auto"
                )
            )

    return doc_id


# ═══════════════════════════════════════════════════════════════════════
#  INDEX CIRUGIA (surgical_programaciones)
# ═══════════════════════════════════════════════════════════════════════

def _index_cirugia(db: Session, qx: Any, patient_uid: str) -> Optional[int]:
    """Indexa una cirugía programada."""
    source_id = getattr(qx, "id", 0)

    existing = db.execute(
        select(EHR_DOCUMENTS.c.id).where(
            EHR_DOCUMENTS.c.source_table == "surgical_programaciones",
            EHR_DOCUMENTS.c.source_id == source_id,
        )
    ).first()
    if existing:
        return existing[0]

    fecha = _to_datetime(getattr(qx, "fecha_programacion", None) or getattr(qx, "fecha_cirugia", None)) or datetime.now()
    procedimiento = _safe_str(getattr(qx, "procedimiento", ""))
    patologia = _safe_str(getattr(qx, "patologia", ""))
    cirujano = _safe_str(getattr(qx, "cirujano", ""))

    content_text = f"Cirugía: {procedimiento}. Patología: {patologia}. Cirujano: {cirujano}"

    result = db.execute(
        EHR_DOCUMENTS.insert().values(
            patient_uid=patient_uid,
            source_table="surgical_programaciones",
            source_id=source_id,
            doc_type="nota_qx",
            title=f"Cirugía — {procedimiento}" if procedimiento else "Cirugía",
            author=cirujano,
            service="QUIRÓFANO",
            content_json=_safe_json({
                "procedimiento": procedimiento,
                "patologia": patologia,
                "cirujano": cirujano,
                "sala": _safe_str(getattr(qx, "sala", "")),
                "fecha": str(fecha),
            }),
            content_text=content_text[:5000],
            version=1,
        )
    )
    db.flush()
    doc_id = result.inserted_primary_key[0] if result.inserted_primary_key else None

    db.execute(
        EHR_TIMELINE_EVENTS.insert().values(
            patient_uid=patient_uid,
            event_type="cirugia",
            event_ts=fecha,
            document_id=doc_id,
            source_table="surgical_programaciones",
            source_id=source_id,
            title=f"Cirugía — {procedimiento}" if procedimiento else "Cirugía",
            summary=content_text[:300],
            service="QUIRÓFANO",
            author=cirujano,
            severity="critical",
        )
    )

    for tag_text, cat in [(procedimiento, "procedimiento"), (patologia, "diagnostico")]:
        if tag_text:
            tid = _get_or_create_tag(db, tag_text, cat)
            if tid and doc_id:
                db.execute(
                    EHR_DOCUMENT_TAGS.insert().values(
                        document_id=doc_id, tag_id=tid, source="auto"
                    )
                )

    return doc_id


# ═══════════════════════════════════════════════════════════════════════
#  PROBLEM LIST AUTO-GENERATION
# ═══════════════════════════════════════════════════════════════════════

def _rebuild_problem_list(db: Session, patient_uid: str) -> int:
    """Regenera la lista de problemas a partir de tags y diagnósticos."""
    # Limpiar existentes
    db.execute(
        delete(EHR_PROBLEM_LIST).where(EHR_PROBLEM_LIST.c.patient_uid == patient_uid)
    )
    db.flush()

    # Obtener tags únicos de diagnóstico para este paciente
    rows = db.execute(
        select(EHR_TAGS.c.tag_name, EHR_TAGS.c.cie10_code, func.min(EHR_TIMELINE_EVENTS.c.event_ts).label("first_seen"))
        .select_from(
            EHR_DOCUMENT_TAGS
            .join(EHR_DOCUMENTS, EHR_DOCUMENTS.c.id == EHR_DOCUMENT_TAGS.c.document_id)
            .join(EHR_TAGS, EHR_TAGS.c.id == EHR_DOCUMENT_TAGS.c.tag_id)
            .outerjoin(EHR_TIMELINE_EVENTS, EHR_TIMELINE_EVENTS.c.document_id == EHR_DOCUMENTS.c.id)
        )
        .where(
            EHR_DOCUMENTS.c.patient_uid == patient_uid,
            EHR_TAGS.c.tag_category == "diagnostico",
        )
        .group_by(EHR_TAGS.c.tag_name, EHR_TAGS.c.cie10_code)
    ).all()

    count = 0
    for r in rows:
        db.execute(
            EHR_PROBLEM_LIST.insert().values(
                patient_uid=patient_uid,
                problem_name=r[0],
                problem_category="activo",
                cie10_code=r[1],
                onset_date=r[2].date() if r[2] and hasattr(r[2], "date") else None,
                status="activo",
            )
        )
        count += 1

    return count


# ═══════════════════════════════════════════════════════════════════════
#  COMPLETENESS SCORE
# ═══════════════════════════════════════════════════════════════════════

def _calc_completeness_score(db: Session, patient_uid: str) -> float:
    """Calcula un score 0-100 de completitud del expediente."""
    checks = {
        "has_consulta": False,
        "has_dx": False,
        "has_antecedentes": False,
        "has_vitals": False,
        "has_ef": False,
        "has_plan": False,
        "has_labs": False,
        "has_hospitalizacion": False,
    }

    # Check docs by type
    doc_types = db.execute(
        select(EHR_DOCUMENTS.c.doc_type)
        .where(EHR_DOCUMENTS.c.patient_uid == patient_uid)
        .distinct()
    ).fetchall()
    doc_type_set = {r[0] for r in doc_types}

    checks["has_consulta"] = "nota_consulta" in doc_type_set
    checks["has_hospitalizacion"] = "nota_ingreso" in doc_type_set

    # Check content from most recent doc
    latest = db.execute(
        select(EHR_DOCUMENTS.c.content_json)
        .where(EHR_DOCUMENTS.c.patient_uid == patient_uid, EHR_DOCUMENTS.c.doc_type == "nota_consulta")
        .order_by(EHR_DOCUMENTS.c.created_at.desc())
        .limit(1)
    ).first()

    if latest and latest[0]:
        try:
            data = json.loads(latest[0]) if isinstance(latest[0], str) else latest[0]
            checks["has_dx"] = bool(data.get("diagnostico_principal"))
            checks["has_vitals"] = bool(data.get("peso") or data.get("ta") or data.get("fc"))
            checks["has_ef"] = bool(data.get("exploracion_fisica"))
            checks["has_plan"] = bool(data.get("plan") or data.get("nota_soap"))
        except Exception:
            pass

    # Check tags (proxy for antecedentes)
    tag_count = db.execute(
        select(func.count(EHR_DOCUMENT_TAGS.c.id))
        .select_from(
            EHR_DOCUMENT_TAGS.join(EHR_DOCUMENTS, EHR_DOCUMENTS.c.id == EHR_DOCUMENT_TAGS.c.document_id)
        )
        .where(EHR_DOCUMENTS.c.patient_uid == patient_uid)
    ).scalar() or 0
    checks["has_antecedentes"] = tag_count > 0

    # Check labs events
    lab_count = db.execute(
        select(func.count(EHR_TIMELINE_EVENTS.c.id))
        .where(
            EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid,
            EHR_TIMELINE_EVENTS.c.event_type == "lab",
        )
    ).scalar() or 0
    checks["has_labs"] = lab_count > 0

    total = sum(1 for v in checks.values() if v)
    return round((total / len(checks)) * 100, 1)


# ═══════════════════════════════════════════════════════════════════════
#  FEATURES DAILY
# ═══════════════════════════════════════════════════════════════════════

def _update_features_daily(db: Session, patient_uid: str) -> None:
    """Actualiza o crea el registro diario de features."""
    today = date.today()

    existing = db.execute(
        select(EHR_FEATURES_DAILY.c.id).where(
            EHR_FEATURES_DAILY.c.patient_uid == patient_uid,
            EHR_FEATURES_DAILY.c.feature_date == today,
        )
    ).first()

    total_consultas = db.execute(
        select(func.count(EHR_TIMELINE_EVENTS.c.id))
        .where(EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid, EHR_TIMELINE_EVENTS.c.event_type == "consulta")
    ).scalar() or 0

    total_hosp = db.execute(
        select(func.count(EHR_TIMELINE_EVENTS.c.id))
        .where(EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid, EHR_TIMELINE_EVENTS.c.event_type == "hospitalizacion")
    ).scalar() or 0

    total_qx = db.execute(
        select(func.count(EHR_TIMELINE_EVENTS.c.id))
        .where(EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid, EHR_TIMELINE_EVENTS.c.event_type == "cirugia")
    ).scalar() or 0

    # Alertas activas
    alertas = db.execute(
        select(func.count(EHR_ALERT_LIFECYCLE.c.id))
        .where(
            EHR_ALERT_LIFECYCLE.c.patient_uid == patient_uid,
            EHR_ALERT_LIFECYCLE.c.status.in_(["generated", "acknowledged"]),
        )
    ).scalar() or 0

    completeness = _calc_completeness_score(db, patient_uid)

    # Días desde última consulta
    last_consulta = db.execute(
        select(EHR_TIMELINE_EVENTS.c.event_ts)
        .where(EHR_TIMELINE_EVENTS.c.patient_uid == patient_uid, EHR_TIMELINE_EVENTS.c.event_type == "consulta")
        .order_by(EHR_TIMELINE_EVENTS.c.event_ts.desc())
        .limit(1)
    ).first()
    dias_ult_consulta = None
    if last_consulta and last_consulta[0]:
        try:
            dt = last_consulta[0] if isinstance(last_consulta[0], (date, datetime)) else datetime.fromisoformat(str(last_consulta[0]))
            dias_ult_consulta = (datetime.now() - (dt if isinstance(dt, datetime) else datetime(dt.year, dt.month, dt.day))).days
        except Exception:
            pass

    vals = dict(
        patient_uid=patient_uid,
        feature_date=today,
        total_consultas=total_consultas,
        total_hospitalizaciones=total_hosp,
        total_cirugias=total_qx,
        alertas_activas=alertas,
        completeness_score=completeness,
        dias_desde_ultima_consulta=dias_ult_consulta,
    )

    if existing:
        db.execute(
            EHR_FEATURES_DAILY.update()
            .where(EHR_FEATURES_DAILY.c.id == existing[0])
            .values(**{k: v for k, v in vals.items() if k != "patient_uid" and k != "feature_date"})
        )
    else:
        db.execute(EHR_FEATURES_DAILY.insert().values(**vals))


# ═══════════════════════════════════════════════════════════════════════
#  INDEX ALERTS FROM INCONSISTENCIES
# ═══════════════════════════════════════════════════════════════════════

def _index_alerts_from_consulta(db: Session, consulta: Any, patient_uid: str) -> int:
    """Genera alertas en lifecycle a partir de inconsistencias legacy."""
    inc = _safe_str(getattr(consulta, "inconsistencias", ""))
    if not inc:
        return 0

    count = 0
    for texto in inc.split(";"):
        texto = texto.strip()
        if not texto:
            continue

        # Evitar duplicados
        existing = db.execute(
            select(EHR_ALERT_LIFECYCLE.c.id).where(
                EHR_ALERT_LIFECYCLE.c.patient_uid == patient_uid,
                EHR_ALERT_LIFECYCLE.c.alert_text == texto,
                EHR_ALERT_LIFECYCLE.c.status == "generated",
            )
        ).first()
        if existing:
            continue

        db.execute(
            EHR_ALERT_LIFECYCLE.insert().values(
                patient_uid=patient_uid,
                alert_type="inconsistencia",
                alert_text=texto,
                severity="warning",
                status="generated",
            )
        )
        count += 1

    return count


# ═══════════════════════════════════════════════════════════════════════
#  MAIN REINDEX FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def reindex_patient(db: Session, patient_uid: str, m: Any = None) -> Dict[str, Any]:
    """Reindexa completamente un paciente. Idempotente (skip si ya existe).

    Args:
        db: sesión SQLAlchemy (clinical)
        patient_uid: NSS del paciente
        m: proxy de main (app_context.main_proxy)

    Returns:
        dict con estadísticas de indexación
    """
    if m is None:
        from app.core.app_context import main_proxy as m

    ensure_ehr_schema(db)

    stats = {"docs": 0, "events": 0, "tags": 0, "alerts": 0, "problems": 0}

    # 1. Indexar consultas
    try:
        consultas = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == patient_uid).all()
        for c in consultas:
            doc_id = _index_consulta(db, c, patient_uid)
            if doc_id:
                stats["docs"] += 1
            stats["alerts"] += _index_alerts_from_consulta(db, c, patient_uid)
    except Exception as e:
        logger.warning({"event": "ehr_index_consultas_error", "patient_uid": patient_uid, "error": str(e)})

    # 2. Indexar hospitalizaciones
    try:
        from sqlalchemy import or_
        hosps = db.query(m.HospitalizacionDB).filter(
            or_(m.HospitalizacionDB.nss == patient_uid)
        ).all()
        for h in hosps:
            doc_id = _index_hospitalizacion(db, h, patient_uid)
            if doc_id:
                stats["docs"] += 1
    except Exception as e:
        logger.warning({"event": "ehr_index_hosp_error", "patient_uid": patient_uid, "error": str(e)})

    # 3. Indexar atenciones de consulta externa
    try:
        from app.services.consulta_externa_flow import CONSULTA_EXTERNA_ATENCIONES, ensure_consulta_externa_schema
        ensure_consulta_externa_schema(db)
        rows = db.execute(
            select(CONSULTA_EXTERNA_ATENCIONES)
            .where(CONSULTA_EXTERNA_ATENCIONES.c.nss == patient_uid)
        ).mappings().all()
        for r in rows:
            doc_id = _index_atencion(db, dict(r), patient_uid)
            if doc_id:
                stats["docs"] += 1
    except Exception as e:
        logger.warning({"event": "ehr_index_atenciones_error", "patient_uid": patient_uid, "error": str(e)})

    # 4. Indexar cirugías (de la DB quirúrgica si accesible)
    try:
        sdb = m.SurgicalSessionLocal()
        try:
            qx_list = sdb.query(m.SurgicalProgramacionDB).filter(
                m.SurgicalProgramacionDB.nss == patient_uid
            ).all()
            for qx in qx_list:
                # Escribimos en la sesión clínica, no quirúrgica
                doc_id = _index_cirugia(db, qx, patient_uid)
                if doc_id:
                    stats["docs"] += 1
        finally:
            sdb.close()
    except Exception as e:
        logger.warning({"event": "ehr_index_qx_error", "patient_uid": patient_uid, "error": str(e)})

    # 5. Rebuild problem list
    stats["problems"] = _rebuild_problem_list(db, patient_uid)

    # 6. Update daily features
    try:
        _update_features_daily(db, patient_uid)
    except Exception as e:
        logger.warning({"event": "ehr_features_error", "patient_uid": patient_uid, "error": str(e)})

    db.commit()
    logger.info({"event": "ehr_reindex_complete", "patient_uid": patient_uid, "stats": stats})
    return stats


def reindex_last_days(db: Session, days: int = 7, m: Any = None) -> Dict[str, Any]:
    """Reindexa pacientes que tuvieron actividad en los últimos N días."""
    if m is None:
        from app.core.app_context import main_proxy as m

    ensure_ehr_schema(db)

    cutoff = date.today() - timedelta(days=days)
    total_stats = {"patients": 0, "docs": 0, "alerts": 0}

    # Obtener NSS únicos con consultas recientes
    try:
        recent_nss = db.query(m.ConsultaDB.nss).filter(
            m.ConsultaDB.fecha_registro >= cutoff
        ).distinct().all()
        nss_set = {r[0] for r in recent_nss if r[0]}
    except Exception:
        nss_set = set()

    for nss in nss_set:
        try:
            stats = reindex_patient(db, nss, m=m)
            total_stats["patients"] += 1
            total_stats["docs"] += stats.get("docs", 0)
            total_stats["alerts"] += stats.get("alerts", 0)
        except Exception as e:
            logger.warning({"event": "ehr_reindex_patient_error", "nss": nss, "error": str(e)})

    logger.info({"event": "ehr_reindex_batch_complete", "days": days, "stats": total_stats})
    return total_stats
