from __future__ import annotations
from app.core.time_utils import utcnow

import csv
import io
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Request
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.orm import Session


PLUS_METADATA = MetaData()

EXPEDIENTE_ENRIQUECIDO = Table(
    "expediente_enriquecido",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=False, index=True, unique=True),
    Column("nss", String(20), index=True),
    Column("nombre", String(220), index=True),
    Column("sexo", String(20), index=True),
    Column("farmacoterapia_json", Text),
    Column("adherencia_farmacologica", String(80)),
    Column("reacciones_adversas", Text),
    Column("consentimiento_investigacion", String(20), index=True),
    Column("consentimiento_uso_datos", String(20), index=True),
    Column("consentimiento_fecha", Date, index=True),
    Column("consentimiento_responsable", String(120)),
    Column("gineco_obstetricos_json", Text),
    Column("antecedentes_laborales_json", Text),
    Column("calidad_vida_json", Text),
    Column("qol_ipss_score", Integer, index=True),
    Column("qol_iief5_score", Integer, index=True),
    Column("qol_iciqsf_score", Integer, index=True),
    Column("alertas_json", Text),
    Column("completitud_pct", Float, index=True),
    Column("provenance_json", Text),
    Column("creado_en", DateTime, default=utcnow, nullable=False),
    Column("actualizado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_ACCESO_AUDIT = Table(
    "expediente_acceso_audit",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("ruta", String(220), nullable=False, index=True),
    Column("metodo", String(10), nullable=False),
    Column("usuario", String(120), nullable=True, index=True),
    Column("ip", String(120), nullable=True),
    Column("user_agent", Text),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_REGLAS_VERSION = Table(
    "expediente_reglas_version",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("nombre", String(80), nullable=False),
    Column("activa", Boolean, default=False, nullable=False, index=True),
    Column("reglas_json", Text, nullable=False),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_COHORTES = Table(
    "expediente_cohortes",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("nombre", String(140), nullable=False, index=True),
    Column("descripcion", Text),
    Column("criterios_json", Text, nullable=False),
    Column("creado_por", String(120)),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_GENOMICA = Table(
    "expediente_genomica",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, nullable=False, index=True),
    Column("nss", String(20), index=True),
    Column("nombre", String(220), index=True),
    Column("panel", String(120), index=True),
    Column("mutaciones_json", Text),
    Column("expresion_genica_json", Text),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_OFFLINE_SYNC = Table(
    "expediente_offline_sync",
    PLUS_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("device_id", String(120), index=True),
    Column("usuario", String(120), index=True),
    Column("payload_json", Text, nullable=False),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_nss(value: Any) -> str:
    return re.sub(r"\D", "", _safe_text(value))[:10]


def _norm_name(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_text(value).upper()).strip()


def _dump_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _load_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, type(default)):
                return parsed
        except Exception:
            return default
    return default


def _to_int(raw: Any) -> Optional[int]:
    txt = _safe_text(raw)
    if not txt:
        return None
    try:
        return int(float(txt))
    except Exception:
        return None


def _to_date(raw: Any) -> Optional[date]:
    txt = _safe_text(raw)
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            continue
    return None


def default_rules() -> Dict[str, Any]:
    return {
        "labs": {
            "creatinina_alta": 2.0,
            "hb_baja": 8.0,
            "leucocitos_altos": 10000,
            "plaquetas_bajas": 150,
            "na_min": 135,
            "na_max": 145,
            "k_min": 3.5,
            "k_max": 5.0,
        },
        "captura": {
            "requeridos_identidad": ["nss", "nombre", "sexo", "diagnostico_principal"],
            "requeridos_fase1": ["consentimiento", "farmacoterapia", "calidad_vida"],
        },
    }


def ensure_expediente_plus_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    PLUS_METADATA.create_all(bind=bind, checkfirst=True)


def ensure_default_rule(db: Session) -> None:
    ensure_expediente_plus_schema(db)
    row = db.execute(
        select(EXPEDIENTE_REGLAS_VERSION.c.id).where(EXPEDIENTE_REGLAS_VERSION.c.activa.is_(True)).limit(1)
    ).first()
    if row:
        return
    db.execute(
        insert(EXPEDIENTE_REGLAS_VERSION).values(
            nombre="BASE_V1",
            activa=True,
            reglas_json=_dump_json(default_rules()),
            creado_en=utcnow(),
        )
    )
    db.commit()


def get_active_rules(db: Session) -> Dict[str, Any]:
    ensure_default_rule(db)
    row = db.execute(
        select(EXPEDIENTE_REGLAS_VERSION.c.reglas_json)
        .where(EXPEDIENTE_REGLAS_VERSION.c.activa.is_(True))
        .order_by(EXPEDIENTE_REGLAS_VERSION.c.id.desc())
        .limit(1)
    ).first()
    if not row:
        return default_rules()
    return _load_json(row[0], default_rules())


def _compute_qol_labels(ipss: Optional[int], iief5: Optional[int], iciq: Optional[int]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if ipss is not None:
        if ipss <= 7:
            out["ipss"] = "LEVE"
        elif ipss <= 19:
            out["ipss"] = "MODERADO"
        else:
            out["ipss"] = "SEVERO"
    if iief5 is not None:
        if iief5 >= 22:
            out["iief5"] = "SIN_DISFUNCION"
        elif iief5 >= 17:
            out["iief5"] = "LEVE"
        elif iief5 >= 12:
            out["iief5"] = "LEVE_MODERADA"
        elif iief5 >= 8:
            out["iief5"] = "MODERADA"
        else:
            out["iief5"] = "SEVERA"
    if iciq is not None:
        if iciq <= 5:
            out["iciq_sf"] = "LEVE"
        elif iciq <= 12:
            out["iciq_sf"] = "MODERADA"
        else:
            out["iciq_sf"] = "SEVERA"
    return out


def _split_meds(raw: str) -> List[Dict[str, str]]:
    meds: List[Dict[str, str]] = []
    for line in (raw or "").splitlines():
        txt = line.strip()
        if not txt:
            continue
        parts = [p.strip() for p in txt.split("|")]
        meds.append(
            {
                "medicamento": parts[0] if len(parts) > 0 else "",
                "dosis": parts[1] if len(parts) > 1 else "",
                "frecuencia": parts[2] if len(parts) > 2 else "",
                "via": parts[3] if len(parts) > 3 else "",
            }
        )
    return meds


def _compute_completitud(consulta: Any, enriched: Dict[str, Any]) -> float:
    checks: List[bool] = []
    checks.append(bool(_norm_nss(getattr(consulta, "nss", None))))
    checks.append(bool(_norm_name(getattr(consulta, "nombre", None))))
    checks.append(bool(_safe_text(getattr(consulta, "sexo", None))))
    checks.append(bool(_safe_text(getattr(consulta, "diagnostico_principal", None))))

    consent_ok = _safe_text(enriched.get("consentimiento_investigacion")) in {"SI", "NO"}
    checks.append(consent_ok)
    meds = enriched.get("farmacoterapia", [])
    checks.append(len(meds) > 0)
    checks.append(any(enriched.get("calidad_vida", {}).get(k) is not None for k in ["ipss", "iief5", "iciq_sf"]))

    sexo = _safe_text(getattr(consulta, "sexo", None)).upper()
    if sexo.startswith("F"):
        gyn = enriched.get("gineco_obstetricos", {})
        checks.append(any(_safe_text(gyn.get(k)) for k in ["gestas", "partos", "abortos", "menopausia"]))

    total = len(checks)
    if total == 0:
        return 0.0
    return round((sum(1 for c in checks if c) / total) * 100.0, 2)


def upsert_enriched_record(
    db: Session,
    *,
    consulta: Any,
    raw_form: Dict[str, Any],
    source: str = "manual",
) -> Dict[str, Any]:
    ensure_default_rule(db)

    sexo = _safe_text(getattr(consulta, "sexo", None)).upper()

    meds = _split_meds(_safe_text(raw_form.get("farmaco_medicamentos")))
    if not meds and _safe_text(raw_form.get("farmaco_principal")):
        meds = [
            {
                "medicamento": _safe_text(raw_form.get("farmaco_principal")),
                "dosis": _safe_text(raw_form.get("farmaco_dosis")),
                "frecuencia": _safe_text(raw_form.get("farmaco_frecuencia")),
                "via": _safe_text(raw_form.get("farmaco_via")),
            }
        ]

    qal_ipss = _to_int(raw_form.get("qol_ipss"))
    qal_iief5 = _to_int(raw_form.get("qol_iief5"))
    qal_iciq = _to_int(raw_form.get("qol_iciqsf"))

    calidad_vida = {
        "ipss": qal_ipss,
        "iief5": qal_iief5,
        "iciq_sf": qal_iciq,
        "labels": _compute_qol_labels(qal_ipss, qal_iief5, qal_iciq),
    }

    gyn = {
        "gestas": _safe_text(raw_form.get("gyn_gestas")),
        "partos": _safe_text(raw_form.get("gyn_partos")),
        "abortos": _safe_text(raw_form.get("gyn_abortos")),
        "cesareas": _safe_text(raw_form.get("gyn_cesareas")),
        "fum": _safe_text(raw_form.get("gyn_fum")),
        "menopausia": _safe_text(raw_form.get("gyn_menopausia")),
        "terapia_hormonal": _safe_text(raw_form.get("gyn_terapia_hormonal")),
    }

    laborales = {
        "exposicion_toxicos": _safe_text(raw_form.get("laboral_toxicos")),
        "turnos_nocturnos": _safe_text(raw_form.get("laboral_turnos_nocturnos")),
        "descripcion": _safe_text(raw_form.get("laboral_descripcion")),
    }

    consent = {
        "consentimiento_investigacion": _safe_text(raw_form.get("consentimiento_investigacion")).upper(),
        "consentimiento_uso_datos": _safe_text(raw_form.get("consentimiento_uso_datos")).upper(),
        "consentimiento_fecha": _to_date(raw_form.get("consentimiento_fecha")),
        "consentimiento_responsable": _safe_text(raw_form.get("consentimiento_responsable")),
    }

    if consent["consentimiento_investigacion"] not in {"SI", "NO"}:
        consent["consentimiento_investigacion"] = "PENDIENTE"
    if consent["consentimiento_uso_datos"] not in {"SI", "NO"}:
        consent["consentimiento_uso_datos"] = "PENDIENTE"

    alerts: List[str] = []
    if _safe_text(getattr(consulta, "tabaquismo_status", "")).lower() == "positivo":
        alerts.append("Paciente fumador activo: sugerir consejería intensiva")
    if calidad_vida["ipss"] is not None and calidad_vida["ipss"] >= 20:
        alerts.append("IPSS severo: priorizar reevaluación funcional")
    if sexo.startswith("F") and not any(_safe_text(gyn.get(k)) for k in ["gestas", "partos", "abortos"]):
        alerts.append("Faltan antecedentes gineco-obstétricos")
    if consent["consentimiento_investigacion"] == "PENDIENTE":
        alerts.append("Consentimiento de investigación pendiente")

    provenance = {
        "farmacoterapia": source,
        "consentimiento": source,
        "gineco_obstetricos": source,
        "calidad_vida": source,
        "antecedentes_laborales": source,
        "updated_at": utcnow().isoformat(),
    }

    structured = {
        "farmacoterapia": meds,
        "adherencia_farmacologica": _safe_text(raw_form.get("farmaco_adherencia")) or "NO_REGISTRADA",
        "reacciones_adversas": _safe_text(raw_form.get("farmaco_reacciones_adversas")),
        "consentimiento_investigacion": consent["consentimiento_investigacion"],
        "consentimiento_uso_datos": consent["consentimiento_uso_datos"],
        "consentimiento_fecha": consent["consentimiento_fecha"],
        "consentimiento_responsable": consent["consentimiento_responsable"],
        "gineco_obstetricos": gyn,
        "antecedentes_laborales": laborales,
        "calidad_vida": calidad_vida,
        "qol_ipss_score": qal_ipss,
        "qol_iief5_score": qal_iief5,
        "qol_iciqsf_score": qal_iciq,
        "alertas": alerts,
        "provenance": provenance,
    }

    completitud = _compute_completitud(consulta, structured)

    existing = db.execute(
        select(EXPEDIENTE_ENRIQUECIDO.c.id).where(EXPEDIENTE_ENRIQUECIDO.c.consulta_id == int(consulta.id)).limit(1)
    ).first()

    values = {
        "consulta_id": int(consulta.id),
        "nss": _norm_nss(getattr(consulta, "nss", None)),
        "nombre": _norm_name(getattr(consulta, "nombre", None)),
        "sexo": sexo,
        "farmacoterapia_json": _dump_json(meds),
        "adherencia_farmacologica": structured["adherencia_farmacologica"],
        "reacciones_adversas": structured["reacciones_adversas"],
        "consentimiento_investigacion": structured["consentimiento_investigacion"],
        "consentimiento_uso_datos": structured["consentimiento_uso_datos"],
        "consentimiento_fecha": structured["consentimiento_fecha"],
        "consentimiento_responsable": structured["consentimiento_responsable"],
        "gineco_obstetricos_json": _dump_json(gyn),
        "antecedentes_laborales_json": _dump_json(laborales),
        "calidad_vida_json": _dump_json(calidad_vida),
        "qol_ipss_score": qal_ipss,
        "qol_iief5_score": qal_iief5,
        "qol_iciqsf_score": qal_iciq,
        "alertas_json": _dump_json(alerts),
        "completitud_pct": completitud,
        "provenance_json": _dump_json(provenance),
        "actualizado_en": utcnow(),
    }

    if existing:
        db.execute(
            update(EXPEDIENTE_ENRIQUECIDO)
            .where(EXPEDIENTE_ENRIQUECIDO.c.consulta_id == int(consulta.id))
            .values(**values)
        )
    else:
        values["creado_en"] = utcnow()
        db.execute(insert(EXPEDIENTE_ENRIQUECIDO).values(**values))

    db.commit()
    return values


def get_enriched_by_consulta_id(db: Session, consulta_id: int) -> Dict[str, Any]:
    ensure_default_rule(db)
    row = db.execute(
        select(EXPEDIENTE_ENRIQUECIDO).where(EXPEDIENTE_ENRIQUECIDO.c.consulta_id == int(consulta_id)).limit(1)
    ).mappings().first()
    if not row:
        return {}
    return {
        "consulta_id": row["consulta_id"],
        "nss": row["nss"],
        "nombre": row["nombre"],
        "sexo": row["sexo"],
        "farmacoterapia": _load_json(row["farmacoterapia_json"], []),
        "adherencia_farmacologica": row["adherencia_farmacologica"],
        "reacciones_adversas": row["reacciones_adversas"],
        "consentimiento_investigacion": row["consentimiento_investigacion"],
        "consentimiento_uso_datos": row["consentimiento_uso_datos"],
        "consentimiento_fecha": row["consentimiento_fecha"].isoformat() if row["consentimiento_fecha"] else None,
        "consentimiento_responsable": row["consentimiento_responsable"],
        "gineco_obstetricos": _load_json(row["gineco_obstetricos_json"], {}),
        "antecedentes_laborales": _load_json(row["antecedentes_laborales_json"], {}),
        "calidad_vida": _load_json(row["calidad_vida_json"], {}),
        "qol_ipss_score": row["qol_ipss_score"],
        "qol_iief5_score": row["qol_iief5_score"],
        "qol_iciqsf_score": row["qol_iciqsf_score"],
        "alertas": _load_json(row["alertas_json"], []),
        "completitud_pct": row["completitud_pct"] or 0.0,
        "provenance": _load_json(row["provenance_json"], {}),
        "actualizado_en": row["actualizado_en"].isoformat() if row["actualizado_en"] else None,
    }


def log_expediente_access(db: Session, request: Request, *, consulta_id: Optional[int] = None) -> None:
    ensure_expediente_plus_schema(db)
    try:
        db.execute(
            insert(EXPEDIENTE_ACCESO_AUDIT).values(
                consulta_id=consulta_id,
                ruta=_safe_text(request.url.path),
                metodo=_safe_text(request.method) or "GET",
                usuario=_safe_text(request.headers.get("X-User") or request.headers.get("X-Forwarded-User")) or "anon",
                ip=_safe_text(request.client.host if request.client else ""),
                user_agent=_safe_text(request.headers.get("user-agent")),
                creado_en=utcnow(),
            )
        )
        db.commit()
    except Exception:
        db.rollback()


def build_completitud_index(db: Session) -> Dict[str, Any]:
    ensure_default_rule(db)
    rows = db.execute(select(EXPEDIENTE_ENRIQUECIDO)).mappings().all()
    total = len(rows)
    if total == 0:
        return {
            "total_pacientes": 0,
            "completitud_promedio": 0.0,
            "por_sexo": {},
            "por_rango": {"<50": 0, "50-79": 0, ">=80": 0},
        }

    by_sex: Dict[str, List[float]] = {}
    bins = {"<50": 0, "50-79": 0, ">=80": 0}
    total_pct = 0.0
    for r in rows:
        pct = float(r.get("completitud_pct") or 0.0)
        total_pct += pct
        sex = _safe_text(r.get("sexo") or "NO_REGISTRADO")
        by_sex.setdefault(sex, []).append(pct)
        if pct < 50:
            bins["<50"] += 1
        elif pct < 80:
            bins["50-79"] += 1
        else:
            bins[">=80"] += 1

    return {
        "total_pacientes": total,
        "completitud_promedio": round(total_pct / total, 2),
        "por_sexo": {k: round(sum(v) / len(v), 2) for k, v in by_sex.items()},
        "por_rango": bins,
    }


def save_cohort(db: Session, *, nombre: str, criterios: Dict[str, Any], descripcion: str = "", creado_por: str = "system") -> int:
    ensure_expediente_plus_schema(db)
    res = db.execute(
        insert(EXPEDIENTE_COHORTES).values(
            nombre=_safe_text(nombre)[:140] or "Cohorte",
            descripcion=_safe_text(descripcion),
            criterios_json=_dump_json(criterios),
            creado_por=_safe_text(creado_por) or "system",
            creado_en=utcnow(),
        )
    )
    db.commit()
    return int(res.inserted_primary_key[0])


def list_cohorts(db: Session) -> List[Dict[str, Any]]:
    ensure_expediente_plus_schema(db)
    rows = db.execute(select(EXPEDIENTE_COHORTES).order_by(EXPEDIENTE_COHORTES.c.id.desc())).mappings().all()
    return [
        {
            "id": r["id"],
            "nombre": r["nombre"],
            "descripcion": r["descripcion"] or "",
            "criterios": _load_json(r["criterios_json"], {}),
            "creado_por": r["creado_por"] or "system",
            "creado_en": r["creado_en"].isoformat() if r["creado_en"] else None,
        }
        for r in rows
    ]


def _apply_cohort_filters(query, m: Any, criterios: Dict[str, Any]):
    edad_min = _to_int(criterios.get("edad_min"))
    edad_max = _to_int(criterios.get("edad_max"))
    sexo = _safe_text(criterios.get("sexo")).upper()
    dx = _safe_text(criterios.get("diagnostico")).upper()
    estatus = _safe_text(criterios.get("estatus_protocolo")).lower()

    if edad_min is not None:
        query = query.filter(m.ConsultaDB.edad >= edad_min)
    if edad_max is not None:
        query = query.filter(m.ConsultaDB.edad <= edad_max)
    if sexo:
        query = query.filter(func.upper(m.ConsultaDB.sexo) == sexo)
    if dx:
        query = query.filter(func.upper(m.ConsultaDB.diagnostico_principal).contains(dx))
    if estatus:
        query = query.filter(func.lower(m.ConsultaDB.estatus_protocolo) == estatus)
    return query


def run_cohort(db: Session, m: Any, criterios: Dict[str, Any], limit: int = 500) -> Dict[str, Any]:
    query = db.query(m.ConsultaDB).order_by(m.ConsultaDB.id.desc())
    query = _apply_cohort_filters(query, m, criterios)
    rows = query.limit(max(1, min(limit, 2000))).all()

    return {
        "total": len(rows),
        "criterios": criterios,
        "pacientes": [
            {
                "consulta_id": r.id,
                "nss": _norm_nss(r.nss),
                "nombre": _norm_name(r.nombre),
                "edad": r.edad,
                "sexo": r.sexo,
                "diagnostico": r.diagnostico_principal,
                "estatus_protocolo": r.estatus_protocolo,
            }
            for r in rows
        ],
    }


def build_fhir_medication_requests(db: Session, m: Any, *, subject: Optional[str] = None) -> Dict[str, Any]:
    ensure_default_rule(db)
    q = db.query(m.ConsultaDB)
    if subject and subject.startswith("Patient/"):
        ref = subject.replace("Patient/", "")
        if ref.isdigit():
            q = q.filter(m.ConsultaDB.id == int(ref))
        else:
            q = q.filter(m.ConsultaDB.nss == _norm_nss(ref))

    consultas = q.order_by(m.ConsultaDB.id.desc()).limit(200).all()
    entries: List[Dict[str, Any]] = []
    for c in consultas:
        enriched = get_enriched_by_consulta_id(db, int(c.id))
        meds = enriched.get("farmacoterapia", [])
        for idx, med in enumerate(meds, start=1):
            entries.append(
                {
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": f"medreq-{c.id}-{idx}",
                        "status": "active",
                        "intent": "order",
                        "subject": {"reference": f"Patient/{c.id}"},
                        "authoredOn": (c.fecha_registro.isoformat() if c.fecha_registro else None),
                        "medicationCodeableConcept": {"text": med.get("medicamento") or "Medicamento"},
                        "dosageInstruction": [
                            {
                                "text": " | ".join([
                                    _safe_text(med.get("dosis")),
                                    _safe_text(med.get("frecuencia")),
                                    _safe_text(med.get("via")),
                                ]).strip(" |")
                            }
                        ],
                    }
                }
            )

    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(entries),
        "entry": entries,
    }


def build_fhir_careplans(db: Session, m: Any, *, subject: Optional[str] = None) -> Dict[str, Any]:
    q = db.query(m.ConsultaDB)
    if subject and subject.startswith("Patient/"):
        ref = subject.replace("Patient/", "")
        if ref.isdigit():
            q = q.filter(m.ConsultaDB.id == int(ref))
        else:
            q = q.filter(m.ConsultaDB.nss == _norm_nss(ref))
    consultas = q.order_by(m.ConsultaDB.id.desc()).limit(200).all()
    entries: List[Dict[str, Any]] = []
    for c in consultas:
        entries.append(
            {
                "resource": {
                    "resourceType": "CarePlan",
                    "id": f"careplan-{c.id}",
                    "status": "active",
                    "intent": "plan",
                    "subject": {"reference": f"Patient/{c.id}"},
                    "title": f"Plan terapéutico urología - {c.diagnostico_principal or 'NO_REGISTRADO'}",
                    "description": c.plan_especifico or "Plan clínico no especificado",
                    "created": c.fecha_registro.isoformat() if c.fecha_registro else None,
                }
            }
        )

    return {"resourceType": "Bundle", "type": "searchset", "total": len(entries), "entry": entries}


def build_fhir_goals(db: Session, m: Any, *, subject: Optional[str] = None) -> Dict[str, Any]:
    q = db.query(m.ConsultaDB)
    if subject and subject.startswith("Patient/"):
        ref = subject.replace("Patient/", "")
        if ref.isdigit():
            q = q.filter(m.ConsultaDB.id == int(ref))
        else:
            q = q.filter(m.ConsultaDB.nss == _norm_nss(ref))
    consultas = q.order_by(m.ConsultaDB.id.desc()).limit(200).all()

    entries: List[Dict[str, Any]] = []
    for c in consultas:
        enriched = get_enriched_by_consulta_id(db, int(c.id))
        calidad = enriched.get("calidad_vida", {})
        targets: List[Dict[str, Any]] = []
        if calidad.get("ipss") is not None:
            targets.append({"measure": {"text": "IPSS"}, "detailString": f"Actual: {calidad.get('ipss')}"})
        if calidad.get("iief5") is not None:
            targets.append({"measure": {"text": "IIEF-5"}, "detailString": f"Actual: {calidad.get('iief5')}"})
        if calidad.get("iciq_sf") is not None:
            targets.append({"measure": {"text": "ICIQ-SF"}, "detailString": f"Actual: {calidad.get('iciq_sf')}"})

        entries.append(
            {
                "resource": {
                    "resourceType": "Goal",
                    "id": f"goal-{c.id}",
                    "lifecycleStatus": "active",
                    "description": {"text": f"Optimizar resultado clínico en {c.diagnostico_principal or 'urología'}"},
                    "subject": {"reference": f"Patient/{c.id}"},
                    "target": targets,
                }
            }
        )

    return {"resourceType": "Bundle", "type": "searchset", "total": len(entries), "entry": entries}


def export_enriched_dataset(db: Session, m: Any, *, mode: str) -> Tuple[str, str, str]:
    """Retorna (filename, media_type, content)."""
    consultas = db.query(m.ConsultaDB).order_by(m.ConsultaDB.id.desc()).limit(5000).all()
    rows: List[Dict[str, Any]] = []
    for c in consultas:
        enriched = get_enriched_by_consulta_id(db, int(c.id))
        rows.append(
            {
                "record_id": c.id,
                "nss": _norm_nss(c.nss),
                "nombre": _norm_name(c.nombre),
                "sexo": _safe_text(c.sexo),
                "edad": c.edad,
                "diagnostico_principal": _safe_text(c.diagnostico_principal),
                "estatus_protocolo": _safe_text(c.estatus_protocolo),
                "consentimiento_investigacion": _safe_text(enriched.get("consentimiento_investigacion")),
                "consentimiento_uso_datos": _safe_text(enriched.get("consentimiento_uso_datos")),
                "qol_ipss_score": enriched.get("qol_ipss_score"),
                "qol_iief5_score": enriched.get("qol_iief5_score"),
                "qol_iciqsf_score": enriched.get("qol_iciqsf_score"),
                "completitud_pct": enriched.get("completitud_pct", 0.0),
            }
        )

    if mode == "redcap":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()) if rows else ["record_id"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        return "export_redcap.csv", "text/csv", output.getvalue()

    if mode == "openclinica":
        payload = {
            "study": "RNP_UROLOGIA",
            "generated_at": utcnow().isoformat() + "Z",
            "records": rows,
        }
        return "export_openclinica.json", "application/json", json.dumps(payload, ensure_ascii=False)

    # SAS-compatible CSV (flat)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()) if rows else ["record_id"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return "export_sas.csv", "text/csv", output.getvalue()


def ingest_offline_payload(db: Session, payload: Dict[str, Any], *, device_id: str, usuario: str = "system") -> Dict[str, Any]:
    ensure_expediente_plus_schema(db)
    db.execute(
        insert(EXPEDIENTE_OFFLINE_SYNC).values(
            device_id=_safe_text(device_id) or "unknown-device",
            usuario=_safe_text(usuario) or "system",
            payload_json=_dump_json(payload),
            creado_en=utcnow(),
        )
    )
    db.commit()
    return {"status": "ok", "saved": True}


def summarize_access_audit(db: Session, *, days: int = 30) -> Dict[str, Any]:
    ensure_expediente_plus_schema(db)
    since = utcnow() - timedelta(days=max(1, min(days, 365)))
    rows = db.execute(
        select(EXPEDIENTE_ACCESO_AUDIT.c.ruta, func.count(EXPEDIENTE_ACCESO_AUDIT.c.id).label("total"))
        .where(EXPEDIENTE_ACCESO_AUDIT.c.creado_en >= since)
        .group_by(EXPEDIENTE_ACCESO_AUDIT.c.ruta)
        .order_by(func.count(EXPEDIENTE_ACCESO_AUDIT.c.id).desc())
    ).all()
    return {
        "desde": since.isoformat(),
        "top_rutas": [{"ruta": r[0], "accesos": int(r[1])} for r in rows],
    }
