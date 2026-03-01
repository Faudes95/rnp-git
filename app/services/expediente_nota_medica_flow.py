from __future__ import annotations
from app.core.time_utils import utcnow

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, and_, desc, func, insert, or_, select
from sqlalchemy.orm import Session

from app.services.hospital_guardia_flow import HOSP_GUARDIA_REGISTROS, ensure_hospital_guardia_schema


EXPEDIENTE_NOTA_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")

EXPEDIENTE_NOTAS_DIARIAS = Table(
    "expediente_notas_diarias",
    EXPEDIENTE_NOTA_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("consulta_id", Integer, index=True, nullable=False),
    Column("hospitalizacion_id", Integer, index=True),
    Column("fecha_nota", Date, index=True, nullable=False),
    Column("nss", String(20), index=True),
    Column("nombre", String(220), index=True),
    Column("cama", String(30), index=True),
    Column("servicio_nota", String(120), index=True),
    Column("cie10_codigo", String(20), index=True),
    Column("diagnostico_cie10", String(320), index=True),
    Column("hr", Float),
    Column("sbp", Float),
    Column("dbp", Float),
    Column("temp", Float),
    Column("peso", Float),
    Column("talla", Float),
    Column("imc", Float),
    Column("labs_json", JSON_SQL),
    Column("nota_texto", Text),
    Column("creado_por", String(120), index=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

EXPEDIENTE_CIE10_CATALOGO = Table(
    "expediente_cie10_catalogo",
    EXPEDIENTE_NOTA_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("codigo", String(20), index=True, nullable=False),
    Column("descripcion", String(280), index=True, nullable=False),
    Column("area", String(120), index=True),
    Column("activo", Integer, default=1, index=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)


NOTA_MEDICA_SERVICIOS = [
    "UROLOGIA",
    "MEDICINA INTERNA",
    "ANESTESIOLOGIA",
    "NEFROLOGIA",
    "CARDIOLOGIA",
    "ANGIOLOGIA",
    "TERAPIA INTENSIVA",
    "INFECTOLOGIA",
    "HEMATOLOGIA",
    "OTRO",
]


CIE10_BASE = [
    ("A00-B99", "Ciertas enfermedades infecciosas y parasitarias"),
    ("C00-D48", "Neoplasias"),
    ("D50-D89", "Enfermedades de la sangre y órganos hematopoyéticos"),
    ("E00-E90", "Enfermedades endocrinas, nutricionales y metabólicas"),
    ("F00-F99", "Trastornos mentales y del comportamiento"),
    ("G00-G99", "Enfermedades del sistema nervioso"),
    ("H00-H59", "Enfermedades del ojo y anexos"),
    ("H60-H95", "Enfermedades del oído y apófisis mastoides"),
    ("I00-I99", "Enfermedades del sistema circulatorio"),
    ("J00-J99", "Enfermedades del sistema respiratorio"),
    ("K00-K93", "Enfermedades del sistema digestivo"),
    ("L00-L99", "Enfermedades de piel y tejido subcutáneo"),
    ("M00-M99", "Enfermedades del sistema osteomuscular y tejido conectivo"),
    ("N00-N99", "Enfermedades del aparato genitourinario"),
    ("O00-O99", "Embarazo, parto y puerperio"),
    ("P00-P96", "Ciertas afecciones originadas en el periodo perinatal"),
    ("Q00-Q99", "Malformaciones congénitas"),
    ("R00-R99", "Síntomas, signos y hallazgos anormales"),
    ("S00-T98", "Traumatismos, envenenamientos y otras consecuencias externas"),
    ("V01-Y98", "Causas externas de morbilidad y mortalidad"),
    ("Z00-Z99", "Factores que influyen en el estado de salud y contacto con servicios"),
]


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_nss(value: Any) -> str:
    return re.sub(r"\D", "", _safe_text(value))[:10]


def _norm_name(value: Any) -> str:
    txt = _safe_text(value).upper()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _extract_float(raw: Any) -> Optional[float]:
    txt = _safe_text(raw).replace(",", "")
    if not txt:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", txt)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _safe_int(raw: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if raw is None:
            return default
        txt = _safe_text(raw)
        if not txt:
            return default
        return int(float(txt))
    except Exception:
        return default


def _extract_lab_markers_from_payload(payload: Dict[str, Any]) -> Dict[str, float]:
    markers: Dict[str, float] = {}
    map_keys = {
        "creatinina": ["CREATININA", "CR", "CREATININA MG/DL"],
        "hemoglobina": ["HEMOGLOBINA", "HB", "HGB"],
        "leucocitos": ["LEUCOCITOS", "LEUCOS", "WBC"],
        "plaquetas": ["PLAQUETAS", "PLT"],
        "sodio": ["SODIO", "NA"],
        "potasio": ["POTASIO", "K"],
    }
    for out_key, keys in map_keys.items():
        value_num: Optional[float] = None
        for key in keys:
            raw = _safe_text(payload.get(key))
            if not raw:
                continue
            value_num = _extract_float(raw)
            if value_num is not None:
                break
        if value_num is not None:
            markers[out_key] = value_num
    return markers


def ensure_expediente_nota_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    EXPEDIENTE_NOTA_METADATA.create_all(bind=bind, checkfirst=True)


def seed_cie10_catalog(db: Session, m: Any) -> None:
    ensure_expediente_nota_schema(db)
    existing = db.execute(select(EXPEDIENTE_CIE10_CATALOGO.c.id).limit(1)).first()
    if existing:
        return

    rows: List[Dict[str, Any]] = []
    for code, desc in CIE10_BASE:
        rows.append(
            {
                "codigo": code,
                "descripcion": desc,
                "area": "GENERAL",
                "activo": 1,
                "creado_en": utcnow(),
            }
        )

    # Carga específica urología desde mapeos existentes.
    for pat, code in (getattr(m, "QUIROFANO_PATOLOGIA_CIE10_MAP", {}) or {}).items():
        rows.append(
            {
                "codigo": _safe_text(code),
                "descripcion": _safe_text(pat),
                "area": "UROLOGIA",
                "activo": 1,
                "creado_en": utcnow(),
            }
        )

    seen = set()
    clean = []
    for r in rows:
        key = (_safe_text(r["codigo"]).upper(), _safe_text(r["descripcion"]).upper())
        if key in seen:
            continue
        seen.add(key)
        clean.append(r)

    if clean:
        db.execute(insert(EXPEDIENTE_CIE10_CATALOGO), clean)
        db.commit()


def get_cie10_catalog(db: Session, m: Any, *, query: str = "", limit: int = 1500) -> List[Dict[str, str]]:
    seed_cie10_catalog(db, m)
    q = select(EXPEDIENTE_CIE10_CATALOGO).where(EXPEDIENTE_CIE10_CATALOGO.c.activo == 1)
    qtxt = _safe_text(query)
    if qtxt:
        q = q.where(
            or_(
                EXPEDIENTE_CIE10_CATALOGO.c.codigo.contains(qtxt.upper()),
                EXPEDIENTE_CIE10_CATALOGO.c.descripcion.contains(qtxt.upper()),
            )
        )
    rows = db.execute(q.order_by(EXPEDIENTE_CIE10_CATALOGO.c.codigo.asc()).limit(max(20, min(limit, 5000)))).mappings().all()
    return [
        {
            "codigo": _safe_text(r["codigo"]).upper(),
            "descripcion": _safe_text(r["descripcion"]).upper(),
            "area": _safe_text(r["area"]).upper(),
            "label": f"{_safe_text(r['codigo']).upper()} - {_safe_text(r['descripcion']).upper()}",
        }
        for r in rows
    ]


def resolve_profile_identity(
    db: Session,
    m: Any,
    *,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
) -> Tuple[Optional[Any], List[int], str, str]:
    consulta = None
    if consulta_id:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(consulta_id)).first()
    if consulta is None and nss:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == _norm_nss(nss)).order_by(m.ConsultaDB.id.desc()).first()
    if consulta is None and nombre:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.nombre.contains(_norm_name(nombre))).order_by(m.ConsultaDB.id.desc()).first()

    target_nss = _norm_nss(nss or (consulta.nss if consulta else ""))
    target_name = _norm_name(nombre or (consulta.nombre if consulta else ""))

    consultas_q = db.query(m.ConsultaDB)
    if target_nss:
        consultas_q = consultas_q.filter(m.ConsultaDB.nss == target_nss)
    elif target_name:
        consultas_q = consultas_q.filter(m.ConsultaDB.nombre.contains(target_name))
    consulta_ids = [int(r.id) for r in consultas_q.order_by(m.ConsultaDB.id.desc()).limit(500).all() if r.id is not None]

    if consulta and int(consulta.id) not in consulta_ids:
        consulta_ids.insert(0, int(consulta.id))

    return consulta, consulta_ids, target_nss, target_name


def get_active_hospitalizacion_for_profile(
    db: Session,
    m: Any,
    *,
    consulta_ids: List[int],
    target_nss: str,
    target_name: str,
) -> Optional[Any]:
    q = db.query(m.HospitalizacionDB)
    cond = []
    if consulta_ids:
        cond.append(m.HospitalizacionDB.consulta_id.in_(consulta_ids))
    if target_nss:
        cond.append(m.HospitalizacionDB.nss == target_nss)
    if target_name:
        cond.append(m.HospitalizacionDB.nombre_completo.contains(target_name))
    if cond:
        q = q.filter(or_(*cond))
    rows = q.order_by(m.HospitalizacionDB.fecha_ingreso.desc(), m.HospitalizacionDB.id.desc()).all()
    if not rows:
        return None
    for r in rows:
        if _safe_text(r.estatus).upper() == "ACTIVO":
            return r
    return rows[0]


def get_labs_for_profile_date(
    db: Session,
    m: Any,
    *,
    consulta_ids: List[int],
    target_nss: str,
    target_name: str,
    target_date: date,
) -> Dict[str, float]:
    labs: Dict[str, float] = {}

    # 1) Capa lab estructurada.
    q = db.query(m.LabDB).filter(func.date(m.LabDB.timestamp) == target_date)
    cond = []
    if consulta_ids:
        cond.append(m.LabDB.consulta_id.in_(consulta_ids))
    if target_nss:
        cond.append(m.LabDB.patient_id == target_nss)
    if cond:
        q = q.filter(or_(*cond))
    rows = q.order_by(m.LabDB.timestamp.desc(), m.LabDB.id.desc()).all()

    for r in rows:
        joined = f"{_safe_text(r.test_name)} {_safe_text(r.test_code)}".upper()
        value = _extract_float(r.value)
        if value is None:
            continue
        if ("CREATIN" in joined or joined.strip() == "CR") and "creatinina" not in labs:
            labs["creatinina"] = value
        elif ("HEMOGLOB" in joined or "HGB" in joined or joined.strip().startswith("HB")) and "hemoglobina" not in labs:
            labs["hemoglobina"] = value
        elif ("LEUCO" in joined or "WBC" in joined) and "leucocitos" not in labs:
            labs["leucocitos"] = value
        elif ("PLAQUET" in joined or "PLT" in joined) and "plaquetas" not in labs:
            labs["plaquetas"] = value
        elif ("SODIO" in joined or joined.strip() == "NA") and "sodio" not in labs:
            labs["sodio"] = value
        elif ("POTASIO" in joined or joined.strip() == "K") and "potasio" not in labs:
            labs["potasio"] = value

    # 2) Capa resumen de guardia/laboratorios.
    ensure_hospital_guardia_schema(db)
    gq = select(HOSP_GUARDIA_REGISTROS).where(
        and_(
            HOSP_GUARDIA_REGISTROS.c.fecha == target_date,
            HOSP_GUARDIA_REGISTROS.c.dataset == "laboratorios",
        )
    )
    g_cond = []
    if consulta_ids:
        g_cond.append(HOSP_GUARDIA_REGISTROS.c.consulta_id.in_(consulta_ids))
    if target_nss:
        g_cond.append(HOSP_GUARDIA_REGISTROS.c.nss == target_nss)
    if target_name:
        g_cond.append(HOSP_GUARDIA_REGISTROS.c.nombre.contains(target_name))
    if g_cond:
        gq = gq.where(or_(*g_cond))

    grows = db.execute(gq.order_by(HOSP_GUARDIA_REGISTROS.c.id.desc())).mappings().all()
    for row in grows:
        payload = row.get("payload_json")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            continue
        parsed = _extract_lab_markers_from_payload(payload)
        for key, val in parsed.items():
            if key not in labs:
                labs[key] = val

    return labs


def save_nota_medica_diaria(db: Session, m: Any, *, raw_form: Dict[str, Any], request_user: str = "system") -> Dict[str, Any]:
    ensure_expediente_nota_schema(db)

    consulta_id = int(raw_form.get("consulta_id") or 0)
    nss = _norm_nss(raw_form.get("nss"))
    nombre = _norm_name(raw_form.get("nombre"))
    fecha_nota = raw_form.get("fecha_nota")
    try:
        fecha_obj = date.fromisoformat(_safe_text(fecha_nota)) if fecha_nota else date.today()
    except Exception:
        fecha_obj = date.today()

    consulta, consulta_ids, target_nss, target_name = resolve_profile_identity(
        db,
        m,
        consulta_id=consulta_id,
        nss=nss,
        nombre=nombre,
    )
    if consulta is None:
        raise ValueError("Consulta no encontrada")

    hosp = get_active_hospitalizacion_for_profile(
        db,
        m,
        consulta_ids=consulta_ids,
        target_nss=target_nss,
        target_name=target_name,
    )

    cama = _safe_text(raw_form.get("cama")) or _safe_text(getattr(hosp, "cama", ""))
    servicio_nota = _safe_text(raw_form.get("servicio_nota")).upper() or "UROLOGIA"

    labs_dia = get_labs_for_profile_date(
        db,
        m,
        consulta_ids=consulta_ids,
        target_nss=target_nss,
        target_name=target_name,
        target_date=fecha_obj,
    )

    hr = _extract_float(raw_form.get("hr"))
    sbp = _extract_float(raw_form.get("sbp"))
    dbp = _extract_float(raw_form.get("dbp"))
    temp = _extract_float(raw_form.get("temp"))
    peso = _extract_float(raw_form.get("peso"))
    talla = _extract_float(raw_form.get("talla"))
    imc = _extract_float(raw_form.get("imc"))

    result = db.execute(
        insert(EXPEDIENTE_NOTAS_DIARIAS).values(
            consulta_id=int(consulta.id),
            hospitalizacion_id=getattr(hosp, "id", None),
            fecha_nota=fecha_obj,
            nss=target_nss or _norm_nss(consulta.nss),
            nombre=target_name or _norm_name(consulta.nombre),
            cama=cama,
            servicio_nota=servicio_nota,
            cie10_codigo=_safe_text(raw_form.get("cie10_codigo")).upper(),
            diagnostico_cie10=_safe_text(raw_form.get("diagnostico_cie10")).upper(),
            hr=hr,
            sbp=sbp,
            dbp=dbp,
            temp=temp,
            peso=peso,
            talla=talla,
            imc=imc,
            labs_json=json.dumps(labs_dia, ensure_ascii=False),
            nota_texto=_safe_text(raw_form.get("nota_texto")),
            creado_por=_safe_text(request_user),
            creado_en=utcnow(),
        )
    )
    nota_id = int(result.inserted_primary_key[0])
    inpatient_episode_id: Optional[int] = None
    inpatient_note_id: Optional[int] = None

    # Capa aditiva: objeto estructurado de episodio y nota intrahospitalaria.
    # Mantiene almacenamiento legacy existente para compatibilidad total.
    try:
        from app.services.hospitalization_notes_flow import (
            create_or_get_active_episode,
            upsert_daily_note,
        )

        hospitalizacion_id = _safe_int(raw_form.get("hospitalizacion_id"), getattr(hosp, "id", None))
        if hospitalizacion_id is not None:
            episode = create_or_get_active_episode(
                db,
                m,
                patient_id=target_nss or _norm_nss(consulta.nss),
                consulta_id=int(consulta.id),
                hospitalizacion_id=int(hospitalizacion_id),
                service=_safe_text(getattr(hosp, "servicio", "")) or servicio_nota,
                location=cama,
                shift=_safe_text(raw_form.get("turno_nota")).upper(),
                author_user_id=_safe_text(request_user),
                started_on=getattr(hosp, "fecha_ingreso", None) or fecha_obj,
                source_route="/expediente/nota-medica",
                metrics={
                    "estado_clinico": _safe_text(getattr(hosp, "estado_clinico", "")),
                    "estatus_hospitalizacion": _safe_text(getattr(hosp, "estatus", "")),
                },
            )
            inpatient_episode_id = int(episode["id"])
            inpatient_note = upsert_daily_note(
                db,
                episode_id=inpatient_episode_id,
                note_date=fecha_obj,
                note_type="EVOLUCION",
                service=servicio_nota,
                location=cama,
                shift=_safe_text(raw_form.get("turno_nota")).upper(),
                author_user_id=_safe_text(request_user),
                cie10_codigo=_safe_text(raw_form.get("cie10_codigo")).upper(),
                diagnostico=_safe_text(raw_form.get("diagnostico_cie10")).upper(),
                vitals={
                    "hr": hr,
                    "sbp": sbp,
                    "dbp": dbp,
                    "temp": temp,
                    "peso": peso,
                    "talla": talla,
                    "imc": imc,
                },
                labs=labs_dia,
                devices={},
                events={},
                payload={
                    "nombre": target_name or _norm_name(consulta.nombre),
                    "cama": cama,
                    "servicio_nota": servicio_nota,
                    "hospitalizacion_id": int(hospitalizacion_id),
                },
                note_text=_safe_text(raw_form.get("nota_texto")),
                status="FINALIZADA",
                source_route="/expediente/nota-medica",
                mirror_legacy=False,
            )
            inpatient_note_id = int(inpatient_note["id"])
    except Exception:
        pass

    if any(v is not None for v in [hr, sbp, dbp, temp, peso, talla, imc]):
        db.add(
            m.VitalDB(
                consulta_id=int(consulta.id),
                patient_id=target_nss or str(consulta.id),
                timestamp=utcnow(),
                hr=hr,
                sbp=sbp,
                dbp=dbp,
                temp=temp,
                peso=peso,
                talla=talla,
                imc=imc,
                source="expediente_nota_medica",
            )
        )

    db.commit()

    return {
        "nota_id": nota_id,
        "consulta_id": int(consulta.id),
        "nss": target_nss or _norm_nss(consulta.nss),
        "nombre": target_name or _norm_name(consulta.nombre),
        "fecha_nota": fecha_obj.isoformat(),
        "servicio_nota": servicio_nota,
        "cama": cama,
        "labs": labs_dia,
        "inpatient_episode_id": inpatient_episode_id,
        "inpatient_note_id": inpatient_note_id,
    }


def list_notas_medicas_profile(
    db: Session,
    *,
    consulta_ids: List[int],
    target_nss: str,
    target_name: str,
    limit: int = 300,
) -> List[Dict[str, Any]]:
    ensure_expediente_nota_schema(db)

    q = select(EXPEDIENTE_NOTAS_DIARIAS)
    cond = []
    if consulta_ids:
        cond.append(EXPEDIENTE_NOTAS_DIARIAS.c.consulta_id.in_(consulta_ids))
    if target_nss:
        cond.append(EXPEDIENTE_NOTAS_DIARIAS.c.nss == target_nss)
    if target_name:
        cond.append(EXPEDIENTE_NOTAS_DIARIAS.c.nombre.contains(target_name))
    if cond:
        q = q.where(or_(*cond))

    rows = db.execute(q.order_by(desc(EXPEDIENTE_NOTAS_DIARIAS.c.fecha_nota), desc(EXPEDIENTE_NOTAS_DIARIAS.c.id)).limit(max(1, min(limit, 2000)))).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        labs_json = r.get("labs_json")
        if isinstance(labs_json, str):
            try:
                labs_json = json.loads(labs_json)
            except Exception:
                labs_json = {}
        if not isinstance(labs_json, dict):
            labs_json = {}
        out.append(
            {
                "id": int(r["id"]),
                "consulta_id": int(r["consulta_id"]),
                "hospitalizacion_id": r["hospitalizacion_id"],
                "fecha_nota": r["fecha_nota"].isoformat() if r["fecha_nota"] else "",
                "nss": _safe_text(r["nss"]),
                "nombre": _safe_text(r["nombre"]),
                "cama": _safe_text(r["cama"]),
                "servicio_nota": _safe_text(r["servicio_nota"]),
                "cie10_codigo": _safe_text(r["cie10_codigo"]),
                "diagnostico_cie10": _safe_text(r["diagnostico_cie10"]),
                "vitales": {
                    "hr": r["hr"],
                    "sbp": r["sbp"],
                    "dbp": r["dbp"],
                    "temp": r["temp"],
                    "peso": r["peso"],
                    "talla": r["talla"],
                    "imc": r["imc"],
                },
                "labs": labs_json,
                "nota_texto": _safe_text(r["nota_texto"]),
                "creado_por": _safe_text(r["creado_por"]),
                "creado_en": r["creado_en"].isoformat() if r["creado_en"] else "",
            }
        )
    return out


def list_profile_alertas(
    db: Session,
    sdb: Session,
    *,
    consulta_ids: List[int],
    target_nss: str,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    from app.services.fau_bot_flow import FAU_CENTRAL_ALERTS, _load as _fau_load
    from app.services.fau_hospitalizacion_agent import HOSPITALIZACION_ALERTAS, ensure_hospital_agent_schema

    ensure_hospital_agent_schema(sdb)
    alerts: List[Dict[str, Any]] = []

    # Alertas específicas del agente de hospitalización.
    q = select(HOSPITALIZACION_ALERTAS)
    cond = []
    if consulta_ids:
        cond.append(HOSPITALIZACION_ALERTAS.c.consulta_id.in_(consulta_ids))
    if target_nss:
        cond.append(HOSPITALIZACION_ALERTAS.c.nss == target_nss)
    if cond:
        q = q.where(or_(*cond))
    rows = sdb.execute(q.order_by(desc(HOSPITALIZACION_ALERTAS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    for r in rows:
        alerts.append(
            {
                "source": "AI_HOSPITALIZACION",
                "fecha": r["alert_ts"].isoformat() if r["alert_ts"] else "",
                "severity": _safe_text(r["severity"]).upper() or "MEDIA",
                "title": _safe_text(r["alert_type"]).upper() or "ALERTA",
                "description": _safe_text(r["message"]),
                "recommendation": _safe_text(r["recommendation"]),
                "resolved": bool(r["resolved"]),
            }
        )

    # Alertas centrales FAU_BOT con payload ligado a consulta/paciente.
    rows_central = db.execute(select(FAU_CENTRAL_ALERTS).order_by(desc(FAU_CENTRAL_ALERTS.c.id)).limit(1500)).mappings().all()
    wanted_ids = {int(x) for x in consulta_ids}
    for r in rows_central:
        payload = _fau_load(r.get("payload_json"), {})
        if not isinstance(payload, dict):
            payload = {}
        match = False
        pid = payload.get("consulta_id") or payload.get("patient_id")
        try:
            if pid is not None and int(pid) in wanted_ids:
                match = True
        except Exception:
            pass

        payload_txt = json.dumps(payload, ensure_ascii=False).upper()
        if not match and target_nss and target_nss in payload_txt:
            match = True
        if not match:
            continue

        alerts.append(
            {
                "source": "FAU_BOT",
                "fecha": r["created_at"].isoformat() if r["created_at"] else "",
                "severity": _safe_text(r["severity"]).upper() or "MEDIA",
                "title": _safe_text(r["title"]) or "ALERTA CENTRAL",
                "description": _safe_text(r["description"]),
                "recommendation": _safe_text(r["recommendation"]),
                "resolved": False,
            }
        )

    alerts.sort(key=lambda x: x.get("fecha") or "", reverse=True)
    return alerts[: max(1, min(limit, 500))]
