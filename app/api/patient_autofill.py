"""
API de Auto-Poblado de Paciente — Eliminación de Re-Captura.

ADITIVO: No modifica ninguna lógica existente.
Proporciona un endpoint unificado `/api/patient/autofill` que devuelve
todos los datos conocidos de un paciente a partir de NSS, CURP o consulta_id.

Esto permite que cualquier formulario (quirófano, hospitalización, urgencias)
auto-rellene los campos demográficos y clínicos sin que el médico
tenga que escribirlos otra vez.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

router = APIRouter(tags=["patient_autofill"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_text(value: Any) -> str:
    return str(value or "").strip()


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


def _norm_name(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_text(value).upper())


def _nss10(value: Any, m: Any = None) -> str:
    raw = _safe_text(value)
    if m is not None:
        try:
            return _safe_text(m.normalize_nss(raw))
        except Exception:
            pass
    return re.sub(r"\D", "", raw)[:10]


def _iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    txt = _safe_text(value)
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt[:10], fmt).date().isoformat()
        except Exception:
            continue
    return None


def _load_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _get_db():
    from app.core.app_context import main_proxy as m
    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m
    sdb = m._new_surgical_session(enable_dual_write=True)
    try:
        yield sdb
    finally:
        sdb.close()


# ---------------------------------------------------------------------------
# Endpoint principal
# ---------------------------------------------------------------------------

@router.get("/api/patient/autofill", response_class=JSONResponse)
async def patient_autofill(
    nss: Optional[str] = Query(None, description="NSS del paciente (10 dígitos)"),
    curp: Optional[str] = Query(None, description="CURP del paciente"),
    consulta_id: Optional[int] = Query(None, description="ID de consulta"),
    db: Session = Depends(_get_db),
):
    """Devuelve todos los datos conocidos de un paciente para auto-poblar formularios.

    Busca en: ConsultaDB, HospitalizacionDB, SurgicalProgramacionDB, PATIENT_MASTER_IDENTITY,
    VitalDB, LabDB. Devuelve un payload unificado para que los formularios auto-rellenen
    campos demográficos, clínicos y de somatometría.

    Prioridad de búsqueda: consulta_id > nss > curp > nombre.
    """
    from app.core.app_context import main_proxy as m

    target_nss = _nss10(nss, m) if nss else ""
    target_curp = _safe_text(curp).upper() if curp else ""
    target_consulta_id = _safe_int(consulta_id)

    # --- 1. Buscar consulta más reciente ---
    consulta = None
    if target_consulta_id:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(target_consulta_id)).first()
    if consulta is None and target_nss and len(target_nss) == 10:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nss == target_nss)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    if consulta is None and target_curp and len(target_curp) >= 16:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.curp == target_curp)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )

    if consulta is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "Paciente no encontrado", "searched": {"nss": target_nss, "curp": target_curp, "consulta_id": target_consulta_id}},
        )

    # Resolver NSS y CURP desde consulta si no los teníamos
    if not target_nss:
        target_nss = _nss10(getattr(consulta, "nss", ""), m)
    if not target_curp:
        target_curp = _safe_text(getattr(consulta, "curp", "")).upper()

    # --- 2. Datos demográficos (de ConsultaDB) ---
    demograficos = {
        "consulta_id": int(getattr(consulta, "id", 0)),
        "nss": target_nss,
        "curp": target_curp,
        "agregado_medico": _safe_text(getattr(consulta, "agregado_medico", "")).upper(),
        "nombre": _norm_name(getattr(consulta, "nombre", "")),
        "nombre_completo": _norm_name(getattr(consulta, "nombre", "")),
        "paciente_nombre": _norm_name(getattr(consulta, "nombre", "")),
        "fecha_nacimiento": _iso_date(getattr(consulta, "fecha_nacimiento", None)),
        "edad": _safe_int(getattr(consulta, "edad", None)),
        "sexo": _safe_text(getattr(consulta, "sexo", "")).upper() or "MASCULINO",
        "tipo_sangre": _safe_text(getattr(consulta, "tipo_sangre", "")),
        "ocupacion": _safe_text(getattr(consulta, "ocupacion", "")),
        "escolaridad": _safe_text(getattr(consulta, "escolaridad", "")),
    }

    # --- 3. Dirección ---
    direccion = {
        "cp": _safe_text(getattr(consulta, "cp", "")),
        "alcaldia": _safe_text(getattr(consulta, "alcaldia", "")),
        "colonia": _safe_text(getattr(consulta, "colonia", "")),
        "estado_foraneo": _safe_text(getattr(consulta, "estado_foraneo", "")),
        "calle": _safe_text(getattr(consulta, "calle", "")),
        "no_ext": _safe_text(getattr(consulta, "no_ext", "")),
        "no_int": _safe_text(getattr(consulta, "no_int", "")),
        "telefono": _safe_text(getattr(consulta, "telefono", "")),
        "email": _safe_text(getattr(consulta, "email", "")),
    }

    # --- 4. Somatometría más reciente ---
    somatometria_consulta = {
        "peso": _safe_float(getattr(consulta, "peso", None)),
        "talla": _safe_float(getattr(consulta, "talla", None)),
        "imc": _safe_float(getattr(consulta, "imc", None)),
        "ta": _safe_text(getattr(consulta, "ta", "")),
        "fc": _safe_int(getattr(consulta, "fc", None)),
        "temp": _safe_float(getattr(consulta, "temp", None)),
    }

    # Buscar vitales más recientes de VitalDB
    vital_latest = None
    try:
        consulta_ids = [int(consulta.id)]
        if target_nss:
            extra_ids = [
                int(r.id) for r in
                db.query(m.ConsultaDB).filter(m.ConsultaDB.nss == target_nss).order_by(m.ConsultaDB.id.desc()).limit(50).all()
            ]
            consulta_ids = list(set(consulta_ids + extra_ids))

        vital_latest = (
            db.query(m.VitalDB)
            .filter(m.VitalDB.consulta_id.in_(consulta_ids))
            .order_by(m.VitalDB.timestamp.desc())
            .first()
        )
    except Exception:
        pass

    somatometria_vitals = {}
    if vital_latest:
        somatometria_vitals = {
            "peso_kg": _safe_float(getattr(vital_latest, "peso", None)),
            "talla_m": _safe_float(getattr(vital_latest, "talla", None)),
            "imc": _safe_float(getattr(vital_latest, "imc", None)),
            "ta_sis": _safe_int(getattr(vital_latest, "sbp", None)),
            "ta_dia": _safe_int(getattr(vital_latest, "dbp", None)),
            "fc": _safe_int(getattr(vital_latest, "hr", None)),
            "temp_c": _safe_float(getattr(vital_latest, "temp", None)),
            "source": _safe_text(getattr(vital_latest, "source", "")),
            "timestamp": getattr(vital_latest, "timestamp", None).isoformat() if getattr(vital_latest, "timestamp", None) else None,
        }

    # --- 5. Antecedentes (de ConsultaDB) ---
    antecedentes = {
        "ahf_text": _build_ahf_text(consulta),
        "apnp_text": _build_apnp_text(consulta),
        "app_text": _build_app_text(consulta),
        "alergias_text": _build_alergias_text(consulta),
        "aqx_text": _build_aqx_text(consulta),
        "meds_cronicos_text": "",  # No hay campo directo, se puede inferir de app_tratamiento
        "toxicomanias_text": _build_toxicomanias_text(consulta),
        "transfusiones_text": _build_transfusiones_text(consulta),
    }

    # --- 6. Clínico ---
    clinico = {
        "diagnostico_principal": _safe_text(getattr(consulta, "diagnostico_principal", "")).upper(),
        "padecimiento_actual": _safe_text(getattr(consulta, "padecimiento_actual", "")),
        "exploracion_fisica": _safe_text(getattr(consulta, "exploracion_fisica", "")),
        "estudios_hallazgos": _safe_text(getattr(consulta, "estudios_hallazgos", "")),
        "estatus_protocolo": _safe_text(getattr(consulta, "estatus_protocolo", "")),
        "plan_especifico": _safe_text(getattr(consulta, "plan_especifico", "")),
    }

    # --- 7. Datos de protocolo urológico ---
    protocolo = _build_protocolo(consulta)

    # --- 8. Hospitalización activa (si existe) ---
    hosp_activa = None
    try:
        hosp_row = (
            db.query(m.HospitalizacionDB)
            .filter(m.HospitalizacionDB.consulta_id.in_(consulta_ids))
            .filter(m.HospitalizacionDB.estatus == "ACTIVO")
            .order_by(m.HospitalizacionDB.id.desc())
            .first()
        )
        if hosp_row:
            hosp_activa = {
                "hospitalizacion_id": int(getattr(hosp_row, "id", 0)),
                "cama": _safe_text(getattr(hosp_row, "cama", "")),
                "servicio": _safe_text(getattr(hosp_row, "servicio", "")),
                "fecha_ingreso": _iso_date(getattr(hosp_row, "fecha_ingreso", None)),
                "medico_a_cargo": _safe_text(getattr(hosp_row, "medico_a_cargo", "")),
                "diagnostico": _safe_text(getattr(hosp_row, "diagnostico", "")),
                "hgz_envio": _safe_text(getattr(hosp_row, "hgz_envio", "")),
            }
    except Exception:
        pass

    # --- 9. Quirófano programado (si existe) ---
    qx_programado = None
    try:
        sdb_factory = getattr(m, "_new_surgical_session", None)
        if callable(sdb_factory):
            sdb = sdb_factory(enable_dual_write=True)
            try:
                prog = (
                    sdb.query(m.SurgicalProgramacionDB)
                    .filter(m.SurgicalProgramacionDB.consulta_id.in_(consulta_ids))
                    .order_by(m.SurgicalProgramacionDB.id.desc())
                    .first()
                )
                if prog:
                    qx_programado = {
                        "surgical_programacion_id": int(getattr(prog, "id", 0)),
                        "procedimiento_programado": _safe_text(getattr(prog, "procedimiento_programado", "")),
                        "patologia": _safe_text(getattr(prog, "patologia", "")),
                        "cirujano": _safe_text(getattr(prog, "cirujano", "")),
                        "fecha_programada": _iso_date(getattr(prog, "fecha_programada", None)),
                        "estatus": _safe_text(getattr(prog, "estatus", "")),
                    }
            finally:
                sdb.close()
    except Exception:
        pass

    # --- 10. Labs más recientes ---
    labs_recientes = []
    try:
        labs_rows = (
            db.query(m.LabDB)
            .filter(m.LabDB.consulta_id.in_(consulta_ids))
            .order_by(m.LabDB.timestamp.desc())
            .limit(20)
            .all()
        )
        for lab in labs_rows:
            labs_recientes.append({
                "test_name": _safe_text(getattr(lab, "test_name", "")),
                "test_code": _safe_text(getattr(lab, "test_code", "")),
                "value": _safe_text(getattr(lab, "value", "")),
                "unit": _safe_text(getattr(lab, "unit", "")),
                "timestamp": getattr(lab, "timestamp", None).isoformat() if getattr(lab, "timestamp", None) else None,
            })
    except Exception:
        pass

    # --- 11. Construir respuesta unificada ---
    return JSONResponse(content={
        "ok": True,
        "source": "autofill_api_v1",
        "demograficos": demograficos,
        "direccion": direccion,
        "somatometria": {
            "from_consulta": somatometria_consulta,
            "from_vitals": somatometria_vitals,
        },
        "antecedentes": antecedentes,
        "clinico": clinico,
        "protocolo_urologico": protocolo,
        "hospitalizacion_activa": hosp_activa,
        "quirofano_programado": qx_programado,
        "labs_recientes": labs_recientes,
        "consulta_ids": consulta_ids if 'consulta_ids' in dir() else [int(consulta.id)],
        "generated_at": datetime.utcnow().isoformat() + "Z",
    })


# ---------------------------------------------------------------------------
# Builders de texto para antecedentes (extraídos de ConsultaDB)
# ---------------------------------------------------------------------------

def _build_ahf_text(c: Any) -> str:
    parts = []
    status = _safe_text(getattr(c, "ahf_status", ""))
    if status:
        parts.append(f"Estatus: {status}")
    linea = _safe_text(getattr(c, "ahf_linea", ""))
    if linea:
        parts.append(f"Línea: {linea}")
    padecimiento = _safe_text(getattr(c, "ahf_padecimiento", ""))
    if padecimiento:
        parts.append(f"Padecimiento: {padecimiento}")
    estatus_ahf = _safe_text(getattr(c, "ahf_estatus", ""))
    if estatus_ahf:
        parts.append(f"Estado: {estatus_ahf}")
    return ". ".join(parts) if parts else ""


def _build_apnp_text(c: Any) -> str:
    parts = []
    ocupacion = _safe_text(getattr(c, "ocupacion", ""))
    if ocupacion:
        parts.append(f"Ocupación: {ocupacion}")
    escolaridad = _safe_text(getattr(c, "escolaridad", ""))
    if escolaridad:
        parts.append(f"Escolaridad: {escolaridad}")
    return ". ".join(parts) if parts else ""


def _build_app_text(c: Any) -> str:
    parts = []
    patologia = _safe_text(getattr(c, "app_patologia", ""))
    if patologia:
        parts.append(f"Patología: {patologia}")
    evolucion = _safe_text(getattr(c, "app_evolucion", ""))
    if evolucion:
        parts.append(f"Evolución: {evolucion}")
    tratamiento = _safe_text(getattr(c, "app_tratamiento", ""))
    if tratamiento:
        parts.append(f"Tratamiento: {tratamiento}")
    comp = _safe_text(getattr(c, "app_complicaciones", ""))
    if comp and comp.upper() != "NO":
        desc_comp = _safe_text(getattr(c, "app_desc_complicacion", ""))
        parts.append(f"Complicaciones: {comp}" + (f" ({desc_comp})" if desc_comp else ""))
    return ". ".join(parts) if parts else ""


def _build_alergias_text(c: Any) -> str:
    parts = []
    alergeno = _safe_text(getattr(c, "alergeno", ""))
    if alergeno:
        parts.append(f"Alérgeno: {alergeno}")
    reaccion = _safe_text(getattr(c, "alergia_reaccion", ""))
    if reaccion:
        parts.append(f"Reacción: {reaccion}")
    fecha = _iso_date(getattr(c, "alergia_fecha", None))
    if fecha:
        parts.append(f"Fecha: {fecha}")
    return ". ".join(parts) if parts else "Sin alergias conocidas"


def _build_aqx_text(c: Any) -> str:
    parts = []
    fecha = _iso_date(getattr(c, "aqx_fecha", None))
    if fecha:
        parts.append(f"Fecha: {fecha}")
    proc = _safe_text(getattr(c, "aqx_procedimiento", ""))
    if proc:
        parts.append(f"Procedimiento: {proc}")
    hallazgos = _safe_text(getattr(c, "aqx_hallazgos", ""))
    if hallazgos:
        parts.append(f"Hallazgos: {hallazgos}")
    medico = _safe_text(getattr(c, "aqx_medico", ""))
    if medico:
        parts.append(f"Médico: {medico}")
    comp = _safe_text(getattr(c, "aqx_complicaciones_status", ""))
    if comp and comp.upper() != "NO":
        desc = _safe_text(getattr(c, "aqx_desc_complicacion", ""))
        parts.append(f"Complicaciones: {comp}" + (f" ({desc})" if desc else ""))
    return ". ".join(parts) if parts else ""


def _build_toxicomanias_text(c: Any) -> str:
    parts = []
    tab = _safe_text(getattr(c, "tabaquismo_status", ""))
    if tab:
        parts.append(f"Tabaquismo: {tab}")
        cigarros = _safe_int(getattr(c, "cigarros_dia", None))
        anios = _safe_int(getattr(c, "anios_fumando", None))
        it = _safe_text(getattr(c, "indice_tabaquico", ""))
        if cigarros:
            parts.append(f"Cigarros/día: {cigarros}")
        if anios:
            parts.append(f"Años: {anios}")
        if it:
            parts.append(f"Índice tabáquico: {it}")
    alc = _safe_text(getattr(c, "alcoholismo", ""))
    if alc:
        parts.append(f"Alcoholismo: {alc}")
    drogas = _safe_text(getattr(c, "otras_drogas", ""))
    if drogas:
        parts.append(f"Otras: {drogas}")
    return ". ".join(parts) if parts else ""


def _build_transfusiones_text(c: Any) -> str:
    parts = []
    status = _safe_text(getattr(c, "transfusiones_status", ""))
    if status:
        parts.append(f"Transfusiones: {status}")
    fecha = _iso_date(getattr(c, "trans_fecha", None))
    if fecha:
        parts.append(f"Fecha: {fecha}")
    reacciones = _safe_text(getattr(c, "trans_reacciones", ""))
    if reacciones:
        parts.append(f"Reacciones: {reacciones}")
    return ". ".join(parts) if parts else ""


def _build_protocolo(c: Any) -> Dict[str, Any]:
    """Extrae datos del protocolo urológico específico de ConsultaDB."""
    proto: Dict[str, Any] = {}

    # Próstata
    for field in ["pros_ape_pre", "pros_ape_act", "pros_ecog", "pros_rmn",
                  "pros_tr", "pros_briganti", "pros_gleason", "pros_tnm",
                  "pros_riesgo", "pros_prostatectomia", "pros_rhp",
                  "pros_radioterapia", "pros_continencia", "pros_ereccion"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = _safe_text(val)

    # Riñón
    for field in ["rinon_tiempo", "rinon_tnm", "rinon_etapa", "rinon_ecog",
                  "rinon_charlson", "rinon_nefrectomia", "rinon_rhp", "rinon_sistemico"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = _safe_text(val)

    # Vejiga
    for field in ["vejiga_tnm", "vejiga_ecog", "vejiga_hematuria_tipo",
                  "vejiga_procedimiento_qx", "vejiga_rhp", "vejiga_sistemico"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = _safe_text(val)

    # Litiasis
    for field in ["lit_tamano", "lit_localizacion", "lit_densidad_uh",
                  "lit_estatus_postop", "lit_guys_score", "lit_croes_score"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = _safe_text(val)

    # HPB
    for field in ["hpb_tamano_prostata", "hpb_ape", "hpb_ipss",
                  "hpb_tamsulosina", "hpb_finasteride"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = _safe_text(val)

    # Testículo
    for field in ["testiculo_tnm", "testiculo_orquiectomia_fecha",
                  "testiculo_marcadores_pre", "testiculo_marcadores_post", "testiculo_rhp"]:
        val = getattr(c, field, None)
        if val is not None and _safe_text(val):
            proto[field] = str(val) if isinstance(val, date) else _safe_text(val)

    # Protocolo JSON
    proto_json = _load_json(getattr(c, "protocolo_detalles", None), {})
    if proto_json:
        proto["protocolo_detalles"] = proto_json

    return proto
