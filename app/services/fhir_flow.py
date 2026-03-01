from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.fhir_query_utils import (
    apply_date_filter_to_date_column,
    lookup_consulta_ids_by_subject,
    parse_fhir_date_filter,
)


async def fhir_expediente_flow(
    consulta_id: Optional[int],
    curp: Optional[str],
    db: Session,
) -> Any:
    from app.core.app_context import main_proxy as m

    cache_key = consulta_id or curp
    if cache_key:
        cached = m.get_cached_patient(cache_key)
        if cached:
            return JSONResponse(content=cached)
    if consulta_id:
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == consulta_id).first()
    elif curp:
        consulta = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.curp == m.normalize_curp(curp))
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    else:
        raise HTTPException(status_code=400, detail="Debe especificar consulta_id o curp")

    if not consulta:
        raise HTTPException(status_code=404, detail="Paciente no encontrado")

    payload = m.map_to_fhir(consulta)
    m.cache_patient(cache_key or consulta.id, payload)
    return JSONResponse(content=payload)


async def fhir_patient_by_curp_flow(curp: str, db: Session) -> Any:
    from app.core.app_context import main_proxy as m

    consulta = (
        db.query(m.ConsultaDB)
        .filter(m.ConsultaDB.curp == m.normalize_curp(curp))
        .order_by(m.ConsultaDB.id.desc())
        .first()
    )
    if not consulta:
        raise HTTPException(status_code=404, detail="Patient not found")
    return JSONResponse(content=m.map_to_fhir_patient_only(consulta))


async def fhir_condition_search_flow(subject: str, db: Session) -> Any:
    from app.core.app_context import main_proxy as m

    curp = subject.replace("Patient/", "").strip()
    consulta = (
        db.query(m.ConsultaDB)
        .filter(m.ConsultaDB.curp == m.normalize_curp(curp))
        .order_by(m.ConsultaDB.id.desc())
        .first()
    )
    if not consulta:
        return JSONResponse(
            content={"resourceType": "Bundle", "type": "searchset", "entry": []}
        )
    condition = m.map_to_fhir_condition(consulta)
    return JSONResponse(
        content={
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [{"resource": condition}],
        }
    )


async def fhir_export_cohort_flow(
    diagnostico: Optional[str],
    fecha_desde: Optional[date],
    fecha_hasta: Optional[date],
    db: Session,
) -> Any:
    from app.core.app_context import main_proxy as m

    query = db.query(m.ConsultaDB)
    if diagnostico:
        query = query.filter(m.ConsultaDB.diagnostico_principal == diagnostico)
    if fecha_desde:
        query = query.filter(m.ConsultaDB.fecha_registro >= fecha_desde)
    if fecha_hasta:
        query = query.filter(m.ConsultaDB.fecha_registro <= fecha_hasta)
    pacientes = query.order_by(m.ConsultaDB.id.desc()).limit(500).all()
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": []}
    for paciente in pacientes:
        bundle["entry"].append({"resource": m.map_to_fhir_patient_only(paciente)})
        bundle["entry"].append({"resource": m.map_to_fhir_condition(paciente)})
    return JSONResponse(content=bundle)

async def fhir_procedure_search_flow(
    subject: Optional[str],
    date_filter: Optional[str],
    code: Optional[str],
    status: Optional[str],
    db: Session,
    sdb: Session,
) -> Any:
    from app.core.app_context import main_proxy as m

    query = sdb.query(m.SurgicalProgramacionDB)

    if subject:
        if subject.startswith("Patient/"):
            ref = subject.replace("Patient/", "").strip()
            if m.re.match(r"^[A-Z]{4}\d{6}[HM]", ref):
                query = query.filter(m.SurgicalProgramacionDB.curp == m.normalize_curp(ref))
            elif ref.isdigit():
                query = query.filter(m.SurgicalProgramacionDB.consulta_id == int(ref))
            else:
                query = query.filter(m.SurgicalProgramacionDB.paciente_nombre.contains(ref))

    if date_filter:
        try:
            query = apply_date_filter_to_date_column(
                query,
                m.SurgicalProgramacionDB.fecha_programada,
                date_filter,
            )
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

    if code:
        query = query.filter(m.SurgicalProgramacionDB.cie9mc_codigo == code.strip())

    if status:
        status_map = {
            "in-progress": "PROGRAMADA",
            "completed": "REALIZADA",
            "cancelled": "CANCELADA",
        }
        mapped = status_map.get(status.strip().lower())
        if mapped:
            query = query.filter(m.SurgicalProgramacionDB.estatus == mapped)

    procedimientos = query.order_by(m.SurgicalProgramacionDB.id.desc()).limit(200).all()

    bundle = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(procedimientos),
        "entry": [],
    }

    for p in procedimientos:
        coding = []
        if p.cie9mc_codigo:
            coding.append(
                {
                    "system": "http://hl7.org/fhir/sid/icd-9-cm",
                    "code": p.cie9mc_codigo,
                    "display": p.procedimiento_programado or p.procedimiento,
                }
            )
        procedure_resource = {
            "resourceType": "Procedure",
            "id": f"proc-{p.id}",
            "status": {
                "PROGRAMADA": "in-progress",
                "REALIZADA": "completed",
                "CANCELADA": "cancelled",
            }.get((p.estatus or "").upper(), "unknown"),
            "code": {
                "coding": coding,
                "text": p.procedimiento_programado or p.procedimiento or "NO_REGISTRADO",
            },
            "subject": {"reference": f"Patient/{p.curp or p.consulta_id}"},
            "performedDateTime": p.fecha_programada.isoformat() if p.fecha_programada else None,
            "performedPeriod": {
                "start": p.fecha_programada.isoformat() if p.fecha_programada else None,
                "end": p.fecha_realizacion.isoformat() if p.fecha_realizacion else None,
            },
            "asserter": {"display": p.cirujano or "No registrado"},
        }
        if p.notas:
            procedure_resource["note"] = [{"text": p.notas}]
        bundle["entry"].append({"resource": procedure_resource})

    return JSONResponse(content=bundle)


async def fhir_observation_search_flow(
    subject: Optional[str],
    code: Optional[str],
    date_filter: Optional[str],
    db: Session,
) -> Any:
    from app.core.app_context import main_proxy as m

    query = db.query(m.VitalDB)

    if subject:
        ids = lookup_consulta_ids_by_subject(
            subject,
            db,
            consulta_model=m.ConsultaDB,
            normalize_curp_fn=m.normalize_curp,
        )
        if ids:
            query = query.filter(m.VitalDB.consulta_id.in_(ids))
        elif subject.startswith("Patient/"):
            ref = subject.replace("Patient/", "").strip()
            query = query.filter(m.VitalDB.patient_id == ref)

    if date_filter:
        try:
            op, parsed = parse_fhir_date_filter(date_filter)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        ts_date = func.date(m.VitalDB.timestamp)
        if op == "ge":
            query = query.filter(ts_date >= parsed)
        elif op == "le":
            query = query.filter(ts_date <= parsed)
        elif op == "gt":
            query = query.filter(ts_date > parsed)
        elif op == "lt":
            query = query.filter(ts_date < parsed)
        else:
            query = query.filter(ts_date == parsed)

    observations = query.order_by(m.VitalDB.timestamp.desc()).limit(200).all()

    loinc_map_local = {
        "hr": ("8867-4", "Heart rate", "/min"),
        "sbp": ("8480-6", "Systolic blood pressure", "mmHg"),
        "dbp": ("8462-4", "Diastolic blood pressure", "mmHg"),
        "temp": ("8310-5", "Body temperature", "°C"),
        "peso": ("29463-7", "Body weight", "kg"),
        "talla": ("8302-2", "Body height", "cm"),
        "imc": ("39156-5", "Body mass index", "kg/m2"),
    }
    code_to_attr = {v[0]: k for k, v in loinc_map_local.items()}
    requested_attrs = set(loinc_map_local.keys())
    if code:
        c = code.strip()
        if c in code_to_attr:
            requested_attrs = {code_to_attr[c]}
        elif c in loinc_map_local:
            requested_attrs = {c}
        else:
            requested_attrs = set()

    bundle = {"resourceType": "Bundle", "type": "searchset", "total": 0, "entry": []}

    for obs in observations:
        for attr, (loinc, display, unit) in loinc_map_local.items():
            if attr not in requested_attrs:
                continue
            valor = getattr(obs, attr, None)
            if valor is None:
                continue
            obs_resource = {
                "resourceType": "Observation",
                "id": f"obs-{obs.id}-{attr}",
                "status": "final",
                "code": {
                    "coding": [{"system": "http://loinc.org", "code": loinc, "display": display}],
                    "text": display,
                },
                "subject": {"reference": f"Patient/{obs.patient_id or obs.consulta_id}"},
                "effectiveDateTime": obs.timestamp.isoformat() if obs.timestamp else None,
                "valueQuantity": {"value": float(valor), "unit": unit},
            }
            bundle["entry"].append({"resource": obs_resource})

    bundle["total"] = len(bundle["entry"])
    return JSONResponse(content=bundle)


async def fhir_encounter_search_flow(
    subject: Optional[str],
    date_filter: Optional[str],
    db: Session,
) -> Any:
    from app.core.app_context import main_proxy as m

    query = db.query(m.HospitalizacionDB)

    if subject:
        ids = lookup_consulta_ids_by_subject(
            subject,
            db,
            consulta_model=m.ConsultaDB,
            normalize_curp_fn=m.normalize_curp,
        )
        if ids:
            query = query.filter(m.HospitalizacionDB.consulta_id.in_(ids))

    if date_filter:
        try:
            query = apply_date_filter_to_date_column(
                query,
                m.HospitalizacionDB.fecha_ingreso,
                date_filter,
            )
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

    hospitalizaciones = query.order_by(m.HospitalizacionDB.fecha_ingreso.desc()).limit(200).all()

    consulta_map: Dict[int, Any] = {}
    if hospitalizaciones:
        ids = list({h.consulta_id for h in hospitalizaciones if h.consulta_id})
        if ids:
            for c in db.query(m.ConsultaDB).filter(m.ConsultaDB.id.in_(ids)).all():
                consulta_map[c.id] = c

    bundle = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(hospitalizaciones),
        "entry": [],
    }

    for h in hospitalizaciones:
        paciente = consulta_map.get(h.consulta_id)
        subject_ref = (
            f"Patient/{paciente.curp}"
            if (paciente and paciente.curp)
            else f"Patient/{h.consulta_id}"
        )
        enc_resource = {
            "resourceType": "Encounter",
            "id": f"enc-{h.id}",
            "status": "finished" if (h.estatus or "").upper() == "EGRESADO" else "in-progress",
            "class": {"code": "IMP", "display": "inpatient encounter"},
            "subject": {"reference": subject_ref},
            "period": {
                "start": h.fecha_ingreso.isoformat() if h.fecha_ingreso else None,
                "end": h.fecha_egreso.isoformat() if h.fecha_egreso else None,
            },
            "reasonCode": [{"text": h.motivo or "No registrado"}],
            "hospitalization": {
                "admitSource": {"text": h.servicio or "No registrado"},
                "dischargeDisposition": {"text": h.estatus or "No registrado"},
            },
        }
        bundle["entry"].append({"resource": enc_resource})

    return JSONResponse(content=bundle)
