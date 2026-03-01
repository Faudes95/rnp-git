from __future__ import annotations

import base64
import json
from datetime import date
from typing import Any, Dict, Tuple

try:
    from catalogs import get_icd11_map, get_loinc_map, get_snomed_map
except Exception:
    def get_icd11_map():
        return {}

    def get_loinc_map():
        return {}

    def get_snomed_map():
        return {}


ICD11_DEFAULT_MAP = {
    "ca_prostata": ("2C82.0", "Cáncer de próstata"),
    "ca_vejiga": ("2C90.0", "Cáncer de vejiga"),
    "ca_rinon": ("2C80.0", "Cáncer de riñón"),
    "ca_testiculo": ("2C60.0", "Cáncer de testículo"),
    "ca_pene": ("2C70.0", "Cáncer de pene"),
    "ca_urotelial_alto": ("2C91.0", "Cáncer urotelial tracto superior"),
    "litiasis_rinon": ("GB60.0", "Cálculo renal"),
    "litiasis_ureter": ("GB61.0", "Cálculo ureteral"),
    "litiasis_vejiga": ("GB62.0", "Cálculo vesical"),
    "hpb": ("GA70.0", "Hiperplasia prostática benigna"),
}


def _gender_from_sexo(sexo: Any) -> str:
    raw = str(sexo or "").strip().lower()
    if raw.startswith("m"):
        return "male"
    if raw.startswith("f"):
        return "female"
    return "unknown"


def _iso(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return value
    return value


def _diagnostic_mapping(diag_key: str, diagnostico_principal: str) -> Tuple[Any, Any, Any]:
    icd_map = get_icd11_map()
    snomed_map = get_snomed_map()
    icd_code, icd_display = icd_map.get(
        diag_key,
        ICD11_DEFAULT_MAP.get(diag_key, (None, diagnostico_principal)),
    )
    return icd_code, icd_display, snomed_map.get(diag_key)


def build_fhir_bundle(consulta: Any) -> Dict[str, Any]:
    diag_key = consulta.diagnostico_principal or "SIN_DIAGNOSTICO"
    loinc_map = get_loinc_map()
    icd_code, icd_display, snomed_code = _diagnostic_mapping(diag_key, consulta.diagnostico_principal)

    patient = {
        "resourceType": "Patient",
        "id": consulta.curp or str(consulta.id),
        "identifier": [
            {"system": "http://imss.gob.mx/curp", "value": consulta.curp},
            {"system": "http://imss.gob.mx/nss", "value": consulta.nss},
        ],
        "name": [{"text": consulta.nombre}],
        "gender": _gender_from_sexo(consulta.sexo),
        "birthDate": _iso(consulta.fecha_nacimiento),
        "address": [{
            "line": [consulta.calle or "", consulta.no_ext or "", consulta.no_int or ""],
            "city": consulta.alcaldia,
            "postalCode": consulta.cp,
            "district": consulta.colonia,
        }],
        "telecom": [
            {"system": "phone", "value": consulta.telefono},
            {"system": "email", "value": consulta.email},
        ],
    }

    condition_coding = []
    if icd_code:
        condition_coding.append(
            {
                "system": "http://hl7.org/fhir/sid/icd-11",
                "code": icd_code,
                "display": icd_display,
            }
        )
    if snomed_code:
        condition_coding.append(
            {
                "system": "http://snomed.info/sct",
                "code": snomed_code,
                "display": consulta.diagnostico_principal,
            }
        )
    condition_coding.append(
        {
            "system": "http://imss.gob.mx/diagnosticos",
            "code": diag_key,
            "display": consulta.diagnostico_principal,
        }
    )

    condition = {
        "resourceType": "Condition",
        "id": f"cond-{consulta.id}",
        "subject": {"reference": f"Patient/{patient['id']}"},
        "recordedDate": _iso(consulta.fecha_registro),
        "code": {"coding": condition_coding, "text": consulta.diagnostico_principal},
        "stage": [{"summary": {"text": consulta.pros_riesgo or consulta.rinon_etapa or "Desconocido"}}],
        "note": [{"text": json.dumps(consulta.protocolo_detalles, ensure_ascii=False) if consulta.protocolo_detalles else ""}],
    }

    encounter = {
        "resourceType": "Encounter",
        "id": f"enc-{consulta.id}",
        "status": "finished",
        "class": {"code": "AMB"},
        "subject": {"reference": f"Patient/{patient['id']}"},
        "period": {"start": _iso(consulta.fecha_registro)},
    }

    def obs(default_code: str, display: str, value: Any, unit: str | None = None, key: str | None = None):
        if value is None:
            return None
        code = loinc_map.get(key, default_code) if key else default_code
        payload: Dict[str, Any] = {
            "resourceType": "Observation",
            "status": "final",
            "code": {"text": display, "coding": [{"system": "http://loinc.org", "code": code, "display": display}]},
            "subject": {"reference": f"Patient/{patient['id']}"},
            "effectiveDateTime": _iso(consulta.fecha_registro),
        }
        payload["valueQuantity"] = {"value": value, "unit": unit} if unit else {"value": value}
        return payload

    observations = []
    for entry in [
        obs("29463-7", "Peso", consulta.peso, "kg", key="peso"),
        obs("8302-2", "Talla", consulta.talla, "cm", key="talla"),
        obs("39156-5", "IMC", consulta.imc, "kg/m2", key="imc"),
        obs("8480-6", "Presión arterial sistólica", consulta.ta, key="ta"),
        obs("8867-4", "Frecuencia cardíaca", consulta.fc, "bpm", key="fc"),
        obs("8310-5", "Temperatura corporal", consulta.temp, "°C", key="temp"),
    ]:
        if entry:
            observations.append(entry)

    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": patient}, {"resource": encounter}, {"resource": condition}],
    }
    for item in observations:
        bundle["entry"].append({"resource": item})

    raw_payload: Dict[str, Any] = {}
    for col in type(consulta).__table__.columns:
        raw_payload[col.name] = _iso(getattr(consulta, col.name))
    raw_json = json.dumps(raw_payload, ensure_ascii=False)
    document_reference = {
        "resourceType": "DocumentReference",
        "id": f"doc-{consulta.id}",
        "status": "current",
        "type": {"text": "Expediente clínico completo"},
        "subject": {"reference": f"Patient/{patient['id']}"},
        "date": _iso(consulta.fecha_registro),
        "content": [
            {
                "attachment": {
                    "contentType": "application/json",
                    "data": base64.b64encode(raw_json.encode("utf-8")).decode("utf-8"),
                }
            }
        ],
    }
    bundle["entry"].append({"resource": document_reference})

    return bundle


def build_fhir_patient_only(consulta: Any) -> Dict[str, Any]:
    bundle = build_fhir_bundle(consulta)
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            return resource
    return {"resourceType": "Patient", "id": consulta.curp or str(consulta.id)}


def build_fhir_condition_only(consulta: Any) -> Dict[str, Any]:
    bundle = build_fhir_bundle(consulta)
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Condition":
            return resource
    return {
        "resourceType": "Condition",
        "subject": {"reference": f"Patient/{consulta.curp or consulta.id}"},
        "code": {"text": consulta.diagnostico_principal or "NO_REGISTRADO"},
    }

