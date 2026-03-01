from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    func,
    select,
)
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.core.validators import normalize_curp, normalize_nss_10

FORM_METADATA = MetaData()

FORM_DEFINITION = Table(
    "form_definition",
    FORM_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("form_code", String(80), nullable=False, unique=True, index=True),
    Column("form_name", String(180), nullable=False),
    Column("module", String(80), nullable=False, index=True),
    Column("description", Text, nullable=True),
    Column("active", Boolean, nullable=False, default=True, index=True),
    Column("created_at", DateTime, default=utcnow, nullable=False),
    Column("updated_at", DateTime, default=utcnow, nullable=False),
)

FORM_VERSION = Table(
    "form_version",
    FORM_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("form_code", String(80), nullable=False, index=True),
    Column("version", Integer, nullable=False, index=True),
    Column("is_current", Boolean, nullable=False, default=True, index=True),
    Column("notes", Text, nullable=True),
    Column("created_at", DateTime, default=utcnow, nullable=False),
)

FORM_SECTION = Table(
    "form_section",
    FORM_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("form_code", String(80), nullable=False, index=True),
    Column("version", Integer, nullable=False, default=1, index=True),
    Column("section_code", String(20), nullable=False, index=True),
    Column("section_title", String(180), nullable=False),
    Column("section_order", Integer, nullable=False, default=0, index=True),
    Column("description", Text, nullable=True),
    Column("active", Boolean, nullable=False, default=True, index=True),
)

FORM_FIELD = Table(
    "form_field",
    FORM_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("form_code", String(80), nullable=False, index=True),
    Column("version", Integer, nullable=False, default=1, index=True),
    Column("section_code", String(20), nullable=False, index=True),
    Column("field_name", String(120), nullable=False, index=True),
    Column("field_label", String(220), nullable=False),
    Column("field_type", String(30), nullable=False, default="text"),
    Column("required", Boolean, nullable=False, default=False),
    Column("placeholder", String(255), nullable=True),
    Column("default_value", String(255), nullable=True),
    Column("options_json", Text, nullable=True),
    Column("validation_json", Text, nullable=True),
    Column("ui_json", Text, nullable=True),
    Column("field_order", Integer, nullable=False, default=0, index=True),
    Column("active", Boolean, nullable=False, default=True, index=True),
)



@dataclass
class MetadataValidationResult:
    valid: bool
    errors: Dict[str, str]
    warnings: Dict[str, str]
    normalized_payload: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "normalized_payload": self.normalized_payload,
        }


_TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"
_SKIP_TECHNICAL_FIELDS = {"csrf_token", "return_to", "consulta_draft_id", "busqueda"}

_SELECT_OPTIONS: Dict[str, List[Dict[str, str]]] = {
    "sexo": [{"value": "Masculino", "label": "Masculino"}, {"value": "Femenino", "label": "Femenino"}],
    "estatus_protocolo": [{"value": "incompleto", "label": "Incompleto"}, {"value": "completo", "label": "Completo"}],
    "ahf_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "app_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "aqx_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "aqx_complicaciones_flag": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "aqx_complicacion_tipo": [
        {"value": "ASOCIADA_A_PROCEDIMIENTO", "label": "Asociada al procedimiento"},
        {"value": "ASOCIADA_A_HOSPITALIZACION", "label": "Asociada a la hospitalización"},
    ],
    "transfusiones_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "alergias_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "toxicomanias_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "hosp_previas": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "hosp_uci": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "tabaquismo_status": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "consentimiento_uso_datos": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "consentimiento_investigacion": [{"value": "si", "label": "Sí"}, {"value": "no", "label": "No"}],
    "incapacidad": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "incapacidad_emitida": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "programado": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "urgencia": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "uci": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "solicita_hemoderivados": [{"value": "SI", "label": "Sí"}, {"value": "NO", "label": "No"}],
    "diagnostico_principal": [
        {"value": "ca_rinon", "label": "CÁNCER DE RIÑÓN"},
        {"value": "ca_urotelial_alto", "label": "CÁNCER UROTELIAL TRACTO SUPERIOR"},
        {"value": "ca_vejiga", "label": "CÁNCER DE VEJIGA"},
        {"value": "ca_prostata", "label": "CÁNCER DE PRÓSTATA"},
        {"value": "ca_pene", "label": "CÁNCER DE PENE"},
        {"value": "ca_testiculo", "label": "CÁNCER DE TESTÍCULO"},
        {"value": "tumor_suprarrenal", "label": "TUMOR SUPRARRENAL"},
        {"value": "tumor_incierto_prostata", "label": "TUMOR COMPORTAMIENTO INCIERTO PRÓSTATA"},
        {"value": "litiasis_rinon", "label": "CÁLCULO DEL RIÑÓN"},
        {"value": "litiasis_ureter", "label": "CÁLCULO DEL URÉTER"},
        {"value": "litiasis_vejiga", "label": "CÁLCULO DE LA VEJIGA"},
        {"value": "priapismo", "label": "PRIAPISMO / DISFUNCIÓN ERÉCTIL"},
        {"value": "incontinencia", "label": "INCONTINENCIA URINARIA"},
        {"value": "fistula", "label": "FÍSTULA (V-V / U-V)"},
        {"value": "trasplante", "label": "TRASPLANTE RENAL (DONADOR VIVO)"},
        {"value": "hpb", "label": "HIPERPLASIA PROSTÁTICA BENIGNA"},
        {"value": "infeccion", "label": "ABSCESO / PIELONEFRITIS"},
    ],
}

_DATE_FIELDS = {
    "fecha_nacimiento",
    "fecha_ingreso",
    "fecha_egreso",
    "fecha_urgencia",
    "consentimiento_fecha",
    "aqx_fecha",
    "alergia_fecha",
    "trans_fecha",
    "testiculo_orquiectomia_fecha",
}

_NUMBER_FIELDS = {
    "edad",
    "peso",
    "talla",
    "imc",
    "fc",
    "temp",
    "hosp_previas",
    "hosp_dias",
    "hosp_dias_uci",
    "cigarros_dia",
    "anios_fumando",
    "indice_tabaquico",
    "hemoderivados_pg_solicitados",
    "hemoderivados_pfc_solicitados",
    "hemoderivados_cp_solicitados",
    "dias_hospitalizacion",
    "dias_postquirurgicos",
    "consulta_id",
    "surgical_programacion_id",
}

_TEXTAREA_HINTS = {
    "padecimiento_actual",
    "exploracion_fisica",
    "plan_especifico",
    "estudios_hallazgos",
    "observaciones",
    "subsecuente_subjetivo",
    "subsecuente_objetivo",
    "subsecuente_analisis",
    "subsecuente_plan",
}


def ensure_form_metadata_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    FORM_METADATA.create_all(bind=bind, checkfirst=True)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _json_load(value: Any, default: Any) -> Any:
    if not value:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _json_dump(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


def _extract_names_from_template(template_name: str) -> List[str]:
    path = _TEMPLATES_DIR / template_name
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8")
    names = sorted(set(re.findall(r'name="([^"]+)"', content)))
    return [n for n in names if n and n not in _SKIP_TECHNICAL_FIELDS]


def _human_label(name: str) -> str:
    txt = name.replace("_", " ").strip()
    return txt[:1].upper() + txt[1:]


def _field_type_for(name: str) -> str:
    if name == "estudios_files":
        return "file"
    if name in _SELECT_OPTIONS:
        return "select"
    if name in _TEXTAREA_HINTS or name.endswith("_json") or name.endswith("_hallazgos"):
        return "textarea"
    if name in _DATE_FIELDS or name.startswith("fecha_"):
        return "date"
    if name in _NUMBER_FIELDS:
        return "number"
    return "text"


def _validation_for(name: str) -> Dict[str, Any]:
    if name == "curp":
        return {"pattern": r"^[A-Z]{4}[0-9]{6}[HM][A-Z]{5}[0-9A-Z][0-9]$", "upper": True}
    if name == "nss":
        return {"digits": True, "length": 10}
    if name == "telefono":
        return {"digits": True, "length": 10}
    if name == "email":
        return {"pattern": r"^[^@]+@[^@]+\.[^@]+$", "lower": True}
    if name == "edad":
        return {"min": 0, "max": 120}
    if name in {"peso", "talla", "imc", "temp", "fc"}:
        return {"min": 0}
    return {}


def _ui_for_field(name: str) -> Dict[str, Any]:
    field = str(name or "")
    yes_values = ["si", "SI", "Sí", "sí", "Si", "1", "true", "TRUE", "yes", "YES"]
    if field.startswith("gyn_"):
        return {"visible_when": {"field": "sexo", "in": ["Femenino", "femenino"]}}

    dx_map = {
        "rinon_": ["ca_rinon"],
        "utuc_": ["ca_urotelial_alto"],
        "vejiga_": ["ca_vejiga"],
        "pros_": ["ca_prostata"],
        "pene_": ["ca_pene"],
        "testiculo_": ["ca_testiculo"],
        "suprarrenal_": ["tumor_suprarrenal"],
        "incierto_": ["tumor_incierto_prostata"],
        "lit_": ["litiasis_rinon", "litiasis_ureter", "litiasis_vejiga"],
        "hpb_": ["hpb"],
    }
    for prefix, diag_values in dx_map.items():
        if field.startswith(prefix):
            return {"visible_when": {"field": "diagnostico_principal", "in": diag_values}}

    if field in {"imc", "imc_clasificacion"}:
        return {"readonly": True}
    if field == "ahf_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "ahf_status", "in": yes_values}}
    if field == "app_patologias_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "app_status", "in": yes_values}}
    if field == "hosp_previas_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "hosp_previas", "in": yes_values}}
    if field == "alergias_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "alergias_status", "in": yes_values}}
    if field == "transfusiones_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "transfusiones_status", "in": yes_values}}
    if field == "toxicomanias_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "toxicomanias_status", "in": yes_values}}
    if field == "aqx_json":
        return {"widget": "json_array_editor", "visible_when": {"field": "aqx_status", "in": yes_values}}
    if field == "estudios_files":
        return {
            "widget": "file_uploader",
            "accept": ".doc,.docx,.pdf,.png,.pgn,.jpg,.jpeg,.dcm,.dicom",
            "multiple": True,
        }
    if field in {"trans_reacciones", "trans_fecha"}:
        return {"visible_when": {"field": "transfusiones_status", "in": yes_values}}
    if field == "hosp_dias_uci":
        return {"visible_when": {"field": "hosp_uci", "in": yes_values}}
    if field in {"cigarros_dia", "anios_fumando", "indice_tabaquico"}:
        return {"visible_when": {"field": "tabaquismo_status", "in": yes_values}}
    if field == "aqx_complicaciones_flag":
        return {"visible_when": {"field": "aqx_status", "in": yes_values}}
    if field == "aqx_complicacion_tipo":
        return {"visible_when": {"field": "aqx_complicaciones_flag", "in": yes_values}}
    if field == "aqx_complicacion_detalle":
        return {"visible_when": {"field": "aqx_complicacion_tipo", "in": ["ASOCIADA_A_PROCEDIMIENTO", "ASOCIADA_A_HOSPITALIZACION"]}}
    return {}


def _consulta_section_for_field(name: str) -> str:
    if name in {
        "curp",
        "nss",
        "agregado_medico",
        "nombre",
        "fecha_nacimiento",
        "edad",
        "sexo",
        "tipo_sangre",
        "ocupacion",
        "nombre_empresa",
        "escolaridad",
        "cp",
        "alcaldia",
        "colonia",
        "estado_foraneo",
        "calle",
        "no_ext",
        "no_int",
        "telefono",
        "email",
    }:
        return "1"
    if name.startswith("gyn_") or name.startswith("laboral_") or name.startswith("farmaco_") or name.startswith("consentimiento_"):
        return "1"
    if name in {"peso", "talla", "imc", "imc_clasificacion", "ta", "fc", "temp", "qol_ipss", "qol_iief5", "qol_iciqsf"}:
        return "2"
    if name.startswith("ahf_"):
        return "3"
    if name.startswith("app_") or name in {
        "hosp_previas",
        "hosp_previas_json",
        "transfusiones_json",
        "alergias_status",
        "toxicomanias_status",
        "hosp_motivo",
        "hosp_dias",
        "hosp_uci",
        "hosp_dias_uci",
        "tabaquismo_status",
        "cigarros_dia",
        "anios_fumando",
        "indice_tabaquico",
        "alcoholismo",
        "otras_drogas",
        "droga_manual",
        "toxicomanias_json",
        "alergias_json",
        "alergeno",
        "alergia_reaccion",
        "alergia_fecha",
        "transfusiones_status",
        "trans_reacciones",
        "trans_fecha",
    }:
        return "4"
    if name.startswith("aqx_"):
        return "5"
    if name in {"padecimiento_actual", "exploracion_fisica", "plan_especifico", "otro_detalles"} or name.startswith("subsecuente_"):
        return "6"
    if name == "diagnostico_principal" or name.startswith(("pros_", "rinon_", "utuc_", "suprarrenal_", "vejiga_", "testiculo_", "pene_", "hpb_", "incierto_", "lit_")):
        return "7"
    if name in {"estudios_hallazgos", "estudios_files"}:
        return "8"
    if name == "estatus_protocolo":
        return "9"
    return "9"


def _hospital_section_for_field(name: str) -> str:
    if name in {"consulta_id", "nss", "nombre_completo", "edad", "sexo", "agregado_medico", "cama", "medico_a_cargo", "hgz_envio", "servicio"}:
        return "1"
    if name in {
        "motivo",
        "diagnostico",
        "origen_flujo",
        "programado",
        "medico_programado",
        "turno_programado",
        "urgencia",
        "urgencia_tipo",
        "estatus",
        "estatus_detalle",
        "dias_hospitalizacion",
        "dias_postquirurgicos",
        "incapacidad",
        "incapacidad_emitida",
        "uci",
        "observaciones",
    }:
        return "2"
    return "3"


def _urgencias_section_for_field(name: str) -> str:
    if name in {"nss", "agregado_medico", "nombre_completo", "edad", "sexo", "hgz", "fecha_urgencia", "estatus"}:
        return "1"
    if name in {"patologia", "patologia_cie10", "procedimiento_programado", "insumos_solicitados", "insumos_solicitados_list", "abordaje", "sistema_succion", "tipo_neovejiga"}:
        return "2"
    return "3"


def _fields_from_template(
    *,
    template_name: str,
    section_resolver: Callable[[str], str],
    required_fields: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    required = required_fields or set()
    names = _extract_names_from_template(template_name)
    section_counts: Dict[str, int] = {}
    out: List[Dict[str, Any]] = []
    default_no_fields = {
        "ahf_status",
        "app_status",
        "aqx_status",
        "transfusiones_status",
        "alergias_status",
        "toxicomanias_status",
        "hosp_previas",
        "hosp_uci",
        "tabaquismo_status",
        "aqx_complicaciones_flag",
    }
    for name in names:
        sec = section_resolver(name)
        section_counts[sec] = section_counts.get(sec, 0) + 1
        order = section_counts[sec] * 10
        default_value: Optional[str] = None
        if name in default_no_fields:
            default_value = "no"
        out.append(
            {
                "section_code": sec,
                "field_name": name,
                "field_label": _human_label(name),
                "field_type": _field_type_for(name),
                "required": name in required,
                "placeholder": None,
                "options_json": _SELECT_OPTIONS.get(name, []),
                "validation_json": _validation_for(name),
                "ui_json": _ui_for_field(name),
                "field_order": order,
                "default_value": default_value,
            }
        )
    return out


def _default_forms_catalog() -> List[Dict[str, Any]]:
    consulta_fields = _fields_from_template(
        template_name="consulta.html",
        section_resolver=_consulta_section_for_field,
        required_fields={"curp", "nss", "nombre", "sexo", "diagnostico_principal", "estatus_protocolo"},
    )
    existing_consulta = {str(f.get("field_name") or "") for f in consulta_fields}
    manual_consulta_fields = [
        {
            "section_code": "4",
            "field_name": "app_status",
            "field_label": "Antecedentes personales patológicos",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "no",
            "options_json": _SELECT_OPTIONS.get("app_status") or [],
            "validation_json": {},
            "ui_json": {},
            "field_order": 405,
        },
        {
            "section_code": "4",
            "field_name": "alergias_status",
            "field_label": "Alergias previas",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "no",
            "options_json": _SELECT_OPTIONS.get("alergias_status") or [],
            "validation_json": {},
            "ui_json": {},
            "field_order": 430,
        },
        {
            "section_code": "4",
            "field_name": "toxicomanias_status",
            "field_label": "Toxicomanías",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "no",
            "options_json": _SELECT_OPTIONS.get("toxicomanias_status") or [],
            "validation_json": {},
            "ui_json": {},
            "field_order": 431,
        },
        {
            "section_code": "4",
            "field_name": "transfusiones_json",
            "field_label": "Transfusiones previas (captura múltiple)",
            "field_type": "textarea",
            "required": False,
            "placeholder": '[{"fecha_ultima":"2026-02-18","reacciones":"NO"}]',
            "default_value": "",
            "options_json": [],
            "validation_json": {},
            "ui_json": {"widget": "json_array_editor", "visible_when": {"field": "transfusiones_status", "in": ["si", "SI", "Sí", "sí", "Si", "1", "true", "TRUE", "yes", "YES"]}},
            "field_order": 495,
        },
        {
            "section_code": "5",
            "field_name": "aqx_status",
            "field_label": "Antecedentes quirúrgicos",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "no",
            "options_json": _SELECT_OPTIONS.get("aqx_status") or [],
            "validation_json": {},
            "ui_json": {},
            "field_order": 550,
        },
        {
            "section_code": "5",
            "field_name": "aqx_complicaciones_flag",
            "field_label": "Complicaciones quirúrgicas",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "no",
            "options_json": _SELECT_OPTIONS.get("aqx_complicaciones_flag") or [],
            "validation_json": {},
            "ui_json": _ui_for_field("aqx_complicaciones_flag"),
            "field_order": 560,
        },
        {
            "section_code": "5",
            "field_name": "aqx_complicacion_tipo",
            "field_label": "Tipo de complicación",
            "field_type": "select",
            "required": False,
            "placeholder": None,
            "default_value": "",
            "options_json": _SELECT_OPTIONS.get("aqx_complicacion_tipo") or [],
            "validation_json": {},
            "ui_json": _ui_for_field("aqx_complicacion_tipo"),
            "field_order": 570,
        },
        {
            "section_code": "5",
            "field_name": "aqx_complicacion_detalle",
            "field_label": "Especificar complicación",
            "field_type": "textarea",
            "required": False,
            "placeholder": "Detalle libre",
            "default_value": "",
            "options_json": [],
            "validation_json": {},
            "ui_json": _ui_for_field("aqx_complicacion_detalle"),
            "field_order": 580,
        },
    ]
    for fld in manual_consulta_fields:
        if str(fld.get("field_name") or "") not in existing_consulta:
            consulta_fields.append(fld)

    hospital_fields = _fields_from_template(
        template_name="hospitalizacion_nuevo.html",
        section_resolver=_hospital_section_for_field,
        required_fields={"nss", "nombre_completo", "sexo", "diagnostico", "cama", "estatus"},
    )
    urgencias_fields = _fields_from_template(
        template_name="quirofano_urgencias_nuevo.html",
        section_resolver=_urgencias_section_for_field,
        required_fields={"nss", "nombre_completo", "sexo", "patologia", "procedimiento_programado", "hgz"},
    )

    return [
        {
            "form_code": "consulta_externa",
            "form_name": "Consulta Externa (Metadata)",
            "module": "consulta_externa",
            "description": "Diccionario de datos para consulta externa.",
            "version": 1,
            "sections": [
                {"section_code": "1", "section_title": "Ficha de identificación", "section_order": 10},
                {"section_code": "2", "section_title": "Somatometría y signos vitales", "section_order": 20},
                {"section_code": "3", "section_title": "Antecedentes heredofamiliares", "section_order": 30},
                {"section_code": "4", "section_title": "Personales patológicos", "section_order": 40},
                {"section_code": "5", "section_title": "Antecedentes quirúrgicos", "section_order": 50},
                {"section_code": "6", "section_title": "Padecimiento actual", "section_order": 60},
                {"section_code": "7", "section_title": "Diagnóstico", "section_order": 70},
                {"section_code": "8", "section_title": "Estudios de imagen", "section_order": 80},
                {"section_code": "9", "section_title": "Estatus del protocolo", "section_order": 90},
            ],
            "fields": consulta_fields,
        },
        {
            "form_code": "hospitalizacion_ingreso",
            "form_name": "Hospitalización - Ingreso (Metadata)",
            "module": "hospitalizacion",
            "description": "Diccionario de datos para ingreso hospitalario.",
            "version": 1,
            "sections": [
                {"section_code": "1", "section_title": "Identificación del ingreso", "section_order": 10},
                {"section_code": "2", "section_title": "Detalle clínico-operativo", "section_order": 20},
                {"section_code": "3", "section_title": "Trazabilidad / egreso", "section_order": 30},
            ],
            "fields": hospital_fields,
        },
        {
            "form_code": "urgencias_solicitud_qx",
            "form_name": "Urgencias - Solicitud quirúrgica (Metadata)",
            "module": "urgencias",
            "description": "Diccionario de datos para solicitud quirúrgica de urgencias.",
            "version": 1,
            "sections": [
                {"section_code": "1", "section_title": "Identificación de solicitud", "section_order": 10},
                {"section_code": "2", "section_title": "Diagnóstico y procedimiento", "section_order": 20},
                {"section_code": "3", "section_title": "Variables clínicas dinámicas", "section_order": 30},
            ],
            "fields": urgencias_fields,
        },
    ]


def _upsert_form_seed(session: Session, seed: Dict[str, Any]) -> Dict[str, Any]:
    form_code = seed["form_code"]
    version = int(seed.get("version") or 1)

    exists = session.execute(select(FORM_DEFINITION.c.id).where(FORM_DEFINITION.c.form_code == form_code)).first()
    if exists is None:
        session.execute(
            FORM_DEFINITION.insert().values(
                form_code=form_code,
                form_name=seed["form_name"],
                module=seed["module"],
                description=seed.get("description"),
                active=True,
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )

    version_exists = session.execute(
        select(FORM_VERSION.c.id).where(and_(FORM_VERSION.c.form_code == form_code, FORM_VERSION.c.version == version))
    ).first()
    if version_exists is None:
        session.execute(
            FORM_VERSION.insert().values(
                form_code=form_code,
                version=version,
                is_current=True,
                notes="Seed metadata-driven (aditivo)",
                created_at=utcnow(),
            )
        )

    existing_sections = {
        r[0]
        for r in session.execute(
            select(FORM_SECTION.c.section_code).where(and_(FORM_SECTION.c.form_code == form_code, FORM_SECTION.c.version == version))
        ).all()
    }
    for sec in seed.get("sections") or []:
        code = str(sec.get("section_code") or "")
        if not code or code in existing_sections:
            continue
        session.execute(
            FORM_SECTION.insert().values(
                form_code=form_code,
                version=version,
                section_code=code,
                section_title=sec.get("section_title") or code,
                section_order=int(sec.get("section_order") or 0),
                description=sec.get("description"),
                active=True,
            )
        )

    existing_field_rows = session.execute(
        select(
            FORM_FIELD.c.id,
            FORM_FIELD.c.field_name,
            FORM_FIELD.c.field_type,
            FORM_FIELD.c.required,
            FORM_FIELD.c.default_value,
            FORM_FIELD.c.options_json,
            FORM_FIELD.c.validation_json,
            FORM_FIELD.c.ui_json,
        ).where(and_(FORM_FIELD.c.form_code == form_code, FORM_FIELD.c.version == version))
    ).mappings().all()
    existing_by_name: Dict[str, Dict[str, Any]] = {str(r["field_name"]): dict(r) for r in existing_field_rows}
    insert_count = 0
    update_count = 0
    for fld in seed.get("fields") or []:
        name = str(fld.get("field_name") or "")
        if not name:
            continue
        existing = existing_by_name.get(name)
        if existing is not None:
            updates: Dict[str, Any] = {}
            existing_options = _json_load(existing.get("options_json"), [])
            existing_validation = _json_load(existing.get("validation_json"), {})
            existing_ui = _json_load(existing.get("ui_json"), {})
            target_options = fld.get("options_json") or []
            target_validation = fld.get("validation_json") or {}
            target_ui = fld.get("ui_json") or {}

            if target_options and not existing_options:
                updates["options_json"] = _json_dump(target_options)
            if target_validation and not existing_validation:
                updates["validation_json"] = _json_dump(target_validation)
            if target_ui:
                if not existing_ui:
                    updates["ui_json"] = _json_dump(target_ui)
                else:
                    merged_ui = dict(existing_ui)
                    changed = False
                    for k, v in (target_ui or {}).items():
                        if k not in merged_ui:
                            merged_ui[k] = v
                            changed = True
                    if changed:
                        updates["ui_json"] = _json_dump(merged_ui)
            target_ft = str(fld.get("field_type") or "").lower()
            existing_ft = str(existing.get("field_type") or "").lower()
            if target_ft in {"select", "file"} and existing_ft != target_ft:
                updates["field_type"] = target_ft
            if bool(fld.get("required", False)) and not bool(existing.get("required", False)):
                updates["required"] = True
            target_default = fld.get("default_value")
            if target_default not in (None, "") and _safe_text(existing.get("default_value")) == "":
                updates["default_value"] = target_default

            if updates:
                session.execute(
                    FORM_FIELD.update()
                    .where(FORM_FIELD.c.id == int(existing["id"]))
                    .values(**updates)
                )
                update_count += 1
            continue

        session.execute(
            FORM_FIELD.insert().values(
                form_code=form_code,
                version=version,
                section_code=str(fld.get("section_code") or "9"),
                field_name=name,
                field_label=fld.get("field_label") or _human_label(name),
                field_type=fld.get("field_type") or "text",
                required=bool(fld.get("required", False)),
                placeholder=fld.get("placeholder"),
                default_value=fld.get("default_value"),
                options_json=_json_dump(fld.get("options_json") or []),
                validation_json=_json_dump(fld.get("validation_json") or {}),
                ui_json=_json_dump(fld.get("ui_json") or {}),
                field_order=int(fld.get("field_order") or 0),
                active=True,
            )
        )
        insert_count += 1

    return {"form_code": form_code, "version": version, "inserted_fields": insert_count, "updated_fields": update_count}


def seed_default_form_metadata(bind_or_session: Any) -> Dict[str, Any]:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return {"ok": False, "reason": "missing_bind"}

    ensure_form_metadata_schema(bind)
    session = bind_or_session if isinstance(bind_or_session, Session) else Session(bind=bind)
    owns_session = not isinstance(bind_or_session, Session)

    try:
        details = []
        for seed in _default_forms_catalog():
            details.append(_upsert_form_seed(session, seed))
        session.commit()
        return {"ok": True, "forms": details}
    except Exception as exc:
        session.rollback()
        return {"ok": False, "reason": str(exc)}
    finally:
        if owns_session:
            session.close()


def ensure_default_forms_seeded(bind_or_session: Any) -> Dict[str, Any]:
    """Garantiza seed aditivo del diccionario base (idempotente)."""
    # Se ejecuta siempre: la operación es idempotente y permite propagar
    # ajustes aditivos de metadatos sin requerir reinicio del servidor.
    return seed_default_form_metadata(bind_or_session)


def _current_version(session: Session, form_code: str) -> int:
    row = session.execute(
        select(FORM_VERSION.c.version)
        .where(and_(FORM_VERSION.c.form_code == form_code, FORM_VERSION.c.is_current == True))
        .order_by(FORM_VERSION.c.version.desc())
    ).first()
    return int(row[0] or 1) if row else 1


def list_forms(session: Session) -> List[Dict[str, Any]]:
    ensure_form_metadata_schema(session)
    rows = session.execute(
        select(
            FORM_DEFINITION.c.form_code,
            FORM_DEFINITION.c.form_name,
            FORM_DEFINITION.c.module,
            FORM_DEFINITION.c.description,
            FORM_DEFINITION.c.active,
            FORM_DEFINITION.c.updated_at,
        ).order_by(FORM_DEFINITION.c.form_code.asc())
    ).all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "form_code": r.form_code,
                "form_name": r.form_name,
                "module": r.module,
                "description": r.description,
                "active": bool(r.active),
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                "current_version": _current_version(session, r.form_code),
            }
        )
    return out


def get_form_schema(session: Session, form_code: str) -> Dict[str, Any]:
    ensure_form_metadata_schema(session)
    # Aditivo: asegura campos nuevos aun en sesiones largas sin reinicio total.
    ensure_default_forms_seeded(session)
    code = _safe_text(form_code)
    if not code:
        raise ValueError("form_code requerido")

    form_row = session.execute(
        select(
            FORM_DEFINITION.c.form_code,
            FORM_DEFINITION.c.form_name,
            FORM_DEFINITION.c.module,
            FORM_DEFINITION.c.description,
            FORM_DEFINITION.c.active,
        ).where(FORM_DEFINITION.c.form_code == code)
    ).first()
    if form_row is None:
        raise ValueError("Formulario no encontrado")

    version = _current_version(session, code)

    sec_rows = session.execute(
        select(
            FORM_SECTION.c.section_code,
            FORM_SECTION.c.section_title,
            FORM_SECTION.c.section_order,
            FORM_SECTION.c.description,
        )
        .where(and_(FORM_SECTION.c.form_code == code, FORM_SECTION.c.version == version, FORM_SECTION.c.active == True))
        .order_by(FORM_SECTION.c.section_order.asc(), FORM_SECTION.c.section_code.asc())
    ).all()

    field_rows = session.execute(
        select(
            FORM_FIELD.c.section_code,
            FORM_FIELD.c.field_name,
            FORM_FIELD.c.field_label,
            FORM_FIELD.c.field_type,
            FORM_FIELD.c.required,
            FORM_FIELD.c.placeholder,
            FORM_FIELD.c.default_value,
            FORM_FIELD.c.options_json,
            FORM_FIELD.c.validation_json,
            FORM_FIELD.c.ui_json,
            FORM_FIELD.c.field_order,
        )
        .where(and_(FORM_FIELD.c.form_code == code, FORM_FIELD.c.version == version, FORM_FIELD.c.active == True))
        .order_by(FORM_FIELD.c.section_code.asc(), FORM_FIELD.c.field_order.asc(), FORM_FIELD.c.field_name.asc())
    ).all()

    field_map: Dict[str, List[Dict[str, Any]]] = {}
    for r in field_rows:
        field_map.setdefault(r.section_code, []).append(
            {
                "field_name": r.field_name,
                "field_label": r.field_label,
                "field_type": r.field_type,
                "required": bool(r.required),
                "placeholder": r.placeholder,
                "default_value": r.default_value,
                "options": _json_load(r.options_json, []),
                "validation": _json_load(r.validation_json, {}),
                "ui": _json_load(r.ui_json, {}),
                "field_order": int(r.field_order or 0),
            }
        )

    sections: List[Dict[str, Any]] = []
    for s in sec_rows:
        sections.append(
            {
                "section_code": s.section_code,
                "section_title": s.section_title,
                "section_order": int(s.section_order or 0),
                "description": s.description,
                "fields": field_map.get(s.section_code, []),
            }
        )

    return {
        "form_code": form_row.form_code,
        "form_name": form_row.form_name,
        "module": form_row.module,
        "description": form_row.description,
        "active": bool(form_row.active),
        "version": version,
        "sections": sections,
    }


def _normalize_field_value(field: Dict[str, Any], value: Any) -> Any:
    field_name = str(field.get("field_name") or "")
    field_type = str(field.get("field_type") or "text").lower()
    validation = field.get("validation") or {}

    if field_type in {"number", "float"}:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except Exception:
            return value

    txt = _safe_text(value)
    if validation.get("upper"):
        txt = txt.upper()
    if validation.get("lower"):
        txt = txt.lower()

    if field_name == "nss" and txt:
        txt = normalize_nss_10(txt, strategy="legacy_left")
    if field_name == "curp" and txt:
        txt = normalize_curp(txt)

    return txt


def validate_form_payload(
    session: Session,
    *,
    form_code: str,
    section_code: str,
    payload: Optional[Dict[str, Any]],
) -> MetadataValidationResult:
    schema = get_form_schema(session, form_code)
    section = next((s for s in (schema.get("sections") or []) if str(s.get("section_code")) == str(section_code)), None)
    if section is None:
        return MetadataValidationResult(
            valid=False,
            errors={"section_code": "Sección no existe en metadatos"},
            warnings={},
            normalized_payload=dict(payload or {}),
        )

    src = payload or {}
    errors: Dict[str, str] = {}
    warnings: Dict[str, str] = {}
    normalized: Dict[str, Any] = {}

    for field in section.get("fields") or []:
        name = str(field.get("field_name") or "")
        if not name:
            continue
        raw_value = src.get(name)
        value = _normalize_field_value(field, raw_value)
        normalized[name] = value

        required = bool(field.get("required"))
        if required and (value is None or value == ""):
            errors[name] = "Campo obligatorio"
            continue

        validation = field.get("validation") or {}

        if value in (None, ""):
            continue

        if str(field.get("field_type") or "").lower() in {"number", "float"}:
            try:
                num = float(value)
            except Exception:
                errors[name] = "Debe ser numérico"
                continue
            min_v = validation.get("min")
            max_v = validation.get("max")
            if min_v is not None and num < float(min_v):
                errors[name] = f"Valor mínimo: {min_v}"
            if max_v is not None and num > float(max_v):
                errors[name] = f"Valor máximo: {max_v}"
        else:
            txt = _safe_text(value)
            min_len = validation.get("min_length")
            max_len = validation.get("max_length")
            if min_len is not None and len(txt) < int(min_len):
                errors[name] = f"Longitud mínima: {min_len}"
            if max_len is not None and len(txt) > int(max_len):
                errors[name] = f"Longitud máxima: {max_len}"

            pattern = validation.get("pattern")
            if pattern:
                try:
                    if not re.match(pattern, txt):
                        errors[name] = "Formato inválido"
                except re.error:
                    warnings[name] = "Patrón de validación inválido en diccionario"

            if validation.get("digits"):
                only_digits = re.sub(r"\D", "", txt)
                if only_digits != txt:
                    warnings[name] = "Se removieron caracteres no numéricos"
                normalized[name] = only_digits
                length = validation.get("length")
                if length is not None and len(only_digits) != int(length):
                    errors[name] = f"Debe tener {length} dígitos"

            if name == "nss" and txt and len(normalized.get(name) or "") != 10:
                errors[name] = "NSS debe tener 10 dígitos"

    return MetadataValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings, normalized_payload=normalized)
