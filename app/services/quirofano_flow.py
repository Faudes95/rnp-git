"""Flujos legacy de quirófano extraídos de main.py (refactor aditivo)."""

from __future__ import annotations
from app.core.time_utils import utcnow

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from fastapi import Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.core.terminology import normalize_diagnostico, normalize_procedimiento
from app.services.common import nss_aliases, nss_compat_expr, nss_matches
from app.services.event_log_flow import emit_event
from app.services.resident_profiles_flow import (
    APPROACH_OPTIONS,
    PARTICIPATION_OPTIONS,
    ROLE_OPTIONS,
    index_postqx_feedback,
    load_resident_catalog,
    parse_resident_team,
)

QUIROFANO_CANCELACION_CATALOG: List[Dict[str, str]] = [
    {"codigo": "1.1", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "POR INDICACION MEDICA"},
    {"codigo": "1.2", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "POR URGENCIA"},
    {"codigo": "1.3", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE TIEMPO QUIRURGICO"},
    {"codigo": "1.4", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "MAL PROGRAMADO"},
    {"codigo": "1.5", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE COBERTURA DE PERSONAL"},
    {"codigo": "1.6", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE EQUIPAMIENTO"},
    {"codigo": "1.7", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "PROTOCOLO ADMINISTRATIVO INCOMPLETO"},
    {"codigo": "1.8", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "QUIROFANO NO FUNCIONAL"},
    {"codigo": "1.9", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE INSTRUMENTAL"},
    {"codigo": "1.10", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "PREPARACION INADECUADA"},
    {"codigo": "1.11", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "REMODELACION DE QUIROFANO"},
    {"codigo": "1.12", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE CAMAS"},
    {"codigo": "1.13", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE INSUMOS QUIRURGICOS DE GESTION INTERNA"},
    {"codigo": "1.14", "categoria": "ATRIBUIBLES AL SISTEMA", "concepto": "FALTA DE HEMODERIVADOS"},
    {"codigo": "2.1", "categoria": "ATRIBUIBLES AL PACIENTE", "concepto": "PATOLOGIA AGREGADA"},
    {"codigo": "2.2", "categoria": "ATRIBUIBLES AL PACIENTE", "concepto": "NO SE PRESENTO EL PACIENTE"},
    {"codigo": "2.3", "categoria": "ATRIBUIBLES AL PACIENTE", "concepto": "SIN VIGENCIA"},
    {"codigo": "3.1", "categoria": "ATRIBUIBLES AL PROVEEDOR", "concepto": "FALTA DE OSTEOSINTESIS Y ENDOPROTESIS"},
    {"codigo": "3.2", "categoria": "ATRIBUIBLES AL PROVEEDOR", "concepto": "FALTA DE SERVICIOS INTEGRALES"},
    {"codigo": "3.3", "categoria": "ATRIBUIBLES AL PROVEEDOR", "concepto": "FALTA DE INSUMOS QUIRURGICOS DE GESTION CON PROVEEDOR"},
    {"codigo": "4.1", "categoria": "FACTORES EXTERNOS", "concepto": "EPIDEMIAS/PLAGAS/MARCHAS/TERRORISMO/CONTAMINACION/SISMO"},
]

_QUIROFANO_CANCELACION_BY_CODE = {
    str(item["codigo"]).strip().upper(): item for item in QUIROFANO_CANCELACION_CATALOG
}
_QUIROFANO_CANCELACION_BY_CONCEPTO = {
    str(item["concepto"]).strip().upper(): item for item in QUIROFANO_CANCELACION_CATALOG
}

logger = logging.getLogger(__name__)


def _resolve_cancelacion_concepto(raw_value: Any) -> Optional[Dict[str, str]]:
    value = str(raw_value or "").strip().upper()
    if not value:
        return None
    item = _QUIROFANO_CANCELACION_BY_CODE.get(value)
    if item:
        return item
    return _QUIROFANO_CANCELACION_BY_CONCEPTO.get(value)


def _cancelacion_catalogo_ui() -> List[Dict[str, str]]:
    return list(QUIROFANO_CANCELACION_CATALOG)


def _next_urgencia_quirofano_id(m: Any, sdb: Session) -> int:
    row = (
        sdb.query(m.SurgicalProgramacionDB.quirofano_id)
        .filter(m.SurgicalProgramacionDB.quirofano_id < 0)
        .order_by(m.SurgicalProgramacionDB.quirofano_id.asc())
        .first()
    )
    current_min = int(row[0]) if row and row[0] is not None else 0
    return current_min - 1 if current_min < 0 else -1


def _find_consulta_for_urgencia(m: Any, db: Session, nss: str, nombre_completo: str):
    normalized_nss = m.normalize_nss(nss or "")
    if normalized_nss:
        query = (
            db.query(m.ConsultaDB)
            .filter(nss_compat_expr(m.ConsultaDB.nss, normalized_nss))
            .order_by(m.ConsultaDB.id.desc())
        )
        for row in query.limit(100).all():
            if nss_matches(getattr(row, "nss", ""), nss):
                return row
    nombre = (nombre_completo or "").strip().upper()
    if nombre:
        row = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nombre == nombre)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
        if row is not None:
            return row
    return None


def _build_quirofano_urgencias_context(m: Any) -> Dict[str, Any]:
    return {
        "sexo_options": m.QUIROFANO_SEXOS,
        "patologia_options": m.QUIROFANO_PATOLOGIAS,
        "patologia_cie10_catalog": m.qx_patologia_cie10_catalog(),
        "patologia_options_json": json.dumps(m.QUIROFANO_PATOLOGIAS, ensure_ascii=False),
        "patologia_onco_json": json.dumps(sorted(m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS), ensure_ascii=False),
        "patologia_litiasis_json": json.dumps(sorted(m.QUIROFANO_PATOLOGIAS_LITIASIS), ensure_ascii=False),
        "procedimiento_options": m.QUIROFANO_PROCEDIMIENTOS,
        "procedimientos_abordaje": sorted(m.QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE),
        "procedimientos_abiertos": sorted(m.QUIROFANO_PROCEDIMIENTOS_ABIERTOS),
        "insumo_options": m.QUIROFANO_INSUMOS,
        "hemoderivados_options": m.QUIROFANO_HEMODERIVADOS,
        "patologias_onco": sorted(m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS),
        "patologias_litiasis": sorted(m.QUIROFANO_PATOLOGIAS_LITIASIS),
    }


async def render_quirofano_urgencias_flow(request: Request):
    from app.core.app_context import main_proxy as m

    return m.render_template("quirofano_urgencias_home.html", request=request)


async def render_quirofano_urgencias_solicitud_flow(request: Request):
    from app.core.app_context import main_proxy as m

    context = _build_quirofano_urgencias_context(m)
    return m.render_template("quirofano_urgencias_nuevo.html", request=request, **context)


async def listar_quirofanos_urgencias_flow(
    request: Request,
    sdb: Session,
    *,
    campo: Optional[str] = None,
    q: Optional[str] = None,
):
    from app.core.app_context import main_proxy as m

    query = sdb.query(m.SurgicalUrgenciaProgramacionDB).order_by(
        m.SurgicalUrgenciaProgramacionDB.fecha_urgencia.desc(),
        m.SurgicalUrgenciaProgramacionDB.id.desc(),
    )
    search_value = (q or "").strip()
    field_map = {
        "nss": m.SurgicalUrgenciaProgramacionDB.nss,
        "paciente_nombre": m.SurgicalUrgenciaProgramacionDB.paciente_nombre,
        "sexo": m.SurgicalUrgenciaProgramacionDB.sexo,
        "patologia": m.SurgicalUrgenciaProgramacionDB.patologia,
        "procedimiento_programado": m.SurgicalUrgenciaProgramacionDB.procedimiento_programado,
        "hgz": m.SurgicalUrgenciaProgramacionDB.hgz,
        "estatus": m.SurgicalUrgenciaProgramacionDB.estatus,
    }
    if campo in field_map and search_value:
        query = query.filter(field_map[campo].contains(search_value.upper()))

    rows = query.limit(1000).all()
    resultado = []
    for row in rows:
        resultado.append(
            {
                "id": row.id,
                "consulta_id": row.consulta_id,
                "surgical_programacion_id": row.surgical_programacion_id,
                "nss": row.nss,
                "paciente_nombre": row.paciente_nombre or "NO_REGISTRADO",
                "edad": row.edad,
                "sexo": row.sexo,
                "patologia": row.patologia,
                "procedimiento_programado": row.procedimiento_programado,
                "insumos_solicitados": row.insumos_solicitados,
                "solicita_hemoderivados": row.solicita_hemoderivados,
                "hemoderivados_pg_solicitados": row.hemoderivados_pg_solicitados,
                "hemoderivados_pfc_solicitados": row.hemoderivados_pfc_solicitados,
                "hemoderivados_cp_solicitados": row.hemoderivados_cp_solicitados,
                "hgz": row.hgz,
                "fecha_urgencia": row.fecha_urgencia,
                "estatus": row.estatus,
                "agregado_medico": row.agregado_medico,
                "cirujano": row.cirujano,
                "cancelacion_codigo": getattr(row, "cancelacion_codigo", None),
                "cancelacion_categoria": getattr(row, "cancelacion_categoria", None),
                "cancelacion_concepto": getattr(row, "cancelacion_concepto", None),
                "cancelacion_detalle": getattr(row, "cancelacion_detalle", None),
                "cancelacion_fecha": getattr(row, "cancelacion_fecha", None),
            }
        )

    return m.render_template(
        "quirofano_urgencias_lista.html",
        request=request,
        urgencias=resultado,
        campo=campo or "",
        q=search_value,
        cancelacion_conceptos=_cancelacion_catalogo_ui(),
    )


async def guardar_quirofano_urgencia_flow(request: Request, db: Session, sdb: Session):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)
    back_href = "/quirofano/urgencias/solicitud"

    insumos_list = [i.strip() for i in form.getlist("insumos_solicitados_list") if str(i).strip()]
    insumos_solicitados = " | ".join(insumos_list)

    def _parse_units(raw: Any) -> int:
        try:
            val = int(float(raw))
            return val if val > 0 else 0
        except Exception:
            return 0

    payload = {
        "nss": m.normalize_nss(form_dict.get("nss")),
        "agregado_medico": (form_dict.get("agregado_medico") or "").strip(),
        "nombre_completo": (form_dict.get("nombre_completo") or "").strip().upper(),
        "edad": m.parse_int(form_dict.get("edad")),
        "sexo": m.normalize_upper(form_dict.get("sexo")),
        "patologia": m.normalize_upper(form_dict.get("patologia")),
        "patologia_cie10": m.normalize_upper(form_dict.get("patologia_cie10")),
        "procedimiento_programado": m.normalize_upper(form_dict.get("procedimiento_programado")),
        "insumos_solicitados": insumos_solicitados,
        "hgz": (form_dict.get("hgz") or "").strip().upper(),
        "estatus": m.normalize_upper(form_dict.get("estatus")) or "PENDIENTE",
        "tnm": (form_dict.get("tnm") or "").strip().upper(),
        "ecog_onco": (form_dict.get("ecog_onco") or "").strip().upper(),
        "ecog_incierto": (form_dict.get("ecog_incierto") or "").strip().upper(),
        "charlson": (form_dict.get("charlson") or "").strip().upper(),
        "etapa_clinica": (form_dict.get("etapa_clinica") or "").strip().upper(),
        "ipss": (form_dict.get("ipss") or "").strip().upper(),
        "gleason": (form_dict.get("gleason") or "").strip().upper(),
        "ape": (form_dict.get("ape") or "").strip().upper(),
        "rtup_previa": m.normalize_upper(form_dict.get("rtup_previa")),
        "tacto_rectal": (form_dict.get("tacto_rectal") or "").strip().upper(),
        "historial_ape": (form_dict.get("historial_ape") or "").strip().upper(),
        "uh_rango": (form_dict.get("uh_rango") or "").strip().upper(),
        "litiasis_tamano_rango": (form_dict.get("litiasis_tamano_rango") or "").strip().upper(),
        "litiasis_subtipo_20": (form_dict.get("litiasis_subtipo_20") or "").strip().upper(),
        "litiasis_ubicacion": (form_dict.get("litiasis_ubicacion") or "").strip().upper(),
        "litiasis_ubicacion_multiple": (form_dict.get("litiasis_ubicacion_multiple") or "").strip().upper(),
        "hidronefrosis": m.normalize_upper(form_dict.get("hidronefrosis")),
        "tipo_neovejiga": (form_dict.get("tipo_neovejiga") or "").strip().upper(),
        "sistema_succion": m.normalize_upper(form_dict.get("sistema_succion")),
        "abordaje": m.normalize_upper(form_dict.get("abordaje")),
        "fecha_urgencia": (form_dict.get("fecha_urgencia") or "").strip(),
        "solicita_hemoderivados": m.normalize_upper(form_dict.get("solicita_hemoderivados")) or "NO",
        "hemoderivados_pg_solicitados": _parse_units(form_dict.get("hemoderivados_pg_solicitados")),
        "hemoderivados_pfc_solicitados": _parse_units(form_dict.get("hemoderivados_pfc_solicitados")),
        "hemoderivados_cp_solicitados": _parse_units(form_dict.get("hemoderivados_cp_solicitados")),
    }

    ready, missing_fields = m.is_required_form_complete(payload)
    if not ready:
        faltantes = ", ".join(missing_fields)
        return HTMLResponse(
            content=f"<h1>Campos obligatorios faltantes</h1><p>{faltantes}</p><a href='{back_href}'>Volver</a>",
            status_code=400,
        )
    payload["estatus"] = "PROGRAMADA"
    if payload["solicita_hemoderivados"] not in {"SI", "NO"}:
        payload["solicita_hemoderivados"] = "NO"
    if payload["solicita_hemoderivados"] == "SI":
        total_hemoderivados = (
            int(payload["hemoderivados_pg_solicitados"])
            + int(payload["hemoderivados_pfc_solicitados"])
            + int(payload["hemoderivados_cp_solicitados"])
        )
        if total_hemoderivados <= 0:
            return HTMLResponse(
                content=f"<h1>Si solicitas hemoderivados debes indicar al menos una unidad</h1><a href='{back_href}'>Volver</a>",
                status_code=400,
            )
    else:
        payload["hemoderivados_pg_solicitados"] = 0
        payload["hemoderivados_pfc_solicitados"] = 0
        payload["hemoderivados_cp_solicitados"] = 0

    if len(payload["nss"]) != 10:
        return HTMLResponse(content=f"<h1>NSS inválido: debe contener 10 dígitos</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["sexo"] not in m.QUIROFANO_SEXOS:
        return HTMLResponse(content=f"<h1>Sexo inválido</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["patologia"] not in m.QUIROFANO_PATOLOGIAS:
        return HTMLResponse(content=f"<h1>Patología inválida</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["procedimiento_programado"] not in m.QUIROFANO_PROCEDIMIENTOS:
        return HTMLResponse(content=f"<h1>Procedimiento inválido</h1><a href='{back_href}'>Volver</a>", status_code=400)

    if payload["patologia"] in m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS:
        required_onco = ["tnm", "ecog_onco", "charlson", "etapa_clinica"]
        if any(not str(payload.get(k) or "").strip() for k in required_onco):
            return HTMLResponse(content=f"<h1>Complete TNM, ECOG, Charlson y etapa clínica para patología oncológica</h1><a href='{back_href}'>Volver</a>", status_code=400)
        if payload["patologia"] == "CANCER DE PROSTATA":
            for key_name, label in [("ipss", "IPSS"), ("gleason", "Gleason"), ("ape", "APE"), ("rtup_previa", "RTUP previa")]:
                if not str(payload.get(key_name) or "").strip():
                    return HTMLResponse(content=f"<h1>Falta {label} para cáncer de próstata</h1><a href='{back_href}'>Volver</a>", status_code=400)

    if payload["patologia"] == "TUMOR DE COMPORTAMIENTO INCIERTO PROSTATA":
        for key_name, label in [("tacto_rectal", "tacto rectal"), ("historial_ape", "historial de APE"), ("ecog_incierto", "ECOG")]:
            if not str(payload.get(key_name) or "").strip():
                return HTMLResponse(content=f"<h1>Falta {label} en tumor de comportamiento incierto de próstata</h1><a href='{back_href}'>Volver</a>", status_code=400)

    if payload["patologia"] == "CALCULO DEL RIÑON":
        for key_name, label in [("uh_rango", "unidades Hounsfield"), ("litiasis_tamano_rango", "tamaño"), ("litiasis_ubicacion", "ubicación"), ("hidronefrosis", "hidronefrosis")]:
            if not str(payload.get(key_name) or "").strip():
                return HTMLResponse(content=f"<h1>Falta {label} para cálculo del riñón</h1><a href='{back_href}'>Volver</a>", status_code=400)
        if payload["litiasis_tamano_rango"] == "> 20 MM" and not str(payload.get("litiasis_subtipo_20") or "").strip():
            return HTMLResponse(content=f"<h1>Seleccione subtipo para litiasis mayor a 20 mm</h1><a href='{back_href}'>Volver</a>", status_code=400)
        if payload["litiasis_ubicacion"] == "LITIASIS CALICIAL MULTIPLE" and not str(payload.get("litiasis_ubicacion_multiple") or "").strip():
            return HTMLResponse(content=f"<h1>Especifique ubicaciones en litiasis calicial múltiple</h1><a href='{back_href}'>Volver</a>", status_code=400)

    ecog_final = payload["ecog_onco"] or payload["ecog_incierto"]
    if payload["patologia"] in m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS and not ecog_final:
        return HTMLResponse(content=f"<h1>Falta ECOG para patología oncológica</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["procedimiento_programado"] == m.QUIROFANO_PROCEDIMIENTO_SUCCION and payload["sistema_succion"] not in {"FANS", "DISS"}:
        return HTMLResponse(content=f"<h1>Seleccione sistema de succión (FANS o DISS)</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["procedimiento_programado"] in m.QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE and payload["abordaje"] not in {"ABIERTO", "LAPAROSCOPICO", "ABIERTO + LAPAROSCOPICO"}:
        return HTMLResponse(content=f"<h1>Seleccione abordaje (Abierto/Laparoscópico/Ambos)</h1><a href='{back_href}'>Volver</a>", status_code=400)
    if payload["procedimiento_programado"] in m.QUIROFANO_PROCEDIMIENTOS_ABIERTOS:
        payload["abordaje"] = "ABIERTO"

    fecha_urgencia = date.today()
    if payload["fecha_urgencia"]:
        try:
            fecha_urgencia = datetime.strptime(payload["fecha_urgencia"], "%Y-%m-%d").date()
        except Exception:
            return HTMLResponse(content=f"<h1>Fecha de urgencia inválida</h1><a href='{back_href}'>Volver</a>", status_code=400)

    grupo_patologia = m.classify_pathology_group(payload["patologia"])
    grupo_procedimiento = m.classify_procedure_group(payload["procedimiento_programado"], payload["abordaje"], payload["sistema_succion"])
    dx_norm = normalize_diagnostico(payload["patologia"], cie10_codigo=payload["patologia_cie10"])
    proc_norm = normalize_procedimiento(payload["procedimiento_programado"])
    if not payload.get("patologia_cie10"):
        payload["patologia_cie10"] = str(dx_norm.get("cie10_codigo") or "").upper()
    requiere_intermed = "SI" if any("INTERMED" in item.upper() for item in insumos_list) else "NO"
    cie11_codigo = m.get_cie11_from_patologia(payload["patologia"]) or str(dx_norm.get("cie11_codigo") or "")
    snomed_codigo = m.get_snomed_from_patologia(payload["patologia"]) or str(proc_norm.get("snomed_codigo") or "")
    cie9mc_codigo = m.get_cie9mc_from_procedimiento(payload["procedimiento_programado"])

    consulta_row = _find_consulta_for_urgencia(m, db, payload["nss"], payload["nombre_completo"])
    consulta_id_val = int(consulta_row.id) if consulta_row is not None else None
    curp_val = consulta_row.curp if consulta_row is not None else None
    consulta_vinculada_real = consulta_row is not None
    if consulta_id_val is None:
        synthetic_seed = int(utcnow().timestamp())
        consulta_id_val = int(synthetic_seed)

    mirrored_programacion_id: Optional[int] = None
    try:
        urg_row = m.SurgicalUrgenciaProgramacionDB(
            consulta_id=consulta_id_val,
            curp=curp_val,
            nss=payload["nss"],
            agregado_medico=payload["agregado_medico"],
            paciente_nombre=payload["nombre_completo"],
            edad=payload["edad"],
            edad_grupo=m.classify_age_group(payload["edad"]),
            sexo=payload["sexo"],
            grupo_sexo=payload["sexo"],
            patologia=payload["patologia"],
            patologia_cie10=payload["patologia_cie10"] or m.get_cie10_from_patologia(payload["patologia"]),
            grupo_patologia=grupo_patologia,
            procedimiento_programado=payload["procedimiento_programado"],
            grupo_procedimiento=grupo_procedimiento,
            abordaje=payload["abordaje"],
            tipo_neovejiga=payload["tipo_neovejiga"],
            sistema_succion=payload["sistema_succion"],
            insumos_solicitados=insumos_solicitados,
            requiere_intermed=requiere_intermed,
            solicita_hemoderivados=payload["solicita_hemoderivados"],
            hemoderivados_pg_solicitados=payload["hemoderivados_pg_solicitados"],
            hemoderivados_pfc_solicitados=payload["hemoderivados_pfc_solicitados"],
            hemoderivados_cp_solicitados=payload["hemoderivados_cp_solicitados"],
            hgz=payload["hgz"],
            tnm=payload["tnm"],
            ecog=ecog_final,
            charlson=payload["charlson"],
            etapa_clinica=payload["etapa_clinica"],
            ipss=payload["ipss"],
            gleason=payload["gleason"],
            ape=payload["ape"],
            rtup_previa=payload["rtup_previa"],
            tacto_rectal=payload["tacto_rectal"],
            historial_ape=payload["historial_ape"],
            uh_rango=payload["uh_rango"],
            litiasis_tamano_rango=payload["litiasis_tamano_rango"],
            litiasis_subtipo_20=payload["litiasis_subtipo_20"],
            litiasis_ubicacion=payload["litiasis_ubicacion"],
            litiasis_ubicacion_multiple=payload["litiasis_ubicacion_multiple"],
            hidronefrosis=payload["hidronefrosis"],
            estatus="PROGRAMADA",
            fecha_urgencia=fecha_urgencia,
            cie11_codigo=cie11_codigo,
            snomed_codigo=snomed_codigo,
            cie9mc_codigo=cie9mc_codigo,
            modulo_origen="QUIROFANO_URGENCIA",
        )
        sdb.add(urg_row)
        sdb.flush()

        mirrored_consulta_id = consulta_row.id if consulta_row is not None else (2000000000 + int(urg_row.id))
        mirrored = m.SurgicalProgramacionDB(
            quirofano_id=_next_urgencia_quirofano_id(m, sdb),
            consulta_id=int(mirrored_consulta_id),
            curp=curp_val,
            nss=payload["nss"],
            agregado_medico=payload["agregado_medico"],
            paciente_nombre=payload["nombre_completo"],
            edad=payload["edad"],
            edad_grupo=m.classify_age_group(payload["edad"]),
            sexo=payload["sexo"],
            grupo_sexo=payload["sexo"],
            diagnostico_principal=payload["patologia"],
            patologia=payload["patologia"],
            grupo_patologia=grupo_patologia,
            procedimiento=payload["procedimiento_programado"],
            procedimiento_programado=payload["procedimiento_programado"],
            grupo_procedimiento=grupo_procedimiento,
            abordaje=payload["abordaje"],
            tipo_neovejiga=payload["tipo_neovejiga"],
            sistema_succion=payload["sistema_succion"],
            insumos_solicitados=insumos_solicitados,
            requiere_intermed=requiere_intermed,
            solicita_hemoderivados=payload["solicita_hemoderivados"],
            hemoderivados_pg_solicitados=payload["hemoderivados_pg_solicitados"],
            hemoderivados_pfc_solicitados=payload["hemoderivados_pfc_solicitados"],
            hemoderivados_cp_solicitados=payload["hemoderivados_cp_solicitados"],
            hgz=payload["hgz"],
            tnm=payload["tnm"],
            ecog=ecog_final,
            charlson=payload["charlson"],
            etapa_clinica=payload["etapa_clinica"],
            ipss=payload["ipss"],
            gleason=payload["gleason"],
            ape=payload["ape"],
            rtup_previa=payload["rtup_previa"],
            tacto_rectal=payload["tacto_rectal"],
            historial_ape=payload["historial_ape"],
            uh_rango=payload["uh_rango"],
            litiasis_tamano_rango=payload["litiasis_tamano_rango"],
            litiasis_subtipo_20=payload["litiasis_subtipo_20"],
            litiasis_ubicacion=payload["litiasis_ubicacion"],
            litiasis_ubicacion_multiple=payload["litiasis_ubicacion_multiple"],
            hidronefrosis=payload["hidronefrosis"],
            fecha_programada=fecha_urgencia,
            estatus="PROGRAMADA",
            protocolo_completo="SI",
            pendiente_programar="NO",
            fecha_ingreso_pendiente_programar=utcnow(),
            dias_en_espera=0,
            prioridad_clinica="ALTA",
            motivo_prioridad="PROGRAMACION DE URGENCIA",
            riesgo_cancelacion_predicho=0.1,
            score_preventivo=0.9,
            cie11_codigo=cie11_codigo,
            snomed_codigo=snomed_codigo,
            cie9mc_codigo=cie9mc_codigo,
            modulo_origen="QUIROFANO_URGENCIA",
            urgencia_programacion_id=urg_row.id,
        )
        sdb.add(mirrored)
        sdb.flush()
        mirrored_programacion_id = int(mirrored.id)

        urg_row.surgical_programacion_id = mirrored.id
        sdb.commit()

        try:
            from app.services.master_identity_flow import upsert_master_identity

            upsert_master_identity(
                db,
                nss=payload["nss"],
                curp=curp_val,
                nombre=payload["nombre_completo"],
                sexo=payload["sexo"],
                consulta_id=int(mirrored_consulta_id) if consulta_row is not None else None,
                source_table="surgical_urgencias_programaciones",
                source_pk=urg_row.id,
                module="quirofano_urgencias",
                fecha_evento=fecha_urgencia,
                payload={
                    "patologia": payload["patologia"],
                    "procedimiento_programado": payload["procedimiento_programado"],
                    "hgz": payload["hgz"],
                    "estatus": "PROGRAMADA",
                },
                commit=True,
            )
        except Exception:
            db.rollback()

        try:
            m.push_module_feedback(
                consulta_id=int(mirrored_consulta_id),
                modulo="quirofano_urgencias",
                referencia_id=f"urgencias:{urg_row.id}",
                payload={
                    "nss": payload["nss"],
                    "sexo": payload["sexo"],
                    "patologia": payload["patologia"],
                    "patologia_cie10": urg_row.patologia_cie10,
                    "procedimiento_programado": payload["procedimiento_programado"],
                    "grupo_patologia": grupo_patologia,
                    "grupo_procedimiento": grupo_procedimiento,
                    "hgz": payload["hgz"],
                    "requiere_intermed": requiere_intermed,
                    "solicita_hemoderivados": payload["solicita_hemoderivados"],
                    "hemoderivados_pg_solicitados": payload["hemoderivados_pg_solicitados"],
                    "hemoderivados_pfc_solicitados": payload["hemoderivados_pfc_solicitados"],
                    "hemoderivados_cp_solicitados": payload["hemoderivados_cp_solicitados"],
                    "estatus": "PROGRAMADA",
                    "modulo_origen": "QUIROFANO_URGENCIA",
                },
            )
        except Exception:
            logger.exception(
                "push_module_feedback falló en cirugía de urgencia nss=%s urgencia_id=%s",
                payload.get("nss"),
                getattr(urg_row, "id", None),
            )
        try:
            m.registrar_evento_flujo_quirurgico(
                consulta_id=int(mirrored_consulta_id),
                evento="URG_PROGRAMADA",
                estatus="PROGRAMADA",
                surgical_programacion_id=mirrored.id,
                quirofano_id=mirrored.quirofano_id,
                edad=payload["edad"],
                sexo=payload["sexo"],
                nss=payload["nss"],
                hgz=payload["hgz"],
                diagnostico=payload["patologia"],
                procedimiento=payload["procedimiento_programado"],
                ecog=ecog_final,
                metadata_json={
                    "origen": "URG",
                    "urgencia_programacion_id": urg_row.id,
                    "consulta_vinculada_real": "SI" if consulta_vinculada_real else "NO",
                    "patologia_cie10": urg_row.patologia_cie10,
                    "solicita_hemoderivados": payload["solicita_hemoderivados"],
                    "hemoderivados_pg_solicitados": payload["hemoderivados_pg_solicitados"],
                    "hemoderivados_pfc_solicitados": payload["hemoderivados_pfc_solicitados"],
                    "hemoderivados_cp_solicitados": payload["hemoderivados_cp_solicitados"],
                },
            )
        except Exception:
            logger.exception(
                "registrar_evento_flujo_quirurgico falló en cirugía de urgencia nss=%s urgencia_id=%s",
                payload.get("nss"),
                getattr(urg_row, "id", None),
            )
    except Exception:
        sdb.rollback()
        logger.exception(
            "guardar_quirofano_urgencia_flow falló para nss=%s nombre=%s",
            payload.get("nss"),
            payload.get("nombre_completo"),
        )
        return HTMLResponse(content=f"<h1>Error al guardar cirugía de urgencia</h1><a href='{back_href}'>Volver</a>", status_code=500)

    try:
        emit_event(
            db,
            module="quirofano_urgencias",
            event_type="URG_SOLICITUD_GUARDADA",
            entity="surgical_urgencias_programaciones",
            entity_id=str(int(getattr(urg_row, "id", 0) or 0)),
            consulta_id=int(consulta_id_val) if consulta_id_val else None,
            actor=request.headers.get("X-User", "system"),
            source_route=request.url.path,
            payload={
                "surgical_programacion_id": int(mirrored_programacion_id or 0),
                "nss": payload.get("nss"),
                "patologia": payload.get("patologia"),
                "procedimiento_programado": payload.get("procedimiento_programado"),
                "hgz": payload.get("hgz"),
                "estatus": "PROGRAMADA",
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    if m.celery_app is not None:
        try:
            m.async_actualizar_data_mart_task.delay()
        except Exception:
            pass

    postqx_href = (
        f"/quirofano/urgencias/{mirrored_programacion_id}/postquirurgica"
        if mirrored_programacion_id
        else "/quirofano/urgencias/postquirurgica"
    )
    expediente_href = (
        f"/expediente?consulta_id={consulta_id_val}"
        if consulta_row is not None and consulta_row.id is not None
        else f"/expediente?nss={payload['nss']}&nombre={payload['nombre_completo'].replace(' ', '%20')}"
    )
    hospitalizacion_handoff_href = (
        f"/hospitalizacion/nuevo?origen_flujo=urgencia&surgical_programacion_id={mirrored_programacion_id or ''}"
        f"&consulta_id={consulta_row.id if consulta_row is not None else ''}"
        f"&nss={payload['nss']}"
        f"&nombre_completo={payload['nombre_completo'].replace(' ', '%20')}"
        f"&edad={payload['edad'] if payload['edad'] is not None else ''}"
        f"&sexo={payload['sexo']}"
        f"&diagnostico={payload['patologia'].replace(' ', '%20')}"
        f"&hgz_envio={payload['hgz'].replace(' ', '%20')}"
        f"&agregado_medico={payload['agregado_medico'].replace(' ', '%20')}"
        f"&medico_a_cargo={payload['agregado_medico'].replace(' ', '%20')}"
        "&urgencia=SI&urgencia_tipo=URGENCIA%20QUIRURGICA&programado=NO"
    )
    hospitalizacion_link_html = (
        f"<p><a href='{hospitalizacion_handoff_href}'>🏥 Ingresar a hospitalización (handoff automático)</a></p>"
        if consulta_vinculada_real
        else (
            "<p><strong>⚠ Para continuar a hospitalización debe existir consulta externa real asociada "
            "(NSS/NOMBRE).</strong></p>"
            "<p><a href='/consulta_externa'>🧾 Ir a Consulta Externa para registrar ficha y reintentar handoff</a></p>"
        )
    )
    return HTMLResponse(
        content=(
            "<h1>Cirugía de urgencia programada exitosamente</h1>"
            f"<p><a href='{postqx_href}'>🩺 Completar nota postquirúrgica de este paciente</a></p>"
            f"{hospitalizacion_link_html}"
            "<p><a href='/quirofano/urgencias/lista'>📋 Ver lista de urgencias programadas</a></p>"
            f"<p><a href='{expediente_href}'>📁 Ver expediente clínico único del paciente</a></p>"
        )
    )


async def listar_quirofanos_flow(
    request: Request,
    db: Session,
    sdb: Session,
    *,
    campo: Optional[str] = None,
    q: Optional[str] = None,
):
    from app.core.app_context import main_proxy as m

    query = sdb.query(m.SurgicalProgramacionDB).order_by(
        m.SurgicalProgramacionDB.fecha_programada.desc(),
        m.SurgicalProgramacionDB.id.desc(),
    )
    query = query.filter(
        (m.SurgicalProgramacionDB.modulo_origen.is_(None))
        | (m.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA")
    )
    search_value = (q or "").strip()
    field_map = {
        "nss": m.SurgicalProgramacionDB.nss,
        "paciente_nombre": m.SurgicalProgramacionDB.paciente_nombre,
        "sexo": m.SurgicalProgramacionDB.sexo,
        "patologia": m.SurgicalProgramacionDB.patologia,
        "procedimiento_programado": m.SurgicalProgramacionDB.procedimiento_programado,
        "hgz": m.SurgicalProgramacionDB.hgz,
        "estatus": m.SurgicalProgramacionDB.estatus,
    }
    rows = None
    if campo == "nss" and search_value:
        digits = "".join(ch for ch in search_value if ch.isdigit())
        if digits:
            alias_set = set(nss_aliases(digits))
            query_rows = query.limit(1000).all()
            rows = []
            for row in query_rows:
                row_nss = str(getattr(row, "nss", "") or "")
                row_digits = "".join(ch for ch in row_nss if ch.isdigit())
                if alias_set:
                    if set(nss_aliases(row_nss)).intersection(alias_set):
                        rows.append(row)
                elif digits in row_digits:
                    rows.append(row)
        else:
            query = query.filter(field_map[campo].contains(search_value.upper()))
    elif campo in field_map and search_value:
        query = query.filter(field_map[campo].contains(search_value.upper()))

    if rows is None:
        rows = query.limit(500).all()
    resultado = []
    if rows:
        for row in rows:
            resultado.append(
                {
                    "id": row.quirofano_id or row.id,
                    "surgical_programacion_id": row.id,
                    "consulta_id": row.consulta_id,
                    "nss": row.nss,
                    "paciente_nombre": row.paciente_nombre or "Desconocido",
                    "edad": row.edad,
                    "sexo": row.sexo,
                    "patologia": row.patologia,
                    "procedimiento_programado": row.procedimiento_programado,
                    "procedimiento": row.procedimiento,
                    "insumos_solicitados": row.insumos_solicitados,
                    "hgz": row.hgz,
                    "fecha_programada": row.fecha_programada,
                    "estatus": row.estatus,
                    "agregado_medico": row.agregado_medico,
                    "cirujano": row.cirujano,
                    "cancelacion_codigo": getattr(row, "cancelacion_codigo", None),
                    "cancelacion_categoria": getattr(row, "cancelacion_categoria", None),
                    "cancelacion_concepto": getattr(row, "cancelacion_concepto", None),
                    "cancelacion_detalle": getattr(row, "cancelacion_detalle", None),
                    "cancelacion_fecha": getattr(row, "cancelacion_fecha", None),
                }
            )
    else:
        # Fallback para datos históricos en tabla core
        filas = (
            db.query(m.QuirofanoDB, m.ConsultaDB.nombre)
            .outerjoin(m.ConsultaDB, m.ConsultaDB.id == m.QuirofanoDB.consulta_id)
            .filter(m.QuirofanoDB.estatus == "PROGRAMADA")
            .all()
        )
        for q_core, nombre_paciente in filas:
            resultado.append(
                {
                    "id": q_core.id,
                    "surgical_programacion_id": None,
                    "consulta_id": q_core.consulta_id,
                    "nss": "",
                    "paciente_nombre": nombre_paciente or "Desconocido",
                    "edad": None,
                    "sexo": None,
                    "patologia": None,
                    "procedimiento_programado": q_core.procedimiento,
                    "procedimiento": q_core.procedimiento,
                    "insumos_solicitados": None,
                    "hgz": None,
                    "fecha_programada": q_core.fecha_programada,
                    "estatus": q_core.estatus,
                    "agregado_medico": None,
                    "cirujano": q_core.cirujano,
                    "cancelacion_codigo": None,
                    "cancelacion_categoria": None,
                    "cancelacion_concepto": None,
                    "cancelacion_detalle": None,
                    "cancelacion_fecha": None,
                }
            )

    return m.render_template(
        m.QUIROFANO_LISTA_TEMPLATE,
        request=request,
        quirofanos=resultado,
        campo=campo or "",
        q=search_value,
        cancelacion_conceptos=_cancelacion_catalogo_ui(),
    )


async def guardar_quirofano_flow(request: Request, db: Session):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)

    try:
        consulta_id = int(form_dict.get("consulta_id"))
    except (TypeError, ValueError):
        return HTMLResponse(content="<h1>Consulta ID inválido</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == consulta_id).first()
    if not consulta:
        return HTMLResponse(content="<h1>Consulta no encontrada</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=404)

    fecha_programada_raw = form_dict.get("fecha_programada")
    try:
        fecha_programada = datetime.strptime(fecha_programada_raw, "%Y-%m-%d").date() if fecha_programada_raw else None
    except ValueError:
        return HTMLResponse(content="<h1>Fecha programada inválida</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    insumos_list = [i.strip() for i in form.getlist("insumos_solicitados_list") if str(i).strip()]
    insumos_solicitados = " | ".join(insumos_list)

    def _parse_units(raw: Any) -> int:
        try:
            val = int(float(raw))
            return val if val > 0 else 0
        except Exception:
            return 0

    payload = {
        "nss": m.normalize_nss(form_dict.get("nss")),
        "agregado_medico": (form_dict.get("agregado_medico") or "").strip(),
        "nombre_completo": (form_dict.get("nombre_completo") or "").strip().upper(),
        "edad": m.parse_int(form_dict.get("edad")),
        "sexo": m.normalize_upper(form_dict.get("sexo")),
        "patologia": m.normalize_upper(form_dict.get("patologia")),
        "procedimiento_programado": m.normalize_upper(form_dict.get("procedimiento_programado")),
        "insumos_solicitados": insumos_solicitados,
        "hgz": (form_dict.get("hgz") or "").strip().upper(),
        "estatus": m.normalize_upper(form_dict.get("estatus")) or "PENDIENTE",
        "tnm": (form_dict.get("tnm") or "").strip().upper(),
        "ecog_onco": (form_dict.get("ecog_onco") or "").strip().upper(),
        "ecog_incierto": (form_dict.get("ecog_incierto") or "").strip().upper(),
        "charlson": (form_dict.get("charlson") or "").strip().upper(),
        "etapa_clinica": (form_dict.get("etapa_clinica") or "").strip().upper(),
        "ipss": (form_dict.get("ipss") or "").strip().upper(),
        "gleason": (form_dict.get("gleason") or "").strip().upper(),
        "ape": (form_dict.get("ape") or "").strip().upper(),
        "rtup_previa": m.normalize_upper(form_dict.get("rtup_previa")),
        "tacto_rectal": (form_dict.get("tacto_rectal") or "").strip().upper(),
        "historial_ape": (form_dict.get("historial_ape") or "").strip().upper(),
        "uh_rango": (form_dict.get("uh_rango") or "").strip().upper(),
        "litiasis_tamano_rango": (form_dict.get("litiasis_tamano_rango") or "").strip().upper(),
        "litiasis_subtipo_20": (form_dict.get("litiasis_subtipo_20") or "").strip().upper(),
        "litiasis_ubicacion": (form_dict.get("litiasis_ubicacion") or "").strip().upper(),
        "litiasis_ubicacion_multiple": (form_dict.get("litiasis_ubicacion_multiple") or "").strip().upper(),
        "hidronefrosis": m.normalize_upper(form_dict.get("hidronefrosis")),
        "tipo_neovejiga": (form_dict.get("tipo_neovejiga") or "").strip().upper(),
        "sistema_succion": m.normalize_upper(form_dict.get("sistema_succion")),
        "abordaje": m.normalize_upper(form_dict.get("abordaje")),
        "solicita_hemoderivados": m.normalize_upper(form_dict.get("solicita_hemoderivados")) or "NO",
        "hemoderivados_pg_solicitados": _parse_units(form_dict.get("hemoderivados_pg_solicitados")),
        "hemoderivados_pfc_solicitados": _parse_units(form_dict.get("hemoderivados_pfc_solicitados")),
        "hemoderivados_cp_solicitados": _parse_units(form_dict.get("hemoderivados_cp_solicitados")),
    }

    ready, missing_fields = m.is_required_form_complete(payload)
    if not ready:
        faltantes = ", ".join(missing_fields)
        return HTMLResponse(
            content=f"<h1>Campos obligatorios faltantes</h1><p>{faltantes}</p><a href='/quirofano/nuevo'>Volver</a>",
            status_code=400,
        )
    payload["estatus"] = "PROGRAMADA"
    if payload["solicita_hemoderivados"] not in {"SI", "NO"}:
        payload["solicita_hemoderivados"] = "NO"
    if payload["solicita_hemoderivados"] == "SI":
        total_hemoderivados = (
            int(payload["hemoderivados_pg_solicitados"])
            + int(payload["hemoderivados_pfc_solicitados"])
            + int(payload["hemoderivados_cp_solicitados"])
        )
        if total_hemoderivados <= 0:
            return HTMLResponse(
                content="<h1>Si solicitas hemoderivados debes indicar al menos una unidad</h1><a href='/quirofano/nuevo'>Volver</a>",
                status_code=400,
            )
    else:
        payload["hemoderivados_pg_solicitados"] = 0
        payload["hemoderivados_pfc_solicitados"] = 0
        payload["hemoderivados_cp_solicitados"] = 0

    if len(payload["nss"]) != 10:
        return HTMLResponse(content="<h1>NSS inválido: debe contener 10 dígitos</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["sexo"] not in m.QUIROFANO_SEXOS:
        return HTMLResponse(content="<h1>Sexo inválido</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)
    if payload["patologia"] not in m.QUIROFANO_PATOLOGIAS:
        return HTMLResponse(content="<h1>Patología inválida</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)
    if payload["procedimiento_programado"] not in m.QUIROFANO_PROCEDIMIENTOS:
        return HTMLResponse(content="<h1>Procedimiento inválido</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["patologia"] in m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS:
        required_onco = ["tnm", "ecog_onco", "charlson", "etapa_clinica"]
        if any(not str(payload.get(k) or "").strip() for k in required_onco):
            return HTMLResponse(content="<h1>Complete TNM, ECOG, Charlson y etapa clínica para patología oncológica</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)
        if payload["patologia"] == "CANCER DE PROSTATA":
            for key_name, label in [("ipss", "IPSS"), ("gleason", "Gleason"), ("ape", "APE"), ("rtup_previa", "RTUP previa")]:
                if not str(payload.get(key_name) or "").strip():
                    return HTMLResponse(content=f"<h1>Falta {label} para cáncer de próstata</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["patologia"] == "TUMOR DE COMPORTAMIENTO INCIERTO PROSTATA":
        for key_name, label in [("tacto_rectal", "tacto rectal"), ("historial_ape", "historial de APE"), ("ecog_incierto", "ECOG")]:
            if not str(payload.get(key_name) or "").strip():
                return HTMLResponse(content=f"<h1>Falta {label} en tumor de comportamiento incierto de próstata</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["patologia"] == "CALCULO DEL RIÑON":
        for key_name, label in [("uh_rango", "unidades Hounsfield"), ("litiasis_tamano_rango", "tamaño"), ("litiasis_ubicacion", "ubicación"), ("hidronefrosis", "hidronefrosis")]:
            if not str(payload.get(key_name) or "").strip():
                return HTMLResponse(content=f"<h1>Falta {label} para cálculo del riñón</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)
        if payload["litiasis_tamano_rango"] == "> 20 MM" and not str(payload.get("litiasis_subtipo_20") or "").strip():
            return HTMLResponse(content="<h1>Seleccione subtipo para litiasis mayor a 20 mm</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)
        if payload["litiasis_ubicacion"] == "LITIASIS CALICIAL MULTIPLE" and not str(payload.get("litiasis_ubicacion_multiple") or "").strip():
            return HTMLResponse(content="<h1>Especifique ubicaciones en litiasis calicial múltiple</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    ecog_final = payload["ecog_onco"] or payload["ecog_incierto"]
    if payload["patologia"] in m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS and not ecog_final:
        return HTMLResponse(content="<h1>Falta ECOG para patología oncológica</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["procedimiento_programado"] == m.QUIROFANO_PROCEDIMIENTO_SUCCION and payload["sistema_succion"] not in {"FANS", "DISS"}:
        return HTMLResponse(content="<h1>Seleccione sistema de succión (FANS o DISS)</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["procedimiento_programado"] in m.QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE and payload["abordaje"] not in {"ABIERTO", "LAPAROSCOPICO", "ABIERTO + LAPAROSCOPICO"}:
        return HTMLResponse(content="<h1>Seleccione abordaje (Abierto/Laparoscópico/Ambos)</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    if payload["procedimiento_programado"] in m.QUIROFANO_PROCEDIMIENTOS_ABIERTOS:
        payload["abordaje"] = "ABIERTO"

    grupo_patologia = m.classify_pathology_group(payload["patologia"])
    grupo_procedimiento = m.classify_procedure_group(payload["procedimiento_programado"], payload["abordaje"], payload["sistema_succion"])
    requiere_intermed = "SI" if any("INTERMED" in item.upper() for item in insumos_list) else "NO"
    cie11_codigo = m.get_cie11_from_patologia(payload["patologia"])
    snomed_codigo = m.get_snomed_from_patologia(payload["patologia"])
    cie9mc_codigo = m.get_cie9mc_from_procedimiento(payload["procedimiento_programado"])

    notas_extendidas = (form_dict.get("notas") or "").strip()
    if payload["patologia"]:
        notas_extendidas = f"[PATOLOGIA] {payload['patologia']}\n{notas_extendidas}".strip()

    nueva_cirugia = m.QuirofanoDB(
        consulta_id=consulta_id,
        procedimiento=payload["procedimiento_programado"],
        fecha_programada=fecha_programada,
        cirujano=(form_dict.get("cirujano") or "").strip(),
        anestesiologo=(form_dict.get("anestesiologo") or "").strip(),
        quirofano=(form_dict.get("quirofano") or "").strip(),
        notas=notas_extendidas,
        estatus=payload["estatus"],
    )

    surgical_programacion_id: Optional[int] = None
    try:
        db.add(nueva_cirugia)
        db.commit()
        db.refresh(nueva_cirugia)

        m.sync_quirofano_to_surgical_db(
            consulta,
            nueva_cirugia,
            extra_fields={
                "nss": payload["nss"],
                "agregado_medico": payload["agregado_medico"],
                "paciente_nombre": payload["nombre_completo"],
                "edad": payload["edad"],
                "edad_grupo": m.classify_age_group(payload["edad"]),
                "sexo": payload["sexo"],
                "grupo_sexo": payload["sexo"],
                "patologia": payload["patologia"],
                "diagnostico_principal": payload["patologia"],
                "grupo_patologia": grupo_patologia,
                "procedimiento_programado": payload["procedimiento_programado"],
                "grupo_procedimiento": grupo_procedimiento,
                "abordaje": payload["abordaje"],
                "tipo_neovejiga": payload["tipo_neovejiga"],
                "sistema_succion": payload["sistema_succion"],
                "insumos_solicitados": payload["insumos_solicitados"],
                "requiere_intermed": requiere_intermed,
                "solicita_hemoderivados": payload["solicita_hemoderivados"],
                "hemoderivados_pg_solicitados": payload["hemoderivados_pg_solicitados"],
                "hemoderivados_pfc_solicitados": payload["hemoderivados_pfc_solicitados"],
                "hemoderivados_cp_solicitados": payload["hemoderivados_cp_solicitados"],
                "hgz": payload["hgz"],
                "cie11_codigo": cie11_codigo,
                "snomed_codigo": snomed_codigo,
                "cie9mc_codigo": cie9mc_codigo,
                "tnm": payload["tnm"],
                "ecog": ecog_final,
                "charlson": payload["charlson"],
                "etapa_clinica": payload["etapa_clinica"],
                "ipss": payload["ipss"],
                "gleason": payload["gleason"],
                "ape": payload["ape"],
                "rtup_previa": payload["rtup_previa"],
                "tacto_rectal": payload["tacto_rectal"],
                "historial_ape": payload["historial_ape"],
                "uh_rango": payload["uh_rango"],
                "litiasis_tamano_rango": payload["litiasis_tamano_rango"],
                "litiasis_subtipo_20": payload["litiasis_subtipo_20"],
                "litiasis_ubicacion": payload["litiasis_ubicacion"],
                "litiasis_ubicacion_multiple": payload["litiasis_ubicacion_multiple"],
                "hidronefrosis": payload["hidronefrosis"],
                "estatus": payload["estatus"],
                "protocolo_completo": "SI" if (consulta.estatus_protocolo or "").lower() == "completo" else "NO",
                "pendiente_programar": "NO",
            },
        )
        sdb_link = m._new_surgical_session(enable_dual_write=True)
        try:
            linked = (
                sdb_link.query(m.SurgicalProgramacionDB)
                .filter(m.SurgicalProgramacionDB.quirofano_id == nueva_cirugia.id)
                .order_by(m.SurgicalProgramacionDB.id.desc())
                .first()
            )
            if linked is not None and linked.id is not None:
                surgical_programacion_id = int(linked.id)
        finally:
            sdb_link.close()

        m.push_module_feedback(
            consulta_id=consulta_id,
            modulo="programar_cirugia",
            referencia_id=f"quirofano:{nueva_cirugia.id}",
            payload={
                "nss": payload["nss"],
                "sexo": payload["sexo"],
                "patologia": payload["patologia"],
                "procedimiento_programado": payload["procedimiento_programado"],
                "grupo_patologia": grupo_patologia,
                "grupo_procedimiento": grupo_procedimiento,
                "hgz": payload["hgz"],
                "requiere_intermed": requiere_intermed,
                "solicita_hemoderivados": payload["solicita_hemoderivados"],
                "hemoderivados_pg_solicitados": payload["hemoderivados_pg_solicitados"],
                "hemoderivados_pfc_solicitados": payload["hemoderivados_pfc_solicitados"],
                "hemoderivados_cp_solicitados": payload["hemoderivados_cp_solicitados"],
                "estatus": payload["estatus"],
                "cie11_codigo": cie11_codigo,
                "snomed_codigo": snomed_codigo,
                "cie9mc_codigo": cie9mc_codigo,
            },
        )
        try:
            from app.services.master_identity_flow import upsert_master_identity

            upsert_master_identity(
                db,
                nss=payload["nss"],
                curp=consulta.curp,
                nombre=payload["nombre_completo"],
                sexo=payload["sexo"],
                consulta_id=consulta_id,
                source_table="quirofano",
                source_pk=nueva_cirugia.id,
                module="quirofano_programada",
                fecha_evento=fecha_programada,
                payload={
                    "patologia": payload["patologia"],
                    "procedimiento_programado": payload["procedimiento_programado"],
                    "hgz": payload["hgz"],
                    "estatus": payload["estatus"],
                },
                commit=True,
            )
        except Exception:
            db.rollback()

        sdb_audit = m._new_surgical_session(enable_dual_write=True)
        try:
            m.registrar_auditoria(
                sdb=sdb_audit,
                tabla="surgical_programaciones",
                registro_id=nueva_cirugia.id,
                operacion="INSERT",
                usuario=request.headers.get("X-User", "system"),
                datos_anteriores=None,
                datos_nuevos={
                    "consulta_id": consulta_id,
                    "nss": payload["nss"],
                    "paciente_nombre": payload["nombre_completo"],
                    "patologia": payload["patologia"],
                    "procedimiento_programado": payload["procedimiento_programado"],
                    "estatus": payload["estatus"],
                },
            )
        finally:
            sdb_audit.close()

        if m.celery_app is not None:
            try:
                m.async_actualizar_data_mart_task.delay()
            except Exception:
                pass
    except Exception:
        db.rollback()
        return HTMLResponse(content="<h1>Error al guardar quirófano</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=500)

    postqx_href = (
        f"/quirofano/programada/{surgical_programacion_id}/postquirurgica"
        if surgical_programacion_id
        else "/quirofano/programada/postquirurgica"
    )
    expediente_href = f"/expediente?consulta_id={consulta_id}"
    return HTMLResponse(
        content=(
            "<h1>Cirugía programada exitosamente</h1>"
            f"<p><a href='{postqx_href}'>🩺 Completar nota postquirúrgica de este paciente</a></p>"
            "<p><a href='/quirofano/programada/lista'>📋 Ver lista de pacientes programados</a></p>"
            f"<p><a href='{expediente_href}'>📁 Ver expediente clínico único del paciente</a></p>"
        )
    )


async def cancelar_programacion_flow(
    request: Request,
    db: Session,
    sdb: Session,
    *,
    urgencias_only: bool = False,
    back_href: str = "/quirofano/programada/lista",
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)

    surgical_programacion_id = m.parse_int(
        form_dict.get("surgical_programacion_id")
        or form_dict.get("programacion_id")
        or form_dict.get("id")
    )
    urgencia_programacion_id = m.parse_int(form_dict.get("urgencia_programacion_id"))
    cancelacion_item = _resolve_cancelacion_concepto(
        form_dict.get("cancelacion_codigo") or form_dict.get("cancelacion_concepto")
    )
    cancelacion_detalle = (form_dict.get("cancelacion_detalle") or "").strip().upper() or None

    if cancelacion_item is None:
        return HTMLResponse(
            content=(
                "<h1>Debe seleccionar un concepto de cancelación válido.</h1>"
                f"<p><a href='{back_href}'>← Volver</a></p>"
            ),
            status_code=400,
        )

    if not surgical_programacion_id and urgencia_programacion_id:
        urg_lookup = (
            sdb.query(m.SurgicalUrgenciaProgramacionDB)
            .filter(m.SurgicalUrgenciaProgramacionDB.id == urgencia_programacion_id)
            .first()
        )
        if urg_lookup is not None and urg_lookup.surgical_programacion_id is not None:
            surgical_programacion_id = int(urg_lookup.surgical_programacion_id)

    if not surgical_programacion_id:
        return HTMLResponse(
            content=(
                "<h1>No fue posible identificar la programación quirúrgica a cancelar.</h1>"
                f"<p><a href='{back_href}'>← Volver</a></p>"
            ),
            status_code=400,
        )

    programacion_query = (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.id == surgical_programacion_id)
    )
    if urgencias_only:
        programacion_query = programacion_query.filter(
            m.SurgicalProgramacionDB.modulo_origen == "QUIROFANO_URGENCIA"
        )
    else:
        programacion_query = programacion_query.filter(
            (m.SurgicalProgramacionDB.modulo_origen.is_(None))
            | (m.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA")
        )
    row = programacion_query.first()
    if row is None:
        return HTMLResponse(
            content=(
                "<h1>Programación quirúrgica no encontrada.</h1>"
                f"<p><a href='{back_href}'>← Volver</a></p>"
            ),
            status_code=404,
        )

    if str(row.estatus or "").upper() == "REALIZADA":
        return HTMLResponse(
            content=(
                "<h1>No se puede cancelar una cirugía ya realizada.</h1>"
                f"<p><a href='{back_href}'>← Volver</a></p>"
            ),
            status_code=409,
        )

    now_dt = utcnow()
    usuario = (request.headers.get("X-User") or "system").strip() or "system"

    try:
        row.estatus = "CANCELADA"
        row.cancelacion_codigo = cancelacion_item["codigo"]
        row.cancelacion_categoria = cancelacion_item["categoria"]
        row.cancelacion_concepto = cancelacion_item["concepto"]
        row.cancelacion_detalle = cancelacion_detalle
        row.cancelacion_fecha = now_dt
        row.cancelacion_usuario = usuario

        urg_row = None
        urg_id = row.urgencia_programacion_id or urgencia_programacion_id
        if urg_id:
            urg_row = (
                sdb.query(m.SurgicalUrgenciaProgramacionDB)
                .filter(m.SurgicalUrgenciaProgramacionDB.id == int(urg_id))
                .first()
            )
        if urg_row is not None:
            urg_row.estatus = "CANCELADA"
            urg_row.cancelacion_codigo = cancelacion_item["codigo"]
            urg_row.cancelacion_categoria = cancelacion_item["categoria"]
            urg_row.cancelacion_concepto = cancelacion_item["concepto"]
            urg_row.cancelacion_detalle = cancelacion_detalle
            urg_row.cancelacion_fecha = now_dt
            urg_row.cancelacion_usuario = usuario
            if row.urgencia_programacion_id is None:
                row.urgencia_programacion_id = urg_row.id

        sdb.commit()
    except Exception:
        sdb.rollback()
        return HTMLResponse(
            content=(
                "<h1>No fue posible marcar la cirugía como cancelada.</h1>"
                f"<p><a href='{back_href}'>← Volver</a></p>"
            ),
            status_code=500,
        )

    try:
        core_row = None
        if getattr(row, "quirofano_id", None) and int(row.quirofano_id or 0) > 0:
            core_row = db.query(m.QuirofanoDB).filter(m.QuirofanoDB.id == int(row.quirofano_id)).first()
        if core_row is not None:
            core_row.estatus = "CANCELADA"
            extra = [
                "CANCELADA",
                f"Motivo: {cancelacion_item['codigo']} - {cancelacion_item['concepto']}",
                f"Categoría: {cancelacion_item['categoria']}",
            ]
            if cancelacion_detalle:
                extra.append(f"Detalle: {cancelacion_detalle}")
            core_row.notas = ((core_row.notas or "").strip() + ("\n" if core_row.notas else "") + " | ".join(extra)).strip()
            db.commit()
    except Exception:
        db.rollback()

    try:
        m.push_module_feedback(
            consulta_id=row.consulta_id,
            modulo="quirofano_cancelacion",
            referencia_id=f"cancelacion:{row.id}",
            payload={
                "surgical_programacion_id": row.id,
                "quirofano_id": row.quirofano_id,
                "estatus": "CANCELADA",
                "cancelacion_codigo": cancelacion_item["codigo"],
                "cancelacion_categoria": cancelacion_item["categoria"],
                "cancelacion_concepto": cancelacion_item["concepto"],
                "cancelacion_detalle": cancelacion_detalle,
                "cancelacion_fecha": now_dt.isoformat(),
                "usuario": usuario,
            },
        )
    except Exception:
        pass

    try:
        m.registrar_evento_flujo_quirurgico(
            consulta_id=row.consulta_id,
            evento="CANCELADA",
            estatus="CANCELADA",
            surgical_programacion_id=row.id,
            quirofano_id=row.quirofano_id,
            edad=row.edad,
            sexo=row.sexo,
            nss=row.nss,
            hgz=row.hgz,
            diagnostico=row.patologia or row.diagnostico_principal,
            procedimiento=row.procedimiento_programado or row.procedimiento,
            ecog=row.ecog,
            cirujano=row.cirujano or row.agregado_medico,
            metadata_json={
                "cancelacion_codigo": cancelacion_item["codigo"],
                "cancelacion_categoria": cancelacion_item["categoria"],
                "cancelacion_concepto": cancelacion_item["concepto"],
                "cancelacion_detalle": cancelacion_detalle,
                "cancelacion_usuario": usuario,
                "cancelacion_fecha": now_dt.isoformat(),
            },
        )
    except Exception:
        pass

    try:
        emit_event(
            db,
            module="quirofano",
            event_type="PROGRAMACION_CANCELADA",
            entity="surgical_programaciones",
            entity_id=str(int(row.id)),
            consulta_id=int(row.consulta_id) if row.consulta_id is not None else None,
            actor=usuario,
            source_route=request.url.path,
            payload={
                "cancelacion_codigo": cancelacion_item["codigo"],
                "cancelacion_categoria": cancelacion_item["categoria"],
                "cancelacion_concepto": cancelacion_item["concepto"],
                "cancelacion_detalle": cancelacion_detalle,
                "urgencias_only": bool(urgencias_only),
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    try:
        from app.services.master_identity_flow import upsert_master_identity

        upsert_master_identity(
            db,
            nss=row.nss,
            curp=row.curp,
            nombre=row.paciente_nombre,
            sexo=row.sexo,
            consulta_id=row.consulta_id,
            source_table="surgical_programaciones",
            source_pk=row.id,
            module="quirofano_cancelacion",
            fecha_evento=now_dt.date(),
            payload={
                "estatus": "CANCELADA",
                "cancelacion_codigo": cancelacion_item["codigo"],
                "cancelacion_categoria": cancelacion_item["categoria"],
                "cancelacion_concepto": cancelacion_item["concepto"],
                "cancelacion_detalle": cancelacion_detalle,
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    if m.celery_app is not None:
        try:
            m.async_actualizar_data_mart_task.delay()
        except Exception:
            pass

    concepto_title = quote_plus(cancelacion_item["concepto"])
    return HTMLResponse(
        content=(
            "<h1>Solicitud quirúrgica marcada como CANCELADA</h1>"
            f"<p><strong>Concepto:</strong> {cancelacion_item['codigo']} - {cancelacion_item['concepto']}</p>"
            f"<p><a href='{back_href}'>📋 Volver a la lista</a></p>"
            f"<p><a href='/reporte#seccion-cancelaciones-concepto'>📊 Ver estadística de cancelaciones por concepto</a></p>"
            f"<p><a href='/api/stats/quirofano/cancelaciones/resumen?concepto={concepto_title}'>🔎 Ver resumen JSON de este concepto</a></p>"
        )
    )


def _build_postquirurgica_context(
    sdb: Session,
    selected_programacion_id: Optional[int] = None,
    *,
    urgencias_only: bool = False,
) -> Dict[str, Any]:
    from app.core.app_context import main_proxy as m

    query = sdb.query(m.SurgicalProgramacionDB).filter(m.SurgicalProgramacionDB.estatus == "PROGRAMADA")
    if urgencias_only:
        query = query.filter(m.SurgicalProgramacionDB.modulo_origen == "QUIROFANO_URGENCIA")
    else:
        query = query.filter(
            (m.SurgicalProgramacionDB.modulo_origen.is_(None))
            | (m.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA")
        )
    programadas = query.order_by(m.SurgicalProgramacionDB.fecha_programada.desc(), m.SurgicalProgramacionDB.id.desc()).limit(800).all()
    selected = None
    if selected_programacion_id:
        selected_query = sdb.query(m.SurgicalProgramacionDB).filter(m.SurgicalProgramacionDB.id == selected_programacion_id)
        if urgencias_only:
            selected_query = selected_query.filter(m.SurgicalProgramacionDB.modulo_origen == "QUIROFANO_URGENCIA")
        selected = selected_query.first()
    if selected is None and programadas:
        selected = programadas[0]

    return {
        "programadas": programadas,
        "selected": selected,
    }


async def render_postquirurgica_flow(
    request: Request,
    db: Session,
    sdb: Session,
    *,
    selected_programacion_id: Optional[int] = None,
    message: Optional[str] = None,
    error: Optional[str] = None,
    urgencias_only: bool = False,
    form_action: str = "/quirofano/programada/postquirurgica",
    back_href: str = "/quirofano/programada",
    titulo: str = "🩺 Nota Postquirúrgica",
    next_links: Optional[Dict[str, str]] = None,
):
    from app.core.app_context import main_proxy as m

    context = _build_postquirurgica_context(
        sdb,
        selected_programacion_id=selected_programacion_id,
        urgencias_only=urgencias_only,
    )
    return m.render_template(
        "quirofano_postquirurgica.html",
        request=request,
        programadas=context["programadas"],
        selected=context["selected"],
        approach_options=APPROACH_OPTIONS,
        role_options=ROLE_OPTIONS,
        participation_options=PARTICIPATION_OPTIONS,
        resident_catalog=load_resident_catalog(),
        message=message,
        error=error,
        form_action=form_action,
        back_href=back_href,
        titulo=titulo,
        urgencias_only=urgencias_only,
        next_links=next_links or {},
    )


async def guardar_postquirurgica_flow(
    request: Request,
    db: Session,
    sdb: Session,
    *,
    urgencias_only: bool = False,
    form_action: str = "/quirofano/programada/postquirurgica",
    back_href: str = "/quirofano/programada",
    titulo: str = "🩺 Nota Postquirúrgica",
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    m.validate_csrf(form_dict, request)

    async def _render(
        *,
        selected_programacion_id: Optional[int] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
        next_links: Optional[Dict[str, str]] = None,
    ):
        return await render_postquirurgica_flow(
            request,
            db,
            sdb,
            selected_programacion_id=selected_programacion_id,
            message=message,
            error=error,
            urgencias_only=urgencias_only,
            form_action=form_action,
            back_href=back_href,
            titulo=titulo,
            next_links=next_links,
        )

    programacion_id = m.parse_int(form_dict.get("surgical_programacion_id"))
    if not programacion_id:
        return await _render(message=None, error="Debes seleccionar una cirugía programada.")

    row_query = sdb.query(m.SurgicalProgramacionDB).filter(m.SurgicalProgramacionDB.id == programacion_id)
    if urgencias_only:
        row_query = row_query.filter(m.SurgicalProgramacionDB.modulo_origen == "QUIROFANO_URGENCIA")
    row = row_query.first()
    if row is None:
        return await _render(message=None, error="La programación quirúrgica seleccionada no existe.")

    fecha_realizacion_raw = (form_dict.get("fecha_realizacion") or "").strip()
    try:
        fecha_realizacion = datetime.strptime(fecha_realizacion_raw, "%Y-%m-%d").date() if fecha_realizacion_raw else date.today()
    except Exception:
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Fecha de realización inválida. Usa formato YYYY-MM-DD.",
        )

    sangrado_raw = (form_dict.get("sangrado_ml") or "").strip()
    sangrado_ml: Optional[float] = None
    if sangrado_raw:
        try:
            sangrado_ml = float(sangrado_raw)
            if sangrado_ml < 0:
                raise ValueError("negativo")
        except Exception:
            return await _render(
                selected_programacion_id=programacion_id,
                message=None,
                error="Sangrado (mL) inválido. Debe ser numérico no negativo.",
            )

    tiempo_raw = (form_dict.get("tiempo_quirurgico_min") or "").strip()
    tiempo_quirurgico_min: Optional[float] = None
    if tiempo_raw:
        try:
            tiempo_quirurgico_min = float(tiempo_raw)
            if tiempo_quirurgico_min < 0:
                raise ValueError("negativo")
        except Exception:
            return await _render(
                selected_programacion_id=programacion_id,
                message=None,
                error="Tiempo quirúrgico inválido. Debe ser numérico no negativo.",
            )

    def _yn(raw: Any, *, default: str = "NO") -> str:
        value = (str(raw or "").strip().upper() or default).upper()
        return "SI" if value == "SI" else "NO"

    def _normalized_text(raw: Any) -> Optional[str]:
        txt = (str(raw or "").strip().upper())
        return txt or None

    def _parse_units(raw: Any) -> int:
        try:
            val = int(float(raw))
            return val if val > 0 else 0
        except Exception:
            return 0

    cirujano = (form_dict.get("cirujano") or row.cirujano or "").strip().upper() or "NO_REGISTRADO"
    tipo_abordaje = (form_dict.get("tipo_abordaje") or row.abordaje or "").strip().upper()
    if tipo_abordaje and tipo_abordaje not in APPROACH_OPTIONS:
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Tipo de abordaje inválido.",
        )
    resident_team = parse_resident_team(form_dict)
    procedimiento_realizado = (form_dict.get("procedimiento_realizado") or row.procedimiento_programado or row.procedimiento or "").strip().upper()
    diagnostico_postop = (form_dict.get("diagnostico_postop") or row.patologia or row.diagnostico_principal or "").strip().upper()
    complicaciones = (form_dict.get("complicaciones") or "").strip()
    nota_postquirurgica = (form_dict.get("nota_postquirurgica") or "").strip()
    transfusion = _yn(form_dict.get("transfusion"), default="NO")
    uso_hemoderivados = _yn(form_dict.get("uso_hemoderivados"), default=transfusion)
    hemoderivados_pg_utilizados = _parse_units(form_dict.get("hemoderivados_pg_utilizados"))
    hemoderivados_pfc_utilizados = _parse_units(form_dict.get("hemoderivados_pfc_utilizados"))
    hemoderivados_cp_utilizados = _parse_units(form_dict.get("hemoderivados_cp_utilizados"))
    antibiotico = _normalized_text(form_dict.get("antibiotico"))
    clavien_dindo = _normalized_text(form_dict.get("clavien_dindo"))
    margen_quirurgico = _normalized_text(form_dict.get("margen_quirurgico"))
    neuropreservacion = _normalized_text(form_dict.get("neuropreservacion"))
    linfadenectomia = _normalized_text(form_dict.get("linfadenectomia"))
    reingreso_30d = _yn(form_dict.get("reingreso_30d"), default="NO")
    reintervencion_30d = _yn(form_dict.get("reintervencion_30d"), default="NO")
    mortalidad_30d = _yn(form_dict.get("mortalidad_30d"), default="NO")
    reingreso_90d = _yn(form_dict.get("reingreso_90d"), default="NO")
    reintervencion_90d = _yn(form_dict.get("reintervencion_90d"), default="NO")
    mortalidad_90d = _yn(form_dict.get("mortalidad_90d"), default="NO")
    stone_free = _normalized_text(form_dict.get("stone_free"))
    composicion_lito = _normalized_text(form_dict.get("composicion_lito"))
    recurrencia_litiasis = _yn(form_dict.get("recurrencia_litiasis"), default="NO")
    cateter_jj_colocado = _yn(form_dict.get("cateter_jj_colocado"), default="NO")
    fecha_colocacion_jj_raw = (form_dict.get("fecha_colocacion_jj") or "").strip()
    fecha_colocacion_jj = None
    if fecha_colocacion_jj_raw:
        try:
            fecha_colocacion_jj = datetime.strptime(fecha_colocacion_jj_raw, "%Y-%m-%d").date()
        except Exception:
            return await _render(
                selected_programacion_id=programacion_id,
                message=None,
                error="Fecha de colocación de catéter JJ inválida. Usa formato YYYY-MM-DD.",
            )

    if uso_hemoderivados == "SI":
        if (hemoderivados_pg_utilizados + hemoderivados_pfc_utilizados + hemoderivados_cp_utilizados) <= 0:
            return await _render(
                selected_programacion_id=programacion_id,
                message=None,
                error="Si hubo uso de hemoderivados, indica al menos una unidad.",
            )
    else:
        hemoderivados_pg_utilizados = 0
        hemoderivados_pfc_utilizados = 0
        hemoderivados_cp_utilizados = 0

    patologia = m.normalize_upper(row.patologia or row.diagnostico_principal)
    is_onco = (row.grupo_patologia or "").upper() == "ONCOLOGICO" or patologia in m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS
    is_prostata = patologia == "CANCER DE PROSTATA"
    is_litiasis = (row.grupo_patologia or "").upper() == "LITIASIS_URINARIA" or patologia in m.QUIROFANO_PATOLOGIAS_LITIASIS

    if is_onco and not margen_quirurgico:
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Para pacientes oncológicos se requiere margen quirúrgico.",
        )
    if is_prostata and (not neuropreservacion or not linfadenectomia):
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Para cáncer de próstata se requiere capturar neuropreservación y linfadenectomía.",
        )
    if is_litiasis and not stone_free:
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Para litiasis se requiere capturar stone-free.",
        )
    if is_litiasis and cateter_jj_colocado == "SI" and fecha_colocacion_jj is None:
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="Si se colocó catéter JJ, debes registrar la fecha de colocación.",
        )

    try:
        row.fecha_realizacion = fecha_realizacion
        row.fecha_postquirurgica = fecha_realizacion
        row.estatus = "REALIZADA"
        row.cirujano = cirujano
        row.abordaje = tipo_abordaje or row.abordaje
        row.sangrado_ml = sangrado_ml
        row.tiempo_quirurgico_min = tiempo_quirurgico_min
        row.transfusion = transfusion
        row.uso_hemoderivados = uso_hemoderivados
        row.hemoderivados_pg_utilizados = hemoderivados_pg_utilizados
        row.hemoderivados_pfc_utilizados = hemoderivados_pfc_utilizados
        row.hemoderivados_cp_utilizados = hemoderivados_cp_utilizados
        row.antibiotico = antibiotico
        row.clavien_dindo = clavien_dindo
        row.margen_quirurgico = margen_quirurgico
        row.neuropreservacion = neuropreservacion
        row.linfadenectomia = linfadenectomia
        row.reingreso_30d = reingreso_30d
        row.reintervencion_30d = reintervencion_30d
        row.mortalidad_30d = mortalidad_30d
        row.reingreso_90d = reingreso_90d
        row.reintervencion_90d = reintervencion_90d
        row.mortalidad_90d = mortalidad_90d
        row.stone_free = stone_free
        row.composicion_lito = composicion_lito
        row.recurrencia_litiasis = recurrencia_litiasis
        row.cateter_jj_colocado = cateter_jj_colocado
        row.fecha_colocacion_jj = fecha_colocacion_jj
        row.diagnostico_postop = diagnostico_postop or row.diagnostico_postop
        row.procedimiento_realizado = procedimiento_realizado or row.procedimiento_realizado
        row.complicaciones_postquirurgicas = complicaciones
        row.nota_postquirurgica = nota_postquirurgica

        postqx_row = m.SurgicalPostquirurgicaDB(
            surgical_programacion_id=row.id,
            quirofano_id=row.quirofano_id,
            consulta_id=row.consulta_id,
            fecha_realizacion=fecha_realizacion,
            cirujano=cirujano,
            tipo_abordaje=tipo_abordaje or None,
            sangrado_ml=sangrado_ml,
            tiempo_quirurgico_min=tiempo_quirurgico_min,
            transfusion=transfusion,
            uso_hemoderivados=uso_hemoderivados,
            hemoderivados_pg_utilizados=hemoderivados_pg_utilizados,
            hemoderivados_pfc_utilizados=hemoderivados_pfc_utilizados,
            hemoderivados_cp_utilizados=hemoderivados_cp_utilizados,
            antibiotico=antibiotico,
            clavien_dindo=clavien_dindo,
            margen_quirurgico=margen_quirurgico,
            neuropreservacion=neuropreservacion,
            linfadenectomia=linfadenectomia,
            reingreso_30d=reingreso_30d,
            reintervencion_30d=reintervencion_30d,
            mortalidad_30d=mortalidad_30d,
            reingreso_90d=reingreso_90d,
            reintervencion_90d=reintervencion_90d,
            mortalidad_90d=mortalidad_90d,
            stone_free=stone_free,
            composicion_lito=composicion_lito,
            recurrencia_litiasis=recurrencia_litiasis,
            cateter_jj_colocado=cateter_jj_colocado,
            fecha_colocacion_jj=fecha_colocacion_jj,
            diagnostico_postop=diagnostico_postop or None,
            procedimiento_realizado=procedimiento_realizado or None,
            complicaciones=complicaciones or None,
            nota_postquirurgica=nota_postquirurgica or None,
        )
        sdb.add(postqx_row)
        sdb.commit()
        sdb.refresh(postqx_row)
    except Exception:
        sdb.rollback()
        return await _render(
            selected_programacion_id=programacion_id,
            message=None,
            error="No fue posible guardar la nota postquirúrgica.",
        )

    # Sincronizar tabla histórica de quirófano principal sin romper flujo existente.
    try:
        core_row = db.query(m.QuirofanoDB).filter(m.QuirofanoDB.id == row.quirofano_id).first()
        if core_row:
            core_row.estatus = "REALIZADA"
            core_row.fecha_realizacion = fecha_realizacion
            core_row.cirujano = cirujano
            nota_extra = []
            if sangrado_ml is not None:
                nota_extra.append(f"Sangrado: {sangrado_ml} mL")
            if tiempo_quirurgico_min is not None:
                nota_extra.append(f"Tiempo quirúrgico: {tiempo_quirurgico_min} min")
            nota_extra.append(f"Transfusión: {transfusion}")
            nota_extra.append(f"Uso hemoderivados: {uso_hemoderivados}")
            nota_extra.append(
                f"Hemoderivados usados (PG/PFC/CP): {hemoderivados_pg_utilizados}/{hemoderivados_pfc_utilizados}/{hemoderivados_cp_utilizados}"
            )
            if clavien_dindo:
                nota_extra.append(f"Clavien-Dindo: {clavien_dindo}")
            if antibiotico:
                nota_extra.append(f"Antibiótico: {antibiotico}")
            if margen_quirurgico:
                nota_extra.append(f"Margen quirúrgico: {margen_quirurgico}")
            if neuropreservacion:
                nota_extra.append(f"Neuropreservación: {neuropreservacion}")
            if linfadenectomia:
                nota_extra.append(f"Linfadenectomía: {linfadenectomia}")
            nota_extra.append(f"Reingreso 30d: {reingreso_30d}")
            nota_extra.append(f"Reintervención 30d: {reintervencion_30d}")
            nota_extra.append(f"Mortalidad 30d: {mortalidad_30d}")
            nota_extra.append(f"Reingreso 90d: {reingreso_90d}")
            nota_extra.append(f"Reintervención 90d: {reintervencion_90d}")
            nota_extra.append(f"Mortalidad 90d: {mortalidad_90d}")
            if stone_free:
                nota_extra.append(f"Stone-free: {stone_free}")
            if composicion_lito:
                nota_extra.append(f"Composición de lito: {composicion_lito}")
            nota_extra.append(f"Recurrencia litiasis: {recurrencia_litiasis}")
            nota_extra.append(f"Catéter JJ colocado: {cateter_jj_colocado}")
            if fecha_colocacion_jj:
                nota_extra.append(f"Fecha colocación JJ: {fecha_colocacion_jj.isoformat()}")
            if procedimiento_realizado:
                nota_extra.append(f"Procedimiento realizado: {procedimiento_realizado}")
            if diagnostico_postop:
                nota_extra.append(f"Diagnóstico postquirúrgico: {diagnostico_postop}")
            if complicaciones:
                nota_extra.append(f"Complicaciones: {complicaciones}")
            if nota_postquirurgica:
                nota_extra.append(f"Nota postquirúrgica: {nota_postquirurgica}")
            merge_text = "\n".join(nota_extra).strip()
            if merge_text:
                core_row.notas = ((core_row.notas or "").strip() + ("\n" if core_row.notas else "") + merge_text).strip()
            db.commit()
    except Exception:
        db.rollback()

    # Sincronización aditiva para urgencias: si la cirugía proviene de urgencias, replicar desenlaces.
    try:
        urg_id = getattr(row, "urgencia_programacion_id", None)
        if urg_id:
            urg_row = sdb.query(m.SurgicalUrgenciaProgramacionDB).filter(m.SurgicalUrgenciaProgramacionDB.id == urg_id).first()
            if urg_row is not None:
                urg_row.estatus = "REALIZADA"
                urg_row.fecha_realizacion = fecha_realizacion
                urg_row.cirujano = cirujano
                urg_row.abordaje = tipo_abordaje or urg_row.abordaje
                urg_row.sangrado_ml = sangrado_ml
                urg_row.tiempo_quirurgico_min = tiempo_quirurgico_min
                urg_row.transfusion = transfusion
                urg_row.uso_hemoderivados = uso_hemoderivados
                urg_row.hemoderivados_pg_utilizados = hemoderivados_pg_utilizados
                urg_row.hemoderivados_pfc_utilizados = hemoderivados_pfc_utilizados
                urg_row.hemoderivados_cp_utilizados = hemoderivados_cp_utilizados
                urg_row.stone_free = stone_free
                urg_row.composicion_lito = composicion_lito
                urg_row.recurrencia_litiasis = recurrencia_litiasis
                urg_row.cateter_jj_colocado = cateter_jj_colocado
                urg_row.fecha_colocacion_jj = fecha_colocacion_jj
                urg_row.procedimiento_realizado = procedimiento_realizado or urg_row.procedimiento_realizado
                urg_row.diagnostico_postop = diagnostico_postop or urg_row.diagnostico_postop
                urg_row.complicaciones_postquirurgicas = complicaciones or urg_row.complicaciones_postquirurgicas
                urg_row.nota_postquirurgica = nota_postquirurgica or urg_row.nota_postquirurgica
                sdb.commit()
    except Exception:
        sdb.rollback()

    feedback_payload = {
        "estatus": "REALIZADA",
        "quirofano_id": row.quirofano_id,
        "surgical_programacion_id": row.id,
        "postquirurgica_id": postqx_row.id if postqx_row is not None else None,
        "fecha_realizacion": fecha_realizacion.isoformat() if fecha_realizacion else None,
        "cirujano": cirujano,
        "tipo_abordaje": tipo_abordaje or None,
        "resident_team": resident_team,
        "sangrado_ml": sangrado_ml,
        "sangrado_permisible_ml": None,
        "tiempo_quirurgico_min": tiempo_quirurgico_min,
        "transfusion": transfusion,
        "uso_hemoderivados": uso_hemoderivados,
        "hemoderivados_pg_utilizados": hemoderivados_pg_utilizados,
        "hemoderivados_pfc_utilizados": hemoderivados_pfc_utilizados,
        "hemoderivados_cp_utilizados": hemoderivados_cp_utilizados,
        "antibiotico": antibiotico,
        "clavien_dindo": clavien_dindo,
        "margen_quirurgico": margen_quirurgico,
        "neuropreservacion": neuropreservacion,
        "linfadenectomia": linfadenectomia,
        "reingreso_30d": reingreso_30d,
        "reintervencion_30d": reintervencion_30d,
        "mortalidad_30d": mortalidad_30d,
        "reingreso_90d": reingreso_90d,
        "reintervencion_90d": reintervencion_90d,
        "mortalidad_90d": mortalidad_90d,
        "stone_free": stone_free,
        "composicion_lito": composicion_lito,
        "recurrencia_litiasis": recurrencia_litiasis,
        "cateter_jj_colocado": cateter_jj_colocado,
        "fecha_colocacion_jj": fecha_colocacion_jj.isoformat() if fecha_colocacion_jj else None,
        "diagnostico_postop": diagnostico_postop,
        "procedimiento_realizado": procedimiento_realizado,
        "paciente_nombre": row.paciente_nombre,
        "paciente_nss": row.nss,
        "paciente_edad": row.edad,
        "paciente_sexo": row.sexo,
        "patologia": row.patologia,
        "diagnostico": row.diagnostico_principal or row.patologia,
        "procedimiento_programado": row.procedimiento_programado or row.procedimiento,
        "modulo_origen": row.modulo_origen,
        "urgencia_programacion_id": row.urgencia_programacion_id,
    }

    m.push_module_feedback(
        consulta_id=row.consulta_id,
        modulo="postquirurgica",
        referencia_id=f"postquirurgica:{row.id}",
        payload=feedback_payload,
    )
    try:
        feedback_row = (
            sdb.query(m.SurgicalFeedbackDB)
            .filter(
                m.SurgicalFeedbackDB.consulta_id == row.consulta_id,
                m.SurgicalFeedbackDB.modulo == "postquirurgica",
                m.SurgicalFeedbackDB.referencia_id == f"postquirurgica:{row.id}",
            )
            .order_by(m.SurgicalFeedbackDB.id.desc())
            .first()
        )
        if feedback_row is not None:
            index_postqx_feedback(sdb, feedback_row, payload_override=feedback_payload)
    except Exception:
        pass
    m.registrar_evento_flujo_quirurgico(
        consulta_id=row.consulta_id,
        evento="REALIZADA",
        estatus="REALIZADA",
        surgical_programacion_id=row.id,
        quirofano_id=row.quirofano_id,
        edad=row.edad,
        sexo=row.sexo,
        nss=row.nss,
        hgz=row.hgz,
        diagnostico=row.patologia or row.diagnostico_principal,
        procedimiento=procedimiento_realizado or row.procedimiento_programado or row.procedimiento,
        ecog=row.ecog,
        cirujano=cirujano,
        sangrado_ml=sangrado_ml,
        metadata_json={
            "tiempo_quirurgico_min": tiempo_quirurgico_min,
            "transfusion": transfusion,
            "uso_hemoderivados": uso_hemoderivados,
            "hemoderivados_pg_utilizados": hemoderivados_pg_utilizados,
            "hemoderivados_pfc_utilizados": hemoderivados_pfc_utilizados,
            "hemoderivados_cp_utilizados": hemoderivados_cp_utilizados,
            "antibiotico": antibiotico,
            "clavien_dindo": clavien_dindo,
            "margen_quirurgico": margen_quirurgico,
            "neuropreservacion": neuropreservacion,
            "linfadenectomia": linfadenectomia,
            "reingreso_30d": reingreso_30d,
            "reintervencion_30d": reintervencion_30d,
            "mortalidad_30d": mortalidad_30d,
            "reingreso_90d": reingreso_90d,
            "reintervencion_90d": reintervencion_90d,
            "mortalidad_90d": mortalidad_90d,
            "stone_free": stone_free,
            "composicion_lito": composicion_lito,
            "recurrencia_litiasis": recurrencia_litiasis,
            "cateter_jj_colocado": cateter_jj_colocado,
            "fecha_colocacion_jj": fecha_colocacion_jj.isoformat() if fecha_colocacion_jj else None,
        },
    )
    try:
        from app.services.master_identity_flow import upsert_master_identity

        upsert_master_identity(
            db,
            nss=row.nss,
            curp=row.curp,
            nombre=row.paciente_nombre,
            sexo=row.sexo,
            consulta_id=row.consulta_id,
            source_table="surgical_postquirurgicas",
            source_pk=f"{row.id}:{fecha_realizacion.isoformat()}",
            module="postquirurgica",
            fecha_evento=fecha_realizacion,
            payload={
                "surgical_programacion_id": row.id,
                "quirofano_id": row.quirofano_id,
                "procedimiento_realizado": procedimiento_realizado,
                "diagnostico_postop": diagnostico_postop,
                "cirujano": cirujano,
                "sangrado_ml": sangrado_ml,
            },
            commit=True,
        )
    except Exception:
        db.rollback()

    if m.celery_app is not None:
        try:
            m.async_actualizar_data_mart_task.delay()
        except Exception:
            pass

    next_links: Dict[str, str] = {}
    if row.consulta_id:
        next_links["expediente"] = f"/expediente?consulta_id={row.consulta_id}"
        active_hosp = (
            db.query(m.HospitalizacionDB)
            .filter(m.HospitalizacionDB.consulta_id == row.consulta_id)
            .filter(m.HospitalizacionDB.estatus == "ACTIVO")
            .order_by(m.HospitalizacionDB.fecha_ingreso.desc(), m.HospitalizacionDB.id.desc())
            .first()
        )
        if active_hosp is not None:
            next_links["alta"] = f"/hospitalizacion/alta?hospitalizacion_id={active_hosp.id}"
        else:
            next_links["hospitalizacion_ingreso"] = (
                "/hospitalizacion/nuevo"
                f"?consulta_id={row.consulta_id}"
                f"&nss={row.nss or ''}"
                f"&nombre_completo={(row.paciente_nombre or '').replace(' ', '%20')}"
                f"&edad={row.edad if row.edad is not None else ''}"
                f"&sexo={row.sexo or ''}"
                f"&diagnostico={(row.patologia or row.diagnostico_principal or '').replace(' ', '%20')}"
                f"&hgz_envio={(row.hgz or '').replace(' ', '%20')}"
                "&origen_flujo=postquirurgica"
            )

    return await _render(
        selected_programacion_id=row.id,
        message="Nota postquirúrgica guardada. Paciente agregado a lista de cirugías realizadas.",
        error=None,
        next_links=next_links,
    )
