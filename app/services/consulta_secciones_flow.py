from __future__ import annotations

import re
import json
import secrets
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, func, select, update
from sqlalchemy.orm import Session

from app.core.consulta_payload_utils import calcular_digito_verificador_curp as calcular_digito_verificador_curp_core
from app.core.time_utils import utcnow
from app.core.validators import is_placeholder_text, normalize_nss_10
from app.services.consulta_aditivos_flow import migrate_draft_studies_to_consulta
from app.services.catalog_registry import validate_catalog_value


CONSULTA_SECCIONES_METADATA = MetaData()

CONSULTA_CAPTURA_SECCIONES = Table(
    "consulta_captura_secciones",
    CONSULTA_SECCIONES_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("draft_id", String(64), nullable=False, index=True),
    Column("seccion_codigo", String(10), nullable=False, index=True),
    Column("seccion_nombre", String(120), nullable=False),
    Column("version", Integer, nullable=False, default=1),
    Column("estado", String(30), nullable=False, default="GUARDADO", index=True),
    Column("payload_json", Text, nullable=True),
    Column("payload_tags_json", Text, nullable=True),
    Column("consulta_id", Integer, nullable=True, index=True),
    Column("usuario", String(120), nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    Column("actualizado_en", DateTime, default=utcnow, nullable=False, index=True),
)

CONSULTA_CAPTURA_VALIDACIONES = Table(
    "consulta_captura_validaciones",
    CONSULTA_SECCIONES_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("draft_id", String(64), nullable=False, index=True),
    Column("seccion_codigo", String(10), nullable=False, index=True),
    Column("valid", String(10), nullable=False, default="false"),
    Column("errores_json", Text, nullable=True),
    Column("warnings_json", Text, nullable=True),
    Column("quality_score", Integer, nullable=False, default=0),
    Column("payload_json", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)


SECTION_LABELS = {
    "1": "Ficha de identificación",
    "2": "Somatometría y signos vitales",
    "3": "Antecedentes heredofamiliares",
    "4": "Personales patológicos",
    "5": "Antecedentes quirúrgicos",
    "6": "Padecimiento actual",
    "7": "Diagnóstico",
    "8": "Estudios de imagen",
    "9": "Estatus del protocolo",
}

CURP_CHARSET = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
CURP_MAP = {char: idx for idx, char in enumerate(CURP_CHARSET)}
CURP_RE = re.compile(r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$")
EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
BAD_TOKENS = {
    "asdf",
    "qwerty",
    "test",
    "prueba",
    "xxxx",
    "123",
    "1234",
    "na",
    "n/a",
    "sin dato",
    "ninguno",
}


def ensure_consulta_secciones_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    CONSULTA_SECCIONES_METADATA.create_all(bind=bind, checkfirst=True)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _nss_10(value: Any) -> str:
    # Mantiene comportamiento legacy (primeros 10) para no romper cruces previos.
    return normalize_nss_10(_safe_text(value), strategy="legacy_left")


def _calcular_digito_verificador_curp(curp17: str) -> str:
    return calcular_digito_verificador_curp_core(curp17)


def _looks_fake(value: str) -> bool:
    txt = _safe_text(value).lower()
    if not txt:
        return False
    if is_placeholder_text(txt):
        return True
    if txt in BAD_TOKENS:
        return True
    if txt.isdigit() and len(set(txt)) <= 2:
        return True
    if len(txt) < 2:
        return True
    return False


def _parse_date_any(value: str) -> Optional[date]:
    txt = _safe_text(value)
    if not txt:
        return None
    try:
        return datetime.strptime(txt[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _validate_seccion_1(payload: Dict[str, Any]) -> Dict[str, Any]:
    errors: Dict[str, str] = {}
    warnings: Dict[str, str] = {}
    normalized = dict(payload or {})

    curp = _safe_text(payload.get("curp")).upper().replace(" ", "")
    normalized["curp"] = curp
    if not curp:
        errors["curp"] = "CURP es obligatorio."
    elif not CURP_RE.match(curp):
        errors["curp"] = "CURP inválido: formato incorrecto."
    else:
        digito = _calcular_digito_verificador_curp(curp[:17])
        if curp[-1] != digito:
            errors["curp"] = "CURP inválido: dígito verificador incorrecto."
        elif _looks_fake(curp):
            errors["curp"] = "CURP con patrón no válido."

    nss_raw = _safe_text(payload.get("nss"))
    nss_digits_raw = re.sub(r"\D", "", nss_raw)
    nss = _nss_10(nss_raw)
    normalized["nss"] = nss
    if not nss:
        errors["nss"] = "NSS es obligatorio."
    elif not re.fullmatch(r"\d{10}", nss):
        errors["nss"] = "NSS debe tener exactamente 10 dígitos."
    elif len(nss_digits_raw) > 10:
        warnings["nss"] = "NSS ajustado automáticamente a 10 dígitos."

    nombre = _safe_text(payload.get("nombre")).upper()
    normalized["nombre"] = nombre
    nombre_tokens = [t for t in re.split(r"\s+", nombre) if t]
    if not nombre:
        errors["nombre"] = "Nombre completo es obligatorio."
    elif _looks_fake(nombre):
        errors["nombre"] = "Nombre no parece válido."
    elif len(nombre_tokens) < 2:
        errors["nombre"] = "Capture al menos apellido y nombre."

    sexo = _safe_text(payload.get("sexo"))
    normalized["sexo"] = sexo
    if not sexo:
        errors["sexo"] = "Sexo es obligatorio."
    elif sexo.lower() not in {"masculino", "femenino"}:
        errors["sexo"] = "Sexo debe ser Masculino o Femenino."

    edad_raw = _safe_text(payload.get("edad"))
    fecha_nac_raw = _safe_text(payload.get("fecha_nacimiento"))
    fecha_nac = _parse_date_any(fecha_nac_raw)
    edad_int: Optional[int] = None
    if edad_raw:
        try:
            edad_int = int(float(edad_raw))
            normalized["edad"] = edad_int
            if edad_int < 0 or edad_int > 120:
                errors["edad"] = "Edad fuera de rango clínico (0-120)."
        except Exception:
            errors["edad"] = "Edad inválida."
    if not edad_raw and not fecha_nac_raw:
        errors["edad"] = "Debe capturar edad o fecha de nacimiento."
    if fecha_nac_raw and not fecha_nac:
        errors["fecha_nacimiento"] = "Fecha de nacimiento inválida."
    if fecha_nac and fecha_nac > date.today():
        errors["fecha_nacimiento"] = "La fecha de nacimiento no puede ser futura."
    if fecha_nac and edad_int is not None:
        hoy = date.today()
        calculada = hoy.year - fecha_nac.year - ((hoy.month, hoy.day) < (fecha_nac.month, fecha_nac.day))
        if abs(calculada - edad_int) > 1:
            errors["edad"] = "Edad no coincide con fecha de nacimiento."

    telefono = re.sub(r"\D", "", _safe_text(payload.get("telefono")))
    if telefono:
        normalized["telefono"] = telefono
        if len(telefono) != 10:
            errors["telefono"] = "Teléfono debe tener 10 dígitos."

    email = _safe_text(payload.get("email")).lower()
    normalized["email"] = email
    if email and not EMAIL_RE.match(email):
        errors["email"] = "Correo electrónico inválido."

    agregado = _safe_text(payload.get("agregado_medico"))
    if agregado and _looks_fake(agregado):
        warnings["agregado_medico"] = "Revise agregado médico: valor atípico."

    quality = 100 - (len(errors) * 25) - (len(warnings) * 5)
    if quality < 0:
        quality = 0

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "normalized_payload": normalized,
        "quality_score": int(quality),
    }


def _validate_seccion_7(payload: Dict[str, Any]) -> Dict[str, Any]:
    errors: Dict[str, str] = {}
    warnings: Dict[str, str] = {}
    normalized = dict(payload or {})

    cie10 = _safe_text(payload.get("cie10_codigo") or payload.get("diagnostico_cie10_codigo") or payload.get("cie10"))
    diagnostico = _safe_text(payload.get("diagnostico_principal") or payload.get("diagnostico_cie10"))
    cie11 = _safe_text(payload.get("cie11_codigo") or payload.get("cie11_code"))

    if cie10:
        normalized["cie10_codigo"] = cie10.upper()
        # Formato CIE10 base (validación suave, no bloquea flujo por subcodificación local).
        if not re.fullmatch(r"[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?", normalized["cie10_codigo"]):
            warnings["cie10_codigo"] = "Formato CIE10 no estándar; verifique código."
    if diagnostico:
        normalized["diagnostico_principal"] = diagnostico.upper()
    if not cie10 and not diagnostico:
        warnings["diagnostico_principal"] = "No se capturó diagnóstico/CIE10."

    if cie11:
        normalized["cie11_codigo"] = cie11.upper()
        try:
            check = validate_catalog_value("cie11", code=normalized["cie11_codigo"])
            if not bool(check.get("valid")):
                warnings["cie11_codigo"] = "CIE11 no encontrado en catálogo institucional."
        except Exception:
            warnings["cie11_codigo"] = "No fue posible validar CIE11 contra catálogo."

    quality = 100 - (len(errors) * 25) - (len(warnings) * 4)
    if quality < 0:
        quality = 0
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "normalized_payload": normalized,
        "quality_score": int(quality),
    }


def validate_consulta_seccion(
    db: Session,
    *,
    draft_id: Optional[str],
    seccion_codigo: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    ensure_consulta_secciones_schema(db)
    code = _safe_text(seccion_codigo)
    if code not in SECTION_LABELS:
        raise ValueError("Sección inválida")

    normalized_draft = _safe_text(draft_id) or secrets.token_hex(16)
    input_payload = payload or {}
    if not isinstance(input_payload, dict):
        input_payload = {}

    if code == "1":
        result = _validate_seccion_1(input_payload)
    elif code == "7":
        result = _validate_seccion_7(input_payload)
    else:
        # Validación liviana para el resto (aditiva, no bloquea flujo clínico previo).
        warnings = {}
        for k, v in input_payload.items():
            if _looks_fake(_safe_text(v)):
                warnings[k] = "Valor potencialmente no válido."
        result = {
            "valid": True,
            "errors": {},
            "warnings": warnings,
            "normalized_payload": dict(input_payload),
            "quality_score": max(60, 100 - (len(warnings) * 3)),
        }

    db.execute(
        CONSULTA_CAPTURA_VALIDACIONES.insert().values(
            draft_id=normalized_draft,
            seccion_codigo=code,
            valid="true" if result["valid"] else "false",
            errores_json=json.dumps(result.get("errors") or {}, ensure_ascii=False),
            warnings_json=json.dumps(result.get("warnings") or {}, ensure_ascii=False),
            quality_score=int(result.get("quality_score") or 0),
            payload_json=json.dumps(result.get("normalized_payload") or {}, ensure_ascii=False),
            creado_en=utcnow(),
        )
    )
    db.commit()

    result["draft_id"] = normalized_draft
    result["seccion_codigo"] = code
    return result


def _build_tags(payload: Dict[str, Any], *, seccion_codigo: str) -> Dict[str, Any]:
    def g(*keys: str) -> str:
        for key in keys:
            val = _safe_text(payload.get(key))
            if val:
                return val
        return ""

    tags: Dict[str, Any] = {
        "seccion": seccion_codigo,
        "nss": g("nss"),
        "nombre": g("nombre"),
        "sexo": g("sexo"),
        "edad": g("edad"),
        "diagnostico": g("diagnostico_principal"),
        "estatus_protocolo": g("estatus_protocolo"),
    }

    if seccion_codigo == "2":
        tags["imc"] = g("imc")
        tags["imc_clasificacion"] = g("imc_clasificacion")
    if seccion_codigo == "4":
        tags["hosp_previas"] = g("hosp_previas")
        tags["tabaquismo_status"] = g("tabaquismo_status")
    if seccion_codigo == "7":
        tags["diagnostico_principal"] = g("diagnostico_principal")
    if seccion_codigo == "8":
        tags["files_count"] = len(payload.get("archivos_seleccionados") or [])

    return tags


def _next_version(db: Session, draft_id: str, seccion_codigo: str) -> int:
    row = db.execute(
        select(func.max(CONSULTA_CAPTURA_SECCIONES.c.version))
        .where(CONSULTA_CAPTURA_SECCIONES.c.draft_id == draft_id)
        .where(CONSULTA_CAPTURA_SECCIONES.c.seccion_codigo == seccion_codigo)
    ).first()
    max_ver = int(row[0]) if row and row[0] is not None else 0
    return max_ver + 1


def save_consulta_seccion(
    db: Session,
    *,
    draft_id: Optional[str],
    seccion_codigo: str,
    seccion_nombre: Optional[str],
    payload: Dict[str, Any],
    usuario: str,
) -> Dict[str, Any]:
    ensure_consulta_secciones_schema(db)
    code = _safe_text(seccion_codigo)
    if code not in SECTION_LABELS:
        raise ValueError("Sección inválida")

    normalized_draft = _safe_text(draft_id)
    if not normalized_draft:
        normalized_draft = secrets.token_hex(16)

    label = _safe_text(seccion_nombre) or SECTION_LABELS[code]
    payload_in = dict(payload or {})
    if "nss" in payload_in:
        payload_in["nss"] = _nss_10(payload_in.get("nss"))
    tags = _build_tags(payload_in, seccion_codigo=code)
    version = _next_version(db, normalized_draft, code)

    db.execute(
        CONSULTA_CAPTURA_SECCIONES.insert().values(
            draft_id=normalized_draft,
            seccion_codigo=code,
            seccion_nombre=label,
            version=version,
            estado="GUARDADO",
            payload_json=json.dumps(payload_in, ensure_ascii=False),
            payload_tags_json=json.dumps(tags, ensure_ascii=False),
            usuario=_safe_text(usuario) or "system",
            creado_en=utcnow(),
            actualizado_en=utcnow(),
        )
    )
    db.commit()

    return {
        "draft_id": normalized_draft,
        "seccion_codigo": code,
        "seccion_nombre": label,
        "version": version,
        "estado": "GUARDADO",
    }


def _latest_rows(db: Session, draft_id: str) -> List[Dict[str, Any]]:
    rows = db.execute(
        select(CONSULTA_CAPTURA_SECCIONES)
        .where(CONSULTA_CAPTURA_SECCIONES.c.draft_id == draft_id)
        .order_by(
            CONSULTA_CAPTURA_SECCIONES.c.seccion_codigo.asc(),
            CONSULTA_CAPTURA_SECCIONES.c.version.desc(),
            CONSULTA_CAPTURA_SECCIONES.c.id.desc(),
        )
    ).mappings().all()

    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _safe_text(row.get("seccion_codigo"))
        if code in latest:
            continue
        latest[code] = dict(row)

    out = []
    for code in sorted(SECTION_LABELS.keys(), key=lambda x: int(x)):
        row = latest.get(code)
        if row is None:
            continue
        payload = {}
        try:
            payload = json.loads(row.get("payload_json") or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        tags = {}
        try:
            tags = json.loads(row.get("payload_tags_json") or "{}")
            if not isinstance(tags, dict):
                tags = {}
        except Exception:
            tags = {}

        non_empty = []
        for k, v in payload.items():
            txt = _safe_text(v)
            if txt:
                non_empty.append({"k": k, "v": txt})
        out.append(
            {
                "seccion_codigo": code,
                "seccion_nombre": _safe_text(row.get("seccion_nombre")) or SECTION_LABELS.get(code, code),
                "version": row.get("version"),
                "estado": _safe_text(row.get("estado")) or "GUARDADO",
                "actualizado_en": row.get("actualizado_en").isoformat() if row.get("actualizado_en") else None,
                "payload": payload,
                "tags": tags,
                "preview": non_empty[:12],
            }
        )
    return out


def get_draft_resumen(db: Session, *, draft_id: str) -> Dict[str, Any]:
    ensure_consulta_secciones_schema(db)
    did = _safe_text(draft_id)
    if not did:
        raise ValueError("draft_id requerido")
    sections = _latest_rows(db, did)
    return {
        "draft_id": did,
        "total_secciones": len(sections),
        "secciones": sections,
    }


def get_draft_section_payload(db: Session, *, draft_id: str, seccion_codigo: str) -> Dict[str, Any]:
    ensure_consulta_secciones_schema(db)
    did = _safe_text(draft_id)
    code = _safe_text(seccion_codigo)
    if not did or code not in SECTION_LABELS:
        raise ValueError("Parámetros inválidos")

    row = db.execute(
        select(CONSULTA_CAPTURA_SECCIONES)
        .where(CONSULTA_CAPTURA_SECCIONES.c.draft_id == did)
        .where(CONSULTA_CAPTURA_SECCIONES.c.seccion_codigo == code)
        .order_by(CONSULTA_CAPTURA_SECCIONES.c.version.desc(), CONSULTA_CAPTURA_SECCIONES.c.id.desc())
        .limit(1)
    ).mappings().first()
    if row is None:
        return {
            "draft_id": did,
            "seccion_codigo": code,
            "seccion_nombre": SECTION_LABELS[code],
            "payload": {},
            "version": 0,
        }

    payload = {}
    try:
        payload = json.loads(row.get("payload_json") or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    return {
        "draft_id": did,
        "seccion_codigo": code,
        "seccion_nombre": _safe_text(row.get("seccion_nombre")) or SECTION_LABELS[code],
        "payload": payload,
        "version": row.get("version") or 0,
    }


def get_draft_identity_payload(db: Session, *, draft_id: str) -> Dict[str, Any]:
    """Recupera identidad mínima del borrador (NSS/NOMBRE) desde sección 1."""
    section = get_draft_section_payload(db, draft_id=draft_id, seccion_codigo="1")
    payload = section.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "nss": _safe_text(payload.get("nss")),
        "nombre": _safe_text(payload.get("nombre")),
        "sexo": _safe_text(payload.get("sexo")),
    }


def attach_draft_to_consulta(db: Session, *, draft_id: Optional[str], consulta_id: int) -> Dict[str, Any]:
    ensure_consulta_secciones_schema(db)
    did = _safe_text(draft_id)
    if not did:
        return {"updated": 0, "draft_id": "", "consulta_id": consulta_id}

    result = db.execute(
        update(CONSULTA_CAPTURA_SECCIONES)
        .where(CONSULTA_CAPTURA_SECCIONES.c.draft_id == did)
        .values(consulta_id=int(consulta_id), estado="FINALIZADO", actualizado_en=utcnow())
    )
    db.commit()
    out = {
        "updated": int(result.rowcount or 0),
        "draft_id": did,
        "consulta_id": int(consulta_id),
    }
    try:
        from app.core.app_context import main_proxy as m

        mig = migrate_draft_studies_to_consulta(db, m, draft_id=did, consulta_id=int(consulta_id))
        out["draft_files_migrated"] = int(mig.get("migrated") or 0)
    except Exception:
        db.rollback()
        out["draft_files_migrated"] = 0
    return out


def finalize_draft_endpoint(db: Session, *, draft_id: str, consulta_id: int) -> Dict[str, Any]:
    return attach_draft_to_consulta(db, draft_id=draft_id, consulta_id=consulta_id)
