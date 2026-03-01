"""
Validaciones Clínicas Extendidas — FASE 3.

ADITIVO: No modifica validaciones existentes.
Extiende detectar_inconsistencias (que solo tenía 4 reglas)
con validaciones clínicas robustas para urología.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from app.core.shared_helpers import safe_text, safe_int, safe_float, safe_bool


# ---------------------------------------------------------------------------
# Tipos de resultado
# ---------------------------------------------------------------------------
class ValidationResult:
    def __init__(self):
        self.errors: List[str] = []      # Bloquean guardado
        self.warnings: List[str] = []    # Muestran alerta pero permiten guardar
        self.info: List[str] = []        # Informativos

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "total_issues": len(self.errors) + len(self.warnings),
        }


# ---------------------------------------------------------------------------
# Validaciones de demografía
# ---------------------------------------------------------------------------
def validate_demographics(data: Dict[str, Any]) -> ValidationResult:
    r = ValidationResult()
    nss = safe_text(data.get("nss"))
    if not nss or len(nss) != 10 or not nss.isdigit():
        r.errors.append("NSS debe tener exactamente 10 dígitos")

    nombre = safe_text(data.get("nombre") or data.get("nombre_completo") or data.get("paciente_nombre"))
    if not nombre or len(nombre) < 5:
        r.errors.append("Nombre del paciente es requerido (mínimo 5 caracteres)")

    edad = safe_int(data.get("edad"))
    if edad is None or edad < 0 or edad > 120:
        r.errors.append("Edad debe estar entre 0 y 120 años")

    sexo = safe_text(data.get("sexo")).upper()
    if sexo not in ("MASCULINO", "FEMENINO"):
        r.errors.append("Sexo debe ser MASCULINO o FEMENINO")

    return r


# ---------------------------------------------------------------------------
# Validaciones de somatometría
# ---------------------------------------------------------------------------
def validate_vitals(data: Dict[str, Any]) -> ValidationResult:
    r = ValidationResult()

    peso = safe_float(data.get("peso") or data.get("peso_kg"))
    if peso is not None:
        if peso < 20 or peso > 300:
            r.warnings.append(f"Peso fuera de rango habitual: {peso} kg")

    talla = safe_float(data.get("talla") or data.get("talla_m"))
    if talla is not None:
        if talla > 10:
            talla = talla / 100  # Convertir cm a m
        if talla < 0.5 or talla > 2.5:
            r.warnings.append(f"Talla fuera de rango habitual: {talla} m")

    imc = safe_float(data.get("imc"))
    if imc is not None:
        if imc < 12:
            r.warnings.append(f"IMC peligrosamente bajo: {imc}")
        elif imc > 60:
            r.warnings.append(f"IMC peligrosamente alto: {imc}")

    ta = safe_text(data.get("ta"))
    if ta and "/" in ta:
        parts = ta.split("/")
        sis = safe_int(parts[0])
        dia = safe_int(parts[1]) if len(parts) > 1 else None
        if sis and (sis < 60 or sis > 260):
            r.warnings.append(f"TA sistólica fuera de rango: {sis}")
        if dia and (dia < 30 or dia > 160):
            r.warnings.append(f"TA diastólica fuera de rango: {dia}")
        if sis and dia and sis <= dia:
            r.errors.append(f"TA sistólica ({sis}) no puede ser menor o igual a diastólica ({dia})")

    fc = safe_int(data.get("fc"))
    if fc is not None:
        if fc < 30:
            r.warnings.append(f"Bradicardia severa: FC {fc}")
        elif fc > 180:
            r.warnings.append(f"Taquicardia severa: FC {fc}")

    temp = safe_float(data.get("temp") or data.get("temp_c"))
    if temp is not None:
        if temp < 34:
            r.warnings.append(f"Hipotermia: {temp}°C")
        elif temp > 40:
            r.warnings.append(f"Fiebre alta: {temp}°C")

    return r


# ---------------------------------------------------------------------------
# Validaciones urológicas específicas
# ---------------------------------------------------------------------------
def validate_urology_protocol(data: Dict[str, Any]) -> ValidationResult:
    r = ValidationResult()
    diagnostico = safe_text(data.get("diagnostico_principal")).upper()

    # === Cáncer de próstata ===
    if "CANCER" in diagnostico and "PROSTATA" in diagnostico:
        ape = safe_float(data.get("pros_ape_act") or data.get("ape"))
        if ape is not None and ape < 0:
            r.errors.append("APE no puede ser negativo")
        if ape is not None and ape > 10000:
            r.warnings.append(f"APE extremadamente alto: {ape} ng/mL — verificar resultado")

        gleason = safe_text(data.get("pros_gleason") or data.get("gleason"))
        if gleason:
            # Validar formato Gleason (e.g., "3+4=7", "4+5=9")
            import re
            if not re.match(r"\d\s*\+\s*\d\s*=\s*\d+", gleason) and not gleason.isdigit():
                r.warnings.append(f"Formato Gleason no estándar: '{gleason}' — usar formato X+Y=Z")

        ecog = safe_int(data.get("pros_ecog") or data.get("ecog_onco"))
        if ecog is not None:
            if ecog < 0 or ecog > 5:
                r.errors.append(f"ECOG debe estar entre 0 y 5, recibido: {ecog}")
            if ecog >= 3:
                r.warnings.append(f"ECOG {ecog}: paciente con actividad limitada — evaluar beneficio de procedimiento")

        tnm = safe_text(data.get("pros_tnm") or data.get("tnm")).upper()
        if tnm and not re.match(r"T\d", tnm):
            r.warnings.append(f"TNM no sigue formato estándar: '{tnm}' — usar TxNxMx")

    # === Litiasis ===
    if "CALCULO" in diagnostico or "LITIASIS" in diagnostico:
        tamano = safe_text(data.get("lit_tamano") or data.get("litiasis_tamano_rango"))
        uh = safe_float(data.get("lit_densidad_uh"))
        if uh is not None and (uh < 0 or uh > 3000):
            r.warnings.append(f"Densidad UH fuera de rango habitual: {uh}")

    # === HPB ===
    if "HIPERPLASIA" in diagnostico or "HPB" in diagnostico:
        tamano_prostata = safe_float(data.get("hpb_tamano_prostata"))
        if tamano_prostata is not None:
            if tamano_prostata < 10:
                r.warnings.append(f"Tamaño prostático inusualmente pequeño: {tamano_prostata}g")
            if tamano_prostata > 300:
                r.warnings.append(f"Tamaño prostático inusualmente grande: {tamano_prostata}g")

        ipss = safe_int(data.get("hpb_ipss") or data.get("ipss"))
        if ipss is not None:
            if ipss < 0 or ipss > 35:
                r.errors.append(f"IPSS debe estar entre 0 y 35, recibido: {ipss}")
            elif ipss >= 20:
                r.info.append(f"IPSS {ipss}: sintomatología severa")

    return r


# ---------------------------------------------------------------------------
# Validaciones quirúrgicas
# ---------------------------------------------------------------------------
def validate_surgical_programming(data: Dict[str, Any]) -> ValidationResult:
    r = ValidationResult()

    edad = safe_int(data.get("edad"))
    if edad is not None and edad > 85:
        r.warnings.append(f"Paciente de {edad} años — considerar riesgo quirúrgico aumentado por edad")

    charlson = safe_int(data.get("charlson"))
    if charlson is not None and charlson >= 5:
        r.warnings.append(f"Índice de Charlson {charlson}: alta comorbilidad — evaluar riesgo/beneficio")

    asa = safe_text(data.get("asa") or data.get("preop__asa")).upper()
    if asa:
        asa_num = safe_int(asa.replace("ASA", "").strip())
        if asa_num is not None and asa_num >= 4:
            r.warnings.append(f"ASA {asa_num}: riesgo anestésico muy alto")

    # Hemoderivados
    solicita_hemo = safe_text(data.get("solicita_hemoderivados")).upper()
    if solicita_hemo == "SI":
        pg = safe_int(data.get("hemoderivados_pg_solicitados"), 0)
        pfc = safe_int(data.get("hemoderivados_pfc_solicitados"), 0)
        cp = safe_int(data.get("hemoderivados_cp_solicitados"), 0)
        total = (pg or 0) + (pfc or 0) + (cp or 0)
        if total > 10:
            r.warnings.append(f"Solicitud de {total} unidades de hemoderivados — verificar indicación")
        if total == 0:
            r.errors.append("Si solicita hemoderivados, debe indicar al menos una unidad")

    return r


# ---------------------------------------------------------------------------
# Validador maestro
# ---------------------------------------------------------------------------
def validate_all(data: Dict[str, Any], context: str = "consulta") -> ValidationResult:
    """Ejecuta todas las validaciones relevantes según el contexto.

    context puede ser: 'consulta', 'hospitalizacion', 'quirofano', 'urgencia'
    """
    master = ValidationResult()

    # Siempre validar demografía
    demo = validate_demographics(data)
    master.errors.extend(demo.errors)
    master.warnings.extend(demo.warnings)
    master.info.extend(demo.info)

    # Vitales si hay datos
    has_vitals = any(data.get(k) for k in ["peso", "talla", "ta", "fc", "temp", "peso_kg", "talla_m", "temp_c"])
    if has_vitals:
        vitals = validate_vitals(data)
        master.errors.extend(vitals.errors)
        master.warnings.extend(vitals.warnings)

    # Protocolo urológico si hay diagnóstico
    if data.get("diagnostico_principal"):
        uro = validate_urology_protocol(data)
        master.errors.extend(uro.errors)
        master.warnings.extend(uro.warnings)
        master.info.extend(uro.info)

    # Quirúrgico
    if context in ("quirofano", "urgencia"):
        qx = validate_surgical_programming(data)
        master.errors.extend(qx.errors)
        master.warnings.extend(qx.warnings)

    return master
