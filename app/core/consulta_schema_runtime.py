from __future__ import annotations

import re
from datetime import date
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.core.consulta_payload_utils import calcular_digito_verificador_curp


class ConsultaBase(BaseModel):
    curp: Optional[str] = None
    nss: Optional[str] = None
    agregado_medico: Optional[str] = None
    nombre: Optional[str] = None
    fecha_nacimiento: Optional[date] = None
    edad: Optional[int] = None
    sexo: Optional[str] = None
    tipo_sangre: Optional[str] = None
    ocupacion: Optional[str] = None
    nombre_empresa: Optional[str] = None
    escolaridad: Optional[str] = None
    cp: Optional[str] = None
    alcaldia: Optional[str] = None
    colonia: Optional[str] = None
    estado_foraneo: Optional[str] = None
    calle: Optional[str] = None
    no_ext: Optional[str] = None
    no_int: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None
    peso: Optional[float] = None
    talla: Optional[float] = None
    imc: Optional[float] = None
    ta: Optional[str] = None
    fc: Optional[int] = None
    temp: Optional[float] = None
    ahf_status: Optional[str] = None
    ahf_linea: Optional[str] = None
    ahf_padecimiento: Optional[str] = None
    ahf_estatus: Optional[str] = None
    app_patologia: Optional[str] = None
    app_evolucion: Optional[str] = None
    app_tratamiento: Optional[str] = None
    app_complicaciones: Optional[str] = None
    app_desc_complicacion: Optional[str] = None
    app_seguimiento: Optional[str] = None
    app_ultima_consulta: Optional[date] = None
    hosp_previas: Optional[str] = None
    hosp_motivo: Optional[str] = None
    hosp_dias: Optional[int] = None
    hosp_uci: Optional[str] = None
    hosp_dias_uci: Optional[int] = None
    tabaquismo_status: Optional[str] = None
    cigarros_dia: Optional[int] = None
    anios_fumando: Optional[int] = None
    indice_tabaquico: Optional[str] = None
    alcoholismo: Optional[str] = None
    otras_drogas: Optional[str] = None
    droga_manual: Optional[str] = None
    alergeno: Optional[str] = None
    alergia_reaccion: Optional[str] = None
    alergia_fecha: Optional[date] = None
    transfusiones_status: Optional[str] = None
    trans_fecha: Optional[date] = None
    trans_reacciones: Optional[str] = None
    aqx_fecha: Optional[date] = None
    aqx_procedimiento: Optional[str] = None
    aqx_hallazgos: Optional[str] = None
    aqx_medico: Optional[str] = None
    aqx_complicaciones_status: Optional[str] = None
    aqx_desc_complicacion: Optional[str] = None
    padecimiento_actual: Optional[str] = None
    exploracion_fisica: Optional[str] = None
    diagnostico_principal: Optional[str] = None
    estudios_hallazgos: Optional[str] = None
    estatus_protocolo: Optional[str] = None
    plan_especifico: Optional[str] = None
    evento_clinico: Optional[str] = None
    fecha_evento: Optional[date] = None
    rinon_tiempo: Optional[str] = None
    rinon_tnm: Optional[str] = None
    rinon_etapa: Optional[str] = None
    rinon_ecog: Optional[str] = None
    rinon_charlson: Optional[str] = None
    rinon_nefrectomia: Optional[str] = None
    rinon_rhp: Optional[str] = None
    rinon_sistemico: Optional[str] = None
    utuc_tiempo: Optional[str] = None
    utuc_tnm: Optional[str] = None
    utuc_tx_quirurgico: Optional[str] = None
    utuc_rhp: Optional[str] = None
    utuc_sistemico: Optional[str] = None
    vejiga_tnm: Optional[str] = None
    vejiga_ecog: Optional[str] = None
    vejiga_hematuria_tipo: Optional[str] = None
    vejiga_hematuria_coagulos: Optional[str] = None
    vejiga_hematuria_transfusion: Optional[str] = None
    vejiga_coagulos_tipo: Optional[str] = None
    vejiga_procedimiento_qx: Optional[str] = None
    vejiga_via: Optional[str] = None
    vejiga_rhp: Optional[str] = None
    vejiga_cistoscopias_previas: Optional[str] = None
    vejiga_quimio_intravesical: Optional[str] = None
    vejiga_esquema: Optional[str] = None
    vejiga_sistemico: Optional[str] = None
    pros_ape_pre: Optional[float] = None
    pros_ape_act: Optional[float] = None
    pros_ecog: Optional[str] = None
    pros_rmn: Optional[str] = None
    pros_historial_ape: Optional[str] = None
    pros_tr: Optional[str] = None
    pros_briganti: Optional[str] = None
    pros_gleason: Optional[str] = None
    pros_tnm: Optional[str] = None
    pros_riesgo: Optional[str] = None
    pros_adt_previo: Optional[str] = None
    pros_prostatectomia: Optional[str] = None
    pros_rhp: Optional[str] = None
    pros_radioterapia: Optional[str] = None
    pros_continencia: Optional[str] = None
    pros_ereccion: Optional[str] = None
    pene_tiempo_ecog: Optional[str] = None
    pene_tnm: Optional[str] = None
    pene_tx_quirurgico: Optional[str] = None
    pene_rhp: Optional[str] = None
    pene_sistemico: Optional[str] = None
    testiculo_tiempo_ecog: Optional[str] = None
    testiculo_tnm: Optional[str] = None
    testiculo_orquiectomia_fecha: Optional[date] = None
    testiculo_marcadores_pre: Optional[str] = None
    testiculo_marcadores_post: Optional[str] = None
    testiculo_rhp: Optional[str] = None
    testiculo_historial_marcadores: Optional[str] = None
    suprarrenal_ecog_metanefrinas: Optional[str] = None
    suprarrenal_aldosterona_cortisol: Optional[str] = None
    suprarrenal_tnm: Optional[str] = None
    suprarrenal_tamano: Optional[str] = None
    suprarrenal_cirugia: Optional[str] = None
    suprarrenal_rhp: Optional[str] = None
    incierto_ape_densidad: Optional[str] = None
    incierto_tr: Optional[str] = None
    incierto_rmn: Optional[str] = None
    incierto_velocidad_ape: Optional[str] = None
    incierto_necesidad_btr: Optional[str] = None
    lit_tamano: Optional[float] = None
    lit_localizacion: Optional[str] = None
    lit_densidad_uh: Optional[float] = None
    lit_estatus_postop: Optional[str] = None
    lit_unidad_metabolica: Optional[str] = None
    lit_guys_score: Optional[str] = None
    lit_croes_score: Optional[str] = None
    hpb_tamano_prostata: Optional[str] = None
    hpb_ape: Optional[str] = None
    hpb_ipss: Optional[str] = None
    hpb_tamsulosina: Optional[str] = None
    hpb_finasteride: Optional[str] = None
    otro_detalles: Optional[str] = None
    subsecuente_subjetivo: Optional[str] = None
    subsecuente_objetivo: Optional[str] = None
    subsecuente_analisis: Optional[str] = None
    subsecuente_plan: Optional[str] = None
    subsecuente_rhp_actualizar: Optional[str] = None

    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    @field_validator("curp")
    @classmethod
    def validar_curp(cls, v):
        if v:
            v = re.sub(r"\s+", "", v).upper()
            if not re.match(r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$", v):
                raise ValueError("CURP no válido. Formato: ABCD123456HXYZ123456")
            digito = calcular_digito_verificador_curp(v[:17])
            if v[-1] != digito:
                raise ValueError("CURP inválido: dígito verificador incorrecto")
        return v.upper() if v else v

    @field_validator("nss")
    @classmethod
    def validar_nss(cls, v):
        if v:
            v = re.sub(r"\D", "", v)
            if len(v) > 10:
                v = v[:10]
            if len(v) != 10:
                raise ValueError("NSS debe tener 10 dígitos")
        return v

    @field_validator("email")
    @classmethod
    def validar_email(cls, v):
        if v:
            v = v.strip().lower()
            if not re.match(r"^[^@]+@[^@]+\.[^@]+$", v):
                raise ValueError("Correo electrónico no válido")
        return v

    @field_validator("edad")
    @classmethod
    def edad_no_negativa(cls, v, info: ValidationInfo):
        if v is not None:
            if v < 0:
                raise ValueError("La edad no puede ser negativa")
            if v > 120:
                raise ValueError("La edad no puede ser mayor a 120 años")
            fecha_nacimiento = (info.data or {}).get("fecha_nacimiento")
            if fecha_nacimiento:
                hoy = date.today()
                calculada = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
                if abs(calculada - v) > 1:
                    raise ValueError("La edad no coincide con la fecha de nacimiento")
        return v

    @field_validator("fecha_nacimiento")
    @classmethod
    def fecha_no_futura(cls, v):
        if v and v > date.today():
            raise ValueError("La fecha de nacimiento no puede ser futura")
        return v

    @field_validator("telefono")
    @classmethod
    def validar_telefono(cls, v):
        if v:
            digits = re.sub(r"\D", "", v)
            if len(digits) != 10:
                raise ValueError("El teléfono debe tener 10 dígitos (incluyendo lada)")
            return digits
        return v

    @field_validator("peso", "talla", "pros_ape_pre", "pros_ape_act", "lit_tamano", "lit_densidad_uh")
    @classmethod
    def valores_positivos(cls, v):
        if v is not None and v <= 0:
            raise ValueError("El valor debe ser positivo")
        return v

    @model_validator(mode="after")
    def calcular_imc(self):
        peso = self.peso
        talla = self.talla
        if peso and talla and talla > 0:
            imc = peso / ((talla / 100) ** 2)
            self.imc = round(imc, 2)
        return self


class ConsultaCreate(ConsultaBase):
    pass


PROTOCOL_PREFIXES = (
    "rinon_",
    "utuc_",
    "vejiga_",
    "pros_",
    "pene_",
    "testiculo_",
    "suprarrenal_",
    "incierto_",
    "lit_",
    "hpb_",
    "otro_",
    "subsecuente_",
)

PROTOCOL_FIELDS = [
    name for name in ConsultaCreate.model_fields if any(name.startswith(prefix) for prefix in PROTOCOL_PREFIXES)
]
