# -*- coding: utf-8 -*-
import os
import re
import json
import secrets
import base64
import io
import math
from datetime import date, datetime
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, JSON, ForeignKey, event, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from jinja2 import Environment, select_autoescape
from pydantic import BaseModel, Field, validator, ValidationError
import uvicorn

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    plt = None

try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

try:
    import numpy as np
except Exception:
    np = None

try:
    from sklearn.linear_model import LinearRegression
except Exception:
    LinearRegression = None

try:
    from catalogs import get_icd11_map, get_loinc_map, get_snomed_map
except Exception:
    def get_icd11_map():
        return {}
    def get_loinc_map():
        return {}
    def get_snomed_map():
        return {}

try:
    from clinical_cache import cache_patient, get_cached_patient
except Exception:
    def cache_patient(patient_id, data, ttl=300):
        return None
    def get_cached_patient(patient_id):
        return None

# ==========================================
# CONFIGURACIÓN DE SEGURIDAD BÁSICA
# ==========================================
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() not in ("0", "false", "no")
AUTH_USER = os.getenv("IMSS_USER", "admin")
AUTH_PASS = os.getenv("IMSS_PASS", "admin")
MENU_IMSS_LOGO_URL = os.getenv(
    "MENU_IMSS_LOGO_URL",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f7/IMSS_Logo.svg/1200px-IMSS_Logo.svg.png",
)
MENU_IMSS_LOGO_PATH = os.getenv("MENU_IMSS_LOGO_PATH", "/Users/oscaralvarado/Downloads/LOGOIMSS.jpg")
MENU_UROLOGIA_LOGO_URL = os.getenv(
    "MENU_UROLOGIA_LOGO_URL",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/1/13/Blausen_0718_RenalSystem_01.png/512px-Blausen_0718_RenalSystem_01.png",
)
MENU_UROLOGIA_LOGO_PATH = os.getenv("MENU_UROLOGIA_LOGO_PATH", "/Users/oscaralvarado/Downloads/unnamed.png")
MENU_HOSPITAL_BG_URL = os.getenv(
    "MENU_HOSPITAL_BG_URL",
    "https://images.unsplash.com/photo-1587351021759-3e566b6af7cc?auto=format&fit=crop&w=1800&q=80",
)
MENU_HOSPITAL_BG_PATH = os.getenv("MENU_HOSPITAL_BG_PATH", "")
CSRF_COOKIE_NAME = "csrf_token"
REQUIRED_SENTINELS = {"NO_APLICA", "NEGADO", "DESCONOCIDO"}
ASYNC_EMBEDDINGS = os.getenv("ASYNC_EMBEDDINGS", "false").lower() in ("1", "true", "yes")
SURVIVAL_EVENT_FIELD = os.getenv("SURVIVAL_EVENT_FIELD", "estatus_protocolo")
SURVIVAL_EVENT_VALUE = os.getenv("SURVIVAL_EVENT_VALUE", "completo")

security = HTTPBasic(auto_error=False)

# ==========================================
# CONFIGURACIÓN DE BASE DE DATOS (SQLite + SQLAlchemy)
# ==========================================
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./urologia.db")
IS_SQLITE = SQLALCHEMY_DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False} if IS_SQLITE else {}
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args)

if IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
JSON_TYPE = JSONB if (not IS_SQLITE and SQLALCHEMY_DATABASE_URL.startswith("postgres")) else JSON

# ==========================================
# ENTORNO JINJA2 (AUTOESCAPE)
# ==========================================
jinja_env = Environment(autoescape=select_autoescape(default=True, enabled_extensions=("html", "xml")))
_template_cache: Dict[str, Any] = {}
_semantic_model = None

# ==========================================
# MODELOS SQLALCHEMY (TABLAS)
# ==========================================
class ConsultaDB(Base):
    __tablename__ = "consultas"
    __table_args__ = (
        Index("ix_consultas_protocolo_detalles", "protocolo_detalles", postgresql_using="gin"),
    ) if (not IS_SQLITE and JSON_TYPE is not JSON) else ()

    id = Column(Integer, primary_key=True, index=True)
    fecha_registro = Column(Date, default=date.today)

    # --- Ficha identificación ---
    curp = Column(String(18), index=True)
    nss = Column(String(20), index=True)
    agregado_medico = Column(String(50))
    nombre = Column(String(200), index=True)
    fecha_nacimiento = Column(Date)
    edad = Column(Integer)
    sexo = Column(String(20))
    tipo_sangre = Column(String(10))
    ocupacion = Column(String(100))
    nombre_empresa = Column(String(200))
    escolaridad = Column(String(50))

    # --- Dirección ---
    cp = Column(String(10))
    alcaldia = Column(String(100))
    colonia = Column(String(100))
    estado_foraneo = Column(String(100))
    calle = Column(String(100))
    no_ext = Column(String(20))
    no_int = Column(String(20))
    telefono = Column(String(20))
    email = Column(String(100))

    # --- Somatometría ---
    peso = Column(Float)
    talla = Column(Float)
    imc = Column(Float)
    ta = Column(String(20))
    fc = Column(Integer)
    temp = Column(Float)

    # --- Antecedentes heredofamiliares ---
    ahf_status = Column(String(20))
    ahf_linea = Column(String(50))
    ahf_padecimiento = Column(String(200))
    ahf_estatus = Column(String(100))

    # --- Personales patológicos ---
    app_patologia = Column(String(200))
    app_evolucion = Column(String(100))
    app_tratamiento = Column(String(200))
    app_complicaciones = Column(String(20))
    app_desc_complicacion = Column(String(200))
    app_seguimiento = Column(String(200))
    app_ultima_consulta = Column(Date)

    # --- Hospitalizaciones ---
    hosp_previas = Column(String(20))
    hosp_motivo = Column(String(200))
    hosp_dias = Column(Integer)
    hosp_uci = Column(String(10))
    hosp_dias_uci = Column(Integer)

    # --- Toxicomanías ---
    tabaquismo_status = Column(String(20))
    cigarros_dia = Column(Integer)
    anios_fumando = Column(Integer)
    indice_tabaquico = Column(String(50))
    alcoholismo = Column(String(200))
    otras_drogas = Column(String(100))
    droga_manual = Column(String(100))

    # --- Alergias / Transfusiones ---
    alergeno = Column(String(200))
    alergia_reaccion = Column(String(200))
    alergia_fecha = Column(Date)
    transfusiones_status = Column(String(20))
    trans_fecha = Column(Date)
    trans_reacciones = Column(String(200))

    # --- Antecedentes quirúrgicos ---
    aqx_fecha = Column(Date)
    aqx_procedimiento = Column(String(200))
    aqx_hallazgos = Column(String(200))
    aqx_medico = Column(String(100))
    aqx_complicaciones_status = Column(String(20))
    aqx_desc_complicacion = Column(String(200))

    # --- Padecimiento y exploración ---
    padecimiento_actual = Column(Text)
    exploracion_fisica = Column(Text)

    # --- Diagnóstico principal ---
    diagnostico_principal = Column(String(100), index=True)

    # --- Protocolos específicos (JSON) ---
    protocolo_detalles = Column(JSON_TYPE)

    # --- Estudios ---
    estudios_hallazgos = Column(Text)

    # --- Estatus del protocolo ---
    estatus_protocolo = Column(String(50))
    plan_especifico = Column(String(200))
    evento_clinico = Column(String(100))
    fecha_evento = Column(Date)

    # --- Protocolos específicos (columnas) ---
    rinon_tiempo = Column(String(100))
    rinon_tnm = Column(String(100))
    rinon_etapa = Column(String(100))
    rinon_ecog = Column(String(50))
    rinon_charlson = Column(String(100))
    rinon_nefrectomia = Column(String(100))
    rinon_rhp = Column(String(100))
    rinon_sistemico = Column(String(200))

    utuc_tiempo = Column(String(100))
    utuc_tnm = Column(String(100))
    utuc_tx_quirurgico = Column(String(100))
    utuc_rhp = Column(String(100))
    utuc_sistemico = Column(String(200))

    vejiga_tnm = Column(String(100))
    vejiga_ecog = Column(String(50))
    vejiga_hematuria_tipo = Column(String(50))
    vejiga_hematuria_coagulos = Column(String(50))
    vejiga_hematuria_transfusion = Column(String(50))
    vejiga_coagulos_tipo = Column(String(50))
    vejiga_procedimiento_qx = Column(String(200))
    vejiga_via = Column(String(50))
    vejiga_rhp = Column(String(100))
    vejiga_cistoscopias_previas = Column(Text)
    vejiga_quimio_intravesical = Column(String(100))
    vejiga_esquema = Column(String(200))
    vejiga_sistemico = Column(String(200))

    pros_ape_pre = Column(Float)
    pros_ape_act = Column(Float)
    pros_ecog = Column(String(50))
    pros_rmn = Column(String(100))
    pros_historial_ape = Column(Text)
    pros_tr = Column(String(100))
    pros_briganti = Column(String(100))
    pros_gleason = Column(String(100))
    pros_tnm = Column(String(100))
    pros_riesgo = Column(String(100))
    pros_adt_previo = Column(String(200))
    pros_prostatectomia = Column(String(100))
    pros_rhp = Column(String(100))
    pros_radioterapia = Column(String(200))
    pros_continencia = Column(String(100))
    pros_ereccion = Column(String(100))

    pene_tiempo_ecog = Column(String(100))
    pene_tnm = Column(String(100))
    pene_tx_quirurgico = Column(String(100))
    pene_rhp = Column(String(100))
    pene_sistemico = Column(String(200))

    testiculo_tiempo_ecog = Column(String(100))
    testiculo_tnm = Column(String(100))
    testiculo_orquiectomia_fecha = Column(Date)
    testiculo_marcadores_pre = Column(String(200))
    testiculo_marcadores_post = Column(String(200))
    testiculo_rhp = Column(String(100))
    testiculo_historial_marcadores = Column(Text)

    suprarrenal_ecog_metanefrinas = Column(String(200))
    suprarrenal_aldosterona_cortisol = Column(String(200))
    suprarrenal_tnm = Column(String(100))
    suprarrenal_tamano = Column(String(100))
    suprarrenal_cirugia = Column(String(100))
    suprarrenal_rhp = Column(String(100))

    incierto_ape_densidad = Column(String(200))
    incierto_tr = Column(String(100))
    incierto_rmn = Column(String(100))
    incierto_velocidad_ape = Column(String(100))
    incierto_necesidad_btr = Column(String(100))

    lit_tamano = Column(Float)
    lit_localizacion = Column(String(100))
    lit_densidad_uh = Column(Float)
    lit_estatus_postop = Column(String(100))
    lit_unidad_metabolica = Column(String(100))
    lit_guys_score = Column(String(50))
    lit_croes_score = Column(String(50))

    hpb_tamano_prostata = Column(String(100))
    hpb_ape = Column(String(100))
    hpb_ipss = Column(String(100))
    hpb_tamsulosina = Column(String(200))
    hpb_finasteride = Column(String(200))

    otro_detalles = Column(Text)

    subsecuente_subjetivo = Column(Text)
    subsecuente_objetivo = Column(Text)
    subsecuente_analisis = Column(Text)
    subsecuente_plan = Column(Text)
    subsecuente_rhp_actualizar = Column(String(200))

    # --- IA / Analítica ---
    embedding_diagnostico = Column(JSON_TYPE)
    nota_soap_auto = Column(Text)
    inconsistencias = Column(Text)

class HospitalizacionDB(Base):
    __tablename__ = "hospitalizaciones"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, ForeignKey("consultas.id", ondelete="CASCADE"), index=True, nullable=False)
    fecha_ingreso = Column(Date, default=date.today)
    fecha_egreso = Column(Date, nullable=True)
    motivo = Column(String(200))
    servicio = Column(String(100))
    cama = Column(String(20))
    estatus = Column(String(50), default="ACTIVO")  # ACTIVO, EGRESADO, TRASLADO

    consulta = relationship("ConsultaDB")

class QuirofanoDB(Base):
    __tablename__ = "quirofanos"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, ForeignKey("consultas.id", ondelete="CASCADE"), index=True, nullable=False)
    fecha_programada = Column(Date)
    fecha_realizacion = Column(Date, nullable=True)
    procedimiento = Column(String(200))
    cirujano = Column(String(100))
    anestesiologo = Column(String(100))
    quirofano = Column(String(50))
    estatus = Column(String(50), default="PROGRAMADA")  # PROGRAMADA, REALIZADA, CANCELADA
    notas = Column(Text)

    consulta = relationship("ConsultaDB")


class VitalDB(Base):
    __tablename__ = "vitals"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, ForeignKey("consultas.id", ondelete="CASCADE"), index=True, nullable=True)
    patient_id = Column(String(50), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    hr = Column(Float)
    sbp = Column(Float)
    dbp = Column(Float)
    temp = Column(Float)
    peso = Column(Float)
    talla = Column(Float)
    imc = Column(Float)
    source = Column(String(100))


class LabDB(Base):
    __tablename__ = "labs"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, ForeignKey("consultas.id", ondelete="CASCADE"), index=True, nullable=True)
    patient_id = Column(String(50), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    test_code = Column(String(100))
    test_name = Column(String(200))
    value = Column(String(200))
    unit = Column(String(50))
    source = Column(String(100))

Base.metadata.create_all(bind=engine)

# ==========================================
# DEPENDENCIA PARA OBTENER SESIÓN DE DB
# ==========================================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


CURP_CHARSET = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
CURP_MAP = {char: idx for idx, char in enumerate(CURP_CHARSET)}


def calcular_digito_verificador_curp(curp17: str) -> str:
    suma = 0
    for idx, char in enumerate(curp17):
        valor = CURP_MAP.get(char, 0)
        suma += valor * (18 - idx)
    digito = (10 - (suma % 10)) % 10
    return str(digito)

# ==========================================
# MODELOS PYDANTIC (VALIDACIÓN)
# ==========================================
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

    class Config:
        anystr_strip_whitespace = True
        extra = "ignore"

    # Validadores
    @validator('curp')
    def validar_curp(cls, v):
        if v:
            v = re.sub(r'\s+', '', v).upper()
            if not re.match(r'^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$', v):
                raise ValueError('CURP no válido. Formato: ABCD123456HXYZ123456')
            digito = calcular_digito_verificador_curp(v[:17])
            if v[-1] != digito:
                raise ValueError('CURP inválido: dígito verificador incorrecto')
        return v.upper() if v else v

    @validator('nss')
    def validar_nss(cls, v):
        if v:
            v = re.sub(r'\D', '', v)
            if len(v) != 11:
                raise ValueError('NSS debe tener 11 dígitos')
        return v

    @validator('email')
    def validar_email(cls, v):
        if v:
            v = v.strip().lower()
            if not re.match(r'^[^@]+@[^@]+\.[^@]+$', v):
                raise ValueError('Correo electrónico no válido')
        return v

    @validator('edad')
    def edad_no_negativa(cls, v, values):
        if v is not None:
            if v < 0:
                raise ValueError('La edad no puede ser negativa')
            if v > 120:
                raise ValueError('La edad no puede ser mayor a 120 años')
            fecha_nacimiento = values.get('fecha_nacimiento')
            if fecha_nacimiento:
                hoy = date.today()
                calculada = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
                if abs(calculada - v) > 1:
                    raise ValueError('La edad no coincide con la fecha de nacimiento')
        return v

    @validator('fecha_nacimiento')
    def fecha_no_futura(cls, v):
        if v and v > date.today():
            raise ValueError('La fecha de nacimiento no puede ser futura')
        return v

    @validator('telefono')
    def validar_telefono(cls, v):
        if v:
            digits = re.sub(r'\D', '', v)
            if len(digits) != 10:
                raise ValueError('El teléfono debe tener 10 dígitos (incluyendo lada)')
            return digits
        return v

    @validator('peso', 'talla', 'pros_ape_pre', 'pros_ape_act', 'lit_tamano', 'lit_densidad_uh')
    def valores_positivos(cls, v):
        if v is not None and v <= 0:
            raise ValueError('El valor debe ser positivo')
        return v

    @validator('imc', always=True)
    def calcular_imc(cls, v, values):
        peso = values.get('peso')
        talla = values.get('talla')
        if peso and talla and talla > 0:
            imc = peso / ((talla/100) ** 2)
            return round(imc, 2)
        return v

class ConsultaCreate(ConsultaBase):
    pass

PROTOCOL_PREFIXES = (
    'rinon_', 'utuc_', 'vejiga_', 'pros_', 'pene_', 'testiculo_',
    'suprarrenal_', 'incierto_', 'lit_', 'hpb_', 'otro_', 'subsecuente_'
)
PROTOCOL_FIELDS = [
    name for name in ConsultaCreate.__fields__
    if any(name.startswith(prefix) for prefix in PROTOCOL_PREFIXES)
]

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================
def normalize_curp(value: str) -> str:
    return re.sub(r'\s+', '', value or '').upper()


def normalize_nss(value: str) -> str:
    return re.sub(r'\D', '', value or '')


def get_semantic_model():
    global _semantic_model
    if SentenceTransformer is None:
        return None
    if _semantic_model is None:
        _semantic_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _semantic_model


def build_embedding_text(data: Dict[str, Any]) -> str:
    parts = [
        data.get("diagnostico_principal"),
        data.get("padecimiento_actual"),
        data.get("exploracion_fisica"),
        data.get("estudios_hallazgos"),
        data.get("plan_especifico"),
    ]
    return ". ".join([p for p in parts if p])


def compute_embedding(text: str) -> Optional[List[float]]:
    if not text:
        return None
    model = get_semantic_model()
    if model is None:
        return None
    try:
        return model.encode(text).tolist()
    except Exception:
        return None


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def enqueue_embedding(note_id: int, text: str):
    if not text:
        return
    if not ASYNC_EMBEDDINGS:
        return
    try:
        from ai_tasks import embedding_task
        embedding_task.delay(note_id, text)
    except Exception:
        # fallback silencioso
        return


def normalize_form_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for key, value in form_data.items():
        if isinstance(value, str):
            val = value.strip()
            if val == "":
                cleaned[key] = None
            else:
                upper = val.upper()
                cleaned[key] = upper if upper in REQUIRED_SENTINELS else val
        else:
            cleaned[key] = value
    return cleaned


def apply_aliases(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    if data_dict.get("vejiga_coagulos_tipo") and not data_dict.get("vejiga_hematuria_coagulos"):
        data_dict["vejiga_hematuria_coagulos"] = data_dict.get("vejiga_coagulos_tipo")
    if data_dict.get("vejiga_hematuria_coagulos") and not data_dict.get("vejiga_coagulos_tipo"):
        data_dict["vejiga_coagulos_tipo"] = data_dict.get("vejiga_hematuria_coagulos")
    return data_dict


def calcular_indice_tabaquico(cigarros_dia: Optional[int], anios_fumando: Optional[int]) -> Optional[str]:
    if cigarros_dia and anios_fumando and cigarros_dia > 0 and anios_fumando > 0:
        it = (cigarros_dia * anios_fumando) / 20
        return f"{it:.1f} pq/año"
    return None


def calcular_scores_litiasis(tamano: Optional[float], loc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not tamano or not loc:
        return None, None
    guys = "I"
    if loc == "coraliforme":
        guys = "IV"
    elif loc == "renal_inf":
        guys = "III"
    elif tamano > 20:
        guys = "II"

    base_croes = 250 - (tamano * 1.5)
    if loc == "coraliforme":
        base_croes -= 80
    if base_croes < 0:
        base_croes = 0
    croes = f"{int(round(base_croes))} (Est)"
    return f"Grado {guys}", croes


def generar_nota_soap(data_dict: Dict[str, Any]) -> Dict[str, str]:
    nombre = data_dict.get("nombre", "Paciente")
    diagnostico = data_dict.get("diagnostico_principal", "diagnóstico no especificado")
    imc = data_dict.get("imc", "N/E")
    plan = data_dict.get("plan_especifico", "Plan pendiente de definir")
    ta = data_dict.get("ta", "N/E")
    fc = data_dict.get("fc", "N/E")
    temp = data_dict.get("temp", "N/E")
    indice_tabaquico = data_dict.get("indice_tabaquico", "N/E")
    padecimiento = data_dict.get("padecimiento_actual", "Sin síntomas referidos.")

    subjetivo = f"{nombre} refiere: {padecimiento}"
    objetivo = f"TA {ta}, FC {fc}, Temp {temp}. IMC {imc}. Índice tabáquico {indice_tabaquico}."
    analisis = f"Cuadro compatible con {diagnostico}. Evolución clínica en seguimiento."
    plan_text = f"{plan}. Control y seguimiento según protocolo."
    return {
        "subjetivo": subjetivo,
        "objetivo": objetivo,
        "analisis": analisis,
        "plan": plan_text,
    }


def detectar_inconsistencias(data_dict: Dict[str, Any]) -> List[str]:
    inconsistencias = []
    edad = data_dict.get("edad")
    sexo = data_dict.get("sexo")
    diag = data_dict.get("diagnostico_principal") or ""

    male_only = {"ca_prostata", "hpb", "tumor_incierto_prostata"}
    if sexo and sexo.lower().startswith("fem") and diag in male_only:
        inconsistencias.append("Diagnóstico típicamente masculino en paciente femenino. Verificar sexo/diagnóstico.")

    if edad is not None and diag.startswith("ca_") and edad < 18:
        inconsistencias.append("Paciente menor de edad con diagnóstico oncológico. Verificar edad y diagnóstico.")

    if edad is not None and diag == "ca_prostata" and edad < 30:
        inconsistencias.append("Paciente joven con cáncer de próstata. Verificar edad y diagnóstico.")

    imc = data_dict.get("imc")
    if imc is not None and (imc < 10 or imc > 60):
        inconsistencias.append("IMC fuera de rangos esperados. Verificar peso/talla.")

    return inconsistencias


def aplicar_derivaciones(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    if not data_dict.get("indice_tabaquico"):
        it = calcular_indice_tabaquico(data_dict.get("cigarros_dia"), data_dict.get("anios_fumando"))
        if it:
            data_dict["indice_tabaquico"] = it

    if not data_dict.get("lit_guys_score") or not data_dict.get("lit_croes_score"):
        guys, croes = calcular_scores_litiasis(data_dict.get("lit_tamano"), data_dict.get("lit_localizacion"))
        if guys and not data_dict.get("lit_guys_score"):
            data_dict["lit_guys_score"] = guys
        if croes and not data_dict.get("lit_croes_score"):
            data_dict["lit_croes_score"] = croes

    subsecuente_fields = [
        "subsecuente_subjetivo",
        "subsecuente_objetivo",
        "subsecuente_analisis",
        "subsecuente_plan",
    ]
    if not any(data_dict.get(field) for field in subsecuente_fields):
        nota = generar_nota_soap(data_dict)
        data_dict.setdefault("subsecuente_subjetivo", nota["subjetivo"])
        data_dict.setdefault("subsecuente_objetivo", nota["objetivo"])
        data_dict.setdefault("subsecuente_analisis", nota["analisis"])
        data_dict.setdefault("subsecuente_plan", nota["plan"])

    return data_dict


def kaplan_meier(durations: List[int], events: List[int]) -> Tuple[List[int], List[float]]:
    data = sorted(zip(durations, events), key=lambda x: x[0])
    at_risk = len(data)
    survival = 1.0
    times = []
    surv_values = []
    last_time = None
    for t, e in data:
        if last_time is None or t != last_time:
            times.append(t)
            surv_values.append(survival)
            last_time = t
        if e == 1:
            survival *= (at_risk - 1) / at_risk
        at_risk -= 1
        if at_risk <= 0:
            break
    return times, surv_values


def resolve_survival_event(consulta: ConsultaDB) -> Tuple[bool, Optional[date]]:
    field = SURVIVAL_EVENT_FIELD
    value = getattr(consulta, field, None) if hasattr(consulta, field) else None
    if value is None:
        return False, None
    if SURVIVAL_EVENT_VALUE:
        event = str(value).lower() == str(SURVIVAL_EVENT_VALUE).lower()
    else:
        event = bool(value)
    event_date = consulta.fecha_evento or consulta.fecha_registro
    return event, event_date


def fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generar_reporte_bi(db: Session) -> Dict[str, Any]:
    consultas = db.query(ConsultaDB).all()
    total = len(consultas)
    total_onco = len([c for c in consultas if (c.diagnostico_principal or "").startswith("ca_")])
    completos = len([c for c in consultas if c.estatus_protocolo == "completo"])
    incompletos = len([c for c in consultas if c.estatus_protocolo == "incompleto"])

    notice = ""
    numeric_charts = []
    chart_diagnosticos = None
    chart_survival = None
    chart_waitlist = None

    if plt is None:
        notice = "Matplotlib no disponible. Instale matplotlib para generar gráficas."
        return {
            "total": total,
            "total_onco": total_onco,
            "completos": completos,
            "incompletos": incompletos,
            "numeric_charts": numeric_charts,
            "chart_diagnosticos": chart_diagnosticos,
            "chart_survival": chart_survival,
            "chart_waitlist": chart_waitlist,
            "notice": notice,
        }

    # Diagnósticos
    diag_counts: Dict[str, int] = {}
    for consulta in consultas:
        diag = consulta.diagnostico_principal or "SIN_DIAGNOSTICO"
        diag_counts[diag] = diag_counts.get(diag, 0) + 1
    if diag_counts:
        labels = list(diag_counts.keys())
        values = list(diag_counts.values())
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(labels, values, color="#13322B")
        ax.set_title("Diagnósticos")
        ax.tick_params(axis='x', rotation=45)
        chart_diagnosticos = fig_to_base64(fig)
        plt.close(fig)

    # Variables numéricas
    numeric_fields = []
    for col in ConsultaDB.__table__.columns:
        if col.name == "id":
            continue
        if isinstance(col.type, (Integer, Float)):
            numeric_fields.append(col.name)

    for field in numeric_fields:
        values = [getattr(c, field) for c in consultas if getattr(c, field) is not None]
        if len(values) < 2:
            continue
        fig, ax = plt.subplots(figsize=(5, 3))
        ax.hist(values, bins=10, color="#B38E5D", edgecolor="#13322B")
        ax.set_title(field)
        numeric_charts.append(fig_to_base64(fig))
        plt.close(fig)

    # Kaplan-Meier para oncológicos (evento configurable)
    onco = [c for c in consultas if (c.diagnostico_principal or "").startswith("ca_")]
    if onco:
        durations = []
        events = []
        today = date.today()
        for c in onco:
            if c.fecha_registro:
                event, event_date = resolve_survival_event(c)
                end_date = event_date or today
                durations.append(max((end_date - c.fecha_registro).days, 1))
                events.append(1 if event else 0)
        if durations:
            times, surv = kaplan_meier(durations, events)
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.step(times, surv, where="post", color="#13322B")
            ax.set_ylim(0, 1.05)
            ax.set_title(f"Kaplan-Meier (Oncológicos) - Evento: {SURVIVAL_EVENT_FIELD}")
            ax.set_xlabel("Días desde registro")
            ax.set_ylabel("Supervivencia")
            chart_survival = fig_to_base64(fig)
            plt.close(fig)

    # Proyección lista de espera quirúrgica
    completos_por_fecha: Dict[date, int] = {}
    for c in consultas:
        if c.estatus_protocolo == "completo" and c.fecha_registro:
            completos_por_fecha[c.fecha_registro] = completos_por_fecha.get(c.fecha_registro, 0) + 1
    if completos_por_fecha:
        fechas_ordenadas = sorted(completos_por_fecha.keys())
        acumulado = []
        total_acum = 0
        for f in fechas_ordenadas:
            total_acum += completos_por_fecha[f]
            acumulado.append(total_acum)
        x = [f.toordinal() for f in fechas_ordenadas]
        y = acumulado
        if len(x) >= 2:
            if LinearRegression is not None and np is not None:
                model = LinearRegression().fit(np.array(x).reshape(-1, 1), np.array(y))
                futuros = [fechas_ordenadas[-1].toordinal() + i for i in range(1, 31)]
                y_pred = model.predict(np.array(futuros).reshape(-1, 1))
            elif np is not None:
                coef = np.polyfit(x, y, 1)
                futuros = [fechas_ordenadas[-1].toordinal() + i for i in range(1, 31)]
                y_pred = [coef[0] * f + coef[1] for f in futuros]
            else:
                futuros = []
                y_pred = []

            fig, ax = plt.subplots(figsize=(6, 4))
            ax.plot(fechas_ordenadas, y, label="Histórico", color="#13322B")
            if futuros:
                fechas_futuras = [date.fromordinal(f) for f in futuros]
                ax.plot(fechas_futuras, y_pred, label="Proyección", color="#B38E5D", linestyle="--")
            ax.set_title("Lista de espera quirúrgica (completos)")
            ax.legend()
            chart_waitlist = fig_to_base64(fig)
            plt.close(fig)

    return {
        "total": total,
        "total_onco": total_onco,
        "completos": completos,
        "incompletos": incompletos,
        "numeric_charts": numeric_charts,
        "chart_diagnosticos": chart_diagnosticos,
        "chart_survival": chart_survival,
        "chart_waitlist": chart_waitlist,
        "notice": notice,
    }


ICD11_MAP = {
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


def map_to_fhir(consulta: ConsultaDB) -> Dict[str, Any]:
    diag_key = consulta.diagnostico_principal or "SIN_DIAGNOSTICO"
    icd_map = get_icd11_map()
    snomed_map = get_snomed_map()
    loinc_map = get_loinc_map()
    icd_code, icd_display = icd_map.get(diag_key, ICD11_MAP.get(diag_key, (None, consulta.diagnostico_principal)))
    snomed_code = snomed_map.get(diag_key)
    gender = "unknown"
    if consulta.sexo:
        if consulta.sexo.lower().startswith("m"):
            gender = "male"
        elif consulta.sexo.lower().startswith("f"):
            gender = "female"

    patient = {
        "resourceType": "Patient",
        "id": consulta.curp or str(consulta.id),
        "identifier": [
            {"system": "http://imss.gob.mx/curp", "value": consulta.curp},
            {"system": "http://imss.gob.mx/nss", "value": consulta.nss},
        ],
        "name": [{"text": consulta.nombre}],
        "gender": gender,
        "birthDate": consulta.fecha_nacimiento.isoformat() if consulta.fecha_nacimiento else None,
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
        condition_coding.append({
            "system": "http://hl7.org/fhir/sid/icd-11",
            "code": icd_code,
            "display": icd_display,
        })
    if snomed_code:
        condition_coding.append({
            "system": "http://snomed.info/sct",
            "code": snomed_code,
            "display": consulta.diagnostico_principal,
        })
    condition_coding.append({
        "system": "http://imss.gob.mx/diagnosticos",
        "code": diag_key,
        "display": consulta.diagnostico_principal,
    })

    condition = {
        "resourceType": "Condition",
        "id": f"cond-{consulta.id}",
        "subject": {"reference": f"Patient/{patient['id']}"},
        "recordedDate": consulta.fecha_registro.isoformat() if consulta.fecha_registro else None,
        "code": {
            "coding": condition_coding,
            "text": consulta.diagnostico_principal,
        },
        "stage": [{
            "summary": {"text": consulta.pros_riesgo or consulta.rinon_etapa or "Desconocido"}
        }],
        "note": [{
            "text": json.dumps(consulta.protocolo_detalles, ensure_ascii=False) if consulta.protocolo_detalles else ""
        }],
    }

    encounter = {
        "resourceType": "Encounter",
        "id": f"enc-{consulta.id}",
        "status": "finished",
        "class": {"code": "AMB"},
        "subject": {"reference": f"Patient/{patient['id']}"},
        "period": {"start": consulta.fecha_registro.isoformat() if consulta.fecha_registro else None},
    }

    observations = []
    def obs(default_code, display, value, unit=None, key=None):
        if value is None:
            return None
        code = loinc_map.get(key, default_code) if key else default_code
        return {
            "resourceType": "Observation",
            "status": "final",
            "code": {"text": display, "coding": [{"system": "http://loinc.org", "code": code, "display": display}]},
            "subject": {"reference": f"Patient/{patient['id']}"},
            "effectiveDateTime": consulta.fecha_registro.isoformat() if consulta.fecha_registro else None,
            "valueQuantity": {"value": value, "unit": unit} if unit else {"value": value},
        }

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
    for obs_entry in observations:
        bundle["entry"].append({"resource": obs_entry})

    raw_payload = {}
    for col in ConsultaDB.__table__.columns:
        value = getattr(consulta, col.name)
        if isinstance(value, date):
            value = value.isoformat()
        raw_payload[col.name] = value
    raw_json = json.dumps(raw_payload, ensure_ascii=False)
    document_reference = {
        "resourceType": "DocumentReference",
        "id": f"doc-{consulta.id}",
        "status": "current",
        "type": {"text": "Expediente clínico completo"},
        "subject": {"reference": f"Patient/{patient['id']}"},
        "date": consulta.fecha_registro.isoformat() if consulta.fecha_registro else None,
        "content": [{
            "attachment": {
                "contentType": "application/json",
                "data": base64.b64encode(raw_json.encode("utf-8")).decode("utf-8")
            }
        }]
    }
    bundle["entry"].append({"resource": document_reference})

    return bundle


SKIP_REQUIRED_FIELDS = {"imc", "evento_clinico", "fecha_evento"}


def enforce_required_fields(model: ConsultaCreate):
    missing = []
    for name in model.__fields__:
        if name in SKIP_REQUIRED_FIELDS:
            continue
        value = getattr(model, name)
        if value is None:
            missing.append(name)
        elif isinstance(value, str) and value.strip() == "":
            missing.append(name)
    if missing:
        missing_sorted = ", ".join(sorted(missing))
        raise ValueError(
            "Campos obligatorios faltantes. Use valores válidos o "
            f"{', '.join(sorted(REQUIRED_SENTINELS))} cuando aplique. "
            f"Faltantes: {missing_sorted}"
        )


def extraer_protocolo_detalles(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extrae todos los campos de protocolos específicos y los agrupa en un dict."""
    detalles = {}
    for key in PROTOCOL_FIELDS:
        value = data.get(key)
        detalles[key] = value if value not in [None, ""] else "NO_APLICA"
    return detalles


def validate_csrf(form_data: Dict[str, Any], request: Request):
    token_form = form_data.get("csrf_token")
    token_cookie = request.cookies.get(CSRF_COOKIE_NAME)
    if not token_form or not token_cookie or token_form != token_cookie:
        raise HTTPException(status_code=400, detail="CSRF token inválido")


def require_auth(credentials: Optional[HTTPBasicCredentials] = Depends(security)):
    if not AUTH_ENABLED:
        return
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales requeridas",
            headers={"WWW-Authenticate": "Basic"},
        )
    valid_user = secrets.compare_digest(credentials.username, AUTH_USER)
    valid_pass = secrets.compare_digest(credentials.password, AUTH_PASS)
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Basic"},
        )

# ==========================================
# PLANTILLAS JINJA2 (EN STRINGS)
# ==========================================
MENU_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Registro Nacional de Pacientes - Urología CMNR</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root { --imss-verde-oscuro: #13322B; --imss-dorado: #B38E5D; --imss-gris: #545454; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Montserrat', sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            position: relative;
            background-image:
                linear-gradient(rgba(19, 50, 43, 0.68), rgba(19, 50, 43, 0.78)),
                url('{{ hospital_bg_url }}');
            background-size: cover;
            background-position: center center;
            background-attachment: fixed;
        }
        .page-shell {
            flex: 1;
            display: flex;
            flex-direction: column;
        }
        .header-bar {
            background-color: rgba(255, 255, 255, 0.96);
            border-bottom: 4px solid var(--imss-dorado);
            padding: 10px 40px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 20px;
            min-height: 170px;
            box-shadow: 0 4px 22px rgba(0,0,0,0.25);
            z-index: 10;
            position: relative;
        }
        .logo-block {
            width: 180px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex: 0 0 auto;
        }
        .logo-block-imss { width: 320px; }
        .logo-block-uro { width: 210px; justify-content: flex-end; }
        .logo-imss {
            max-height: 190px;
            max-width: 300px;
            width: auto;
            object-fit: contain;
            filter: drop-shadow(0 3px 8px rgba(0,0,0,0.25));
        }
        .logo-urologia {
            max-height: 110px;
            max-width: 160px;
            width: auto;
            object-fit: contain;
            display: block;
            filter: drop-shadow(0 3px 8px rgba(0,0,0,0.25));
        }
        .hospital-title {
            flex: 1 1 auto;
            min-width: 0;
            color: var(--imss-verde-oscuro);
            text-align: center;
            text-transform: uppercase;
            display: flex;
            flex-direction: column;
            justify-content: center;
            line-height: 1.15;
        }
        .hospital-title h1 {
            margin: 0;
            font-size: 32px;
            font-weight: 800;
            letter-spacing: 1.2px;
            line-height: 1.1;
        }
        .hospital-title h2 {
            margin: 5px 0 0 0;
            font-size: 21px;
            font-weight: 700;
            color: var(--imss-dorado);
            letter-spacing: 1.6px;
        }
        .main-content {
            flex: 1;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 40px 20px;
            position: relative;
            z-index: 5;
        }
        .card-menu {
            background: rgba(248, 249, 250, 0.95);
            padding: 50px;
            border-radius: 20px;
            box-shadow: 0 30px 60px rgba(0,0,0,0.45);
            text-align: center;
            max-width: 1100px;
            width: 100%;
            border-top: 8px solid var(--imss-verde-oscuro);
            border: 1px solid rgba(255,255,255,0.35);
        }
        .card-menu h1 {
            color: var(--imss-verde-oscuro);
            font-size: 32px;
            margin-bottom: 10px;
            text-transform: uppercase;
            font-weight: 800;
        }
        .card-menu h2.subtitle {
            color: var(--imss-gris);
            font-size: 16px;
            margin-top: 0;
            margin-bottom: 50px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 2px;
        }
        .grid-buttons {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 25px;
            margin-bottom: 10px;
        }
        .btn-menu {
            background: #fff;
            border: 1px solid #e0e0e0;
            border-radius: 15px;
            padding: 30px 15px;
            text-decoration: none;
            color: var(--imss-verde-oscuro);
            transition: all 0.3s;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.05);
        }
        .btn-menu:hover {
            transform: translateY(-8px);
            box-shadow: 0 20px 30px rgba(19,50,43,0.2);
            border-color: var(--imss-dorado);
        }
        .icon-emoji { font-size: 48px; display: block; }
        .btn-text { font-weight: 700; font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; }
        .btn-full { grid-column: 1 / -1; background: linear-gradient(to right, #ffffff, #f0f7f4); }

        .global-footer {
            width: 100%;
            text-align: center;
            background: rgba(19, 50, 43, 0.92);
            color: #ffffff;
            border-top: 2px solid var(--imss-dorado);
            padding: 14px 10px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.7px;
            text-transform: uppercase;
        }
        @media (max-width: 1024px) {
            .header-bar { padding: 12px 20px; }
            .logo-block { width: 120px; }
            .logo-block-imss { width: 210px; }
            .logo-block-uro { width: 140px; }
            .logo-imss { max-height: 135px; max-width: 200px; }
            .logo-urologia { max-height: 90px; max-width: 130px; }
            .hospital-title h1 { font-size: 24px; }
            .hospital-title h2 { font-size: 16px; }
            .grid-buttons { grid-template-columns: repeat(2, 1fr); }
        }
        @media (max-width: 760px) {
            .header-bar {
                flex-wrap: wrap;
                justify-content: center;
                gap: 10px;
            }
            .logo-block {
                width: 90px;
                order: 2;
            }
            .hospital-title {
                order: 1;
                flex: 1 1 100%;
            }
            .hospital-title h1 {
                font-size: 18px;
                letter-spacing: 0.4px;
            }
            .hospital-title h2 {
                font-size: 13px;
                letter-spacing: 0.8px;
            }
            .card-menu {
                padding: 22px 14px;
                border-radius: 16px;
            }
            .card-menu h1 { font-size: 25px; }
            .card-menu h2.subtitle {
                margin-bottom: 24px;
                font-size: 13px;
            }
            .grid-buttons { grid-template-columns: 1fr; gap: 14px; }
            .btn-menu { padding: 20px 12px; }
            .icon-emoji { font-size: 36px; }
        }
    </style>
</head>
<body>
    <div class="page-shell">
        <div class="header-bar">
            <div class="logo-block logo-block-imss">
                <img src="{{ imss_logo_url }}" class="logo-imss" alt="Logo IMSS">
            </div>
            <div class="hospital-title">
                <h1>HOSPITAL DE ESPECIALIDADES DR. ANTONIO FRAGA MOURET</h1>
                <h2>CENTRO MEDICO NACIONAL LA RAZA</h2>
            </div>
            <div class="logo-block logo-block-uro">
                <img src="{{ urologia_logo_url }}" class="logo-urologia" alt="Logo Urología">
            </div>
        </div>
        <div class="main-content">
            <div class="card-menu">
                <h1>REGISTRO NACIONAL DE PACIENTES</h1>
                <h2 class="subtitle">(SERVICIO DE UROLOGÍA)</h2>
                <div class="grid-buttons">
                    <a href="/consulta" class="btn-menu">
                        <span class="icon-emoji">🩺</span>
                        <span class="btn-text">CONSULTA EXTERNA</span>
                    </a>
                    <a href="/hospitalizacion" class="btn-menu">
                        <span class="icon-emoji">🏥</span>
                        <span class="btn-text">HOSPITALIZACIÓN</span>
                    </a>
                    <a href="/quirofano" class="btn-menu">
                        <span class="icon-emoji">🔪</span>
                        <span class="btn-text">QUIRÓFANO</span>
                    </a>
                    <a href="/expediente" class="btn-menu">
                        <span class="icon-emoji">📁</span>
                        <span class="btn-text">EXPEDIENTE CLÍNICO ÚNICO</span>
                    </a>
                    <a href="/busqueda" class="btn-menu">
                        <span class="icon-emoji">🔎</span>
                        <span class="btn-text">BÚSQUEDA</span>
                    </a>
                    <a href="/reporte" class="btn-menu btn-full">
                        <span class="icon-emoji">📊</span>
                        <span class="btn-text">REPORTE A JEFATURA</span>
                    </a>
                </div>
            </div>
        </div>
    </div>
    <div class="global-footer">2026 PROGRAMA PILOTO (TODOS LOS DERECHOS RESERVADOS)</div>
</body>
</html>
"""

# =============================================================================
# PLANTILLA DEL FORMULARIO COMPLETO (EXTRAÍDA DIRECTAMENTE DEL CÓDIGO MAESTRO)
# =============================================================================
CONSULTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Expediente Clínico Único - Urología</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --fondo-gris: #f4f6f9; }
        body { font-family: 'Montserrat', sans-serif; background: var(--fondo-gris); margin: 0; padding: 20px; }
        .main-container { max-width: 1200px; margin: auto; background: white; border-top: 6px solid var(--imss-verde); border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); overflow: hidden; }
        .header-form { background: white; padding: 30px 40px; border-bottom: 2px solid var(--imss-dorado); display: flex; justify-content: space-between; align-items: center; }
        .header-form h2 { margin: 0; color: var(--imss-verde); text-transform: uppercase; font-size: 22px; font-weight: 800; }
        .back-btn { text-decoration: none; color: var(--imss-verde); font-weight: 700; border: 2px solid var(--imss-verde); padding: 10px 20px; border-radius: 5px; transition: all 0.3s; }
        .back-btn:hover { background: var(--imss-verde); color: white; }
        form { padding: 40px; }
        fieldset { border: 1px solid #e0e0e0; padding: 25px; margin-bottom: 35px; border-radius: 6px; background-color: #fff; }
        legend { font-weight: 700; color: white; background: var(--imss-verde); padding: 8px 20px; border-radius: 20px; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); }
        .section-title-internal { color: var(--imss-verde); font-size: 15px; font-weight: 700; margin-top: 25px; margin-bottom: 15px; border-bottom: 1px solid #eee; padding-bottom: 5px; text-transform: uppercase;}
        .row { display: flex; gap: 20px; margin-bottom: 18px; flex-wrap: wrap; }
        .col-1 { flex: 1 1 100%; } .col-2 { flex: 1 1 45%; } .col-3 { flex: 1 1 30%; } .col-4 { flex: 1 1 22%; }
        label { display: block; font-size: 11px; font-weight: 700; color: #555; margin-bottom: 6px; text-transform: uppercase; }
        input, select, textarea { width: 100%; padding: 12px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; font-family: 'Montserrat', sans-serif; }
        input:focus, select:focus, textarea:focus { border-color: var(--imss-verde); outline: none; box-shadow: 0 0 0 2px rgba(19, 50, 43, 0.1); }
        .calculated-field { background-color: #e8f5e9; color: #2e7d32; font-weight: bold; border: 1px solid #c8e6c9; }
        .save-btn { background-color: var(--imss-verde); color: white; width: 100%; padding: 20px; font-size: 18px; font-weight: 800; border: none; border-radius: 6px; cursor: pointer; margin-top: 30px; transition: 0.3s; text-transform: uppercase; letter-spacing: 1px; }
        .save-btn:hover { background-color: #0e2621; box-shadow: 0 5px 15px rgba(0,0,0,0.3); }
        .dynamic-section { display: none; background: #fcfcfc; padding: 25px; border-radius: 8px; border: 1px solid #ddd; border-left: 5px solid var(--imss-dorado); margin-top: 20px; box-shadow: inset 0 0 10px rgba(0,0,0,0.02); }

        /* Toggle Switch Style */
        .toggle-container { display: flex; justify-content: center; margin-bottom: 30px; background: #e0f2f1; padding: 15px; border-radius: 10px; }
        .toggle-btn { padding: 10px 30px; border: 2px solid var(--imss-verde); cursor: pointer; font-weight: bold; color: var(--imss-verde); background: white; margin: 0 5px; border-radius: 5px; }
        .toggle-btn.active { background: var(--imss-verde); color: white; }

        #seccion_subsecuente { display: none; }
    </style>
</head>
<body>
    <div class="main-container">
        <div class="header-form">
            <h2>Expediente Clínico Único</h2>
            <a href="/" class="back-btn">← MENÚ PRINCIPAL</a>
        </div>

        <div class="toggle-container">
            <div class="toggle-btn active" onclick="toggleConsultaTipo('primera')" id="btn-primera">CONSULTA PRIMERA VEZ</div>
            <div class="toggle-btn" onclick="toggleConsultaTipo('subsecuente')" id="btn-subsecuente">CONSULTA SUBSECUENTE</div>
        </div>

        <form action="/guardar_consulta_completa" method="post" enctype="multipart/form-data">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">

            <div id="seccion_primera_vez">

                <fieldset>
                    <legend>1. Ficha de Identificación</legend>
                    <div class="row">
                        <div class="col-3"><label>CURP *</label><input type="text" name="curp" required style="text-transform: uppercase;"></div>
                        <div class="col-3"><label>NSS</label><input type="text" name="nss"></div>
                        <div class="col-3"><label>Agregado Médico</label><input type="text" name="agregado_medico"></div>
                    </div>
                    <div class="row">
                        <div class="col-2"><label>Nombre Completo (Paterno Materno Nombres) *</label><input type="text" name="nombre" required style="text-transform: uppercase;" placeholder="APELLIDO PATERNO / MATERNO / NOMBRES"></div>
                        <div class="col-4"><label>Fecha Nac. (DD/MM/AAAA)</label><input type="date" name="fecha_nacimiento"></div>
                        <div class="col-4"><label>Edad</label><input type="number" name="edad"></div>
                    </div>
                    <div class="row">
                         <div class="col-4"><label>Sexo</label><select name="sexo"><option value="">Seleccionar...</option><option>Masculino</option><option>Femenino</option></select></div>
                        <div class="col-4"><label>Tipo Sangre</label>
                            <select name="tipo_sangre">
                                <option>Se desconoce</option>
                                <option>O+</option><option>O-</option><option>A+</option><option>A-</option><option>B+</option><option>B-</option><option>AB+</option><option>AB-</option>
                            </select>
                        </div>
                        <div class="col-2"><label>Ocupación Actual</label>
                            <select name="ocupacion">
                                <optgroup label="TRABAJADOR DE LA SALUD"><option>Médico</option><option>Enfermero(a)</option><option>Psicólogo</option><option>Nutriólogo</option><option>Odontólogo</option><option>Otro técnico salud</option></optgroup>
                                <optgroup label="PROFESIONISTAS"><option>Abogado</option><option>Contador</option><option>Arquitecto</option><option>Ingeniero</option><option>Músico</option></optgroup>
                                <optgroup label="EMPLEADO"><option>Empresa Privada (Especificar)</option><option>Empresa Pública (Especificar)</option></optgroup>
                                <optgroup label="COMERCIANTE"><option>Ambulante</option><option>Comercio Nacional</option><option>Comercio Internacional</option></optgroup>
                                <option>Desempleado</option>
                                <optgroup label="JUBILADO/PENSIONADO"><option>Pensionado</option><option>No pensionado</option></optgroup>
                            </select>
                        </div>
                    </div>
                    <div class="row">
                        <div class="col-4"><label>Empresa (Si aplica)</label><input type="text" name="nombre_empresa" placeholder="Nombre de la empresa"></div>
                        <div class="col-4"><label>Escolaridad</label><select name="escolaridad"><option>Primaria</option><option>Secundaria</option><option>Preparatoria/Bachillerato</option><option>Licenciatura</option><option>Maestría</option><option>Especialidad</option><option>Doctorado</option><option>Sin escolaridad</option></select></div>
                    </div>

                    <div class="section-title-internal">Dirección y Contacto</div>
                    <div class="row">
                        <div class="col-4"><label>CP (Zonas)</label><input type="text" name="cp"></div>
                        <div class="col-3"><label>Alcaldía/Municipio/Foráneo</label>
                            <select name="alcaldia" id="select_alcaldia" onchange="toggleForaneo()">
                                <optgroup label="CDMX"><option>Azcapotzalco</option><option>Gustavo A. Madero</option><option>Cuauhtémoc</option><option>Miguel Hidalgo</option><option>Venustiano Carranza</option><option>Iztacalco</option><option>Benito Juárez</option><option>Coyoacán</option><option>Iztapalapa</option><option>Tlalpan</option><option>Magdalena Contreras</option><option>Cuajimalpa</option><option>Álvaro Obregón</option><option>Xochimilco</option><option>Milpa Alta</option><option>Tláhuac</option></optgroup>
                                <optgroup label="Edomex"><option>Ecatepec</option><option>Tlalnepantla</option><option>Naucalpan</option><option>Nezahualcóyotl</option><option>Chimalhuacán</option><option>Toluca</option><option>Metepec</option></optgroup>
                                <option value="foraneo">FORÁNEO</option>
                            </select>
                        </div>
                         <div class="col-2"><label>Colonia</label><input type="text" name="colonia"></div>
                    </div>
                    <div class="row" id="div_foraneo" style="display:none; background:#e0f7fa; padding:10px;">
                        <div class="col-1"><label>Especifique Estado/Municipio (Foráneo)</label><input type="text" name="estado_foraneo"></div>
                    </div>
                     <div class="row">
                        <div class="col-3"><label>Calle</label><input type="text" name="calle"></div>
                        <div class="col-4"><label>No. Ext</label><input type="text" name="no_ext"></div>
                        <div class="col-4"><label>No. Int</label><input type="text" name="no_int"></div>
                        <div class="col-3"><label>Teléfono</label><input type="tel" name="telefono"></div>
                        <div class="col-3"><label>Email</label><input type="email" name="email"></div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>2. Somatometría y Signos Vitales</legend>
                    <div class="row">
                        <div class="col-4"><label>Peso (kg)</label><input type="number" step="0.1" id="peso" name="peso" oninput="calcularIMC()"></div>
                        <div class="col-4"><label>Talla (cm)</label><input type="number" id="talla" name="talla" oninput="calcularIMC()"></div>
                        <div class="col-4"><label>IMC (Auto)</label><input type="text" id="imc" name="imc" readonly class="calculated-field"></div>
                        <div class="col-4"><label>T/A</label><input type="text" name="ta"></div>
                        <div class="col-4"><label>Frec. Cardíaca</label><input type="number" name="fc"></div>
                        <div class="col-4"><label>Temp.</label><input type="number" step="0.1" name="temp"></div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>3. Antecedentes Heredofamiliares</legend>
                    <div class="row">
                        <div class="col-1">
                            <label>Seleccione SI/NO</label>
                            <select id="ahf_select" name="ahf_status" onchange="toggleAHF()"><option value="no">Negados</option><option value="si">SI</option></select>
                        </div>
                    </div>
                    <div id="ahf_detalles" style="display:none; background:#f0f4c3; padding:15px; border-radius:5px;">
                        <div class="row">
                            <div class="col-2"><label>Línea</label><select name="ahf_linea"><option>Materna</option><option>Paterna</option><option>Ambas</option></select></div>
                            <div class="col-2"><label>Padecimiento</label><input type="text" name="ahf_padecimiento" placeholder="Escribir manual"></div>
                            <div class="col-2"><label>Estatus</label><select name="ahf_estatus"><option>Finado por el padecimiento</option><option>Vive con el padecimiento</option></select></div>
                        </div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>4. Personales Patológicos</legend>
                    <div class="section-title-internal">Patologías Crónicas <span style="cursor:pointer; color:blue; font-size:18px;" onclick="alert('Función para agregar más filas de patología activada')">[+] AGREGAR PATOLOGÍA</span></div>
                    <div class="row">
                        <div class="col-3"><label>Patología Crónica</label><input type="text" name="app_patologia"></div>
                        <div class="col-4"><label>Tiempo Evolución</label><input type="text" name="app_evolucion"></div>
                        <div class="col-3"><label>Tratamiento Actual</label><input type="text" name="app_tratamiento"></div>
                    </div>
                    <div class="row">
                        <div class="col-3"><label>Complicaciones</label>
                            <select id="app_complicaciones" name="app_complicaciones" onchange="toggleDisplay('app_complicaciones', 'div_complicaciones')"><option value="no">NO</option><option value="si">SI</option></select>
                        </div>
                        <div class="col-3" id="div_complicaciones" style="display:none;"><label>Describa Complicación</label><input type="text" name="app_desc_complicacion"></div>
                        <div class="col-3"><label>Lugar Seguimiento</label><input type="text" name="app_seguimiento"></div>
                        <div class="col-3"><label>Última Consulta</label><input type="date" name="app_ultima_consulta"></div>
                    </div>

                    <div class="section-title-internal">Hospitalizaciones Previas (Vinculación Mod 3)</div>
                    <div class="row">
                        <div class="col-4"><label>¿Hospitalizaciones?</label><select id="hosp_previas" name="hosp_previas" onchange="toggleDisplay('hosp_previas', 'div_hosp_detalles')"><option value="no">NO</option><option value="si">SI</option></select></div>
                    </div>
                    <div id="div_hosp_detalles" style="display:none; background:#e1bee7; padding:10px; border-radius:5px;">
                        <div class="row">
                            <div class="col-3"><label>Motivo</label><input type="text" name="hosp_motivo"></div>
                            <div class="col-4"><label>Días Estancia</label><input type="number" name="hosp_dias"></div>
                            <div class="col-4"><label>Ingreso a UCI</label><select name="hosp_uci"><option>NO</option><option>SI</option></select></div>
                            <div class="col-4"><label>Días UCI</label><input type="number" name="hosp_dias_uci"></div>
                        </div>
                    </div>

                    <div class="section-title-internal">Toxicomanías</div>
                    <div class="row" style="background: #fffbe6; padding: 15px; border-radius: 5px; border: 1px solid #ffe58f;">
                        <div class="col-4"><label>Tabaquismo</label><select id="tabaquismo_status" name="tabaquismo_status" onchange="toggleDisplay('tabaquismo_status', 'div_tabaco')"><option value="negativo">Negativo</option><option value="positivo">Positivo</option></select></div>
                        <div id="div_tabaco" style="display:none; width:100%;">
                            <div class="row">
                                <div class="col-4"><label>Cigarros/día</label><input type="number" id="cigarros_dia" name="cigarros_dia" oninput="calcularIT()"></div>
                                <div class="col-4"><label>Años fumando</label><input type="number" id="anios_fumando" name="anios_fumando" oninput="calcularIT()"></div>
                                <div class="col-4"><label>Índice Tabáquico</label><input type="text" id="indice_tabaquico" name="indice_tabaquico" readonly class="calculated-field"></div>
                            </div>
                        </div>
                    </div>
                    <div class="row" style="margin-top:10px;">
                        <div class="col-3"><label>Etilismo (Tiempo/Frec/Cant)</label><input type="text" name="alcoholismo" placeholder="Ej. 10 años, semanal, 3 copas"></div>
                        <div class="col-3"><label>Otras Drogas</label>
                            <select name="otras_drogas">
                                <option>Negadas</option><option>Marihuana</option><option>Cocaína</option><option>Metanfetaminas</option><option>Cristal</option><option>Opioides</option><option>Alucinógenos</option><option>Otras (Especificar)</option>
                            </select>
                        </div>
                        <div class="col-3"><label>Especifique Droga</label><input type="text" name="droga_manual"></div>
                    </div>

                    <div class="section-title-internal">Alergias y Transfusiones <span style="cursor:pointer; color:blue;" onclick="alert('Función agregar alergia +')">[+]</span></div>
                    <div class="row">
                        <div class="col-3"><label>Alérgeno</label><input type="text" name="alergeno"></div>
                        <div class="col-3"><label>Reacción</label><input type="text" name="alergia_reaccion"></div>
                        <div class="col-3"><label>Fecha Exposición</label><input type="date" name="alergia_fecha"></div>
                    </div>
                    <div class="row">
                        <div class="col-4"><label>Transfusiones</label><select id="transfusiones_status" name="transfusiones_status" onchange="toggleDisplay('transfusiones_status', 'div_transfusiones')"><option value="no">NO</option><option value="si">SI</option></select></div>
                        <div id="div_transfusiones" style="display:none; width:100%;">
                            <div class="row">
                                <div class="col-3"><label>Fecha Última</label><input type="date" name="trans_fecha"></div>
                                <div class="col-3"><label>Reacciones</label><input type="text" name="trans_reacciones"></div>
                            </div>
                        </div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>5. Antecedentes Quirúrgicos</legend>
                    <div class="row">
                        <div class="col-4"><label>Fecha Procedimiento</label><input type="date" name="aqx_fecha"></div>
                        <div class="col-2"><label>Procedimiento Realizado</label><input type="text" name="aqx_procedimiento"></div>
                        <div class="col-2"><label>Hallazgos</label><input type="text" name="aqx_hallazgos"></div>
                        <div class="col-2"><label>Médico</label><input type="text" name="aqx_medico"></div>
                    </div>
                     <div class="row">
                        <div class="col-4"><label>Complicaciones</label><select id="aqx_complicaciones_status" name="aqx_complicaciones_status" onchange="toggleDisplay('aqx_complicaciones_status', 'div_aqx_compli')"><option value="no">NO</option><option value="si">SI</option></select></div>
                        <div class="col-2" id="div_aqx_compli" style="display:none;"><label>Especifique Complicación</label><input type="text" name="aqx_desc_complicacion"></div>
                     </div>
                     <small style="color:var(--imss-dorado);">* Vinculado con Módulo de Quirófano (Avance 4)</small>
                </fieldset>

                <fieldset>
                    <legend>6. Padecimiento Actual y Exploración Física</legend>
                    <div class="row">
                        <div class="col-1"><label>Padecimiento Actual (PEEA)</label><textarea name="padecimiento_actual" rows="4"></textarea></div>
                    </div>
                    <div class="row">
                        <div class="col-1"><label>Exploración Física</label><textarea name="exploracion_fisica" rows="4" id="ef_textarea"></textarea></div>
                    </div>
                </fieldset>

                <fieldset style="border: 2px solid var(--imss-verde);">
                    <legend style="background: var(--imss-dorado); color: #13322B;">7. Diagnóstico Principal (CIE-11)</legend>
                    <div class="row" style="background: #e8f5e9; padding: 20px; border-radius: 8px;">
                        <div class="col-1">
                            <label style="font-size: 14px; color: var(--imss-verde);">SELECCIONE DIAGNÓSTICO PARA DESPLEGAR CAMPOS INTELIGENTES</label>
                            <select name="diagnostico_principal" id="diagnostico_principal" required style="font-size: 16px; padding: 15px; border: 2px solid var(--imss-verde);" onchange="mostrarFormularioDinamico()">
                                <option value="">-- SELECCIONAR --</option>
                                <optgroup label="ONCOLOGÍA UROLÓGICA">
                                    <option value="ca_rinon">CÁNCER DE RIÑÓN</option>
                                    <option value="ca_urotelial_alto">CÁNCER UROTELIAL TRACTO SUPERIOR</option>
                                    <option value="ca_vejiga">CÁNCER DE VEJIGA</option>
                                    <option value="ca_prostata">CÁNCER DE PRÓSTATA</option>
                                    <option value="ca_pene">CÁNCER DE PENE</option>
                                    <option value="ca_testiculo">CÁNCER DE TESTÍCULO</option>
                                    <option value="tumor_suprarrenal">TUMOR SUPRARRENAL</option>
                                    <option value="tumor_incierto_prostata">TUMOR COMPORTAMIENTO INCIERTO PRÓSTATA</option>
                                </optgroup>
                                <optgroup label="LITIASIS">
                                    <option value="litiasis_rinon">CÁLCULO DEL RIÑÓN</option>
                                    <option value="litiasis_ureter">CÁLCULO DEL URÉTER</option>
                                    <option value="litiasis_vejiga">CÁLCULO DE LA VEJIGA</option>
                                </optgroup>
                                <optgroup label="ANDROLOGÍA/UROGINE/TRANSPLANTE">
                                    <option value="priapismo">PRIAPISMO / DISFUNCIÓN ERÉCTIL</option>
                                    <option value="incontinencia">INCONTINENCIA URINARIA</option>
                                    <option value="fistula">FÍSTULA (V-V / U-V)</option>
                                    <option value="trasplante">TRASPLANTE RENAL (DONADOR VIVO)</option>
                                </optgroup>
                                <optgroup label="OTRAS">
                                    <option value="hpb">HIPERPLASIA PROSTÁTICA BENIGNA</option>
                                    <option value="infeccion">ABSCESO RENAL/PROSTÁTICO/PIELONEFRITIS</option>
                                </optgroup>
                            </select>
                        </div>
                    </div>

                    <div id="form_ca_rinon" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Renal</div>
                        <div class="row"><div class="col-4"><label>Tiempo Diagnóstico</label><input type="text" name="rinon_tiempo"></div></div>
                        <div class="row">
                            <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="rinon_tnm"></div>
                            <div class="col-4"><label>Etapa Clínica</label><input type="text" name="rinon_etapa"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="rinon_ecog"></div>
                            <div class="col-4"><label>Charlson (Auto APP)</label><input type="text" name="rinon_charlson"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>¿Nefrectomía Radical?</label><select name="rinon_nefrectomia"><option>NO</option><option>SI (Abierta)</option><option>SI (Laparoscópica)</option></select></div>
                            <div class="col-3"><label>RHP (Folio IMSS)</label><input type="text" name="rinon_rhp"></div>
                            <div class="col-3"><label>Tx Sistémico</label><input type="text" name="rinon_sistemico" placeholder="Fármaco, Dosis, Tiempo"></div>
                        </div>
                    </div>

                    <div id="form_ca_urotelial" class="dynamic-section">
                        <div class="section-title-internal">Protocolo UTUC</div>
                        <div class="row">
                            <div class="col-4"><label>Tiempo Dx</label><input type="text" name="utuc_tiempo"></div>
                            <div class="col-4"><label>TNM / Etapa</label><input type="text" name="utuc_tnm"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>Tx Quirúrgico</label><select name="utuc_tx_quirurgico"><option>NO</option><option>Abierto</option><option>Laparoscópico</option></select></div>
                            <div class="col-3"><label>RHP</label><input type="text" name="utuc_rhp"></div>
                            <div class="col-3"><label>Tx Sistémico/Reacciones</label><input type="text" name="utuc_sistemico"></div>
                        </div>
                    </div>

                    <div id="form_ca_vejiga" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Vejiga</div>
                        <div class="row">
                            <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="vejiga_tnm"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="vejiga_ecog"></div>
                        </div>
                        <div class="section-title-internal">Caracterización Hematuria</div>
                        <div class="row" style="background:#ffebee; padding:10px;">
                            <div class="col-3"><label>Tipo</label><select name="vejiga_hematuria_tipo"><option>Macro</option><option>Micro</option></select></div>
                            <div class="col-3"><label>Coágulos</label><select name="vejiga_coagulos_tipo" id="vejiga_coagulos_tipo" onchange="syncCoagulos()"><option>Formadora</option><option>No formadora</option></select></div>
                            <input type="hidden" name="vejiga_hematuria_coagulos" id="vejiga_hematuria_coagulos">
                            <div class="col-3"><label>¿Transfusión?</label><select name="vejiga_hematuria_transfusion"><option>NO</option><option>SI</option></select></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Procedimiento Qx</label>
                                <select name="vejiga_procedimiento_qx">
                                    <option>Ninguno</option>
                                    <option>RTU-V</option>
                                    <option>Cistoprostatectomía + Conducto Ileal</option>
                                    <option>Cistoprostatectomía + Ureterostomas</option>
                                    <option>Cistoprostatectomía + Neovejiga</option>
                                </select>
                            </div>
                            <div class="col-2"><label>Vía</label><select name="vejiga_via"><option>Abierta</option><option>Lap</option><option>Endo</option></select></div>
                            <div class="col-2"><label>RHP</label><input type="text" name="vejiga_rhp"></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Cistoscopias Previas</label><textarea rows="2" name="vejiga_cistoscopias_previas"></textarea></div>
                            <div class="col-2"><label>Quimio Intravesical</label><select name="vejiga_quimio_intravesical"><option>Ninguna</option><option>BCG</option><option>Mitomicina</option></select></div>
                            <div class="col-2"><label>Esquema/Dosis</label><input type="text" name="vejiga_esquema"></div>
                            <div class="col-2"><label>Tx Sistémico</label><input type="text" name="vejiga_sistemico"></div>
                        </div>
                    </div>

                    <div id="form_ca_prostata" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Próstata</div>
                        <div class="row">
                            <div class="col-4"><label>APE Pre-Bx (ng/mL)</label><input type="number" step="0.01" name="pros_ape_pre"></div>
                            <div class="col-4"><label>APE Actual</label><input type="number" step="0.01" name="pros_ape_act"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="pros_ecog"></div>
                            <div class="col-4"><label>RMN (PIRADS/Zona)</label><input type="text" name="pros_rmn"></div>
                        </div>
                        <div class="row">
                            <div class="col-1"><label>Historial APE (Gráfica Recurrencia)</label><input type="text" name="pros_historial_ape" placeholder="Ingrese valores históricos para generar curva..."></div>
                        </div>
                        <div class="section-title-internal">Tacto Rectal (TNM Clínico)</div>
                        <div class="row">
                            <div class="col-4"><label>Tacto Rectal</label><input type="text" name="pros_tr"></div>
                            <div class="col-4"><label>Briganti (Nomograma)</label><input type="text" name="pros_briganti"></div>
                        </div>
                        <div class="row">
                            <div class="col-4"><label>Gleason</label><input type="text" name="pros_gleason"></div>
                            <div class="col-4"><label>TNM (T/N/M)</label><input type="text" name="pros_tnm"></div>
                            <div class="col-4"><label>Etapa / Riesgo</label><input type="text" name="pros_riesgo"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>ADT Previo</label><input type="text" name="pros_adt_previo" placeholder="¿Cuál?"></div>
                            <div class="col-3"><label>Prostatectomía</label><select name="pros_prostatectomia"><option>NO</option><option>Abierta</option><option>Lap</option></select></div>
                            <div class="col-3"><label>RHP (Factores Adversos)</label><input type="text" name="pros_rhp"></div>
                            <div class="col-3"><label>Radioterapia</label><input type="text" name="pros_radioterapia" placeholder="Ciclos/Dosis"></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Continencia</label><input type="text" name="pros_continencia"></div>
                            <div class="col-2"><label>Erección (Clasificación)</label><input type="text" name="pros_ereccion"></div>
                        </div>
                    </div>

                    <div id="form_ca_pene" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Cáncer Pene</div>
                         <div class="row">
                             <div class="col-4"><label>Tiempo Dx / ECOG</label><input type="text" name="pene_tiempo_ecog"></div>
                             <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="pene_tnm"></div>
                             <div class="col-4"><label>Tx Quirúrgico</label><select name="pene_tx_quirurgico"><option>NO</option><option>Penectomía Parcial</option><option>Total</option><option>Radical + Linfa</option></select></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>RHP</label><input type="text" name="pene_rhp"></div>
                             <div class="col-2"><label>Tx Sistémico</label><input type="text" name="pene_sistemico"></div>
                         </div>
                    </div>

                    <div id="form_ca_testiculo" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Cáncer Testículo</div>
                         <div class="row">
                             <div class="col-4"><label>Tiempo Dx / ECOG</label><input type="text" name="testiculo_tiempo_ecog"></div>
                             <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="testiculo_tnm"></div>
                             <div class="col-4"><label>Orquiectomía (Fecha)</label><input type="date" name="testiculo_orquiectomia_fecha"></div>
                         </div>
                         <div class="row">
                             <div class="col-3"><label>Marcadores PRE (AFP/HGC/DHL)</label><input type="text" name="testiculo_marcadores_pre"></div>
                             <div class="col-3"><label>Marcadores POST</label><input type="text" name="testiculo_marcadores_post"></div>
                             <div class="col-3"><label>RHP</label><input type="text" name="testiculo_rhp"></div>
                         </div>
                         <div class="row"><div class="col-1"><label>Historial Marcadores (Gráfica)</label><input type="text" name="testiculo_historial_marcadores"></div></div>
                    </div>

                    <div id="form_tumor_suprarrenal" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Suprarrenal</div>
                         <div class="row">
                             <div class="col-3"><label>ECOG / Metanefrinas</label><input type="text" name="suprarrenal_ecog_metanefrinas"></div>
                             <div class="col-3"><label>Aldosterona / Cortisol</label><input type="text" name="suprarrenal_aldosterona_cortisol"></div>
                             <div class="col-3"><label>TNM / Etapa</label><input type="text" name="suprarrenal_tnm"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Tamaño (¿Aumento?)</label><input type="text" name="suprarrenal_tamano"></div>
                             <div class="col-2"><label>Cirugía</label><select name="suprarrenal_cirugia"><option>NO</option><option>Lap</option><option>Abierta</option></select></div>
                             <div class="col-2"><label>RHP</label><input type="text" name="suprarrenal_rhp"></div>
                         </div>
                    </div>

                    <div id="form_tumor_incierto" class="dynamic-section">
                         <div class="section-title-internal">Tumor Comportamiento Incierto</div>
                         <div class="row">
                             <div class="col-4"><label>APE / Densidad APE</label><input type="text" name="incierto_ape_densidad"></div>
                             <div class="col-4"><label>Tacto Rectal</label><input type="text" name="incierto_tr"></div>
                             <div class="col-4"><label>RMN (PIRADS)</label><input type="text" name="incierto_rmn"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Velocidad Replicación APE</label><input type="text" name="incierto_velocidad_ape"></div>
                             <div class="col-2"><label>% Necesidad BTR</label><input type="text" name="incierto_necesidad_btr"></div>
                         </div>
                    </div>

                    <div id="form_litiasis" class="dynamic-section" style="border-left-color: #2196F3;">
                         <div class="section-title-internal" style="color:#2196F3;">Protocolo Litiasis</div>
                         <div class="row" style="background:#e3f2fd; padding:10px;">
                             <div class="col-4"><label>Tamaño (mm)</label><input type="number" id="lit_tamano" name="lit_tamano" oninput="calcularScoresLitiasis()"></div>
                             <div class="col-4"><label>Localización</label>
                                <select id="lit_localizacion" name="lit_localizacion" onchange="calcularScoresLitiasis()">
                                    <option value="">Seleccionar</option><option value="renal_inf">Polo Inf</option><option value="coraliforme">Coraliforme</option><option value="ureter">Uréter</option><option value="otro">Otro</option>
                                </select>
                             </div>
                             <div class="col-4"><label>Densidad (UH)</label><input type="number" name="lit_densidad_uh"></div>
                         </div>
                         <div class="row">
                             <div class="col-3"><label>Estatus PostOp</label><select name="lit_estatus_postop"><option>Litiasis Residual</option><option>ZRF (Libre)</option></select></div>
                             <div class="col-3"><label>Unidad Metabólica</label><select name="lit_unidad_metabolica"><option>NO</option><option>SI (>20mm / Alto Riesgo)</option></select></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Guy's Score</label><input type="text" id="guys_score" name="lit_guys_score" readonly class="calculated-field"></div>
                             <div class="col-2"><label>CROES Nomograma</label><input type="text" id="croes_score" name="lit_croes_score" readonly class="calculated-field"></div>
                         </div>
                    </div>

                    <div id="form_hpb" class="dynamic-section">
                         <div class="section-title-internal">Protocolo HPB</div>
                         <div class="row">
                             <div class="col-3"><label>Tamaño Próstata (cc)</label><input type="text" name="hpb_tamano_prostata"></div>
                             <div class="col-3"><label>APE</label><input type="text" name="hpb_ape"></div>
                             <div class="col-3"><label>IPSS</label><input type="text" name="hpb_ipss"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Tamsulosina</label><input type="text" name="hpb_tamsulosina" placeholder="Dosis/Tiempo"></div>
                             <div class="col-2"><label>Finasteride/Dutasteride</label><input type="text" name="hpb_finasteride" placeholder="Dosis/Tiempo"></div>
                         </div>
                    </div>

                    <div id="form_otros" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Específico</div>
                         <div class="row">
                             <div class="col-1"><label>Detalles Específicos (Pañales/día, Datos Donador, IPSS, Tamaño Próstata, etc.)</label><textarea rows="3" name="otro_detalles"></textarea></div>
                         </div>
                    </div>

                </fieldset>

                <fieldset>
                    <legend>8. Estudios Imagen/Lab/Gabinete</legend>
                    <div class="row"><div class="col-1"><label>Hallazgos Relevantes</label><textarea name="estudios_hallazgos" rows="3"></textarea></div></div>
                </fieldset>

                <fieldset style="background: var(--imss-dorado); color:white;">
                    <legend style="background:white; color:var(--imss-dorado);">9. Estatus del Protocolo</legend>
                    <div class="row">
                        <div class="col-1">
                            <label style="color:white; font-size:14px;">SELECCIONE DESTINO:</label>
                            <select name="estatus_protocolo" style="font-size:16px; padding:10px;">
                                <option value="incompleto">PROTOCOLO INCOMPLETO (Solicitar Estudios/Valoración)</option>
                                <option value="completo">PROTOCOLO COMPLETO -> LISTA DE ESPERA QUIRÚRGICA</option>
                                <option value="seguimiento">ACEPTADO PARA SEGUIMIENTO (Consulta)</option>
                            </select>
                        </div>
                    </div>
                    <div class="row">
                        <div class="col-1"><label style="color:white;">Especificaciones (Estudios faltantes o Procedimiento a programar)</label><input type="text" name="plan_especifico"></div>
                    </div>
                </fieldset>

                <button type="submit" class="save-btn">💾 GUARDAR EN EXPEDIENTE CLÍNICO ÚNICO</button>

            </div> <!-- fin seccion primera vez -->

            <div id="seccion_subsecuente">
                <fieldset>
                    <legend>Consulta Subsecuente - Resumen</legend>
                    <div class="row" style="background:#e8f5e9; padding:20px; border-radius:5px;">
                        <div class="col-1">
                            <h3>Resumen del Paciente (Simulado)</h3>
                            <p><strong>Paciente:</strong> (Carga datos previos...)</p>
                            <p><strong>Dx Principal:</strong> (Carga diagnóstico...)</p>
                            <p><strong>Última Nota:</strong> (Fecha y resumen...)</p>
                        </div>
                    </div>
                    <div class="section-title-internal">Nota de Evolución (SOAP)</div>
                    <div class="row">
                        <div class="col-1"><label>Subjetivo (S)</label><textarea rows="2" name="subsecuente_subjetivo"></textarea></div>
                        <div class="col-1"><label>Objetivo (O) - Inc. Labs/Imagen nuevos</label><textarea rows="2" name="subsecuente_objetivo"></textarea></div>
                        <div class="col-1"><label>Análisis (A)</label><textarea rows="2" name="subsecuente_analisis"></textarea></div>
                        <div class="col-1"><label>Plan (P)</label><textarea rows="2" name="subsecuente_plan"></textarea></div>
                    </div>
                    <div class="row">
                        <div class="col-2"><label>Actualizar RHP / Cirugías</label><input type="text" name="subsecuente_rhp_actualizar" placeholder="Si hubo nuevos eventos..."></div>
                    </div>
                    <button type="button" class="save-btn" onclick="alert('Nota Subsecuente Guardada')">💾 GUARDAR NOTA DE EVOLUCIÓN</button>
                </fieldset>
            </div>

        </form>
    </div>

    <script>
        function toggleConsultaTipo(tipo) {
            if(tipo === 'primera') {
                document.getElementById('seccion_primera_vez').style.display = 'block';
                document.getElementById('seccion_subsecuente').style.display = 'none';
                document.getElementById('btn-primera').classList.add('active');
                document.getElementById('btn-subsecuente').classList.remove('active');
            } else {
                document.getElementById('seccion_primera_vez').style.display = 'none';
                document.getElementById('seccion_subsecuente').style.display = 'block';
                document.getElementById('btn-primera').classList.remove('active');
                document.getElementById('btn-subsecuente').classList.add('active');
                generarNotaSOAP();
            }
        }

        function toggleDisplay(idTrigger, idTarget) {
            var val = document.getElementById(idTrigger).value.toLowerCase();
            var target = document.getElementById(idTarget);
            if(val === 'si' || val === 'positivo') target.style.display = 'block';
            else target.style.display = 'none';
        }

        function toggleAHF() {
            var val = document.getElementById('ahf_select').value;
            var target = document.getElementById('ahf_detalles');
            target.style.display = (val === 'si') ? 'block' : 'none';
        }

        function toggleForaneo() {
            var val = document.getElementById('select_alcaldia').value;
            var target = document.getElementById('div_foraneo');
            target.style.display = (val === 'foraneo') ? 'block' : 'none';
        }

        function calcularIMC() {
            var peso = parseFloat(document.getElementById('peso').value);
            var talla = parseFloat(document.getElementById('talla').value);
            var imcField = document.getElementById('imc');
            if(peso > 0 && talla > 50) {
                var imc = peso / ((talla/100) * (talla/100));
                imcField.value = imc.toFixed(2);
            } else {
                imcField.value = '';
            }
        }

        function calcularIT() {
            var cig = parseFloat(document.getElementById('cigarros_dia').value) || 0;
            var anios = parseFloat(document.getElementById('anios_fumando').value) || 0;
            var itField = document.getElementById('indice_tabaquico');
            if(cig > 0 && anios > 0) {
                var it = (cig * anios) / 20;
                itField.value = it.toFixed(1) + ' pq/año';
            } else {
                itField.value = '';
            }
        }

        function calcularScoresLitiasis() {
             var tamano = parseFloat(document.getElementById('lit_tamano').value) || 0;
             var loc = document.getElementById('lit_localizacion').value;
             var guys = "I"; var croes = "N/A";
             if(loc === 'coraliforme') guys = "IV";
             else if(loc === 'renal_inf') guys = "III";
             else if(tamano > 20) guys = "II";

             var baseCroes = 250 - (tamano*1.5);
             if(loc === 'coraliforme') baseCroes -= 80;
             if(baseCroes < 0) baseCroes = 0;
             croes = Math.round(baseCroes);

             document.getElementById('guys_score').value = "Grado " + guys;
             document.getElementById('croes_score').value = croes + " (Est)";
        }

        function generarNotaSOAP() {
            var subj = document.querySelector('textarea[name="subsecuente_subjetivo"]');
            var obj = document.querySelector('textarea[name="subsecuente_objetivo"]');
            var analisis = document.querySelector('textarea[name="subsecuente_analisis"]');
            var plan = document.querySelector('textarea[name="subsecuente_plan"]');

            if (!subj || !obj || !analisis || !plan) return;
            if (subj.value || obj.value || analisis.value || plan.value) return;

            var nombre = document.querySelector('input[name="nombre"]')?.value || 'Paciente';
            var diagnostico = document.querySelector('select[name="diagnostico_principal"]')?.value || 'diagnóstico no especificado';
            var imc = document.getElementById('imc')?.value || 'N/E';
            var planEsp = document.querySelector('input[name="plan_especifico"]')?.value || 'Plan pendiente de definir';
            var ta = document.querySelector('input[name="ta"]')?.value || 'N/E';
            var fc = document.querySelector('input[name="fc"]')?.value || 'N/E';
            var temp = document.querySelector('input[name="temp"]')?.value || 'N/E';
            var indice = document.getElementById('indice_tabaquico')?.value || 'N/E';
            var padecimiento = document.querySelector('textarea[name="padecimiento_actual"]')?.value || 'Sin síntomas referidos.';

            subj.value = nombre + " refiere: " + padecimiento;
            obj.value = "TA " + ta + ", FC " + fc + ", Temp " + temp + ". IMC " + imc + ". Índice tabáquico " + indice + ".";
            analisis.value = "Cuadro compatible con " + diagnostico + ". Evolución clínica en seguimiento.";
            plan.value = planEsp + ". Control y seguimiento según protocolo.";
        }

        function syncCoagulos() {
            var select = document.getElementById('vejiga_coagulos_tipo');
            var hidden = document.getElementById('vejiga_hematuria_coagulos');
            if (select && hidden) hidden.value = select.value;
        }

        function mostrarFormularioDinamico() {
            var diag = document.getElementById('diagnostico_principal').value;
            var sections = document.getElementsByClassName('dynamic-section');
            for(var i=0; i<sections.length; i++) sections[i].style.display = 'none';

            if(diag.startsWith('ca_rinon')) document.getElementById('form_ca_rinon').style.display = 'block';
            else if(diag.startsWith('ca_urotelial')) document.getElementById('form_ca_urotelial').style.display = 'block';
            else if(diag.startsWith('ca_vejiga')) { document.getElementById('form_ca_vejiga').style.display = 'block'; syncCoagulos(); }
            else if(diag.startsWith('ca_prostata')) document.getElementById('form_ca_prostata').style.display = 'block';
            else if(diag.startsWith('ca_pene')) document.getElementById('form_ca_pene').style.display = 'block';
            else if(diag.startsWith('ca_testiculo')) document.getElementById('form_ca_testiculo').style.display = 'block';
            else if(diag.startsWith('tumor_suprarrenal')) document.getElementById('form_tumor_suprarrenal').style.display = 'block';
            else if(diag.startsWith('tumor_incierto')) document.getElementById('form_tumor_incierto').style.display = 'block';
            else if(diag.startsWith('litiasis')) document.getElementById('form_litiasis').style.display = 'block';
            else if(diag.startsWith('hpb')) document.getElementById('form_hpb').style.display = 'block';
            else if(diag !== '') document.getElementById('form_otros').style.display = 'block';
        }
    </script>
</body>
</html>
"""

CONFIRMACION_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background-color: #f4f6f9; text-align: center; padding-top: 50px; margin: 0; }
        .card { background: white; max-width: 600px; margin: auto; padding: 50px; border-radius: 15px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); border-top: 8px solid #13322B; }
        h1 { color: #13322B; }
        h3 { color: #B38E5D; }
        a { display: inline-block; margin-top: 30px; padding: 15px 30px; background: #13322B; color: white; text-decoration: none; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="card">
        <h1>REGISTRO GUARDADO</h1>
        <p><strong>PACIENTE:</strong> {{ nombre }}</p>
        <p><strong>DX:</strong> {{ diag }}</p>
        <h3>{{ msg_estatus }}</h3>
        {% if inconsistencias %}
        <div style="margin-top:20px; padding:15px; background:#fff8e1; border:1px solid #f0c36d; border-radius:10px; text-align:left;">
            <strong>Advertencias clínicas:</strong>
            <ul>
            {% for item in inconsistencias %}
                <li>{{ item }}</li>
            {% endfor %}
            </ul>
        </div>
        {% endif %}
        <br>
        <a href="/">INICIO</a>
    </div>
</body>
</html>
"""

# Plantillas para hospitalización, quirófano, búsqueda, expediente
HOSPITALIZACION_LISTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Hospitalización - IMSS Urología</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); padding: 30px; }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏥 MÓDULO DE HOSPITALIZACIÓN</h1>
        <h2>Pacientes Hospitalizados</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Paciente</th>
                <th>Ingreso</th>
                <th>Motivo</th>
                <th>Servicio</th>
                <th>Cama</th>
                <th>Estatus</th>
                <th>Acciones</th>
            </tr>
            {% for hosp in hospitalizaciones %}
            <tr>
                <td>{{ hosp.id }}</td>
                <td>{{ hosp.paciente_nombre }}</td>
                <td>{{ hosp.fecha_ingreso }}</td>
                <td>{{ hosp.motivo }}</td>
                <td>{{ hosp.servicio }}</td>
                <td>{{ hosp.cama }}</td>
                <td>{{ hosp.estatus }}</td>
                <td><a href="/expediente?consulta_id={{ hosp.consulta_id }}">Ver expediente</a></td>
            </tr>
            {% else %}
            <tr><td colspan="8" style="text-align:center;">No hay hospitalizaciones activas</td></tr>
            {% endfor %}
        </table>
        <a href="/hospitalizacion/nuevo" class="btn">➕ NUEVA HOSPITALIZACIÓN</a>
        <br><br>
        <a href="/" class="btn btn-dorado">← VOLVER AL MENÚ</a>
    </div>
</body>
</html>
"""

HOSPITALIZACION_NUEVO_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Nueva Hospitalización</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 600px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        .form-group { margin-bottom: 20px; }
        label { font-weight: bold; display: block; margin-bottom: 5px; }
        input, select { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        .btn { background: #13322B; color: white; padding: 12px 25px; border: none; border-radius: 5px; cursor: pointer; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
        .busqueda { display: flex; gap: 10px; }
        .busqueda input { flex: 1; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏥 Registrar Hospitalización</h1>
        <form action="/hospitalizacion/nuevo" method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
            <div class="form-group">
                <label>Buscar paciente por CURP o NSS</label>
                <div class="busqueda">
                    <input type="text" name="busqueda" placeholder="Ingrese CURP o NSS" required>
                    <button type="submit" formaction="/hospitalizacion/buscar" class="btn btn-dorado">Buscar</button>
                </div>
            </div>
            <div class="form-group">
                <label>Consulta ID</label>
                <input type="number" name="consulta_id" value="{{ consulta_id }}" readonly>
            </div>
            <div class="form-group">
                <label>Motivo de hospitalización</label>
                <input type="text" name="motivo" required>
            </div>
            <div class="form-group">
                <label>Servicio</label>
                <select name="servicio">
                    <option>Urología</option>
                    <option>Medicina Interna</option>
                    <option>Terapia Intensiva</option>
                    <option>Cirugía General</option>
                </select>
            </div>
            <div class="form-group">
                <label>Cama</label>
                <input type="text" name="cama" placeholder="Ej. 301-A">
            </div>
            <button type="submit" class="btn">Guardar Hospitalización</button>
        </form>
        <br>
        <a href="/hospitalizacion" class="btn btn-dorado">← Volver</a>
    </div>
</body>
</html>
"""

QUIROFANO_LISTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Quirófano - IMSS Urología</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); padding: 30px; }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔪 MÓDULO DE QUIRÓFANO</h1>
        <h2>Cirugías Programadas</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Paciente</th>
                <th>Procedimiento</th>
                <th>Fecha Programada</th>
                <th>Cirujano</th>
                <th>Quirófano</th>
                <th>Estatus</th>
                <th>Acciones</th>
            </tr>
            {% for q in quirofanos %}
            <tr>
                <td>{{ q.id }}</td>
                <td>{{ q.paciente_nombre }}</td>
                <td>{{ q.procedimiento }}</td>
                <td>{{ q.fecha_programada }}</td>
                <td>{{ q.cirujano }}</td>
                <td>{{ q.quirofano }}</td>
                <td>{{ q.estatus }}</td>
                <td><a href="/expediente?consulta_id={{ q.consulta_id }}">Ver expediente</a></td>
            </tr>
            {% else %}
            <tr><td colspan="8" style="text-align:center;">No hay cirugías programadas</td></tr>
            {% endfor %}
        </table>
        <a href="/quirofano/nuevo" class="btn">➕ PROGRAMAR CIRUGÍA</a>
        <br><br>
        <a href="/" class="btn btn-dorado">← VOLVER AL MENÚ</a>
    </div>
</body>
</html>
"""

QUIROFANO_NUEVO_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Programar Cirugía</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 600px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        .form-group { margin-bottom: 20px; }
        label { font-weight: bold; display: block; margin-bottom: 5px; }
        input, select { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        .btn { background: #13322B; color: white; padding: 12px 25px; border: none; border-radius: 5px; cursor: pointer; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔪 Programar Cirugía</h1>
        <form action="/quirofano/nuevo" method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
            <div class="form-group">
                <label>Consulta ID (del paciente)</label>
                <input type="number" name="consulta_id" required>
            </div>
            <div class="form-group">
                <label>Procedimiento</label>
                <input type="text" name="procedimiento" required>
            </div>
            <div class="form-group">
                <label>Fecha Programada</label>
                <input type="date" name="fecha_programada" required>
            </div>
            <div class="form-group">
                <label>Cirujano</label>
                <input type="text" name="cirujano" required>
            </div>
            <div class="form-group">
                <label>Anestesiólogo</label>
                <input type="text" name="anestesiologo">
            </div>
            <div class="form-group">
                <label>Quirófano</label>
                <input type="text" name="quirofano" placeholder="Ej. QUIR-01">
            </div>
            <div class="form-group">
                <label>Notas</label>
                <textarea name="notas" rows="3"></textarea>
            </div>
            <button type="submit" class="btn">Guardar Programación</button>
        </form>
        <br>
        <a href="/quirofano" class="btn btn-dorado">← Volver</a>
    </div>
</body>
</html>
"""

EXPEDIENTE_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Expediente Clínico Único</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        .section { margin-bottom: 30px; }
        .section h2 { color: #13322B; font-size: 18px; border-left: 5px solid #B38E5D; padding-left: 10px; }
        table { width: 100%; border-collapse: collapse; }
        td { padding: 8px; border-bottom: 1px solid #eee; }
        td:first-child { font-weight: bold; width: 30%; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📁 EXPEDIENTE CLÍNICO ÚNICO</h1>
        <div class="section">
            <h2>Datos del Paciente</h2>
            <table>
                <tr><td>Nombre:</td><td>{{ consulta.nombre }}</td></tr>
                <tr><td>CURP:</td><td>{{ consulta.curp }}</td></tr>
                <tr><td>NSS:</td><td>{{ consulta.nss }}</td></tr>
                <tr><td>Edad:</td><td>{{ consulta.edad }}</td></tr>
                <tr><td>Sexo:</td><td>{{ consulta.sexo }}</td></tr>
                <tr><td>Tipo de sangre:</td><td>{{ consulta.tipo_sangre }}</td></tr>
                <tr><td>Teléfono:</td><td>{{ consulta.telefono }}</td></tr>
                <tr><td>Email:</td><td>{{ consulta.email }}</td></tr>
                <tr><td>Dirección:</td><td>{{ consulta.calle }} {{ consulta.no_ext }}, {{ consulta.colonia }}, {{ consulta.alcaldia }}, CP {{ consulta.cp }}</td></tr>
            </table>
        </div>
        <div class="section">
            <h2>Diagnóstico Principal</h2>
            <p><strong>{{ consulta.diagnostico_principal }}</strong></p>
        </div>
        <div class="section">
            <h2>Detalles del Protocolo</h2>
            <pre>{{ protocolo_json }}</pre>
        </div>
        <div class="section">
            <h2>Estatus del Protocolo</h2>
            <p>{{ consulta.estatus_protocolo }} - {{ consulta.plan_especifico }}</p>
        </div>
        <a href="/busqueda" class="btn">← Buscar otro paciente</a>
        <a href="/" class="btn">Menú Principal</a>
    </div>
</body>
</html>
"""

BUSQUEDA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Búsqueda de Pacientes</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        input[type=text] { width: 70%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        button { padding: 10px 20px; background: #13322B; color: white; border: none; border-radius: 4px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        a { color: #13322B; text-decoration: none; font-weight: bold; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔎 Búsqueda de Pacientes</h1>
        <form method="get" action="/busqueda">
            <input type="text" name="q" placeholder="Ingrese CURP, NSS o nombre" value="{{ query }}">
            <button type="submit">Buscar</button>
        </form>
        {% if resultados %}
        <h2>Resultados:</h2>
        <table>
            <tr>
                <th>CURP</th>
                <th>Nombre</th>
                <th>Edad</th>
                <th>Diagnóstico</th>
                <th>Fecha</th>
                <th>Acciones</th>
            </tr>
            {% for r in resultados %}
            <tr>
                <td>{{ r.curp }}</td>
                <td>{{ r.nombre }}</td>
                <td>{{ r.edad }}</td>
                <td>{{ r.diagnostico_principal }}</td>
                <td>{{ r.fecha_registro }}</td>
                <td><a href="/expediente?consulta_id={{ r.id }}">Ver expediente</a></td>
            </tr>
            {% endfor %}
        </table>
        {% elif query %}
        <p>No se encontraron resultados para "{{ query }}".</p>
        {% endif %}
        <br><br>
        <a href="/busqueda_semantica" class="btn">Búsqueda Semántica</a>
        <br><br>
        <a href="/" class="btn btn-dorado">← Volver al Menú</a>
    </div>
</body>
</html>
"""

BUSQUEDA_SEMANTICA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Búsqueda Semántica</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        input[type=text] { width: 70%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        button { padding: 10px 20px; background: #13322B; color: white; border: none; border-radius: 4px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        a { color: #13322B; text-decoration: none; font-weight: bold; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
        .btn-dorado { background: #B38E5D; }
        .notice { margin-top: 15px; padding: 12px; background: #fff8e1; border-radius: 6px; border: 1px solid #f0c36d; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔎 Búsqueda Semántica</h1>
        <form method="get" action="/busqueda_semantica">
            <input type="text" name="q" placeholder="Ej. dolor lumbar hematuria" value="{{ query }}">
            <button type="submit">Buscar</button>
        </form>
        {% if message %}
        <div class="notice">{{ message }}</div>
        {% endif %}
        {% if resultados %}
        <h2>Resultados:</h2>
        <table>
            <tr>
                <th>CURP</th>
                <th>Nombre</th>
                <th>Diagnóstico</th>
                <th>Similitud</th>
                <th>Acciones</th>
            </tr>
            {% for r in resultados %}
            <tr>
                <td>{{ r.curp }}</td>
                <td>{{ r.nombre }}</td>
                <td>{{ r.diagnostico_principal }}</td>
                <td>{{ r.similitud }}</td>
                <td><a href="/expediente?consulta_id={{ r.id }}">Ver expediente</a></td>
            </tr>
            {% endfor %}
        </table>
        {% endif %}
        <br><br>
        <a href="/" class="btn btn-dorado">← Volver al Menú</a>
    </div>
</body>
</html>
"""

REPORTE_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte BI - Urología</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; margin-bottom: 10px; }
        h2 { color: #13322B; margin-top: 30px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; }
        .card { background: #f9fafb; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb; }
        img { max-width: 100%; border-radius: 8px; border: 1px solid #e5e7eb; }
        .notice { padding: 12px; background: #fff8e1; border-radius: 6px; border: 1px solid #f0c36d; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte BI - Expediente Clínico Único</h1>
        <p>Generado: {{ fecha }}</p>

        <div class="summary">
            <div class="card"><strong>Total pacientes:</strong><br>{{ total }}</div>
            <div class="card"><strong>Oncológicos:</strong><br>{{ total_onco }}</div>
            <div class="card"><strong>Protocolos completos:</strong><br>{{ completos }}</div>
            <div class="card"><strong>Protocolos incompletos:</strong><br>{{ incompletos }}</div>
        </div>

        {% if notice %}
        <div class="notice" style="margin-top:20px;">{{ notice }}</div>
        {% endif %}

        {% if chart_diagnosticos %}
        <h2>Distribución de Diagnósticos</h2>
        <img src="data:image/png;base64,{{ chart_diagnosticos }}" alt="Distribución diagnósticos">
        {% endif %}

        {% if numeric_charts %}
        <h2>Variables Numéricas</h2>
        <div class="grid">
            {% for chart in numeric_charts %}
            <div>
                <img src="data:image/png;base64,{{ chart }}" alt="Histograma">
            </div>
            {% endfor %}
        </div>
        {% endif %}

        {% if chart_survival %}
        <h2>Curva de Supervivencia (Kaplan-Meier)</h2>
        <img src="data:image/png;base64,{{ chart_survival }}" alt="Kaplan-Meier">
        <p style="font-size:12px; color:#555;">Evento aproximado: estatus_protocolo = completo. Censura para otros estados.</p>
        {% endif %}

        {% if chart_waitlist %}
        <h2>Predicción Lista de Espera Quirúrgica</h2>
        <img src="data:image/png;base64,{{ chart_waitlist }}" alt="Proyección lista de espera">
        {% endif %}

        <br>
        <a href="/" class="btn">← Volver al Menú</a>
    </div>
</body>
</html>
"""

# ==========================================
# INSTANCIA DE FASTAPI
# ==========================================
app = FastAPI(
    title="IMSS - Urología HES CMNR (Versión Completa con DB, Validación y Módulos)",
    dependencies=[Depends(require_auth)]
)

# ==========================================
# FUNCIÓN PARA RENDERIZAR PLANTILLAS DESDE STRINGS
# ==========================================
def render_template(template_string: str, request: Optional[Request] = None, **context):
    template = _template_cache.get(template_string)
    if not template:
        template = jinja_env.from_string(template_string)
        _template_cache[template_string] = template

    set_cookie = False
    if request is not None:
        token = request.cookies.get(CSRF_COOKIE_NAME)
        if not token:
            token = secrets.token_urlsafe(32)
            set_cookie = True
        context["csrf_token"] = token

    html = template.render(**context)
    response = HTMLResponse(content=html)
    if request is not None and set_cookie:
        response.set_cookie(CSRF_COOKIE_NAME, context["csrf_token"], httponly=True, samesite="lax")
    return response


def _image_file_to_data_url(file_path: str) -> Optional[str]:
    if not file_path:
        return None
    if not os.path.isfile(file_path):
        return None

    ext = os.path.splitext(file_path)[1].lower()
    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
    }
    mime_type = mime_map.get(ext)
    if mime_type is None:
        return None

    try:
        with open(file_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"
    except Exception:
        return None


def _resolve_menu_asset(default_url: str, local_path: str) -> str:
    data_url = _image_file_to_data_url(local_path)
    return data_url if data_url else default_url

# ==========================================
# 1. MENÚ PRINCIPAL (REDISEÑADO)
# ==========================================
@app.get("/", response_class=HTMLResponse)
async def menu_principal(request: Request):
    return render_template(
        MENU_TEMPLATE,
        request=request,
        imss_logo_url=_resolve_menu_asset(MENU_IMSS_LOGO_URL, MENU_IMSS_LOGO_PATH),
        urologia_logo_url=_resolve_menu_asset(MENU_UROLOGIA_LOGO_URL, MENU_UROLOGIA_LOGO_PATH),
        hospital_bg_url=_resolve_menu_asset(MENU_HOSPITAL_BG_URL, MENU_HOSPITAL_BG_PATH),
    )

# ==========================================
# 2. FORMULARIO DE CONSULTA (PLANTILLA JINJA2 - COMPLETO)
# ==========================================
@app.get("/consulta", response_class=HTMLResponse)
async def formulario_consulta(request: Request):
    return render_template(CONSULTA_TEMPLATE, request=request)

# ==========================================
# 3. LÓGICA DE GUARDADO CON VALIDACIÓN Y PERSISTENCIA
# ==========================================
@app.post("/guardar_consulta_completa", response_class=HTMLResponse)
async def guardar_consulta_completa(request: Request, db: Session = Depends(get_db)):
    form_data = await request.form()
    raw_dict = {k: v for k, v in form_data.items()}
    validate_csrf(raw_dict, request)
    raw_dict.pop("csrf_token", None)
    data_dict = normalize_form_data(raw_dict)
    data_dict = apply_aliases(data_dict)
    data_dict = aplicar_derivaciones(data_dict)
    for field_name in ConsultaCreate.__fields__:
        data_dict.setdefault(field_name, None)

    try:
        consulta_validada = ConsultaCreate(**data_dict)
        enforce_required_fields(consulta_validada)
    except ValidationError as e:
        errores = "<br>".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
        return HTMLResponse(content=f"<h1>Error de validación</h1><p>{errores}</p><a href='/consulta'>Regresar al formulario</a>", status_code=400)
    except ValueError as e:
        return HTMLResponse(content=f"<h1>Error de validación</h1><p>{e}</p><a href='/consulta'>Regresar al formulario</a>", status_code=400)

    protocolo_detalles = extraer_protocolo_detalles(data_dict)
    inconsistencias = detectar_inconsistencias(data_dict)
    nota_soap = generar_nota_soap(data_dict)
    note_text = build_embedding_text(data_dict)
    embedding = None if ASYNC_EMBEDDINGS else compute_embedding(note_text)

    db_consulta = ConsultaDB(
        fecha_registro=date.today(),
        **consulta_validada.dict(exclude_unset=True),
        protocolo_detalles=protocolo_detalles,
        embedding_diagnostico=embedding,
        nota_soap_auto=json.dumps(nota_soap, ensure_ascii=False),
        inconsistencias="; ".join(inconsistencias) if inconsistencias else None
    )
    try:
        db.add(db_consulta)
        db.commit()
        db.refresh(db_consulta)
    except Exception:
        db.rollback()
        return HTMLResponse(content="<h1>Error al guardar el expediente</h1><p>Intente nuevamente.</p><a href='/consulta'>Regresar al formulario</a>", status_code=500)

    nombre = consulta_validada.nombre.upper() if consulta_validada.nombre else "PACIENTE"
    diag = consulta_validada.diagnostico_principal.upper() if consulta_validada.diagnostico_principal else ""
    estatus = consulta_validada.estatus_protocolo or ""
    if estatus == "completo":
        msg = "✅ PACIENTE AGREGADO A LISTA DE ESPERA QUIRÚRGICA"
    elif estatus == "incompleto":
        msg = "⚠️ PROTOCOLO INCOMPLETO - PENDIENTE ESTUDIOS"
    else:
        msg = "🔵 SEGUIMIENTO CONSULTA"

    if ASYNC_EMBEDDINGS:
        enqueue_embedding(db_consulta.id, note_text)

    return render_template(
        CONFIRMACION_TEMPLATE,
        request=request,
        nombre=nombre,
        diag=diag,
        msg_estatus=msg,
        inconsistencias=inconsistencias
    )

# ==========================================
# 4. ENDPOINT REPORTE ORIGINAL (JSON) - INTACTO
# ==========================================
@app.get("/reporte", response_class=HTMLResponse)
def reporte(request: Request, db: Session = Depends(get_db)):
    context = generar_reporte_bi(db)
    context["fecha"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return render_template(REPORTE_TEMPLATE, request=request, **context)

# ==========================================
# 5. NUEVOS MÓDULOS (HOSPITALIZACIÓN, QUIRÓFANO, EXPEDIENTE, BÚSQUEDA)
# ==========================================

# --- HOSPITALIZACIÓN ---
@app.get("/hospitalizacion", response_class=HTMLResponse)
async def listar_hospitalizaciones(request: Request, db: Session = Depends(get_db)):
    filas = (
        db.query(HospitalizacionDB, ConsultaDB.nombre)
        .outerjoin(ConsultaDB, ConsultaDB.id == HospitalizacionDB.consulta_id)
        .filter(HospitalizacionDB.estatus == "ACTIVO")
        .all()
    )
    resultado = []
    for hosp, nombre_paciente in filas:
        resultado.append({
            "id": hosp.id,
            "consulta_id": hosp.consulta_id,
            "paciente_nombre": nombre_paciente or "Desconocido",
            "fecha_ingreso": hosp.fecha_ingreso,
            "motivo": hosp.motivo,
            "servicio": hosp.servicio,
            "cama": hosp.cama,
            "estatus": hosp.estatus
        })
    return render_template(HOSPITALIZACION_LISTA_TEMPLATE, request=request, hospitalizaciones=resultado)

@app.get("/hospitalizacion/nuevo", response_class=HTMLResponse)
async def nuevo_hospitalizacion_form(request: Request):
    return render_template(HOSPITALIZACION_NUEVO_TEMPLATE, request=request, consulta_id="")

@app.post("/hospitalizacion/buscar", response_class=HTMLResponse)
async def buscar_paciente_hospitalizacion(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    validate_csrf(form_dict, request)
    busqueda_raw = (form_dict.get("busqueda") or "").strip()
    busqueda_curp = normalize_curp(busqueda_raw)
    busqueda_nss = normalize_nss(busqueda_raw)

    consulta = None
    if re.match(r'^[A-Z]{4}\d{6}[HM]', busqueda_curp):
        consulta = db.query(ConsultaDB).filter(ConsultaDB.curp == busqueda_curp).order_by(ConsultaDB.id.desc()).first()
    elif re.match(r'^\d{11}$', busqueda_nss):
        consulta = db.query(ConsultaDB).filter(ConsultaDB.nss == busqueda_nss).order_by(ConsultaDB.id.desc()).first()
    else:
        consulta = db.query(ConsultaDB).filter(ConsultaDB.nombre.contains(busqueda_raw)).order_by(ConsultaDB.id.desc()).first()

    if consulta:
        return render_template(HOSPITALIZACION_NUEVO_TEMPLATE, request=request, consulta_id=consulta.id)
    return HTMLResponse(content="<h1>Paciente no encontrado</h1><a href='/hospitalizacion/nuevo'>Intentar de nuevo</a>")

@app.post("/hospitalizacion/nuevo", response_class=HTMLResponse)
async def guardar_hospitalizacion(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    validate_csrf(form_dict, request)

    try:
        consulta_id = int(form_dict.get("consulta_id"))
    except (TypeError, ValueError):
        return HTMLResponse(content="<h1>Consulta ID inválido</h1><a href='/hospitalizacion/nuevo'>Volver</a>", status_code=400)

    consulta = db.query(ConsultaDB).filter(ConsultaDB.id == consulta_id).first()
    if not consulta:
        return HTMLResponse(content="<h1>Consulta no encontrada</h1><a href='/hospitalizacion/nuevo'>Volver</a>", status_code=404)

    nueva_hosp = HospitalizacionDB(
        consulta_id=consulta_id,
        motivo=form_dict.get("motivo"),
        servicio=form_dict.get("servicio"),
        cama=form_dict.get("cama"),
        estatus="ACTIVO"
    )
    try:
        db.add(nueva_hosp)
        db.commit()
    except Exception:
        db.rollback()
        return HTMLResponse(content="<h1>Error al guardar hospitalización</h1><a href='/hospitalizacion/nuevo'>Volver</a>", status_code=500)

    return HTMLResponse(content="<h1>Hospitalización registrada exitosamente</h1><a href='/hospitalizacion'>Volver</a>")

# --- QUIRÓFANO ---
@app.get("/quirofano", response_class=HTMLResponse)
async def listar_quirofanos(request: Request, db: Session = Depends(get_db)):
    filas = (
        db.query(QuirofanoDB, ConsultaDB.nombre)
        .outerjoin(ConsultaDB, ConsultaDB.id == QuirofanoDB.consulta_id)
        .filter(QuirofanoDB.estatus == "PROGRAMADA")
        .all()
    )
    resultado = []
    for q, nombre_paciente in filas:
        resultado.append({
            "id": q.id,
            "consulta_id": q.consulta_id,
            "paciente_nombre": nombre_paciente or "Desconocido",
            "procedimiento": q.procedimiento,
            "fecha_programada": q.fecha_programada,
            "cirujano": q.cirujano,
            "quirofano": q.quirofano,
            "estatus": q.estatus
        })
    return render_template(QUIROFANO_LISTA_TEMPLATE, request=request, quirofanos=resultado)

@app.get("/quirofano/nuevo", response_class=HTMLResponse)
async def nuevo_quirofano_form(request: Request):
    return render_template(QUIROFANO_NUEVO_TEMPLATE, request=request)

@app.post("/quirofano/nuevo", response_class=HTMLResponse)
async def guardar_quirofano(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    validate_csrf(form_dict, request)

    try:
        consulta_id = int(form_dict.get("consulta_id"))
    except (TypeError, ValueError):
        return HTMLResponse(content="<h1>Consulta ID inválido</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    consulta = db.query(ConsultaDB).filter(ConsultaDB.id == consulta_id).first()
    if not consulta:
        return HTMLResponse(content="<h1>Consulta no encontrada</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=404)

    fecha_programada_raw = form_dict.get("fecha_programada")
    try:
        fecha_programada = datetime.strptime(fecha_programada_raw, "%Y-%m-%d").date() if fecha_programada_raw else None
    except ValueError:
        return HTMLResponse(content="<h1>Fecha programada inválida</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=400)

    nueva_cirugia = QuirofanoDB(
        consulta_id=consulta_id,
        procedimiento=form_dict.get("procedimiento"),
        fecha_programada=fecha_programada,
        cirujano=form_dict.get("cirujano"),
        anestesiologo=form_dict.get("anestesiologo"),
        quirofano=form_dict.get("quirofano"),
        notas=form_dict.get("notas"),
        estatus="PROGRAMADA"
    )
    try:
        db.add(nueva_cirugia)
        db.commit()
    except Exception:
        db.rollback()
        return HTMLResponse(content="<h1>Error al guardar quirófano</h1><a href='/quirofano/nuevo'>Volver</a>", status_code=500)

    return HTMLResponse(content="<h1>Cirugía programada exitosamente</h1><a href='/quirofano'>Volver</a>")

# --- EXPEDIENTE CLÍNICO ÚNICO ---
@app.get("/expediente", response_class=HTMLResponse)
async def ver_expediente(request: Request, consulta_id: Optional[int] = None, curp: Optional[str] = None, db: Session = Depends(get_db)):
    if consulta_id:
        consulta = db.query(ConsultaDB).filter(ConsultaDB.id == consulta_id).first()
    elif curp:
        consulta = db.query(ConsultaDB).filter(ConsultaDB.curp == normalize_curp(curp)).order_by(ConsultaDB.id.desc()).first()
    else:
        return HTMLResponse(content="<h1>Debe especificar un paciente (ID o CURP)</h1><a href='/busqueda'>Buscar paciente</a>")

    if not consulta:
        return HTMLResponse(content="<h1>Paciente no encontrado</h1><a href='/busqueda'>Buscar</a>")

    protocolo_json = json.dumps(consulta.protocolo_detalles, indent=2, ensure_ascii=False) if consulta.protocolo_detalles else "Sin detalles"
    return render_template(EXPEDIENTE_TEMPLATE, request=request, consulta=consulta, protocolo_json=protocolo_json)


@app.get("/fhir/expediente", response_class=JSONResponse)
async def fhir_expediente(consulta_id: Optional[int] = None, curp: Optional[str] = None, db: Session = Depends(get_db)):
    cache_key = consulta_id or curp
    if cache_key:
        cached = get_cached_patient(cache_key)
        if cached:
            return JSONResponse(content=cached)
    if consulta_id:
        consulta = db.query(ConsultaDB).filter(ConsultaDB.id == consulta_id).first()
    elif curp:
        consulta = db.query(ConsultaDB).filter(ConsultaDB.curp == normalize_curp(curp)).order_by(ConsultaDB.id.desc()).first()
    else:
        raise HTTPException(status_code=400, detail="Debe especificar consulta_id o curp")

    if not consulta:
        raise HTTPException(status_code=404, detail="Paciente no encontrado")

    payload = map_to_fhir(consulta)
    cache_patient(cache_key or consulta.id, payload)
    return JSONResponse(content=payload)

# --- BÚSQUEDA ---
@app.get("/busqueda", response_class=HTMLResponse)
async def busqueda(request: Request, q: Optional[str] = None, db: Session = Depends(get_db)):
    resultados = []
    query = (q or "").strip()
    if query:
        curp_q = normalize_curp(query)
        nss_q = normalize_nss(query)
        if re.match(r'^[A-Z]{4}\d{6}[HM]', curp_q):
            resultados = db.query(ConsultaDB).filter(ConsultaDB.curp == curp_q).all()
        elif re.match(r'^\d{11}$', nss_q):
            resultados = db.query(ConsultaDB).filter(ConsultaDB.nss == nss_q).all()
        else:
            resultados = db.query(ConsultaDB).filter(ConsultaDB.nombre.contains(query)).all()
    return render_template(BUSQUEDA_TEMPLATE, request=request, query=query, resultados=resultados)


@app.get("/busqueda_semantica", response_class=HTMLResponse)
async def busqueda_semantica(request: Request, q: Optional[str] = None, db: Session = Depends(get_db)):
    resultados = []
    message = ""
    query = (q or "").strip()
    if query:
        model = get_semantic_model()
        if model is None:
            message = "Modelo semántico no disponible. Instale sentence-transformers para habilitar esta función."
        else:
            query_vec = model.encode(query).tolist()
            consultas = db.query(ConsultaDB).filter(ConsultaDB.embedding_diagnostico.isnot(None)).all()
            ranked = []
            for consulta in consultas:
                sim = cosine_similarity(consulta.embedding_diagnostico, query_vec)
                ranked.append((sim, consulta))
            ranked.sort(key=lambda x: x[0], reverse=True)
            for sim, consulta in ranked[:20]:
                resultados.append({
                    "id": consulta.id,
                    "curp": consulta.curp,
                    "nombre": consulta.nombre,
                    "diagnostico_principal": consulta.diagnostico_principal,
                    "similitud": f"{sim:.3f}"
                })
    return render_template(BUSQUEDA_SEMANTICA_TEMPLATE, request=request, query=query, resultados=resultados, message=message)

# ==========================================
# 6. EJECUCIÓN DEL SERVIDOR
# ==========================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
