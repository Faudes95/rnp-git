# -*- coding: utf-8 -*-
import os
import re
import json
import mimetypes
import logging
from functools import partial
from time import sleep
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Text, JSON, Boolean, ForeignKey, event, Index, inspect, text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from fastapi import Request, Form, Depends, HTTPException, status, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, Response, FileResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from jinja2 import Environment, select_autoescape
from pydantic import ValidationError
import uvicorn
from app.bootstrap import build_main_lifespan, register_security_middlewares
from app.core.composition_root import create_app_instance, attach_lifespan
from app.core.logging import configure_structured_logging
from app.core.time_utils import utcnow
from app.core.startup_performance import (
    normalize_startup_mode,
    launch_background_task,
    schedule_model_warmup,
)
from app.core.startup_orchestrator import (
    run_startup_interconexion,
    run_startup_ai_agents_bootstrap,
)
from app.core.cache_lifecycle import (
    startup_redis_cache_lifecycle,
    shutdown_redis_cache_lifecycle,
)
from app.core.security_auth import load_auth_settings, require_auth_basic
from app.core.legacy_form_utils import extract_protocolo_detalles, validate_csrf_token
from app.core.geo_identity import build_patient_hash, geocode_address
from app.core.request_security import request_is_https
from app.core.clinical_contracts import (
    classify_pathology_group as classify_pathology_group_core,
    classify_procedure_group as classify_procedure_group_core,
    is_required_form_complete as is_required_form_complete_core,
    enforce_required_fields_model,
)
from app.core.clinical_helpers import (
    registrar_auditoria_row,
    get_code_from_map,
    build_patologia_cie10_catalog,
    edad_quinquenio,
    edad_grupo_epidemiologico,
)
from app.core.surgical_catalog_maps import (
    SURGICAL_CIE11_MAP as SURGICAL_CIE11_MAP_CORE,
    SURGICAL_SNOMED_MAP as SURGICAL_SNOMED_MAP_CORE,
    SURGICAL_CIE9MC_MAP as SURGICAL_CIE9MC_MAP_CORE,
)
from app.core.db_engine_utils import build_optional_engine
from app.core.db_schema_utils import ensure_clinical_sensitive_schema as ensure_clinical_sensitive_schema_core
from app.core.alembic_runner import run_optional_alembic_upgrade
from app.core.analytics_stats import (
    parse_any_date as parse_any_date_core,
    calc_percentile as calc_percentile_core,
    safe_pct as safe_pct_core,
    parse_lab_numeric as parse_lab_numeric_core,
    lab_key_from_text as lab_key_from_text_core,
    lab_positive_clostridium as lab_positive_clostridium_core,
    hospital_stay_days as hospital_stay_days_core,
    distribution_stats_table as distribution_stats_table_core,
    as_date as as_date_core,
)
from app.core.flow_events import emit_flujo_quirurgico_event
from app.core.flow_events import emit_module_feedback as emit_module_feedback_core
from app.core.patient_file_utils import (
    safe_filename as safe_filename_core,
    extract_extension as extract_extension_core,
    detect_mime as detect_mime_core,
    resolve_consulta_para_archivo as resolve_consulta_para_archivo_core,
    serialize_archivo_row as serialize_archivo_row_core,
)
from app.core.consulta_payload_utils import (
    parse_int_from_text as parse_int_from_text_core,
    calcular_digito_verificador_curp as calcular_digito_verificador_curp_core,
    normalize_form_data as normalize_form_data_core,
    apply_aliases as apply_aliases_core,
    calcular_indice_tabaquico as calcular_indice_tabaquico_core,
    calcular_scores_litiasis as calcular_scores_litiasis_core,
    generar_nota_soap as generar_nota_soap_core,
    detectar_inconsistencias as detectar_inconsistencias_core,
    aplicar_derivaciones as aplicar_derivaciones_core,
)
from app.core.semantic_runtime import (
    get_semantic_model as get_semantic_model_core,
    build_embedding_text as build_embedding_text_core,
    compute_embedding as compute_embedding_core,
    cosine_similarity as cosine_similarity_core,
    enqueue_embedding as enqueue_embedding_core,
)
from app.core.reporte_ui_helpers import (
    build_jj_metrics as build_jj_metrics_core,
    build_desglose_from_dict_rows as build_desglose_from_dict_rows_core,
    build_bar_chart_from_counts as build_bar_chart_from_counts_core,
    build_hist_chart_from_values as build_hist_chart_from_values_core,
    rank_preventive_rows as rank_preventive_rows_core,
)
from app.core.celery_task_registry import register_main_celery_tasks as register_main_celery_tasks_core
from app.core.template_runtime import (
    template_file_for_ref as template_file_for_ref_core,
    load_template_file_source as load_template_file_source_core,
    prewarm_template_file_cache as prewarm_template_file_cache_core,
    resolve_template_source as resolve_template_source_core,
    render_template_response as render_template_response_core,
    image_file_to_data_url as image_file_to_data_url_core,
    resolve_menu_asset as resolve_menu_asset_core,
)
from app.core.carga_masiva_runtime import (
    CargaMasivaTaskStatus as CargaMasivaTaskStatusCore,
    prepare_excel_row as prepare_excel_row_core,
    process_massive_excel_dataframe as process_massive_excel_dataframe_core,
)
from app.core.db_session_runtime import (
    serialize_model_row as serialize_model_row_core,
    is_model_for_base as is_model_for_base_core,
    capture_dual_write_ops as capture_dual_write_ops_core,
    apply_dual_write_ops as apply_dual_write_ops_core,
    install_dual_write_commit_wrapper as install_dual_write_commit_wrapper_core,
    new_session_with_optional_dual_write as new_session_with_optional_dual_write_core,
    sync_consulta_sensitive_encrypted as sync_consulta_sensitive_encrypted_core,
    sync_surgical_sensitive_encrypted as sync_surgical_sensitive_encrypted_core,
)
from app.core.data_mart_ml_runtime import (
    poblar_dim_fecha as poblar_dim_fecha_core,
    poblar_dimensiones_catalogo as poblar_dimensiones_catalogo_core,
    get_or_create_dim_paciente as get_or_create_dim_paciente_core,
    get_dim_fecha_id as get_dim_fecha_id_core,
    get_dim_diagnostico_id as get_dim_diagnostico_id_core,
    get_dim_procedimiento_id as get_dim_procedimiento_id_core,
    actualizar_data_mart as actualizar_data_mart_core,
    check_data_quality as check_data_quality_core,
    qx_catalogos_payload as qx_catalogos_payload_core,
    entrenar_modelo_riesgo as entrenar_modelo_riesgo_core,
    entrenar_modelo_riesgo_v2 as entrenar_modelo_riesgo_v2_core,
)
from app.core.research_export_runtime import (
    build_research_records as build_research_records_core,
    records_to_csv as records_to_csv_core,
)
from app.core.analytics_endpoint_runtime import (
    predict_risk_payload as predict_risk_payload_core,
    stats_oncology_payload as stats_oncology_payload_core,
    stats_lithiasis_payload as stats_lithiasis_payload_core,
    stats_surgery_payload as stats_surgery_payload_core,
    trends_diagnosticos_payload as trends_diagnosticos_payload_core,
    trends_procedimientos_payload as trends_procedimientos_payload_core,
    trends_lista_espera_payload as trends_lista_espera_payload_core,
    cie11_search_payload as cie11_search_payload_core,
    survival_km_payload as survival_km_payload_core,
    survival_logrank_payload as survival_logrank_payload_core,
)
from app.core.consulta_schema_runtime import (
    ConsultaBase as ConsultaBaseCore,
    ConsultaCreate as ConsultaCreateCore,
    PROTOCOL_PREFIXES as PROTOCOL_PREFIXES_CORE,
    PROTOCOL_FIELDS as PROTOCOL_FIELDS_CORE,
)
from app.core.startup_wiring import (
    run_startup_interconexion_wired as run_startup_interconexion_wired_core,
    run_startup_ai_agents_bootstrap_wired as run_startup_ai_agents_bootstrap_wired_core,
)
from app.api import get_api_routers
from app.services.common import (
    normalize_upper as svc_normalize_upper,
    parse_int as svc_parse_int,
    classify_age_group as svc_classify_age_group,
    normalize_curp as svc_normalize_curp,
    normalize_nss as svc_normalize_nss,
)
from app.services.consulta import (
    preparar_payload_consulta as svc_preparar_payload_consulta,
    mensaje_estatus_consulta as svc_mensaje_estatus_consulta,
)
from app.services.reporte import agregar_timestamp as svc_agregar_timestamp
from app.services.consulta_flow import (
    guardar_consulta_completa_flow as svc_guardar_consulta_completa_flow,
)
from app.services.reporte_flow import (
    render_reporte_html as svc_render_reporte_html,
    render_qx_catalogos_json as svc_render_qx_catalogos_json,
)
from app.services import reporte_metrics_extracted as svc_reporte_metrics_extracted
from app.services import reporte_bi_extracted as svc_reporte_bi_extracted
from app.services import reporte_datasets_extracted as svc_reporte_datasets_extracted
from app.services import quirofano_sync_extracted as svc_quirofano_sync_extracted
from app.services import preventive_priority as svc_preventive_priority
from app.services import files_flow as svc_files_flow
from app.services import schema_extracted as svc_schema_extracted
from app.services import forecast_geo_extracted as svc_forecast_geo_extracted
from app.services import dashboard_extracted as svc_dashboard_extracted
from app.services import form_metadata_flow as svc_form_metadata_flow
from app.services import outbox_flow as svc_outbox_flow
from app.services import event_log_flow as svc_event_log_flow
from app.services import job_registry_flow as svc_job_registry_flow
from app.services import admin_ml_flow as svc_admin_ml_flow
from app.services import analytics_dashboard_api_flow as svc_analytics_dashboard_api_flow
from app.services import analytics_stats_api_flow as svc_analytics_stats_api_flow
from app.services import clinical_events_bridge_flow as svc_clinical_events_bridge_flow
from app.services.carga_masiva_flow import (
    carga_masiva_excel_flow as svc_carga_masiva_excel_flow,
    carga_masiva_status_flow as svc_carga_masiva_status_flow,
)
from app.services.geospatial_flow import (
    admin_geocodificar_flow as svc_admin_geocodificar_flow,
    api_geostats_pacientes_flow as svc_api_geostats_pacientes_flow,
    mapa_epidemiologico_geojson_flow as svc_mapa_epidemiologico_geojson_flow,
    mapa_epidemiologico_flow as svc_mapa_epidemiologico_flow,
)
from app.services.quirofano_backfill_flow import (
    backfill_quirofano_to_surgical_flow as svc_backfill_quirofano_to_surgical_flow,
)
from app.services.hospital_guardia_flow import (
    ensure_hospital_guardia_schema as svc_ensure_hospital_guardia_schema,
)
from app.services.analytics import (
    kaplan_meier as svc_kaplan_meier,
    resolve_survival_event as svc_resolve_survival_event,
    fig_to_base64 as svc_fig_to_base64,
    count_by as svc_count_by,
    build_programmed_age_counts as svc_build_programmed_age_counts,
)
from app.services.files import format_size as svc_format_size
from app.services.fhir_adapter import (
    build_fhir_bundle as svc_build_fhir_bundle,
    build_fhir_condition_only as svc_build_fhir_condition_only,
    build_fhir_patient_only as svc_build_fhir_patient_only,
)
from app.worker import (
    configure_default_beat_schedule as worker_configure_default_beat_schedule,
    run_actualizar_data_mart_sync as worker_run_actualizar_data_mart_sync,
    run_backfill_quirofano as worker_run_backfill_quirofano,
    run_carga_masiva_excel as worker_run_carga_masiva_excel,
    run_entrenar_modelo_riesgo_v2 as worker_run_entrenar_modelo_riesgo_v2,
    run_fau_bot_central_cycle as worker_run_fau_bot_central_cycle,
    run_fau_bot_self_improvement as worker_run_fau_bot_self_improvement,
    run_quirofano_agent_window as worker_run_quirofano_agent_window,
    run_quirofano_programacion_analizar as worker_run_quirofano_programacion_analizar,
)
from app.ai_agents.model_registry import load_model_cached as ai_load_model_cached, warmup_models as ai_warmup_models
from app.ai_agents.vector_store import ensure_consulta_vector_schema, sync_consulta_embedding_vector

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
    import pandas as pd
except Exception:
    pd = None

try:
    import joblib
except Exception:
    joblib = None

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score
except Exception:
    RandomForestClassifier = None
    train_test_split = None
    roc_auc_score = None

try:
    import redis.asyncio as redis_async
except Exception:
    redis_async = None

try:
    from fastapi_cache import FastAPICache
    from fastapi_cache.decorator import cache
except Exception:
    FastAPICache = None

    def cache(*_args, **_kwargs):
        def _decorator(fn):
            return fn
        return _decorator

try:
    from lifelines import KaplanMeierFitter
    from lifelines.statistics import logrank_test
except Exception:
    KaplanMeierFitter = None
    logrank_test = None

try:
    from prophet import Prophet
except Exception:
    Prophet = None

try:
    import folium
    from folium.plugins import MarkerCluster
except Exception:
    folium = None
    MarkerCluster = None

try:
    import requests
except Exception:
    requests = None

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None

try:
    from ai_queue import celery as celery_app
except Exception:
    celery_app = None

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

configure_structured_logging()
logger = logging.getLogger("rnp.main")

# ==========================================
# CONFIGURACIÓN DE SEGURIDAD BÁSICA
# ==========================================
AUTH_SETTINGS = load_auth_settings(logger)
AUTH_ENABLED = AUTH_SETTINGS.enabled
ALLOW_INSECURE_DEFAULT_CREDENTIALS = AUTH_SETTINGS.allow_insecure_default_credentials
AUTH_USER = AUTH_SETTINGS.user
AUTH_PASS = AUTH_SETTINGS.password
AUTH_PUBLIC_PATHS = {
    "/",
    "/inicio/ingresar",
    "/menu-principal",
    "/api/menu/kpis",
}
AUTH_PUBLIC_PREFIXES = (
    "/static/",
)
MENU_IMSS_LOGO_URL = os.getenv(
    "MENU_IMSS_LOGO_URL",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f7/IMSS_Logo.svg/1200px-IMSS_Logo.svg.png",
)
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
APP_STATIC_DIR = os.path.join(BASE_DIR, "app", "static")
APP_STATIC_IMG_DIR = os.path.join(APP_STATIC_DIR, "img")
DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", os.path.join(os.path.expanduser("~"), "Downloads"))
MENU_IMSS_LOGO_PATH = os.getenv("MENU_IMSS_LOGO_PATH", os.path.join(DOWNLOADS_DIR, "LOGOIMSS.jpg"))
MENU_IMSS_PATTERN_URL = os.getenv("MENU_IMSS_PATTERN_URL", "")
MENU_IMSS_PATTERN_PATH = os.getenv("MENU_IMSS_PATTERN_PATH", os.path.join(DOWNLOADS_DIR, "imss_plumas_pattern.png"))
MENU_UROLOGIA_LOGO_URL = os.getenv(
    "MENU_UROLOGIA_LOGO_URL",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/1/13/Blausen_0718_RenalSystem_01.png/512px-Blausen_0718_RenalSystem_01.png",
)
MENU_UROLOGIA_LOGO_PATH = os.getenv("MENU_UROLOGIA_LOGO_PATH", os.path.join(DOWNLOADS_DIR, "UROLOGIA_LOGO.PGN"))
MENU_HOSPITAL_BG_URL = os.getenv(
    "MENU_HOSPITAL_BG_URL",
    "https://images.unsplash.com/photo-1587351021759-3e566b6af7cc?auto=format&fit=crop&w=1800&q=80",
)
MENU_HOSPITAL_BG_PATH = os.getenv("MENU_HOSPITAL_BG_PATH") or os.path.join(DOWNLOADS_DIR, "hospital_bg.jpg")
MENU_IMSS_LOGO_FALLBACK_PATH = os.path.join(APP_STATIC_IMG_DIR, "imss_logo_fallback.svg")
MENU_IMSS_PATTERN_FALLBACK_PATH = os.path.join(APP_STATIC_IMG_DIR, "imss_pattern_fallback.svg")
MENU_UROLOGIA_LOGO_FALLBACK_PATH = os.path.join(APP_STATIC_IMG_DIR, "urologia_logo_fallback.svg")
MENU_HOSPITAL_BG_FALLBACK_PATH = os.path.join(APP_STATIC_IMG_DIR, "hospital_bg_fallback.svg")
CONNECTIVITY_MODE = (os.getenv("RNP_CONNECTIVITY_MODE", "hybrid") or "hybrid").strip().lower()
OFFLINE_STRICT_MODE = CONNECTIVITY_MODE in ("offline", "strict_offline")
CSRF_COOKIE_NAME = "csrf_token"
SECURE_COOKIES = os.getenv("SECURE_COOKIES", "false").lower() in ("1", "true", "yes")
FORCE_HTTPS = os.getenv("FORCE_HTTPS", "false").lower() in ("1", "true", "yes")
TRUST_X_FORWARDED_PROTO = os.getenv("TRUST_X_FORWARDED_PROTO", "true").lower() in ("1", "true", "yes")
ENABLE_HSTS = os.getenv("ENABLE_HSTS", "false").lower() in ("1", "true", "yes")
try:
    HSTS_MAX_AGE = max(0, int(os.getenv("HSTS_MAX_AGE", "31536000") or "31536000"))
except Exception:
    HSTS_MAX_AGE = 31536000
ENABLE_PII_ENCRYPTION = os.getenv("ENABLE_PII_ENCRYPTION", "true").lower() in ("1", "true", "yes")
DATA_ENCRYPTION_KEY = (os.getenv("DATA_ENCRYPTION_KEY", "") or "").strip()
REQUIRED_SENTINELS = {"NO_APLICA", "NEGADO", "DESCONOCIDO"}
ASYNC_EMBEDDINGS = os.getenv("ASYNC_EMBEDDINGS", "false").lower() in ("1", "true", "yes")
SURVIVAL_EVENT_FIELD = os.getenv("SURVIVAL_EVENT_FIELD", "estatus_protocolo")
SURVIVAL_EVENT_VALUE = os.getenv("SURVIVAL_EVENT_VALUE", "completo")
MODELO_RIESGO_PATH = os.getenv("MODELO_RIESGO_PATH", "modelo_riesgo_quirurgico.pkl")
MODELO_RIESGO_V2_PATH = os.getenv("MODELO_RIESGO_V2_PATH", "modelo_riesgo_quirurgico_v2.pkl")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
GEOCODER_URL = os.getenv("GEOCODER_URL", "https://nominatim.openstreetmap.org/search")
GEOCODER_USER_AGENT = os.getenv("GEOCODER_USER_AGENT", "IMSS-Urologia-App/1.0")
STARTUP_INTERCONEXION_MODE = normalize_startup_mode(
    os.getenv("STARTUP_INTERCONEXION_MODE", "background"),
    default="background",
)
try:
    STARTUP_INTERCONEXION_DELAY_SEC = max(0.0, float(os.getenv("STARTUP_INTERCONEXION_DELAY_SEC", "0") or "0"))
except Exception:
    STARTUP_INTERCONEXION_DELAY_SEC = 0.0
AI_WARMUP_MODE = normalize_startup_mode(
    os.getenv("AI_WARMUP_MODE", "background"),
    default="background",
)
try:
    AI_WARMUP_DELAY_SEC = max(0.0, float(os.getenv("AI_WARMUP_DELAY_SEC", "0") or "0"))
except Exception:
    AI_WARMUP_DELAY_SEC = 0.0
SURGICAL_ALEMBIC_ENABLED = os.getenv("SURGICAL_ALEMBIC_ENABLED", "false").lower() in ("1", "true", "yes")
SURGICAL_ALEMBIC_CONFIG = os.getenv("SURGICAL_ALEMBIC_CONFIG", "alembic_surgical.ini")
DB_MIGRATION_STAGE = (os.getenv("DB_MIGRATION_STAGE", "off") or "off").strip().lower()
DATABASE_PLATFORM_TARGET = (os.getenv("DATABASE_PLATFORM_TARGET", "postgres") or "postgres").strip().lower()
CLINICAL_SHADOW_DATABASE_URL = (os.getenv("CLINICAL_SHADOW_DATABASE_URL", "") or "").strip()
SURGICAL_SHADOW_DATABASE_URL = (os.getenv("SURGICAL_SHADOW_DATABASE_URL", "") or "").strip()
DUAL_WRITE_ENABLED = os.getenv("DB_DUAL_WRITE", "false").lower() in ("1", "true", "yes") or DB_MIGRATION_STAGE == "dual_write"
PATIENT_FILES_DIR = os.path.abspath(os.getenv("PATIENT_FILES_DIR", "./patient_files"))
try:
    MAX_PATIENT_FILE_SIZE_MB = max(1, int(os.getenv("MAX_PATIENT_FILE_SIZE_MB", "50")))
except Exception:
    MAX_PATIENT_FILE_SIZE_MB = 50
ALLOWED_PATIENT_FILE_EXTENSIONS = {
    ".xlsx",
    ".xls",
    ".doc",
    ".docx",
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".pgn",
    ".dcm",
    ".dicom",
}
ALLOWED_MASS_UPLOAD_EXTENSIONS = {".xlsx", ".xls"}
_redis_client = None
_fernet_cipher = None
if ENABLE_PII_ENCRYPTION and Fernet is not None and DATA_ENCRYPTION_KEY:
    try:
        _fernet_cipher = Fernet(DATA_ENCRYPTION_KEY.encode("utf-8"))
    except Exception:
        _fernet_cipher = None
        logger.warning(
            {
                "event": "invalid_data_encryption_key",
                "detail": "DATA_ENCRYPTION_KEY inválida; se omite cifrado de columnas espejo.",
            }
        )
elif ENABLE_PII_ENCRYPTION and Fernet is None:
    logger.warning(
        {
            "event": "encryption_dependency_missing",
            "detail": "cryptography no disponible; se omite cifrado de columnas espejo.",
        }
    )


def _log_suppressed_exception(event_name: str, exc: Exception, **extra: Any) -> None:
    payload: Dict[str, Any] = {"event": event_name}
    payload.update(extra)
    payload["error"] = str(exc)
    logger.warning(payload)


def _request_is_https(request: Request) -> bool:
    return request_is_https(request, trust_forwarded_proto=TRUST_X_FORWARDED_PROTO)


def _encrypt_sensitive_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if _fernet_cipher is None:
        return None
    try:
        token = _fernet_cipher.encrypt(str(value).encode("utf-8"))
        return token.decode("utf-8")
    except Exception as exc:
        _log_suppressed_exception("sensitive_encrypt_failed", exc)
        return None

security = HTTPBasic(auto_error=False)

QUIROFANO_SEXOS = ["MASCULINO", "FEMENINO"]
QUIROFANO_PATOLOGIAS = [
    "CANCER RENAL",
    "CANCER UROTELIAL TRACTO SUPERIOR",
    "TUMOR SUPRARRENAL",
    "CANCER DE PROSTATA",
    "CANCER DE VEJIGA",
    "CANCER DE TESTICULO",
    "CANCER DE PENE",
    "TUMOR DE COMPORTAMIENTO INCIERTO PROSTATA",
    "CALCULO DEL RIÑON",
    "CALCULO DEL URETER",
    "CALCULO DE LA VEJIGA",
    "CRECIMIENTO PROSTATICO OBSTRUCTIVO",
    "FISTULA VESICOVAGINAL",
    "FISTULA URETERO VAGINAL",
    "ABSCESO RENAL",
    "ABSCESO PROSTÁTICO",
    "PIELONEFRITIS",
    "EXCLUSION RENAL",
]
QUIROFANO_PATOLOGIAS_ONCOLOGICAS = {
    "CANCER RENAL",
    "CANCER UROTELIAL TRACTO SUPERIOR",
    "TUMOR SUPRARRENAL",
    "CANCER DE PROSTATA",
    "CANCER DE VEJIGA",
    "CANCER DE TESTICULO",
    "CANCER DE PENE",
}
QUIROFANO_PATOLOGIAS_LITIASIS = {
    "CALCULO DEL RIÑON",
    "CALCULO DEL URETER",
    "CALCULO DE LA VEJIGA",
}

QUIROFANO_PROCEDIMIENTOS = [
    "NEFRECTOMIA RADICAL",
    "NEFRECTOMIA SIMPLE",
    "NEFROURETERECTOMIA CON RODETE VESICAL",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE CONDUCTO ILEAL",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE URETEROSTOMAS",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE NEOVEJIGA",
    "RESECCION TRANSURETRAL DE VEJIGA",
    "PROSTATECTOMIA RADICAL",
    "PROSTATECTOMIA RADICAL + LINFADENECTOMIA PELVICA",
    "ORQUIECTOMIA RADICAL",
    "PENECTOMIA TOTAL",
    "PENECTOMIA PARCIAL",
    "CISTOSTOMIA",
    "UTIO",
    "NEFROLITOTRICIA LASER FLEXIBLE",
    "URETEROLITOTRICIA LASER FLEXIBLE",
    "CISTOLITOTRICIA",
    "REIMPLANTE URETERAL",
    "NEFROLITOTRICIA LASER FLEXIBLE CON SISTEMA DE SUCCION",
    "NEFROLITOTOMIA PERCUTANEA TRACTO ESTANDARD",
    "NEFROLITOTOMIA PERCUTANEA POR TRACTO MINITURIZADO (MINIPERC)",
    "ECIRS",
    "URETEROLITOTOMIA",
    "PIELOLITOTOMIA",
]
QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE = {
    "NEFRECTOMIA RADICAL",
    "NEFRECTOMIA SIMPLE",
    "NEFROURETERECTOMIA CON RODETE VESICAL",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE CONDUCTO ILEAL",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE URETEROSTOMAS",
    "CISTOPROSTATECTOMIA RADICAL + FORMACION DE NEOVEJIGA",
    "PROSTATECTOMIA RADICAL",
    "PROSTATECTOMIA RADICAL + LINFADENECTOMIA PELVICA",
    "URETEROLITOTOMIA",
    "PIELOLITOTOMIA",
    "REIMPLANTE URETERAL",
}
QUIROFANO_PROCEDIMIENTOS_ABIERTOS = {
    "ORQUIECTOMIA RADICAL",
    "PENECTOMIA TOTAL",
    "PENECTOMIA PARCIAL",
    "CISTOSTOMIA",
}
QUIROFANO_PROCEDIMIENTOS_ENDOSCOPICOS = {
    "UTIO",
    "NEFROLITOTRICIA LASER FLEXIBLE",
    "URETEROLITOTRICIA LASER FLEXIBLE",
    "CISTOLITOTRICIA",
    "RESECCION TRANSURETRAL DE VEJIGA",
}
QUIROFANO_PROCEDIMIENTO_SUCCION = "NEFROLITOTRICIA LASER FLEXIBLE CON SISTEMA DE SUCCION"
QUIROFANO_PROCEDIMIENTOS_PERCUTANEOS = {
    "NEFROLITOTOMIA PERCUTANEA TRACTO ESTANDARD",
    "NEFROLITOTOMIA PERCUTANEA POR TRACTO MINITURIZADO (MINIPERC)",
    "ECIRS",
}

QUIROFANO_INSUMOS = [
    "EQUIPO DE CIRUGIA ABIERTA",
    "EQUIPO DE CIRUGIA LAPAROSCOPICA (INTERMED)",
    "ENDOURO (INTERMED)",
]

QUIROFANO_HEMODERIVADOS = [
    "PAQUETE GLOBULAR",
    "PLASMA FRESCO CONGELADO",
    "CONCENTRADO PLAQUETARIO",
]

QUIROFANO_PATOLOGIA_CIE10_MAP = {
    "CANCER RENAL": "C64",
    "CANCER UROTELIAL TRACTO SUPERIOR": "C65-C66",
    "TUMOR SUPRARRENAL": "D44.1",
    "CANCER DE PROSTATA": "C61",
    "CANCER DE VEJIGA": "C67",
    "CANCER DE TESTICULO": "C62",
    "CANCER DE PENE": "C60",
    "TUMOR DE COMPORTAMIENTO INCIERTO PROSTATA": "D40.0",
    "CALCULO DEL RIÑON": "N20.0",
    "CALCULO DEL URETER": "N20.1",
    "CALCULO DE LA VEJIGA": "N21.0",
    "CRECIMIENTO PROSTATICO OBSTRUCTIVO": "N40",
    "FISTULA VESICOVAGINAL": "N82.0",
    "FISTULA URETERO VAGINAL": "N82.1",
    "ABSCESO RENAL": "N15.1",
    "ABSCESO PROSTÁTICO": "N41.2",
    "PIELONEFRITIS": "N10",
    "EXCLUSION RENAL": "N28.8",
}

# ==========================================
# CONFIGURACIÓN DE BASE DE DATOS (SQLite + SQLAlchemy)
# ==========================================
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./urologia.db")
IS_SQLITE = SQLALCHEMY_DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False} if IS_SQLITE else {}
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args)

SURGICAL_DATABASE_URL = os.getenv("SURGICAL_DATABASE_URL", "sqlite:///./urologia_quirurgico.db")
SURGICAL_IS_SQLITE = SURGICAL_DATABASE_URL.startswith("sqlite")
surgical_connect_args = {"check_same_thread": False} if SURGICAL_IS_SQLITE else {}
surgical_engine = create_engine(SURGICAL_DATABASE_URL, connect_args=surgical_connect_args)
if DB_MIGRATION_STAGE == "cutover" and (IS_SQLITE or SURGICAL_IS_SQLITE):
    logger.warning(
        {
            "event": "cutover_database_not_postgresql",
            "clinical_sqlite": IS_SQLITE,
            "surgical_sqlite": SURGICAL_IS_SQLITE,
        }
    )


def _build_optional_engine(db_url: str):
    return build_optional_engine(db_url)


clinical_shadow_engine = None
ClinicalShadowSessionLocal = None
if CLINICAL_SHADOW_DATABASE_URL and CLINICAL_SHADOW_DATABASE_URL != SQLALCHEMY_DATABASE_URL:
    clinical_shadow_engine = _build_optional_engine(CLINICAL_SHADOW_DATABASE_URL)
    if clinical_shadow_engine is not None:
        ClinicalShadowSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=clinical_shadow_engine)

surgical_shadow_engine = None
SurgicalShadowSessionLocal = None
if SURGICAL_SHADOW_DATABASE_URL and SURGICAL_SHADOW_DATABASE_URL != SURGICAL_DATABASE_URL:
    surgical_shadow_engine = _build_optional_engine(SURGICAL_SHADOW_DATABASE_URL)
    if surgical_shadow_engine is not None:
        SurgicalShadowSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=surgical_shadow_engine)

if IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

if SURGICAL_IS_SQLITE:
    @event.listens_for(surgical_engine, "connect")
    def _set_surgical_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
JSON_TYPE = JSONB if (not IS_SQLITE and SQLALCHEMY_DATABASE_URL.startswith("postgres")) else JSON
SurgicalSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=surgical_engine)
SurgicalBase = declarative_base()
SURGICAL_JSON_TYPE = JSONB if (not SURGICAL_IS_SQLITE and SURGICAL_DATABASE_URL.startswith("postgres")) else JSON

# ==========================================
# ENTORNO JINJA2 (AUTOESCAPE)
# ==========================================
jinja_env = Environment(autoescape=select_autoescape(default=True, enabled_extensions=("html", "xml")))
_template_cache: Dict[str, Any] = {}

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
    # Columnas espejo cifradas (aditivas) para protección de datos sensibles.
    curp_enc = Column(Text)
    nss_enc = Column(Text)
    nombre_enc = Column(Text)
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
    telefono_enc = Column(Text)
    email_enc = Column(Text)

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
    nss = Column(String(20), index=True)
    patient_uid = Column(String(64), index=True)
    agregado_medico = Column(String(80))
    medico_a_cargo = Column(String(120), index=True)
    nombre_completo = Column(String(220), index=True)
    edad = Column(Integer, index=True)
    sexo = Column(String(20), index=True)
    diagnostico = Column(String(220), index=True)
    hgz_envio = Column(String(120), index=True)
    estatus_detalle = Column(String(50), index=True)
    dias_hospitalizacion = Column(Integer, index=True)
    dias_postquirurgicos = Column(Integer, index=True)
    incapacidad = Column(String(10), index=True)
    incapacidad_emitida = Column(String(10), index=True)
    programado = Column(String(10), index=True)
    medico_programado = Column(String(120), index=True)
    turno_programado = Column(String(40), index=True)
    urgencia = Column(String(10), index=True)
    urgencia_tipo = Column(String(120), index=True)
    ingreso_tipo = Column(String(40), index=True)
    estado_clinico = Column(String(40), index=True)
    uci = Column(String(10), index=True)
    idempotency_key = Column(String(220), index=True)
    observaciones = Column(Text)
    estatus = Column(String(50), default="ACTIVO")  # ACTIVO, EGRESADO, TRASLADO

    consulta = relationship("ConsultaDB")


class HospitalIngresoPreopDB(Base):
    __tablename__ = "hospital_ingresos_preop"

    id = Column(Integer, primary_key=True, index=True)
    hospitalizacion_id = Column(
        Integer,
        ForeignKey("hospitalizaciones.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        unique=True,
    )
    created_at = Column(DateTime, default=utcnow, index=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, index=True)

    preop_enabled = Column(Boolean, default=False, index=True)
    hora_ingreso = Column(String(20))
    afiliacion_text = Column(Text)
    servicio_entrada = Column(String(120))
    riesgo_caidas = Column(String(20))
    residentes_text = Column(Text)
    resumen_ingreso_text = Column(Text)
    ahf_text = Column(Text)
    apnp_text = Column(Text)
    app_text = Column(Text)
    alergias_text = Column(Text)
    meds_cronicos_text = Column(Text)
    aqx_text = Column(Text)
    padecimiento_actual_text = Column(Text)
    diuresis_24h_ml = Column(Float)

    ta_sis = Column(Integer)
    ta_dia = Column(Integer)
    fc = Column(Integer)
    fr = Column(Integer)
    temp_c = Column(Float)
    spo2 = Column(Float)
    peso_kg = Column(Float)
    talla_m = Column(Float)
    imc = Column(Float)

    exploracion_fisica_text = Column(Text)
    tacto_rectal_text = Column(Text)
    prostata_estimacion_g = Column(Float)
    nodulo_pct = Column(Float)
    labs_text = Column(Text)
    urocultivo_status = Column(String(20))
    urocultivo_result_text = Column(Text)
    ape_text = Column(Text)
    ape_series_json = Column(Text)
    imagenologia_text = Column(Text)
    rmmp_fecha = Column(Date)
    prostata_volumen_cc = Column(Float)
    pirads_max = Column(Integer)
    rx_torax_fecha = Column(Date)

    valoracion_preop_text = Column(Text)
    asa = Column(String(40))
    goldman = Column(String(40))
    detsky = Column(String(40))
    lee = Column(String(40))
    caprini = Column(String(40))
    apto_qx_bool = Column(String(10))
    vpo_text = Column(Text)

    diagnostico_preop = Column(Text)
    procedimiento_text = Column(Text)
    tipo_procedimiento = Column(String(120))
    fecha_cirugia = Column(Date)
    cirujano_text = Column(Text)
    pronostico_text = Column(Text)
    indicaciones_preop_text = Column(Text)
    incapacidad_detalle_text = Column(Text)
    firmas_json = Column(Text)
    payload_json = Column(JSON_TYPE)

    hospitalizacion = relationship("HospitalizacionDB")


class HospitalizacionContextSnapshotDB(Base):
    __tablename__ = "hospitalizacion_context_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    hospitalizacion_id = Column(
        Integer,
        ForeignKey("hospitalizaciones.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    consulta_id = Column(Integer, index=True, nullable=True)
    nss = Column(String(20), index=True)
    curp = Column(String(18), index=True)
    source = Column(String(80), index=True, default="hospitalizacion_nuevo")
    context_json = Column(JSON_TYPE)
    created_at = Column(DateTime, default=utcnow, index=True)

    hospitalizacion = relationship("HospitalizacionDB")


class HospitalGuardiaDB(Base):
    __tablename__ = "hospital_guardias"

    id = Column(Integer, primary_key=True, index=True)
    fecha = Column(Date, index=True, nullable=False)
    medico = Column(String(120), index=True, nullable=False)
    turno = Column(String(40), index=True, nullable=False)
    notas = Column(Text)
    creado_en = Column(DateTime, default=utcnow, index=True)


class HospitalCensoDiarioDB(Base):
    __tablename__ = "hospital_censo_diario"

    id = Column(Integer, primary_key=True, index=True)
    fecha = Column(Date, unique=True, index=True, nullable=False)
    pacientes_json = Column(JSON_TYPE)
    guardia_json = Column(JSON_TYPE)
    total_hospitalizados = Column(Integer, default=0)
    total_operados = Column(Integer, default=0)
    total_altas = Column(Integer, default=0)
    total_ingresos = Column(Integer, default=0)
    actualizado_en = Column(DateTime, default=utcnow, onupdate=datetime.utcnow, index=True)

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
    timestamp = Column(DateTime, default=utcnow, index=True)
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
    timestamp = Column(DateTime, default=utcnow, index=True)
    test_code = Column(String(100))
    test_name = Column(String(200))
    value = Column(String(200))
    unit = Column(String(50))
    source = Column(String(100))


class ArchivoPacienteDB(Base):
    __tablename__ = "archivos_paciente"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, ForeignKey("consultas.id", ondelete="CASCADE"), index=True, nullable=False)
    nombre_original = Column(String(255), nullable=False)
    nombre_guardado = Column(String(255), nullable=False, unique=True, index=True)
    extension = Column(String(20), index=True)
    mime_type = Column(String(120))
    storage_path = Column(Text, nullable=False)
    tamano_bytes = Column(Integer)
    descripcion = Column(String(255))
    subido_por = Column(String(100))
    fecha_subida = Column(DateTime, default=utcnow, index=True)

    consulta = relationship("ConsultaDB")


class SurgicalProgramacionDB(SurgicalBase):
    __tablename__ = "surgical_programaciones"

    id = Column(Integer, primary_key=True, index=True)
    quirofano_id = Column(Integer, index=True, unique=True, nullable=False)
    consulta_id = Column(Integer, index=True, nullable=False)
    curp = Column(String(18), index=True)
    nss = Column(String(20), index=True)
    agregado_medico = Column(String(80))
    paciente_nombre = Column(String(200), index=True)
    # Columnas espejo cifradas (aditivas) para protección de datos sensibles.
    curp_enc = Column(Text)
    nss_enc = Column(Text)
    paciente_nombre_enc = Column(Text)
    edad = Column(Integer, index=True)
    edad_grupo = Column(String(40), index=True)
    sexo = Column(String(20), index=True)
    grupo_sexo = Column(String(20), index=True)
    diagnostico_principal = Column(String(120), index=True)
    patologia = Column(String(180), index=True)
    grupo_patologia = Column(String(80), index=True)
    procedimiento = Column(String(200))
    procedimiento_programado = Column(String(220), index=True)
    grupo_procedimiento = Column(String(80), index=True)
    abordaje = Column(String(50), index=True)
    tipo_neovejiga = Column(String(120))
    sistema_succion = Column(String(50), index=True)
    insumos_solicitados = Column(Text)
    requiere_intermed = Column(String(10), index=True)
    hgz = Column(String(120), index=True)
    cie11_codigo = Column(String(20), index=True)
    snomed_codigo = Column(String(20), index=True)
    cie9mc_codigo = Column(String(20), index=True)
    # Campos oncológicos
    tnm = Column(String(80), index=True)
    ecog = Column(String(40), index=True)
    charlson = Column(String(40), index=True)
    etapa_clinica = Column(String(80), index=True)
    ipss = Column(String(40))
    gleason = Column(String(40))
    ape = Column(String(80))
    rtup_previa = Column(String(20))
    tacto_rectal = Column(String(120))
    historial_ape = Column(Text)
    # Campos litiasis
    uh_rango = Column(String(80), index=True)
    litiasis_tamano_rango = Column(String(40), index=True)
    litiasis_subtipo_20 = Column(String(60), index=True)
    litiasis_ubicacion = Column(String(80), index=True)
    litiasis_ubicacion_multiple = Column(String(200))
    hidronefrosis = Column(String(20), index=True)
    cirujano = Column(String(100))
    anestesiologo = Column(String(100))
    quirofano = Column(String(50))
    fecha_programada = Column(Date, index=True)
    fecha_realizacion = Column(Date, nullable=True)
    estatus = Column(String(50), index=True)
    cancelacion_codigo = Column(String(40), index=True)
    cancelacion_categoria = Column(String(120), index=True)
    cancelacion_concepto = Column(String(255), index=True)
    cancelacion_detalle = Column(Text)
    cancelacion_fecha = Column(DateTime, index=True)
    cancelacion_usuario = Column(String(120), index=True)
    protocolo_completo = Column(String(20), index=True)
    pendiente_programar = Column(String(20), index=True)
    sangrado_ml = Column(Float, index=True)
    tiempo_quirurgico_min = Column(Float, index=True)
    transfusion = Column(String(20), index=True)
    solicita_hemoderivados = Column(String(20), index=True)
    hemoderivados_pg_solicitados = Column(Integer, index=True)
    hemoderivados_pfc_solicitados = Column(Integer, index=True)
    hemoderivados_cp_solicitados = Column(Integer, index=True)
    uso_hemoderivados = Column(String(20), index=True)
    hemoderivados_pg_utilizados = Column(Integer, index=True)
    hemoderivados_pfc_utilizados = Column(Integer, index=True)
    hemoderivados_cp_utilizados = Column(Integer, index=True)
    antibiotico = Column(String(220))
    clavien_dindo = Column(String(40), index=True)
    margen_quirurgico = Column(String(80), index=True)
    neuropreservacion = Column(String(40), index=True)
    linfadenectomia = Column(String(40), index=True)
    reingreso_30d = Column(String(20), index=True)
    reintervencion_30d = Column(String(20), index=True)
    mortalidad_30d = Column(String(20), index=True)
    reingreso_90d = Column(String(20), index=True)
    reintervencion_90d = Column(String(20), index=True)
    mortalidad_90d = Column(String(20), index=True)
    stone_free = Column(String(40), index=True)
    composicion_lito = Column(String(180), index=True)
    recurrencia_litiasis = Column(String(40), index=True)
    cateter_jj_colocado = Column(String(20), index=True)
    fecha_colocacion_jj = Column(Date, index=True)
    diagnostico_postop = Column(String(220), index=True)
    procedimiento_realizado = Column(String(220), index=True)
    nota_postquirurgica = Column(Text)
    complicaciones_postquirurgicas = Column(Text)
    fecha_postquirurgica = Column(Date, index=True)
    fecha_ingreso_pendiente_programar = Column(DateTime, index=True)
    dias_en_espera = Column(Integer, index=True)
    prioridad_clinica = Column(String(20), index=True)
    motivo_prioridad = Column(String(255))
    riesgo_cancelacion_predicho = Column(Float, index=True)
    score_preventivo = Column(Float, index=True)
    notas = Column(Text)
    urgencia_programacion_id = Column(Integer, index=True)
    modulo_origen = Column(String(50), default="quirofano", index=True)
    creado_en = Column(DateTime, default=utcnow, nullable=False)
    actualizado_en = Column(DateTime, default=utcnow, onupdate=datetime.utcnow, nullable=False)


class SurgicalPostquirurgicaDB(SurgicalBase):
    __tablename__ = "surgical_postquirurgicas"

    id = Column(Integer, primary_key=True, index=True)
    surgical_programacion_id = Column(Integer, index=True, nullable=False)
    quirofano_id = Column(Integer, index=True, nullable=False)
    consulta_id = Column(Integer, index=True, nullable=False)
    fecha_realizacion = Column(Date, index=True)
    cirujano = Column(String(100), index=True)
    sangrado_ml = Column(Float, index=True)
    tiempo_quirurgico_min = Column(Float, index=True)
    transfusion = Column(String(20), index=True)
    uso_hemoderivados = Column(String(20), index=True)
    hemoderivados_pg_utilizados = Column(Integer, index=True)
    hemoderivados_pfc_utilizados = Column(Integer, index=True)
    hemoderivados_cp_utilizados = Column(Integer, index=True)
    antibiotico = Column(String(220))
    clavien_dindo = Column(String(40), index=True)
    margen_quirurgico = Column(String(80), index=True)
    neuropreservacion = Column(String(40), index=True)
    linfadenectomia = Column(String(40), index=True)
    reingreso_30d = Column(String(20), index=True)
    reintervencion_30d = Column(String(20), index=True)
    mortalidad_30d = Column(String(20), index=True)
    reingreso_90d = Column(String(20), index=True)
    reintervencion_90d = Column(String(20), index=True)
    mortalidad_90d = Column(String(20), index=True)
    stone_free = Column(String(40), index=True)
    composicion_lito = Column(String(180), index=True)
    recurrencia_litiasis = Column(String(40), index=True)
    cateter_jj_colocado = Column(String(20), index=True)
    fecha_colocacion_jj = Column(Date, index=True)
    diagnostico_postop = Column(String(220), index=True)
    procedimiento_realizado = Column(String(220), index=True)
    complicaciones = Column(Text)
    nota_postquirurgica = Column(Text)
    creado_en = Column(DateTime, default=utcnow, nullable=False, index=True)
    actualizado_en = Column(DateTime, default=utcnow, onupdate=datetime.utcnow, nullable=False)


class SurgicalUrgenciaProgramacionDB(SurgicalBase):
    __tablename__ = "surgical_urgencias_programaciones"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, index=True, nullable=True)
    surgical_programacion_id = Column(Integer, index=True, nullable=True)
    curp = Column(String(18), index=True)
    nss = Column(String(20), index=True)
    agregado_medico = Column(String(80))
    paciente_nombre = Column(String(220), index=True)
    edad = Column(Integer, index=True)
    edad_grupo = Column(String(40), index=True)
    sexo = Column(String(20), index=True)
    grupo_sexo = Column(String(20), index=True)
    patologia = Column(String(220), index=True)
    patologia_cie10 = Column(String(40), index=True)
    grupo_patologia = Column(String(80), index=True)
    procedimiento_programado = Column(String(240), index=True)
    grupo_procedimiento = Column(String(80), index=True)
    abordaje = Column(String(50), index=True)
    tipo_neovejiga = Column(String(120))
    sistema_succion = Column(String(50), index=True)
    insumos_solicitados = Column(Text)
    requiere_intermed = Column(String(10), index=True)
    solicita_hemoderivados = Column(String(20), index=True)
    hemoderivados_pg_solicitados = Column(Integer, index=True)
    hemoderivados_pfc_solicitados = Column(Integer, index=True)
    hemoderivados_cp_solicitados = Column(Integer, index=True)
    hgz = Column(String(120), index=True)
    tnm = Column(String(80), index=True)
    ecog = Column(String(40), index=True)
    charlson = Column(String(40), index=True)
    etapa_clinica = Column(String(80), index=True)
    ipss = Column(String(40))
    gleason = Column(String(40))
    ape = Column(String(80))
    rtup_previa = Column(String(20))
    tacto_rectal = Column(String(120))
    historial_ape = Column(Text)
    uh_rango = Column(String(80), index=True)
    litiasis_tamano_rango = Column(String(40), index=True)
    litiasis_subtipo_20 = Column(String(60), index=True)
    litiasis_ubicacion = Column(String(80), index=True)
    litiasis_ubicacion_multiple = Column(String(220))
    hidronefrosis = Column(String(20), index=True)
    fecha_urgencia = Column(Date, index=True)
    fecha_realizacion = Column(Date, index=True)
    estatus = Column(String(50), index=True)
    cancelacion_codigo = Column(String(40), index=True)
    cancelacion_categoria = Column(String(120), index=True)
    cancelacion_concepto = Column(String(255), index=True)
    cancelacion_detalle = Column(Text)
    cancelacion_fecha = Column(DateTime, index=True)
    cancelacion_usuario = Column(String(120), index=True)
    cirujano = Column(String(120), index=True)
    sangrado_ml = Column(Float, index=True)
    tiempo_quirurgico_min = Column(Float, index=True)
    transfusion = Column(String(20), index=True)
    uso_hemoderivados = Column(String(20), index=True)
    hemoderivados_pg_utilizados = Column(Integer, index=True)
    hemoderivados_pfc_utilizados = Column(Integer, index=True)
    hemoderivados_cp_utilizados = Column(Integer, index=True)
    stone_free = Column(String(40), index=True)
    composicion_lito = Column(String(180), index=True)
    recurrencia_litiasis = Column(String(40), index=True)
    cateter_jj_colocado = Column(String(20), index=True)
    fecha_colocacion_jj = Column(Date, index=True)
    diagnostico_postop = Column(String(220), index=True)
    procedimiento_realizado = Column(String(240), index=True)
    nota_postquirurgica = Column(Text)
    complicaciones_postquirurgicas = Column(Text)
    cie11_codigo = Column(String(20), index=True)
    snomed_codigo = Column(String(20), index=True)
    cie9mc_codigo = Column(String(20), index=True)
    modulo_origen = Column(String(50), default="QUIROFANO_URGENCIA", index=True)
    creado_en = Column(DateTime, default=utcnow, nullable=False)
    actualizado_en = Column(DateTime, default=utcnow, onupdate=datetime.utcnow, nullable=False)


class HechoFlujoQuirurgico(SurgicalBase):
    __tablename__ = "hecho_flujo_quirurgico"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, index=True, nullable=False)
    surgical_programacion_id = Column(Integer, index=True)
    quirofano_id = Column(Integer, index=True)
    evento = Column(String(60), index=True, nullable=False)
    estatus = Column(String(50), index=True)
    edad = Column(Integer, index=True)
    edad_grupo = Column(String(40), index=True)
    sexo = Column(String(20), index=True)
    nss = Column(String(20), index=True)
    hgz = Column(String(120), index=True)
    diagnostico = Column(String(220), index=True)
    procedimiento = Column(String(220), index=True)
    ecog = Column(String(40), index=True)
    cirujano = Column(String(100), index=True)
    sangrado_ml = Column(Float, index=True)
    metadata_json = Column(SURGICAL_JSON_TYPE)
    creado_en = Column(DateTime, default=utcnow, nullable=False, index=True)


class SurgicalFeedbackDB(SurgicalBase):
    __tablename__ = "surgical_feedback"

    id = Column(Integer, primary_key=True, index=True)
    consulta_id = Column(Integer, index=True, nullable=False)
    modulo = Column(String(50), index=True, nullable=False)
    referencia_id = Column(String(80), nullable=True)
    payload = Column(SURGICAL_JSON_TYPE)
    creado_en = Column(DateTime, default=utcnow, nullable=False)


class DimFecha(SurgicalBase):
    __tablename__ = "dim_fecha"

    id = Column(Integer, primary_key=True)
    fecha = Column(Date, unique=True, nullable=False, index=True)
    anio = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    dia = Column(Integer, nullable=False)
    dia_semana = Column(Integer)
    semana_anio = Column(Integer)
    trimestre = Column(Integer)
    es_fin_semana = Column(Boolean, default=False)


class DimPaciente(SurgicalBase):
    __tablename__ = "dim_paciente"

    id = Column(Integer, primary_key=True)
    paciente_hash = Column(String(64), unique=True, index=True)
    sexo = Column(String(20))
    edad_quinquenio = Column(String(20), index=True)
    edad_grupo_epidemiologico = Column(String(20), index=True)
    hgz = Column(String(100), index=True)
    alcaldia = Column(String(100), index=True)
    colonia = Column(String(120), index=True)
    cp = Column(String(20), index=True)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)


class DimDiagnostico(SurgicalBase):
    __tablename__ = "dim_diagnostico"

    id = Column(Integer, primary_key=True)
    nombre = Column(String(200), unique=True, index=True)
    grupo = Column(String(80), index=True)
    cie11_codigo = Column(String(20), index=True)
    snomed_codigo = Column(String(20), index=True)


class DimProcedimiento(SurgicalBase):
    __tablename__ = "dim_procedimiento"

    id = Column(Integer, primary_key=True)
    nombre = Column(String(240), unique=True, index=True)
    grupo = Column(String(80), index=True)
    cie9mc_codigo = Column(String(20), index=True)


class HechoProgramacionQuirurgica(SurgicalBase):
    __tablename__ = "hecho_programacion_quirurgica"

    id = Column(Integer, primary_key=True)
    fecha_id = Column(Integer, ForeignKey("dim_fecha.id"), index=True)
    paciente_id = Column(Integer, ForeignKey("dim_paciente.id"), index=True)
    diagnostico_id = Column(Integer, ForeignKey("dim_diagnostico.id"), index=True)
    procedimiento_id = Column(Integer, ForeignKey("dim_procedimiento.id"), index=True)
    grupo_patologia = Column(String(80), index=True)
    grupo_procedimiento = Column(String(80), index=True)
    ecog = Column(String(40), index=True)
    charlson = Column(String(40), index=True)
    tnm = Column(String(80), index=True)
    uh_rango = Column(String(80), index=True)
    litiasis_tamano = Column(String(40), index=True)
    requiere_intermed = Column(String(10), index=True)
    hgz = Column(String(100), index=True)
    estatus = Column(String(50), index=True)
    cantidad = Column(Integer, default=1)


class CatalogoCIE11(SurgicalBase):
    __tablename__ = "catalogo_cie11"

    id = Column(Integer, primary_key=True)
    codigo = Column(String(20), unique=True, nullable=False, index=True)
    descripcion = Column(String(255), nullable=False, index=True)
    grupo = Column(String(100), index=True)


class CatalogoSNOMED(SurgicalBase):
    __tablename__ = "catalogo_snomed"

    id = Column(Integer, primary_key=True)
    concept_id = Column(String(20), unique=True, nullable=False, index=True)
    descripcion = Column(String(255), nullable=False, index=True)


class CatalogoProcedimientoCIE9(SurgicalBase):
    __tablename__ = "catalogo_cie9mc"

    id = Column(Integer, primary_key=True)
    codigo = Column(String(20), unique=True, nullable=False, index=True)
    descripcion = Column(String(255), nullable=False, index=True)


class DataQualityLog(SurgicalBase):
    __tablename__ = "data_quality_log"

    id = Column(Integer, primary_key=True)
    tabla = Column(String(100), index=True)
    registro_id = Column(Integer, index=True)
    campo = Column(String(100), index=True)
    valor = Column(Text)
    problema = Column(String(255))
    severidad = Column(String(20), index=True)
    fecha_deteccion = Column(DateTime, default=utcnow, index=True)
    corregido = Column(Boolean, default=False, index=True)


class AuditLog(SurgicalBase):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True)
    tabla = Column(String(100), index=True)
    registro_id = Column(Integer, index=True)
    operacion = Column(String(20), index=True)
    usuario = Column(String(100), index=True)
    datos_anteriores = Column(SURGICAL_JSON_TYPE)
    datos_nuevos = Column(SURGICAL_JSON_TYPE)
    fecha = Column(DateTime, default=utcnow, index=True)


class ModeloML(SurgicalBase):
    __tablename__ = "modelos_ml"

    id = Column(Integer, primary_key=True)
    nombre = Column(String(100), index=True)
    version = Column(String(20), index=True)
    auc = Column(Float)
    fecha_entrenamiento = Column(DateTime, default=utcnow, index=True)
    features = Column(Text)
    path = Column(String(255))


class CargaMasivaTask(SurgicalBase):
    __tablename__ = "carga_masiva_tasks"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(100), unique=True, index=True)
    nombre_archivo = Column(String(255))
    estado = Column(String(50), default="PROCESANDO", index=True)
    total = Column(Integer, default=0)
    exitosos = Column(Integer, default=0)
    errores_json = Column(Text)
    iniciado_en = Column(DateTime, default=utcnow)
    finalizado_en = Column(DateTime)
    creado_por = Column(String(100))


def ensure_surgical_schema() -> None:
    return svc_schema_extracted.ensure_surgical_schema()


def ensure_surgical_postquirurgica_schema() -> None:
    return svc_schema_extracted.ensure_surgical_postquirurgica_schema()


def ensure_surgical_urgencias_schema() -> None:
    return svc_schema_extracted.ensure_surgical_urgencias_schema()


def ensure_clinical_sensitive_schema() -> None:
    ensure_clinical_sensitive_schema_core(
        engine=engine,
        log_suppressed_exception=_log_suppressed_exception,
    )


ensure_hospitalizacion_schema = svc_schema_extracted.ensure_hospitalizacion_schema
ensure_modelos_ml_schema = svc_schema_extracted.ensure_modelos_ml_schema
ensure_carga_masiva_schema = svc_schema_extracted.ensure_carga_masiva_schema
ensure_dim_paciente_geo_schema = svc_schema_extracted.ensure_dim_paciente_geo_schema
run_surgical_alembic_upgrade_if_enabled = partial(
    run_optional_alembic_upgrade,
    enabled=SURGICAL_ALEMBIC_ENABLED,
    alembic_config=SURGICAL_ALEMBIC_CONFIG,
    base_dir=os.path.dirname(__file__),
)
_build_patient_hash = build_patient_hash
normalize_upper = svc_normalize_upper
parse_int = svc_parse_int
classify_age_group = svc_classify_age_group
classify_pathology_group = partial(
    classify_pathology_group_core,
    onco_set=set(QUIROFANO_PATOLOGIAS_ONCOLOGICAS),
    litiasis_set=set(QUIROFANO_PATOLOGIAS_LITIASIS),
)
classify_procedure_group = partial(
    classify_procedure_group_core,
    procedimiento_succion=QUIROFANO_PROCEDIMIENTO_SUCCION,
    endoscopicos=set(QUIROFANO_PROCEDIMIENTOS_ENDOSCOPICOS),
    percutaneos=set(QUIROFANO_PROCEDIMIENTOS_PERCUTANEOS),
    abiertos=set(QUIROFANO_PROCEDIMIENTOS_ABIERTOS),
)
is_required_form_complete = is_required_form_complete_core


def geocodificar_direccion(alcaldia: str, colonia: str, cp: str) -> Tuple[Optional[float], Optional[float]]:
    return geocode_address(
        requests_module=requests,
        geocoder_url=GEOCODER_URL,
        geocoder_user_agent=GEOCODER_USER_AGENT,
        offline_strict_mode=OFFLINE_STRICT_MODE,
        alcaldia=alcaldia,
        colonia=colonia,
        cp=cp,
    )


SURGICAL_CIE11_MAP = SURGICAL_CIE11_MAP_CORE
SURGICAL_SNOMED_MAP = SURGICAL_SNOMED_MAP_CORE
SURGICAL_CIE9MC_MAP = SURGICAL_CIE9MC_MAP_CORE


def registrar_auditoria(
    sdb: Session,
    tabla: str,
    registro_id: int,
    operacion: str,
    usuario: str,
    datos_anteriores: Optional[Dict[str, Any]] = None,
    datos_nuevos: Optional[Dict[str, Any]] = None,
) -> None:
    registrar_auditoria_row(
        sdb,
        audit_model=AuditLog,
        tabla=tabla,
        registro_id=registro_id,
        operacion=operacion,
        usuario=usuario,
        datos_anteriores=datos_anteriores,
        datos_nuevos=datos_nuevos,
    )


get_cie11_from_patologia = partial(
    get_code_from_map,
    normalize_fn=normalize_upper,
    code_map=SURGICAL_CIE11_MAP,
)
get_cie10_from_patologia = partial(
    get_code_from_map,
    normalize_fn=normalize_upper,
    code_map=QUIROFANO_PATOLOGIA_CIE10_MAP,
)
qx_patologia_cie10_catalog = partial(
    build_patologia_cie10_catalog,
    QUIROFANO_PATOLOGIAS,
    get_cie10_fn=get_cie10_from_patologia,
)
get_snomed_from_patologia = partial(
    get_code_from_map,
    normalize_fn=normalize_upper,
    code_map=SURGICAL_SNOMED_MAP,
)
get_cie9mc_from_procedimiento = partial(
    get_code_from_map,
    normalize_fn=normalize_upper,
    code_map=SURGICAL_CIE9MC_MAP,
)
get_edad_quinquenio = edad_quinquenio
get_edad_grupo_epidemiologico = edad_grupo_epidemiologico
poblar_dim_fecha = partial(
    poblar_dim_fecha_core,
    dim_fecha_cls=DimFecha,
    sql_func=func,
)
poblar_dimensiones_catalogo = partial(
    poblar_dimensiones_catalogo_core,
    patologias=QUIROFANO_PATOLOGIAS,
    procedimientos=QUIROFANO_PROCEDIMIENTOS,
    dim_diagnostico_cls=DimDiagnostico,
    dim_procedimiento_cls=DimProcedimiento,
    classify_pathology_group_fn=classify_pathology_group,
    classify_procedure_group_fn=classify_procedure_group,
    get_cie11_from_patologia_fn=get_cie11_from_patologia,
    get_snomed_from_patologia_fn=get_snomed_from_patologia,
    get_cie9mc_from_procedimiento_fn=get_cie9mc_from_procedimiento,
)
_get_or_create_dim_paciente = partial(
    get_or_create_dim_paciente_core,
    dim_paciente_cls=DimPaciente,
    build_patient_hash_fn=_build_patient_hash,
    get_edad_quinquenio_fn=get_edad_quinquenio,
    get_edad_grupo_epidemiologico_fn=get_edad_grupo_epidemiologico,
)
_get_dim_fecha_id = partial(
    get_dim_fecha_id_core,
    dim_fecha_cls=DimFecha,
)
_get_dim_diagnostico_id = partial(
    get_dim_diagnostico_id_core,
    dim_diagnostico_cls=DimDiagnostico,
)
_get_dim_procedimiento_id = partial(
    get_dim_procedimiento_id_core,
    dim_procedimiento_cls=DimProcedimiento,
)
actualizar_data_mart = partial(
    actualizar_data_mart_core,
    poblar_dim_fecha_fn=poblar_dim_fecha,
    poblar_dimensiones_catalogo_fn=poblar_dimensiones_catalogo,
    hecho_programacion_cls=HechoProgramacionQuirurgica,
    surgical_programacion_cls=SurgicalProgramacionDB,
    get_dim_fecha_id_fn=_get_dim_fecha_id,
    get_or_create_dim_paciente_fn=_get_or_create_dim_paciente,
    get_dim_diagnostico_id_fn=_get_dim_diagnostico_id,
    get_dim_procedimiento_id_fn=_get_dim_procedimiento_id,
)
check_data_quality = partial(
    check_data_quality_core,
    surgical_programacion_cls=SurgicalProgramacionDB,
    data_quality_log_cls=DataQualityLog,
    patologias_catalogo=QUIROFANO_PATOLOGIAS,
)
qx_catalogos_payload = partial(
    qx_catalogos_payload_core,
    sexos=QUIROFANO_SEXOS,
    patologias=QUIROFANO_PATOLOGIAS,
    patologias_cie10=qx_patologia_cie10_catalog(),
    patologias_oncologicas=QUIROFANO_PATOLOGIAS_ONCOLOGICAS,
    patologias_litiasis=QUIROFANO_PATOLOGIAS_LITIASIS,
    procedimientos=QUIROFANO_PROCEDIMIENTOS,
    procedimientos_requieren_abordaje=QUIROFANO_PROCEDIMIENTOS_REQUIEREN_ABORDAJE,
    procedimientos_abiertos=QUIROFANO_PROCEDIMIENTOS_ABIERTOS,
    procedimientos_endoscopicos=QUIROFANO_PROCEDIMIENTOS_ENDOSCOPICOS,
    procedimientos_percutaneos=QUIROFANO_PROCEDIMIENTOS_PERCUTANEOS,
    insumos=QUIROFANO_INSUMOS,
    hemoderivados=QUIROFANO_HEMODERIVADOS,
)
entrenar_modelo_riesgo = partial(
    entrenar_modelo_riesgo_core,
    pd_module=pd,
    random_forest_classifier_cls=RandomForestClassifier,
    train_test_split_fn=train_test_split,
    roc_auc_score_fn=roc_auc_score,
    joblib_module=joblib,
    surgical_programacion_cls=SurgicalProgramacionDB,
    modelo_riesgo_path=MODELO_RIESGO_PATH,
)
entrenar_modelo_riesgo_v2 = partial(
    entrenar_modelo_riesgo_v2_core,
    pd_module=pd,
    random_forest_classifier_cls=RandomForestClassifier,
    train_test_split_fn=train_test_split,
    roc_auc_score_fn=roc_auc_score,
    joblib_module=joblib,
    surgical_programacion_cls=SurgicalProgramacionDB,
    modelo_ml_cls=ModeloML,
    modelo_riesgo_v2_path=MODELO_RIESGO_V2_PATH,
    ensure_modelos_ml_schema_fn=ensure_modelos_ml_schema,
)


def cargar_modelo_riesgo():
    # Carga perezosa + caché por proceso: evita I/O repetido en requests concurrentes.
    return ai_load_model_cached(MODELO_RIESGO_PATH)


_parse_int_from_text = parse_int_from_text_core


CargaMasivaTaskStatus = CargaMasivaTaskStatusCore


def ensure_patient_files_dir() -> None:
    try:
        os.makedirs(PATIENT_FILES_DIR, exist_ok=True)
    except Exception:
        return


_safe_filename = safe_filename_core
_extract_extension = extract_extension_core
_detect_mime = detect_mime_core
_serialize_archivo = serialize_archivo_row_core
_format_size = svc_format_size
_normalize_form_data_for_upload = partial(
    normalize_form_data_core,
    required_sentinels=REQUIRED_SENTINELS,
)
_prepare_excel_row = partial(
    prepare_excel_row_core,
    pd_module=pd,
    normalize_form_data_fn=_normalize_form_data_for_upload,
    apply_aliases_fn=apply_aliases_core,
    aplicar_derivaciones_fn=aplicar_derivaciones_core,
    normalize_curp_fn=svc_normalize_curp,
    normalize_nss_fn=svc_normalize_nss,
)


def _resolve_consulta_para_archivo(
    db: Session,
    consulta_id: Optional[int],
    curp: Optional[str],
) -> Optional[ConsultaDB]:
    return resolve_consulta_para_archivo_core(
        db=db,
        consulta_id=consulta_id,
        curp=curp,
        consulta_model=ConsultaDB,
        normalize_curp_fn=normalize_curp,
    )


def _process_massive_excel_common_kwargs() -> Dict[str, Any]:
    return {
        "prepare_excel_row_fn": _prepare_excel_row,
        "new_clinical_session_fn": _new_clinical_session,
        "consulta_create_cls": ConsultaCreate,
        "extraer_protocolo_detalles_fn": extraer_protocolo_detalles,
        "detectar_inconsistencias_fn": detectar_inconsistencias,
        "generar_nota_soap_fn": generar_nota_soap,
        "build_embedding_text_fn": build_embedding_text,
        "async_embeddings": ASYNC_EMBEDDINGS,
        "compute_embedding_fn": compute_embedding,
        "consulta_db_cls": ConsultaDB,
        "sync_consulta_embedding_vector_fn": sync_consulta_embedding_vector,
        "enqueue_embedding_fn": enqueue_embedding,
        "json_module": json,
        "today_fn": date.today,
    }


def _process_massive_excel_dataframe(
    df,
    progress_cb=None,
) -> CargaMasivaTaskStatus:
    return process_massive_excel_dataframe_core(
        df,
        required_columns={"curp", "nss", "nombre", "edad", "sexo", "diagnostico_principal"},
        progress_cb=progress_cb,
        **_process_massive_excel_common_kwargs(),
    )


_serialize_model_row = serialize_model_row_core
_is_model_for_base = is_model_for_base_core


def _capture_dual_write_ops(db: Session, model_base: Any):
    return capture_dual_write_ops_core(
        db,
        model_base=model_base,
        serialize_model_row_fn=_serialize_model_row,
        is_model_for_base_fn=_is_model_for_base,
    )


_apply_dual_write_ops = partial(apply_dual_write_ops_core, logger=logger)


def _install_dual_write_commit_wrapper(
    db: Session,
    *,
    shadow_factory,
    model_base: Any,
    label: str,
):
    return install_dual_write_commit_wrapper_core(
        db,
        shadow_factory=shadow_factory,
        model_base=model_base,
        label=label,
        capture_dual_write_ops_fn=_capture_dual_write_ops,
        apply_dual_write_ops_fn=_apply_dual_write_ops,
    )


def _new_clinical_session(*, enable_dual_write: bool = True) -> Session:
    return new_session_with_optional_dual_write_core(
        session_factory=SessionLocal,
        enable_dual_write=enable_dual_write,
        dual_write_enabled=DUAL_WRITE_ENABLED,
        shadow_session_factory=ClinicalShadowSessionLocal,
        model_base=Base,
        label="clinical",
        install_wrapper_fn=_install_dual_write_commit_wrapper,
    )


def _new_surgical_session(*, enable_dual_write: bool = True) -> Session:
    return new_session_with_optional_dual_write_core(
        session_factory=SurgicalSessionLocal,
        enable_dual_write=enable_dual_write,
        dual_write_enabled=DUAL_WRITE_ENABLED,
        shadow_session_factory=SurgicalShadowSessionLocal,
        model_base=SurgicalBase,
        label="surgical",
        install_wrapper_fn=_install_dual_write_commit_wrapper,
    )


_sync_consulta_sensitive_encrypted = partial(
    sync_consulta_sensitive_encrypted_core,
    enable_pii_encryption=ENABLE_PII_ENCRYPTION,
    encrypt_sensitive_value_fn=_encrypt_sensitive_value,
)
_sync_surgical_sensitive_encrypted = partial(
    sync_surgical_sensitive_encrypted_core,
    enable_pii_encryption=ENABLE_PII_ENCRYPTION,
    encrypt_sensitive_value_fn=_encrypt_sensitive_value,
)


@event.listens_for(ConsultaDB, "before_insert")
def _consulta_before_insert(_mapper, _connection, target: "ConsultaDB"):
    _sync_consulta_sensitive_encrypted(target)


@event.listens_for(ConsultaDB, "before_update")
def _consulta_before_update(_mapper, _connection, target: "ConsultaDB"):
    _sync_consulta_sensitive_encrypted(target)


@event.listens_for(SurgicalProgramacionDB, "before_insert")
def _surgical_before_insert(_mapper, _connection, target: "SurgicalProgramacionDB"):
    _sync_surgical_sensitive_encrypted(target)


@event.listens_for(SurgicalProgramacionDB, "before_update")
def _surgical_before_update(_mapper, _connection, target: "SurgicalProgramacionDB"):
    _sync_surgical_sensitive_encrypted(target)


Base.metadata.create_all(bind=engine)
SurgicalBase.metadata.create_all(bind=surgical_engine)
ensure_clinical_sensitive_schema()
ensure_surgical_schema()
ensure_surgical_postquirurgica_schema()
ensure_surgical_urgencias_schema()

# ==========================================
# DEPENDENCIA PARA OBTENER SESIÓN DE DB
# ==========================================
def get_db():
    db = _new_clinical_session(enable_dual_write=True)
    try:
        yield db
    finally:
        db.close()


def get_surgical_db():
    db = _new_surgical_session(enable_dual_write=True)
    try:
        yield db
    finally:
        db.close()


_PUSH_MODULE_FEEDBACK_COMMON_KW = {
    "emit_module_feedback_fn": emit_module_feedback_core,
    "new_surgical_session_fn": _new_surgical_session,
    "surgical_feedback_model": SurgicalFeedbackDB,
    "outbox_emit_fn": svc_outbox_flow.emit_outbox_event,
    "event_emit_fn": svc_event_log_flow.emit_event,
}


def push_module_feedback(
    consulta_id: int,
    modulo: str,
    referencia_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    return svc_clinical_events_bridge_flow.push_module_feedback_flow(
        consulta_id=consulta_id,
        modulo=modulo,
        referencia_id=referencia_id,
        payload=payload,
        **_PUSH_MODULE_FEEDBACK_COMMON_KW,
    )


def registrar_evento_flujo_quirurgico(
    *,
    consulta_id: int,
    evento: str,
    estatus: Optional[str] = None,
    surgical_programacion_id: Optional[int] = None,
    quirofano_id: Optional[int] = None,
    edad: Optional[int] = None,
    sexo: Optional[str] = None,
    nss: Optional[str] = None,
    hgz: Optional[str] = None,
    diagnostico: Optional[str] = None,
    procedimiento: Optional[str] = None,
    ecog: Optional[str] = None,
    cirujano: Optional[str] = None,
    sangrado_ml: Optional[float] = None,
    metadata_json: Optional[Dict[str, Any]] = None,
) -> None:
    return svc_clinical_events_bridge_flow.registrar_evento_flujo_quirurgico_flow(
        consulta_id=consulta_id,
        evento=evento,
        estatus=estatus,
        surgical_programacion_id=surgical_programacion_id,
        quirofano_id=quirofano_id,
        edad=edad,
        sexo=sexo,
        nss=nss,
        hgz=hgz,
        diagnostico=diagnostico,
        procedimiento=procedimiento,
        ecog=ecog,
        cirujano=cirujano,
        sangrado_ml=sangrado_ml,
        metadata_json=metadata_json,
        emit_flujo_quirurgico_event_fn=emit_flujo_quirurgico_event,
        new_surgical_session_fn=_new_surgical_session,
        hecho_model=HechoFlujoQuirurgico,
        normalize_upper_fn=normalize_upper,
        classify_age_group_fn=classify_age_group,
        normalize_nss_fn=normalize_nss,
        outbox_emit_fn=svc_outbox_flow.emit_outbox_event,
        event_emit_fn=svc_event_log_flow.emit_event,
    )


sync_quirofano_to_surgical_db = svc_quirofano_sync_extracted.sync_quirofano_to_surgical_db
backfill_quirofano_to_surgical = partial(
    svc_backfill_quirofano_to_surgical_flow,
    new_clinical_session_fn=_new_clinical_session,
    quirofano_model=QuirofanoDB,
    consulta_model=ConsultaDB,
    sync_quirofano_to_surgical_db_fn=sync_quirofano_to_surgical_db,
)


def _run_data_mart_update_sync(incremental: bool = True) -> Dict[str, Any]:
    sdb = _new_surgical_session(enable_dual_write=True)
    try:
        return actualizar_data_mart(sdb, incremental=incremental)
    finally:
        sdb.close()


def calcular_digito_verificador_curp(curp17: str) -> str:
    return calcular_digito_verificador_curp_core(curp17)

# ==========================================
# MODELOS PYDANTIC (VALIDACIÓN)
# ==========================================
ConsultaBase = ConsultaBaseCore
ConsultaCreate = ConsultaCreateCore
PROTOCOL_PREFIXES = PROTOCOL_PREFIXES_CORE
PROTOCOL_FIELDS = PROTOCOL_FIELDS_CORE

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================
normalize_curp = svc_normalize_curp
normalize_nss = svc_normalize_nss
get_semantic_model = get_semantic_model_core
build_embedding_text = build_embedding_text_core
compute_embedding = compute_embedding_core
cosine_similarity = cosine_similarity_core


def enqueue_embedding(note_id: int, text: str):
    return enqueue_embedding_core(note_id, text, enabled=ASYNC_EMBEDDINGS)


def normalize_form_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_form_data_core(form_data, required_sentinels=REQUIRED_SENTINELS)


apply_aliases = apply_aliases_core
calcular_indice_tabaquico = calcular_indice_tabaquico_core
calcular_scores_litiasis = calcular_scores_litiasis_core
generar_nota_soap = generar_nota_soap_core
detectar_inconsistencias = detectar_inconsistencias_core


def aplicar_derivaciones(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    return aplicar_derivaciones_core(data_dict)


kaplan_meier = svc_kaplan_meier


def resolve_survival_event(consulta: ConsultaDB) -> Tuple[bool, Optional[date]]:
    return svc_resolve_survival_event(
        consulta,
        event_field=SURVIVAL_EVENT_FIELD,
        event_value=SURVIVAL_EVENT_VALUE,
    )


fig_to_base64 = svc_fig_to_base64
count_by = svc_count_by


EDAD_REPORTE_BUCKETS = [
    "18-25",
    "26-35",
    "36-45",
    "46-55",
    "56-60",
] + [str(n) for n in range(61, 81)] + ["MAS DE 80"]
EDAD_REPORTE_INDEX = {name: idx for idx, name in enumerate(EDAD_REPORTE_BUCKETS)}


def build_programmed_age_counts(rows: List[SurgicalProgramacionDB]) -> List[Tuple[str, int]]:
    return svc_build_programmed_age_counts(
        rows,
        age_buckets=EDAD_REPORTE_BUCKETS,
        classify_age_group_fn=classify_age_group,
    )


_extract_numeric_level = svc_preventive_priority.extract_numeric_level
_waiting_days_bucket = svc_preventive_priority.waiting_days_bucket
_estimate_cancelation_risk = svc_preventive_priority.estimate_cancelation_risk
_compute_preventive_priority = svc_preventive_priority.compute_preventive_priority


_build_pending_row_with_priority = svc_reporte_datasets_extracted.build_pending_row_with_priority
_build_pending_programar_dataset = svc_reporte_datasets_extracted.build_pending_programar_dataset
_build_realizadas_dataset = svc_reporte_datasets_extracted.build_realizadas_dataset
_build_programadas_dataset = svc_reporte_datasets_extracted.build_programadas_dataset
_build_urgencias_programadas_dataset = svc_reporte_datasets_extracted.build_urgencias_programadas_dataset


_parse_any_date = parse_any_date_core
_build_jj_metrics = partial(build_jj_metrics_core, parse_any_date_fn=_parse_any_date)
_build_hemoderivados_metrics = partial(
    svc_reporte_metrics_extracted.build_hemoderivados_metrics,
    count_by_fn=count_by,
)
_build_desglose_from_dict_rows = partial(build_desglose_from_dict_rows_core, count_by_fn=count_by)
_build_bar_chart_from_counts = partial(
    build_bar_chart_from_counts_core,
    plt_module=plt,
    fig_to_base64_fn=fig_to_base64,
)
_build_hist_chart_from_values = partial(
    build_hist_chart_from_values_core,
    plt_module=plt,
    fig_to_base64_fn=fig_to_base64,
)
_rank_preventive_rows = rank_preventive_rows_core


_calc_percentile = calc_percentile_core


_safe_pct = safe_pct_core


_parse_lab_numeric = parse_lab_numeric_core


_lab_key_from_text = lab_key_from_text_core


_lab_positive_clostridium = lab_positive_clostridium_core


_hospital_stay_days = hospital_stay_days_core


_distribution_stats_table = distribution_stats_table_core


_as_date = as_date_core


def _build_sangrado_metrics(
    realizadas_rows: List[Dict[str, Any]],
    *,
    anio: Optional[int] = None,
    mes: Optional[int] = None,
    top_n: int = 25,
) -> Dict[str, Any]:
    deps = svc_reporte_metrics_extracted.SangradoDeps(
        as_date=_as_date,
        calc_percentile=_calc_percentile,
        safe_pct=_safe_pct,
        extract_numeric_level=_extract_numeric_level,
        plt=plt,
        fig_to_base64=fig_to_base64,
    )
    return svc_reporte_metrics_extracted.build_sangrado_metrics(
        realizadas_rows,
        anio=anio,
        mes=mes,
        top_n=top_n,
        deps=deps,
    )


_build_advanced_reporte_metrics = svc_reporte_bi_extracted.build_advanced_reporte_metrics
generar_reporte_bi = svc_reporte_bi_extracted.build_reporte_bi
map_to_fhir = svc_build_fhir_bundle
map_to_fhir_patient_only = svc_build_fhir_patient_only
map_to_fhir_condition = svc_build_fhir_condition_only


SKIP_REQUIRED_FIELDS = {"imc", "evento_clinico", "fecha_evento"}


def enforce_required_fields(model: ConsultaCreate):
    enforce_required_fields_model(
        model,
        skip_fields=SKIP_REQUIRED_FIELDS,
        required_sentinels=REQUIRED_SENTINELS,
    )


extraer_protocolo_detalles = partial(extract_protocolo_detalles, protocol_fields=PROTOCOL_FIELDS)


def validate_csrf(form_data: Dict[str, Any], request: Request):
    validate_csrf_token(form_data, request, CSRF_COOKIE_NAME)


def _is_auth_public_path(path: str) -> bool:
    normalized = (path or "").split("?", 1)[0] or "/"
    if normalized in AUTH_PUBLIC_PATHS:
        return True
    return any(normalized.startswith(prefix) for prefix in AUTH_PUBLIC_PREFIXES)


def require_auth(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(security),
):
    if _is_auth_public_path(request.url.path):
        return
    require_auth_basic(credentials, AUTH_SETTINGS)

# ==========================================
# PLANTILLAS JINJA2 (IMPORTADAS)
# ==========================================
from app.legacy_inline_templates import (
    MENU_TEMPLATE,
    CONSULTA_TEMPLATE,
    CONFIRMACION_TEMPLATE,
    HOSPITALIZACION_LISTA_TEMPLATE,
    HOSPITALIZACION_NUEVO_TEMPLATE,
    QUIROFANO_HOME_TEMPLATE,
    QUIROFANO_PROGRAMADA_TEMPLATE,
    QUIROFANO_PLACEHOLDER_TEMPLATE,
    QUIROFANO_LISTA_TEMPLATE,
    QUIROFANO_NUEVO_TEMPLATE,
    EXPEDIENTE_TEMPLATE,
    BUSQUEDA_TEMPLATE,
    BUSQUEDA_SEMANTICA_TEMPLATE,
    REPORTE_TEMPLATE,
    DASHBOARD_TEMPLATE,
    CARGA_ARCHIVOS_TEMPLATE,
)

# ==========================================
# INSTANCIA DE FASTAPI
# ==========================================
app = create_app_instance(
    title="IMSS - Urología HES CMNR (Versión Completa con DB, Validación y Módulos)",
    require_auth=require_auth,
    app_static_dir=APP_STATIC_DIR,
    routers=get_api_routers(),
    register_security_middlewares=register_security_middlewares,
    request_is_https=_request_is_https,
    force_https=FORCE_HTTPS,
    enable_hsts=ENABLE_HSTS,
    hsts_max_age=HSTS_MAX_AGE,
    logger=logger,
)


startup_interconexion = lambda: run_startup_interconexion_wired_core(__import__(__name__))
startup_ai_agents_bootstrap = lambda: run_startup_ai_agents_bootstrap_wired_core(__import__(__name__))


async def startup_redis_cache():
    global _redis_client
    await startup_redis_cache_lifecycle(
        redis_async=redis_async,
        fastapi_cache_cls=FastAPICache,
        redis_url=REDIS_URL,
        set_client=lambda c: globals().__setitem__("_redis_client", c),
        log_suppressed_exception=_log_suppressed_exception,
    )


async def shutdown_redis_cache():
    global _redis_client
    await shutdown_redis_cache_lifecycle(
        get_client=lambda: _redis_client,
        set_client=lambda c: globals().__setitem__("_redis_client", c),
        log_suppressed_exception=_log_suppressed_exception,
    )


attach_lifespan(
    app,
    build_main_lifespan=build_main_lifespan,
    startup_interconexion=startup_interconexion,
    startup_ai_agents_bootstrap=startup_ai_agents_bootstrap,
    startup_redis_cache=startup_redis_cache,
    shutdown_redis_cache=shutdown_redis_cache,
)


async_actualizar_data_mart_task = None
async_backfill_quirofano_task = None
async_entrenar_modelo_riesgo_v2_task = None
async_fau_bot_central_cycle_task = None
async_fau_bot_self_improvement_task = None
async_quirofano_programacion_analizar_task = None
async_quirofano_agent_window_task = None
async_carga_masiva_excel_task = None

_registered_celery_tasks = register_main_celery_tasks_core(
    celery_app=celery_app,
    main_module_name=__name__,
    worker_configure_default_beat_schedule=worker_configure_default_beat_schedule,
    worker_run_actualizar_data_mart_sync=worker_run_actualizar_data_mart_sync,
    worker_run_backfill_quirofano=worker_run_backfill_quirofano,
    worker_run_entrenar_modelo_riesgo_v2=worker_run_entrenar_modelo_riesgo_v2,
    worker_run_fau_bot_central_cycle=worker_run_fau_bot_central_cycle,
    worker_run_fau_bot_self_improvement=worker_run_fau_bot_self_improvement,
    worker_run_quirofano_programacion_analizar=worker_run_quirofano_programacion_analizar,
    worker_run_quirofano_agent_window=worker_run_quirofano_agent_window,
    worker_run_carga_masiva_excel=worker_run_carga_masiva_excel,
)
async_actualizar_data_mart_task = _registered_celery_tasks.get("async_actualizar_data_mart_task")
async_backfill_quirofano_task = _registered_celery_tasks.get("async_backfill_quirofano_task")
async_entrenar_modelo_riesgo_v2_task = _registered_celery_tasks.get("async_entrenar_modelo_riesgo_v2_task")
async_fau_bot_central_cycle_task = _registered_celery_tasks.get("async_fau_bot_central_cycle_task")
async_fau_bot_self_improvement_task = _registered_celery_tasks.get("async_fau_bot_self_improvement_task")
async_quirofano_programacion_analizar_task = _registered_celery_tasks.get("async_quirofano_programacion_analizar_task")
async_quirofano_agent_window_task = _registered_celery_tasks.get("async_quirofano_agent_window_task")
async_carga_masiva_excel_task = _registered_celery_tasks.get("async_carga_masiva_excel_task")

# ==========================================
# FUNCIÓN PARA RENDERIZAR PLANTILLAS DESDE STRINGS
# ==========================================
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "app", "templates")
TEMPLATE_FILE_NAME_BY_CONST: Dict[str, str] = {
    "MENU_TEMPLATE": "menu.html",
    "CONSULTA_TEMPLATE": "consulta.html",
    "CONFIRMACION_TEMPLATE": "confirmacion.html",
    "HOSPITALIZACION_LISTA_TEMPLATE": "hospitalizacion_lista.html",
    "HOSPITALIZACION_NUEVO_TEMPLATE": "hospitalizacion_nuevo.html",
    "QUIROFANO_HOME_TEMPLATE": "quirofano_home.html",
    "QUIROFANO_PROGRAMADA_TEMPLATE": "quirofano_programada.html",
    "QUIROFANO_PLACEHOLDER_TEMPLATE": "quirofano_placeholder.html",
    "QUIROFANO_LISTA_TEMPLATE": "quirofano_lista.html",
    "QUIROFANO_NUEVO_TEMPLATE": "quirofano_nuevo.html",
    "EXPEDIENTE_TEMPLATE": "expediente.html",
    "BUSQUEDA_TEMPLATE": "busqueda.html",
    "BUSQUEDA_SEMANTICA_TEMPLATE": "busqueda_semantica.html",
    "REPORTE_TEMPLATE": "reporte.html",
    "DASHBOARD_TEMPLATE": "dashboard.html",
    "CARGA_ARCHIVOS_TEMPLATE": "carga_archivos.html",
    "WARD_ROUND_DASHBOARD_TEMPLATE": "ward_round_dashboard.html",
    "SMART_EXPEDIENTE_TEMPLATE": "smart_expediente.html",
    "COMMAND_CENTER_TEMPLATE": "command_center.html",
}
_template_literal_to_file_cache: Dict[str, str] = {}
_template_source_cache: Dict[str, Tuple[float, str]] = {}


_template_file_for_ref = partial(
    template_file_for_ref_core,
    template_literal_to_file_cache=_template_literal_to_file_cache,
    template_name_by_const=TEMPLATE_FILE_NAME_BY_CONST,
    global_values=globals(),
)

_load_template_file_source = partial(
    load_template_file_source_core,
    templates_dir=TEMPLATES_DIR,
    template_source_cache=_template_source_cache,
    sleep_fn=sleep,
    log_suppressed_exception=_log_suppressed_exception,
)


def _prewarm_template_file_cache() -> None:
    prewarm_template_file_cache_core(
        template_files=TEMPLATE_FILE_NAME_BY_CONST.values(),
        load_template_file_source_fn=_load_template_file_source,
    )


_resolve_template_source = partial(
    resolve_template_source_core,
    template_file_for_ref_fn=_template_file_for_ref,
    load_template_file_source_fn=_load_template_file_source,
)


def _inject_ui_shell_wrapper(html: str, req: Request, ctx: Dict[str, Any]) -> str:
    from app.core.ui_shell import inject_ui_shell

    return inject_ui_shell(html, request=req, context=ctx)


def render_template(template_string: str, request: Optional[Request] = None, **context):
    return render_template_response_core(
        template_string,
        request=request,
        context=context,
        resolve_template_source_fn=_resolve_template_source,
        template_cache=_template_cache,
        jinja_env=jinja_env,
        csrf_cookie_name=CSRF_COOKIE_NAME,
        secure_cookies=SECURE_COOKIES,
        force_https=FORCE_HTTPS,
        inject_ui_shell_fn=_inject_ui_shell_wrapper if request is not None else None,
        log_suppressed_exception=_log_suppressed_exception,
    )


_image_file_to_data_url = partial(
    image_file_to_data_url_core,
    log_suppressed_exception=_log_suppressed_exception,
)
_resolve_menu_asset = partial(
    resolve_menu_asset_core,
    image_file_to_data_url_fn=_image_file_to_data_url,
    offline_strict_mode=OFFLINE_STRICT_MODE,
)

# ==========================================
# 1-4. Menú, consulta, reporte, analítica, carga de archivos y geoespacial:
# migrados aditivamente a app/api/legacy_core.py


# ==========================================
# 5. NUEVOS MÓDULOS (HOSPITALIZACIÓN, QUIRÓFANO, EXPEDIENTE, BÚSQUEDA)
# ==========================================

# --- HOSPITALIZACIÓN ---
# Rutas de hospitalización y quirófano urgencias:
# migradas de manera aditiva a app/api/hospitalizacion.py y app/api/urgencias.py

# --- QUIRÓFANO / EXPEDIENTE / FHIR / BÚSQUEDA ---
# Rutas legacy UI y FHIR migradas aditivamente a app/api/legacy_web.py

# ==========================================
# 6. EJECUCIÓN DEL SERVIDOR
# ==========================================
if __name__ == "__main__":
    uvicorn_kwargs: Dict[str, Any] = {"host": "0.0.0.0", "port": 8000}
    ssl_certfile = (os.getenv("SSL_CERTFILE", "") or "").strip()
    ssl_keyfile = (os.getenv("SSL_KEYFILE", "") or "").strip()
    if ssl_certfile and ssl_keyfile:
        uvicorn_kwargs["ssl_certfile"] = ssl_certfile
        uvicorn_kwargs["ssl_keyfile"] = ssl_keyfile
    uvicorn.run(app, **uvicorn_kwargs)
