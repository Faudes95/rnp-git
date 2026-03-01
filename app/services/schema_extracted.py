from __future__ import annotations

_CORE_DYNAMIC_SYMBOLS = [
    "inspect",
    "surgical_engine",
    "text",
    "_log_suppressed_exception",
]


def _ensure_symbols(required: list[str] | None = None) -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    required_symbols = required or _CORE_DYNAMIC_SYMBOLS
    missing = []
    for symbol in required_symbols:
        if symbol in module_globals:
            continue
        try:
            module_globals[symbol] = getattr(m, symbol)
        except Exception:
            missing.append(symbol)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def ensure_surgical_schema() -> None:
    _ensure_symbols()
    """
    Agrega columnas nuevas en la BD quirúrgica dedicada sin eliminar ni alterar datos previos.
    """
    try:
        insp = inspect(surgical_engine)
        if "surgical_programaciones" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("surgical_programaciones")}
        sql_types = {
            "agregado_medico": "TEXT",
            "edad": "INTEGER",
            "edad_grupo": "TEXT",
            "sexo": "TEXT",
            "grupo_sexo": "TEXT",
            "patologia": "TEXT",
            "grupo_patologia": "TEXT",
            "procedimiento_programado": "TEXT",
            "grupo_procedimiento": "TEXT",
            "abordaje": "TEXT",
            "tipo_neovejiga": "TEXT",
            "sistema_succion": "TEXT",
            "insumos_solicitados": "TEXT",
            "requiere_intermed": "TEXT",
            "hgz": "TEXT",
            "cie11_codigo": "TEXT",
            "snomed_codigo": "TEXT",
            "cie9mc_codigo": "TEXT",
            "tnm": "TEXT",
            "ecog": "TEXT",
            "charlson": "TEXT",
            "etapa_clinica": "TEXT",
            "ipss": "TEXT",
            "gleason": "TEXT",
            "ape": "TEXT",
            "rtup_previa": "TEXT",
            "tacto_rectal": "TEXT",
            "historial_ape": "TEXT",
            "uh_rango": "TEXT",
            "litiasis_tamano_rango": "TEXT",
            "litiasis_subtipo_20": "TEXT",
            "litiasis_ubicacion": "TEXT",
            "litiasis_ubicacion_multiple": "TEXT",
            "hidronefrosis": "TEXT",
            "protocolo_completo": "TEXT",
            "pendiente_programar": "TEXT",
            "sangrado_ml": "FLOAT",
            "tiempo_quirurgico_min": "FLOAT",
            "transfusion": "TEXT",
            "solicita_hemoderivados": "TEXT",
            "hemoderivados_pg_solicitados": "INTEGER",
            "hemoderivados_pfc_solicitados": "INTEGER",
            "hemoderivados_cp_solicitados": "INTEGER",
            "uso_hemoderivados": "TEXT",
            "hemoderivados_pg_utilizados": "INTEGER",
            "hemoderivados_pfc_utilizados": "INTEGER",
            "hemoderivados_cp_utilizados": "INTEGER",
            "antibiotico": "TEXT",
            "clavien_dindo": "TEXT",
            "margen_quirurgico": "TEXT",
            "neuropreservacion": "TEXT",
            "linfadenectomia": "TEXT",
            "reingreso_30d": "TEXT",
            "reintervencion_30d": "TEXT",
            "mortalidad_30d": "TEXT",
            "reingreso_90d": "TEXT",
            "reintervencion_90d": "TEXT",
            "mortalidad_90d": "TEXT",
            "stone_free": "TEXT",
            "composicion_lito": "TEXT",
            "recurrencia_litiasis": "TEXT",
            "cateter_jj_colocado": "TEXT",
            "fecha_colocacion_jj": "DATE",
            "diagnostico_postop": "TEXT",
            "procedimiento_realizado": "TEXT",
            "nota_postquirurgica": "TEXT",
            "complicaciones_postquirurgicas": "TEXT",
            "fecha_postquirurgica": "DATE",
            "cancelacion_codigo": "TEXT",
            "cancelacion_categoria": "TEXT",
            "cancelacion_concepto": "TEXT",
            "cancelacion_detalle": "TEXT",
            "cancelacion_fecha": "TIMESTAMP",
            "cancelacion_usuario": "TEXT",
            "fecha_ingreso_pendiente_programar": "TIMESTAMP",
            "dias_en_espera": "INTEGER",
            "prioridad_clinica": "TEXT",
            "motivo_prioridad": "TEXT",
            "riesgo_cancelacion_predicho": "FLOAT",
            "score_preventivo": "FLOAT",
            "urgencia_programacion_id": "INTEGER",
            "curp_enc": "TEXT",
            "nss_enc": "TEXT",
            "paciente_nombre_enc": "TEXT",
        }
        with surgical_engine.begin() as conn:
            for col_name, col_type in sql_types.items():
                if col_name in existing:
                    continue
                conn.execute(text(f"ALTER TABLE surgical_programaciones ADD COLUMN {col_name} {col_type}"))
    except Exception as exc:
        _log_suppressed_exception("ensure_surgical_schema_failed", exc)
        # Fallo silencioso para no bloquear arranque del sistema maestro.
        return


def ensure_surgical_postquirurgica_schema() -> None:
    _ensure_symbols()
    """
    Extiende tabla de notas postquirúrgicas sin alterar datos previos.
    """
    try:
        insp = inspect(surgical_engine)
        if "surgical_postquirurgicas" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("surgical_postquirurgicas")}
        sql_types = {
            "tiempo_quirurgico_min": "FLOAT",
            "transfusion": "TEXT",
            "uso_hemoderivados": "TEXT",
            "hemoderivados_pg_utilizados": "INTEGER",
            "hemoderivados_pfc_utilizados": "INTEGER",
            "hemoderivados_cp_utilizados": "INTEGER",
            "antibiotico": "TEXT",
            "clavien_dindo": "TEXT",
            "margen_quirurgico": "TEXT",
            "neuropreservacion": "TEXT",
            "linfadenectomia": "TEXT",
            "reingreso_30d": "TEXT",
            "reintervencion_30d": "TEXT",
            "mortalidad_30d": "TEXT",
            "reingreso_90d": "TEXT",
            "reintervencion_90d": "TEXT",
            "mortalidad_90d": "TEXT",
            "stone_free": "TEXT",
            "composicion_lito": "TEXT",
            "recurrencia_litiasis": "TEXT",
            "cateter_jj_colocado": "TEXT",
            "fecha_colocacion_jj": "DATE",
        }
        with surgical_engine.begin() as conn:
            for col_name, col_type in sql_types.items():
                if col_name in existing:
                    continue
                conn.execute(text(f"ALTER TABLE surgical_postquirurgicas ADD COLUMN {col_name} {col_type}"))
    except Exception as exc:
        _log_suppressed_exception("ensure_surgical_postquirurgica_schema_failed", exc)
        return


def ensure_surgical_urgencias_schema() -> None:
    _ensure_symbols()
    """
    Garantiza columnas del módulo de cirugía de urgencia de forma aditiva.
    """
    try:
        insp = inspect(surgical_engine)
        if "surgical_urgencias_programaciones" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("surgical_urgencias_programaciones")}
        sql_types = {
            "consulta_id": "INTEGER",
            "surgical_programacion_id": "INTEGER",
            "curp": "TEXT",
            "nss": "TEXT",
            "agregado_medico": "TEXT",
            "paciente_nombre": "TEXT",
            "edad": "INTEGER",
            "edad_grupo": "TEXT",
            "sexo": "TEXT",
            "grupo_sexo": "TEXT",
            "patologia": "TEXT",
            "patologia_cie10": "TEXT",
            "grupo_patologia": "TEXT",
            "procedimiento_programado": "TEXT",
            "grupo_procedimiento": "TEXT",
            "abordaje": "TEXT",
            "tipo_neovejiga": "TEXT",
            "sistema_succion": "TEXT",
            "insumos_solicitados": "TEXT",
            "requiere_intermed": "TEXT",
            "solicita_hemoderivados": "TEXT",
            "hemoderivados_pg_solicitados": "INTEGER",
            "hemoderivados_pfc_solicitados": "INTEGER",
            "hemoderivados_cp_solicitados": "INTEGER",
            "hgz": "TEXT",
            "tnm": "TEXT",
            "ecog": "TEXT",
            "charlson": "TEXT",
            "etapa_clinica": "TEXT",
            "ipss": "TEXT",
            "gleason": "TEXT",
            "ape": "TEXT",
            "rtup_previa": "TEXT",
            "tacto_rectal": "TEXT",
            "historial_ape": "TEXT",
            "uh_rango": "TEXT",
            "litiasis_tamano_rango": "TEXT",
            "litiasis_subtipo_20": "TEXT",
            "litiasis_ubicacion": "TEXT",
            "litiasis_ubicacion_multiple": "TEXT",
            "hidronefrosis": "TEXT",
            "fecha_urgencia": "DATE",
            "fecha_realizacion": "DATE",
            "cancelacion_codigo": "TEXT",
            "cancelacion_categoria": "TEXT",
            "cancelacion_concepto": "TEXT",
            "cancelacion_detalle": "TEXT",
            "cancelacion_fecha": "TIMESTAMP",
            "cancelacion_usuario": "TEXT",
            "estatus": "TEXT",
            "cirujano": "TEXT",
            "sangrado_ml": "FLOAT",
            "tiempo_quirurgico_min": "FLOAT",
            "transfusion": "TEXT",
            "uso_hemoderivados": "TEXT",
            "hemoderivados_pg_utilizados": "INTEGER",
            "hemoderivados_pfc_utilizados": "INTEGER",
            "hemoderivados_cp_utilizados": "INTEGER",
            "stone_free": "TEXT",
            "composicion_lito": "TEXT",
            "recurrencia_litiasis": "TEXT",
            "cateter_jj_colocado": "TEXT",
            "fecha_colocacion_jj": "DATE",
            "diagnostico_postop": "TEXT",
            "procedimiento_realizado": "TEXT",
            "nota_postquirurgica": "TEXT",
            "complicaciones_postquirurgicas": "TEXT",
            "cie11_codigo": "TEXT",
            "snomed_codigo": "TEXT",
            "cie9mc_codigo": "TEXT",
            "modulo_origen": "TEXT",
            "creado_en": "TIMESTAMP",
            "actualizado_en": "TIMESTAMP",
        }
        with surgical_engine.begin() as conn:
            for col_name, col_type in sql_types.items():
                if col_name in existing:
                    continue
                conn.execute(text(f"ALTER TABLE surgical_urgencias_programaciones ADD COLUMN {col_name} {col_type}"))
    except Exception as exc:
        _log_suppressed_exception("ensure_surgical_urgencias_schema_failed", exc)
        return


def ensure_hospitalizacion_schema() -> None:
    _ensure_symbols(["inspect", "engine", "text", "_log_suppressed_exception"])
    """
    Extiende la tabla hospitalizaciones de forma aditiva sin alterar rutas/lógica preexistentes.
    """
    try:
        insp = inspect(engine)
        if "hospitalizaciones" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("hospitalizaciones")}
        sql_types = {
            "nss": "TEXT",
            "agregado_medico": "TEXT",
            "medico_a_cargo": "TEXT",
            "nombre_completo": "TEXT",
            "edad": "INTEGER",
            "sexo": "TEXT",
            "diagnostico": "TEXT",
            "hgz_envio": "TEXT",
            "estatus_detalle": "TEXT",
            "dias_hospitalizacion": "INTEGER",
            "dias_postquirurgicos": "INTEGER",
            "incapacidad": "TEXT",
            "incapacidad_emitida": "TEXT",
            "programado": "TEXT",
            "medico_programado": "TEXT",
            "turno_programado": "TEXT",
            "urgencia": "TEXT",
            "urgencia_tipo": "TEXT",
            "ingreso_tipo": "TEXT",
            "estado_clinico": "TEXT",
            "uci": "TEXT",
            "patient_uid": "TEXT",
            "idempotency_key": "TEXT",
            "observaciones": "TEXT",
        }
        with engine.begin() as conn:
            for col_name, col_type in sql_types.items():
                if col_name in existing:
                    continue
                conn.execute(text(f"ALTER TABLE hospitalizaciones ADD COLUMN {col_name} {col_type}"))
    except Exception as exc:
        _log_suppressed_exception("ensure_hospitalizacion_schema_failed", exc)
        return


def ensure_modelos_ml_schema() -> None:
    _ensure_symbols(
        [
            "SurgicalBase",
            "surgical_engine",
            "ModeloML",
            "_log_suppressed_exception",
        ]
    )
    try:
        SurgicalBase.metadata.create_all(bind=surgical_engine, tables=[ModeloML.__table__], checkfirst=True)
    except Exception as exc:
        _log_suppressed_exception("ensure_modelos_ml_schema_failed", exc)
        return


def ensure_carga_masiva_schema() -> None:
    _ensure_symbols(
        [
            "SurgicalBase",
            "surgical_engine",
            "CargaMasivaTask",
            "_log_suppressed_exception",
        ]
    )
    try:
        SurgicalBase.metadata.create_all(bind=surgical_engine, tables=[CargaMasivaTask.__table__], checkfirst=True)
    except Exception as exc:
        _log_suppressed_exception("ensure_carga_masiva_schema_failed", exc)
        return


def ensure_dim_paciente_geo_schema() -> None:
    _ensure_symbols()
    try:
        insp = inspect(surgical_engine)
        if "dim_paciente" not in insp.get_table_names():
            return
        existing = {c["name"] for c in insp.get_columns("dim_paciente")}
        with surgical_engine.begin() as conn:
            if "colonia" not in existing:
                conn.execute(text("ALTER TABLE dim_paciente ADD COLUMN colonia TEXT"))
            if "cp" not in existing:
                conn.execute(text("ALTER TABLE dim_paciente ADD COLUMN cp TEXT"))
            if "lat" not in existing:
                conn.execute(text("ALTER TABLE dim_paciente ADD COLUMN lat FLOAT"))
            if "lon" not in existing:
                conn.execute(text("ALTER TABLE dim_paciente ADD COLUMN lon FLOAT"))
    except Exception as exc:
        _log_suppressed_exception("ensure_dim_paciente_geo_schema_failed", exc)
        return
