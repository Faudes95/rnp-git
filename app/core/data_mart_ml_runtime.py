from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple


def poblar_dim_fecha(
    sdb: Any,
    *,
    dim_fecha_cls: Any,
    sql_func: Any,
    start_date: date = date(2020, 1, 1),
    end_date: Optional[date] = None,
) -> None:
    if end_date is None:
        end_date = date.today() + timedelta(days=366)
    max_existing = sdb.query(sql_func.max(dim_fecha_cls.fecha)).scalar()
    current = (max_existing + timedelta(days=1)) if max_existing else start_date
    if current > end_date:
        return
    while current <= end_date:
        sdb.add(
            dim_fecha_cls(
                fecha=current,
                anio=current.year,
                mes=current.month,
                dia=current.day,
                dia_semana=current.weekday(),
                semana_anio=current.isocalendar()[1],
                trimestre=((current.month - 1) // 3) + 1,
                es_fin_semana=current.weekday() >= 5,
            )
        )
        current += timedelta(days=1)
    sdb.commit()


def poblar_dimensiones_catalogo(
    sdb: Any,
    *,
    patologias: List[str],
    procedimientos: List[str],
    dim_diagnostico_cls: Any,
    dim_procedimiento_cls: Any,
    classify_pathology_group_fn: Callable[[Optional[str]], str],
    classify_procedure_group_fn: Callable[[str, str, str], str],
    get_cie11_from_patologia_fn: Callable[[Optional[str]], Optional[str]],
    get_snomed_from_patologia_fn: Callable[[Optional[str]], Optional[str]],
    get_cie9mc_from_procedimiento_fn: Callable[[Optional[str]], Optional[str]],
) -> None:
    for pat in patologias:
        if sdb.query(dim_diagnostico_cls).filter(dim_diagnostico_cls.nombre == pat).first():
            continue
        sdb.add(
            dim_diagnostico_cls(
                nombre=pat,
                grupo=classify_pathology_group_fn(pat),
                cie11_codigo=get_cie11_from_patologia_fn(pat),
                snomed_codigo=get_snomed_from_patologia_fn(pat),
            )
        )
    for proc in procedimientos:
        if sdb.query(dim_procedimiento_cls).filter(dim_procedimiento_cls.nombre == proc).first():
            continue
        sdb.add(
            dim_procedimiento_cls(
                nombre=proc,
                grupo=classify_procedure_group_fn(proc, "", ""),
                cie9mc_codigo=get_cie9mc_from_procedimiento_fn(proc),
            )
        )
    sdb.commit()


def get_or_create_dim_paciente(
    sdb: Any,
    row: Any,
    *,
    dim_paciente_cls: Any,
    build_patient_hash_fn: Callable[[Optional[str], Optional[str], Optional[int]], Optional[str]],
    get_edad_quinquenio_fn: Callable[[Optional[int]], Optional[str]],
    get_edad_grupo_epidemiologico_fn: Callable[[Optional[int]], Optional[str]],
) -> Optional[int]:
    if not row:
        return None
    paciente_hash = build_patient_hash_fn(row.nss, row.curp, row.consulta_id)
    if not paciente_hash:
        return None
    dim = sdb.query(dim_paciente_cls).filter(dim_paciente_cls.paciente_hash == paciente_hash).first()
    if dim:
        return dim.id
    dim = dim_paciente_cls(
        paciente_hash=paciente_hash,
        sexo=row.sexo,
        edad_quinquenio=get_edad_quinquenio_fn(row.edad),
        edad_grupo_epidemiologico=get_edad_grupo_epidemiologico_fn(row.edad),
        hgz=row.hgz,
        alcaldia=None,
        colonia=None,
        cp=None,
        lat=None,
        lon=None,
    )
    sdb.add(dim)
    sdb.flush()
    return dim.id


def get_dim_fecha_id(sdb: Any, fecha_valor: Optional[date], *, dim_fecha_cls: Any) -> Optional[int]:
    if not fecha_valor:
        return None
    dim = sdb.query(dim_fecha_cls).filter(dim_fecha_cls.fecha == fecha_valor).first()
    return dim.id if dim else None


def get_dim_diagnostico_id(sdb: Any, nombre: Optional[str], *, dim_diagnostico_cls: Any) -> Optional[int]:
    if not nombre:
        return None
    dim = sdb.query(dim_diagnostico_cls).filter(dim_diagnostico_cls.nombre == nombre).first()
    return dim.id if dim else None


def get_dim_procedimiento_id(sdb: Any, nombre: Optional[str], *, dim_procedimiento_cls: Any) -> Optional[int]:
    if not nombre:
        return None
    dim = sdb.query(dim_procedimiento_cls).filter(dim_procedimiento_cls.nombre == nombre).first()
    return dim.id if dim else None


def actualizar_data_mart(
    sdb: Any,
    *,
    incremental: bool,
    poblar_dim_fecha_fn: Callable[[Any], None],
    poblar_dimensiones_catalogo_fn: Callable[[Any], None],
    hecho_programacion_cls: Any,
    surgical_programacion_cls: Any,
    get_dim_fecha_id_fn: Callable[[Any, Optional[date]], Optional[int]],
    get_or_create_dim_paciente_fn: Callable[[Any, Any], Optional[int]],
    get_dim_diagnostico_id_fn: Callable[[Any, Optional[str]], Optional[int]],
    get_dim_procedimiento_id_fn: Callable[[Any, Optional[str]], Optional[int]],
) -> Dict[str, Any]:
    poblar_dim_fecha_fn(sdb)
    poblar_dimensiones_catalogo_fn(sdb)
    if not incremental:
        sdb.query(hecho_programacion_cls).delete()
        sdb.commit()

    fuentes = sdb.query(surgical_programacion_cls).all()
    insertados = 0
    if incremental:
        sdb.query(hecho_programacion_cls).delete()
        sdb.commit()
    for row in fuentes:
        hecho = hecho_programacion_cls(
            fecha_id=get_dim_fecha_id_fn(sdb, row.fecha_programada),
            paciente_id=get_or_create_dim_paciente_fn(sdb, row),
            diagnostico_id=get_dim_diagnostico_id_fn(sdb, row.patologia),
            procedimiento_id=get_dim_procedimiento_id_fn(sdb, row.procedimiento_programado),
            grupo_patologia=row.grupo_patologia,
            grupo_procedimiento=row.grupo_procedimiento,
            ecog=row.ecog,
            charlson=row.charlson,
            tnm=row.tnm,
            uh_rango=row.uh_rango,
            litiasis_tamano=row.litiasis_tamano_rango,
            requiere_intermed=row.requiere_intermed,
            hgz=row.hgz,
            estatus=row.estatus,
            cantidad=1,
        )
        sdb.add(hecho)
        insertados += 1
    sdb.commit()
    return {"fuentes": len(fuentes), "hechos": insertados}


def check_data_quality(
    sdb: Any,
    *,
    surgical_programacion_cls: Any,
    data_quality_log_cls: Any,
    patologias_catalogo: List[str],
) -> int:
    problemas_nuevos = 0
    rows = sdb.query(surgical_programacion_cls).all()
    for row in rows:
        checks: List[Tuple[str, Any, str, str]] = []
        if row.nss and not re.fullmatch(r"\d{10}", row.nss):
            checks.append(("nss", row.nss, "NSS debe tener 10 dígitos numéricos", "error"))
        if row.edad is not None and (row.edad < 0 or row.edad > 120):
            checks.append(("edad", str(row.edad), "Edad fuera de rango (0-120)", "error"))
        if row.patologia and row.patologia not in patologias_catalogo:
            checks.append(("patologia", row.patologia, "Patología no reconocida", "warning"))
        for campo, valor, problema, severidad in checks:
            exists = sdb.query(data_quality_log_cls).filter(
                data_quality_log_cls.tabla == "surgical_programaciones",
                data_quality_log_cls.registro_id == row.id,
                data_quality_log_cls.campo == campo,
                data_quality_log_cls.problema == problema,
                data_quality_log_cls.corregido == False,  # noqa: E712
            ).first()
            if exists:
                continue
            sdb.add(
                data_quality_log_cls(
                    tabla="surgical_programaciones",
                    registro_id=row.id,
                    campo=campo,
                    valor=str(valor) if valor is not None else None,
                    problema=problema,
                    severidad=severidad,
                )
            )
            problemas_nuevos += 1
    sdb.commit()
    return problemas_nuevos


def qx_catalogos_payload(
    *,
    sexos: List[str],
    patologias: List[str],
    patologias_cie10: List[Dict[str, str]],
    patologias_oncologicas: Any,
    patologias_litiasis: Any,
    procedimientos: List[str],
    procedimientos_requieren_abordaje: Any,
    procedimientos_abiertos: Any,
    procedimientos_endoscopicos: Any,
    procedimientos_percutaneos: Any,
    insumos: List[str],
    hemoderivados: List[str],
) -> Dict[str, Any]:
    return {
        "sexos": sexos,
        "patologias": patologias,
        "patologias_cie10": patologias_cie10,
        "patologias_oncologicas": sorted(patologias_oncologicas),
        "patologias_litiasis": sorted(patologias_litiasis),
        "procedimientos": procedimientos,
        "procedimientos_requieren_abordaje": sorted(procedimientos_requieren_abordaje),
        "procedimientos_abiertos": sorted(procedimientos_abiertos),
        "procedimientos_endoscopicos": sorted(procedimientos_endoscopicos),
        "procedimientos_percutaneos": sorted(procedimientos_percutaneos),
        "insumos": insumos,
        "hemoderivados": hemoderivados,
    }


def entrenar_modelo_riesgo(
    sdb: Any,
    *,
    pd_module: Any,
    random_forest_classifier_cls: Any,
    train_test_split_fn: Any,
    roc_auc_score_fn: Any,
    joblib_module: Any,
    surgical_programacion_cls: Any,
    modelo_riesgo_path: str,
) -> Dict[str, Any]:
    if pd_module is None or random_forest_classifier_cls is None or train_test_split_fn is None or roc_auc_score_fn is None or joblib_module is None:
        return {"ok": False, "message": "Dependencias de ML no disponibles"}
    rows = sdb.query(
        surgical_programacion_cls.edad,
        surgical_programacion_cls.sexo,
        surgical_programacion_cls.ecog,
        surgical_programacion_cls.charlson,
        surgical_programacion_cls.estatus,
    ).filter(surgical_programacion_cls.estatus.in_(["REALIZADA", "CANCELADA"])).all()
    if len(rows) < 20:
        return {"ok": False, "message": "No hay datos suficientes para entrenar"}
    df = pd_module.DataFrame(
        [
            {"edad": r.edad, "sexo": r.sexo, "ecog": r.ecog, "charlson": r.charlson, "estatus": r.estatus}
            for r in rows
        ]
    )
    df["sexo_cod"] = df["sexo"].fillna("").str.upper().map({"MASCULINO": 1, "FEMENINO": 0}).fillna(0)
    df["ecog_cod"] = df["ecog"].fillna("").astype(str).str.extract(r"(\d+)")[0].fillna(0).astype(int)
    df["charlson_cod"] = df["charlson"].fillna("").astype(str).str.extract(r"(\d+)")[0].fillna(0).astype(int)
    df["target"] = (df["estatus"] == "CANCELADA").astype(int)
    if df["target"].nunique() < 2:
        return {"ok": False, "message": "Se requieren casos de ambas clases (REALIZADA y CANCELADA)"}

    X = df[["edad", "sexo_cod", "ecog_cod", "charlson_cod"]].fillna(0)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split_fn(X, y, test_size=0.2, random_state=42, stratify=y)
    model = random_forest_classifier_cls(n_estimators=200, max_depth=6, random_state=42)
    model.fit(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score_fn(y_test, y_prob))
    joblib_module.dump(model, modelo_riesgo_path)
    return {"ok": True, "auc": round(auc, 4), "rows": int(len(df))}


def entrenar_modelo_riesgo_v2(
    sdb: Any,
    *,
    pd_module: Any,
    random_forest_classifier_cls: Any,
    train_test_split_fn: Any,
    roc_auc_score_fn: Any,
    joblib_module: Any,
    surgical_programacion_cls: Any,
    modelo_ml_cls: Any,
    modelo_riesgo_v2_path: str,
    ensure_modelos_ml_schema_fn: Callable[[], None],
) -> Dict[str, Any]:
    if pd_module is None or random_forest_classifier_cls is None or train_test_split_fn is None or roc_auc_score_fn is None or joblib_module is None:
        return {"ok": False, "message": "Dependencias de ML no disponibles"}

    rows = sdb.query(
        surgical_programacion_cls.edad,
        surgical_programacion_cls.sexo,
        surgical_programacion_cls.ecog,
        surgical_programacion_cls.charlson,
        surgical_programacion_cls.grupo_procedimiento,
        surgical_programacion_cls.abordaje,
        surgical_programacion_cls.requiere_intermed,
        surgical_programacion_cls.estatus,
    ).filter(surgical_programacion_cls.estatus.in_(["REALIZADA", "CANCELADA"])).all()

    if len(rows) < 50:
        return {"ok": False, "message": "Datos insuficientes para entrenamiento (mínimo 50 registros)"}

    df = pd_module.DataFrame(
        [
            {
                "edad": r.edad,
                "sexo": r.sexo,
                "ecog": r.ecog,
                "charlson": r.charlson,
                "grupo_proc": r.grupo_procedimiento,
                "abordaje": r.abordaje,
                "intermed": r.requiere_intermed,
                "estatus": r.estatus,
            }
            for r in rows
        ]
    )

    df["sexo_cod"] = df["sexo"].fillna("").str.upper().map({"MASCULINO": 1, "FEMENINO": 0}).fillna(0)
    df["ecog_cod"] = df["ecog"].fillna("").astype(str).str.extract(r"(\d+)")[0].fillna(0).astype(int)
    df["charlson_cod"] = df["charlson"].fillna("").astype(str).str.extract(r"(\d+)")[0].fillna(0).astype(int)
    df["intermed_cod"] = (df["intermed"].fillna("").str.upper() == "SI").astype(int)

    grupo_dummies = pd_module.get_dummies(df["grupo_proc"].fillna("NO_REGISTRADO"), prefix="grupo", dummy_na=False)
    abordaje_dummies = pd_module.get_dummies(df["abordaje"].fillna("NO_REGISTRADO"), prefix="ab", dummy_na=False)
    X = pd_module.concat([df[["edad", "sexo_cod", "ecog_cod", "charlson_cod", "intermed_cod"]], grupo_dummies, abordaje_dummies], axis=1).fillna(0)
    y = (df["estatus"] == "CANCELADA").astype(int)

    if y.nunique() < 2:
        return {"ok": False, "message": "Se requieren ambas clases (REALIZADA y CANCELADA)"}

    X_train, X_test, y_train, y_test = train_test_split_fn(X, y, test_size=0.2, random_state=42, stratify=y)
    model = random_forest_classifier_cls(n_estimators=250, max_depth=8, min_samples_split=10, random_state=42)
    model.fit(X_train, y_train)
    y_prob = model.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score_fn(y_test, y_prob))

    payload = {"model": model, "features": list(X.columns), "version": datetime.now().strftime("%Y%m%d_%H%M%S")}
    joblib_module.dump(payload, modelo_riesgo_v2_path)

    ensure_modelos_ml_schema_fn()
    try:
        sdb.add(
            modelo_ml_cls(
                nombre="riesgo_quirurgico_v2",
                version=payload["version"],
                auc=auc,
                features=",".join(payload["features"]),
                path=modelo_riesgo_v2_path,
            )
        )
        sdb.commit()
    except Exception:
        sdb.rollback()

    return {
        "ok": True,
        "auc": round(auc, 4),
        "rows": int(len(df)),
        "features": payload["features"],
        "version": payload["version"],
    }
