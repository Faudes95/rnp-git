from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session


_DYNAMIC_SYMBOLS = [
    "_new_surgical_session",
    "SurgicalProgramacionDB",
    "SurgicalPostquirurgicaDB",
    "SurgicalUrgenciaProgramacionDB",
    "ConsultaDB",
    "classify_age_group",
    "classify_pathology_group",
    "normalize_upper",
    "_compute_preventive_priority",
    "_estimate_cancelation_risk",
    "_waiting_days_bucket",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing: List[str] = []
    for name in _DYNAMIC_SYMBOLS:
        if name in module_globals:
            continue
        try:
            module_globals[name] = getattr(m, name)
        except Exception:
            missing.append(name)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def build_pending_row_with_priority(row: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_symbols()
    fecha_ingreso = row.get("fecha_ingreso_pendiente_programar")
    fecha_ingreso_date: Optional[date] = None
    if isinstance(fecha_ingreso, datetime):
        fecha_ingreso_date = fecha_ingreso.date()
    elif isinstance(fecha_ingreso, date):
        fecha_ingreso_date = fecha_ingreso

    dias_en_espera = row.get("dias_en_espera")
    if dias_en_espera is None and fecha_ingreso_date is not None:
        dias_en_espera = max((date.today() - fecha_ingreso_date).days, 0)

    prioridad = row.get("prioridad_clinica")
    score_prev = row.get("score_preventivo")
    motivo = row.get("motivo_prioridad")
    if not prioridad or score_prev is None or not motivo:
        prioridad_calc, score_calc, motivo_calc = _compute_preventive_priority(
            edad=row.get("edad"),
            grupo_patologia=row.get("grupo_patologia"),
            ecog=row.get("ecog"),
            charlson=row.get("charlson"),
            dias_espera=dias_en_espera,
            requiere_intermed=row.get("requiere_intermed"),
        )
        prioridad = prioridad or prioridad_calc
        score_prev = score_calc if score_prev is None else score_prev
        motivo = motivo or motivo_calc

    riesgo_cancelacion = row.get("riesgo_cancelacion_predicho")
    if riesgo_cancelacion is None:
        riesgo_cancelacion = _estimate_cancelation_risk(
            edad=row.get("edad"),
            ecog=row.get("ecog"),
            charlson=row.get("charlson"),
            dias_espera=dias_en_espera,
            requiere_intermed=row.get("requiere_intermed"),
        )

    row["fecha_ingreso_pendiente_programar"] = fecha_ingreso_date.isoformat() if fecha_ingreso_date else None
    row["dias_en_espera"] = dias_en_espera
    row["dias_en_espera_rango"] = _waiting_days_bucket(dias_en_espera)
    row["prioridad_clinica"] = prioridad
    row["score_preventivo"] = score_prev
    row["motivo_prioridad"] = motivo
    row["riesgo_cancelacion_predicho"] = riesgo_cancelacion
    return row


def build_pending_programar_dataset(
    db: Session,
    *,
    sdb: Optional[Session] = None,
    limit: int = 2000,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    """Cola preventiva: protocolos completos todavía no programados."""
    close_sdb = False
    if sdb is None:
        sdb = _new_surgical_session(enable_dual_write=True)
        close_sdb = True

    try:
        # Programaciones en flujo quirúrgico activo.
        active_ids = {
            int(x[0])
            for x in sdb.query(SurgicalProgramacionDB.consulta_id)
            .filter(SurgicalProgramacionDB.consulta_id.isnot(None))
            .filter(SurgicalProgramacionDB.estatus.in_(["PROGRAMADA", "REALIZADA"]))
            .all()
            if x and x[0] is not None
        }

        # Programaciones explícitamente pendientes registradas en quirófano.
        pendientes_qx = (
            sdb.query(SurgicalProgramacionDB)
            .filter(SurgicalProgramacionDB.estatus == "PENDIENTE")
            .order_by(SurgicalProgramacionDB.id.desc())
            .limit(max(1, int(limit)))
            .all()
        )
        pending_map: Dict[int, Dict[str, Any]] = {}
        for row in pendientes_qx:
            if row.consulta_id is None:
                continue
            pending_map[int(row.consulta_id)] = build_pending_row_with_priority({
                "consulta_id": row.consulta_id,
                "nss": row.nss or "NO_REGISTRADO",
                "paciente_nombre": row.paciente_nombre or "NO_REGISTRADO",
                "edad": row.edad,
                "edad_grupo": classify_age_group(row.edad),
                "sexo": row.sexo or "NO_REGISTRADO",
                "hgz": row.hgz or "NO_REGISTRADO",
                "diagnostico": row.patologia or row.diagnostico_principal or "NO_REGISTRADO",
                "procedimiento": row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO",
                "grupo_patologia": row.grupo_patologia or classify_pathology_group(normalize_upper(row.patologia or row.diagnostico_principal)),
                "ecog": row.ecog,
                "charlson": row.charlson,
                "requiere_intermed": row.requiere_intermed,
                "fecha_ingreso_pendiente_programar": row.fecha_ingreso_pendiente_programar or row.creado_en,
                "dias_en_espera": row.dias_en_espera,
                "prioridad_clinica": row.prioridad_clinica,
                "motivo_prioridad": row.motivo_prioridad,
                "score_preventivo": row.score_preventivo,
                "riesgo_cancelacion_predicho": row.riesgo_cancelacion_predicho,
                "fuente": "QUIROFANO_PENDIENTE",
            })

        consultas_completas = (
            db.query(ConsultaDB)
            .filter(ConsultaDB.estatus_protocolo == "completo")
            .order_by(ConsultaDB.id.desc())
            .limit(max(1, int(limit)))
            .all()
        )
        for c in consultas_completas:
            if c.id in active_ids or c.id in pending_map:
                continue
            pending_map[c.id] = build_pending_row_with_priority({
                "consulta_id": c.id,
                "nss": c.nss or "NO_REGISTRADO",
                "paciente_nombre": c.nombre or "NO_REGISTRADO",
                "edad": c.edad,
                "edad_grupo": classify_age_group(c.edad),
                "sexo": c.sexo or "NO_REGISTRADO",
                "hgz": "NO_REGISTRADO",
                "diagnostico": c.diagnostico_principal or "NO_REGISTRADO",
                "procedimiento": "NO_REGISTRADO",
                "grupo_patologia": classify_pathology_group(normalize_upper(c.diagnostico_principal)),
                "ecog": None,
                "charlson": None,
                "requiere_intermed": None,
                "fecha_ingreso_pendiente_programar": c.fecha_registro,
                "dias_en_espera": None,
                "prioridad_clinica": None,
                "motivo_prioridad": None,
                "score_preventivo": None,
                "riesgo_cancelacion_predicho": None,
                "fuente": "CONSULTA_COMPLETA",
            })

        rows = sorted(
            pending_map.values(),
            key=lambda x: (-(x.get("consulta_id") or 0), str(x.get("paciente_nombre") or "")),
        )
        return rows[: max(1, int(limit))]
    finally:
        if close_sdb and sdb is not None:
            sdb.close()


def build_realizadas_dataset(
    *,
    sdb: Session,
    limit: int = 3000,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    """Lista de cirugías realizadas, enriquecida con datos postquirúrgicos."""
    rows = (
        sdb.query(SurgicalProgramacionDB)
        .filter(SurgicalProgramacionDB.estatus == "REALIZADA")
        .order_by(SurgicalProgramacionDB.fecha_realizacion.desc(), SurgicalProgramacionDB.id.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    if not rows:
        return []

    programacion_ids = [r.id for r in rows if r.id is not None]
    notes_by_prog: Dict[int, SurgicalPostquirurgicaDB] = {}
    if programacion_ids:
        notes = (
            sdb.query(SurgicalPostquirurgicaDB)
            .filter(SurgicalPostquirurgicaDB.surgical_programacion_id.in_(programacion_ids))
            .order_by(SurgicalPostquirurgicaDB.id.desc())
            .all()
        )
        for n in notes:
            if n.surgical_programacion_id not in notes_by_prog:
                notes_by_prog[n.surgical_programacion_id] = n

    out: List[Dict[str, Any]] = []
    for row in rows:
        note = notes_by_prog.get(row.id)
        sangrado = row.sangrado_ml
        tiempo_quirurgico_min = row.tiempo_quirurgico_min
        if sangrado is None and note is not None:
            sangrado = note.sangrado_ml
        if tiempo_quirurgico_min is None and note is not None:
            tiempo_quirurgico_min = note.tiempo_quirurgico_min
        out.append(
            {
                "id": row.id,
                "consulta_id": row.consulta_id,
                "quirofano_id": row.quirofano_id,
                "fecha_realizacion": (row.fecha_realizacion or row.fecha_postquirurgica),
                "nss": row.nss or "NO_REGISTRADO",
                "paciente_nombre": row.paciente_nombre or "NO_REGISTRADO",
                "edad": row.edad,
                "edad_grupo": classify_age_group(row.edad),
                "sexo": row.sexo or "NO_REGISTRADO",
                "hgz": row.hgz or "NO_REGISTRADO",
                "diagnostico": row.patologia or row.diagnostico_principal or "NO_REGISTRADO",
                "procedimiento": row.procedimiento_realizado or row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO",
                "grupo_patologia": row.grupo_patologia or classify_pathology_group(normalize_upper(row.patologia or row.diagnostico_principal)),
                "ecog": row.ecog or "NO_REGISTRADO",
                "charlson": row.charlson or "NO_REGISTRADO",
                "cirujano": row.cirujano or (note.cirujano if note else None) or "NO_REGISTRADO",
                "sangrado_ml": sangrado,
                "tiempo_quirurgico_min": tiempo_quirurgico_min,
                "transfusion": row.transfusion or (note.transfusion if note else None) or "NO_REGISTRADO",
                "solicita_hemoderivados": row.solicita_hemoderivados or "NO",
                "hemoderivados_pg_solicitados": row.hemoderivados_pg_solicitados or 0,
                "hemoderivados_pfc_solicitados": row.hemoderivados_pfc_solicitados or 0,
                "hemoderivados_cp_solicitados": row.hemoderivados_cp_solicitados or 0,
                "uso_hemoderivados": (
                    row.uso_hemoderivados
                    or (note.uso_hemoderivados if note else None)
                    or ("SI" if str(row.transfusion or (note.transfusion if note else "")).upper() == "SI" else "NO")
                ),
                "hemoderivados_pg_utilizados": row.hemoderivados_pg_utilizados if row.hemoderivados_pg_utilizados is not None else (note.hemoderivados_pg_utilizados if note and note.hemoderivados_pg_utilizados is not None else 0),
                "hemoderivados_pfc_utilizados": row.hemoderivados_pfc_utilizados if row.hemoderivados_pfc_utilizados is not None else (note.hemoderivados_pfc_utilizados if note and note.hemoderivados_pfc_utilizados is not None else 0),
                "hemoderivados_cp_utilizados": row.hemoderivados_cp_utilizados if row.hemoderivados_cp_utilizados is not None else (note.hemoderivados_cp_utilizados if note and note.hemoderivados_cp_utilizados is not None else 0),
                "antibiotico": row.antibiotico or (note.antibiotico if note else None) or "NO_REGISTRADO",
                "clavien_dindo": row.clavien_dindo or (note.clavien_dindo if note else None) or "NO_REGISTRADO",
                "margen_quirurgico": row.margen_quirurgico or (note.margen_quirurgico if note else None) or "NO_REGISTRADO",
                "neuropreservacion": row.neuropreservacion or (note.neuropreservacion if note else None) or "NO_REGISTRADO",
                "linfadenectomia": row.linfadenectomia or (note.linfadenectomia if note else None) or "NO_REGISTRADO",
                "reingreso_30d": row.reingreso_30d or (note.reingreso_30d if note else None) or "NO_REGISTRADO",
                "reintervencion_30d": row.reintervencion_30d or (note.reintervencion_30d if note else None) or "NO_REGISTRADO",
                "mortalidad_30d": row.mortalidad_30d or (note.mortalidad_30d if note else None) or "NO_REGISTRADO",
                "reingreso_90d": row.reingreso_90d or (note.reingreso_90d if note else None) or "NO_REGISTRADO",
                "reintervencion_90d": row.reintervencion_90d or (note.reintervencion_90d if note else None) or "NO_REGISTRADO",
                "mortalidad_90d": row.mortalidad_90d or (note.mortalidad_90d if note else None) or "NO_REGISTRADO",
                "stone_free": row.stone_free or (note.stone_free if note else None) or "NO_REGISTRADO",
                "composicion_lito": row.composicion_lito or (note.composicion_lito if note else None) or "NO_REGISTRADO",
                "recurrencia_litiasis": row.recurrencia_litiasis or (note.recurrencia_litiasis if note else None) or "NO_REGISTRADO",
                "cateter_jj_colocado": row.cateter_jj_colocado or (note.cateter_jj_colocado if note else None) or "NO",
                "fecha_colocacion_jj": row.fecha_colocacion_jj or (note.fecha_colocacion_jj if note else None),
                "modulo_origen": row.modulo_origen or "quirofano",
                "urgencia_programacion_id": row.urgencia_programacion_id,
            }
        )
    return out


def build_programadas_dataset(
    *,
    sdb: Session,
    limit: int = 3000,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    """Listado de cirugías programadas para cohortes y métricas."""
    rows = (
        sdb.query(SurgicalProgramacionDB)
        .filter(SurgicalProgramacionDB.estatus == "PROGRAMADA")
        .order_by(SurgicalProgramacionDB.fecha_programada.desc(), SurgicalProgramacionDB.id.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        wait_days = row.dias_en_espera
        if wait_days is None and row.fecha_ingreso_pendiente_programar:
            wait_days = max((date.today() - row.fecha_ingreso_pendiente_programar.date()).days, 0)
        prioridad, score_prev, motivo = _compute_preventive_priority(
            edad=row.edad,
            grupo_patologia=row.grupo_patologia,
            ecog=row.ecog,
            charlson=row.charlson,
            dias_espera=wait_days,
            requiere_intermed=row.requiere_intermed,
        )
        out.append(
            {
                "id": row.id,
                "consulta_id": row.consulta_id,
                "quirofano_id": row.quirofano_id,
                "fecha_programada": row.fecha_programada.isoformat() if row.fecha_programada else None,
                "nss": row.nss or "NO_REGISTRADO",
                "paciente_nombre": row.paciente_nombre or "NO_REGISTRADO",
                "edad": row.edad,
                "edad_grupo": classify_age_group(row.edad),
                "sexo": row.sexo or "NO_REGISTRADO",
                "hgz": row.hgz or "NO_REGISTRADO",
                "diagnostico": row.patologia or row.diagnostico_principal or "NO_REGISTRADO",
                "procedimiento": row.procedimiento_programado or row.procedimiento or "NO_REGISTRADO",
                "grupo_patologia": row.grupo_patologia or classify_pathology_group(normalize_upper(row.patologia or row.diagnostico_principal)),
                "ecog": row.ecog or "NO_REGISTRADO",
                "charlson": row.charlson or "NO_REGISTRADO",
                "requiere_intermed": row.requiere_intermed,
                "solicita_hemoderivados": row.solicita_hemoderivados or "NO",
                "hemoderivados_pg_solicitados": row.hemoderivados_pg_solicitados or 0,
                "hemoderivados_pfc_solicitados": row.hemoderivados_pfc_solicitados or 0,
                "hemoderivados_cp_solicitados": row.hemoderivados_cp_solicitados or 0,
                "prioridad_clinica": row.prioridad_clinica or prioridad,
                "score_preventivo": row.score_preventivo if row.score_preventivo is not None else score_prev,
                "motivo_prioridad": row.motivo_prioridad or motivo,
                "riesgo_cancelacion_predicho": (
                    row.riesgo_cancelacion_predicho
                    if row.riesgo_cancelacion_predicho is not None
                    else _estimate_cancelation_risk(
                        edad=row.edad,
                        ecog=row.ecog,
                        charlson=row.charlson,
                        dias_espera=wait_days,
                        requiere_intermed=row.requiere_intermed,
                    )
                ),
            }
        )
    return out


def build_urgencias_programadas_dataset(
    *,
    sdb: Session,
    limit: int = 3000,
) -> List[Dict[str, Any]]:
    _ensure_symbols()
    rows = (
        sdb.query(SurgicalUrgenciaProgramacionDB)
        .filter(SurgicalUrgenciaProgramacionDB.estatus == "PROGRAMADA")
        .order_by(SurgicalUrgenciaProgramacionDB.fecha_urgencia.desc(), SurgicalUrgenciaProgramacionDB.id.desc())
        .limit(max(1, int(limit)))
        .all()
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": row.id,
                "consulta_id": row.consulta_id,
                "nss": row.nss or "NO_REGISTRADO",
                "paciente_nombre": row.paciente_nombre or "NO_REGISTRADO",
                "edad": row.edad,
                "edad_grupo": classify_age_group(row.edad),
                "sexo": row.sexo or "NO_REGISTRADO",
                "hgz": row.hgz or "NO_REGISTRADO",
                "diagnostico": row.patologia or "NO_REGISTRADO",
                "procedimiento": row.procedimiento_programado or "NO_REGISTRADO",
                "grupo_patologia": row.grupo_patologia or classify_pathology_group(normalize_upper(row.patologia)),
                "ecog": row.ecog or "NO_REGISTRADO",
                "charlson": row.charlson or "NO_REGISTRADO",
                "requiere_intermed": row.requiere_intermed or "NO",
                "solicita_hemoderivados": row.solicita_hemoderivados or "NO",
                "hemoderivados_pg_solicitados": row.hemoderivados_pg_solicitados or 0,
                "hemoderivados_pfc_solicitados": row.hemoderivados_pfc_solicitados or 0,
                "hemoderivados_cp_solicitados": row.hemoderivados_cp_solicitados or 0,
                "uh_rango": row.uh_rango or "NO_REGISTRADO",
                "litiasis_tamano_rango": row.litiasis_tamano_rango or "NO_REGISTRADO",
                "litiasis_subtipo_20": row.litiasis_subtipo_20 or "NO_REGISTRADO",
                "litiasis_ubicacion": row.litiasis_ubicacion or "NO_REGISTRADO",
                "hidronefrosis": row.hidronefrosis or "NO_REGISTRADO",
                "patologia_cie10": row.patologia_cie10 or "NO_REGISTRADO",
                "fecha_urgencia": row.fecha_urgencia.isoformat() if row.fecha_urgencia else None,
            }
        )
    return out
