from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional


_DYNAMIC_SYMBOLS = [
    "_new_surgical_session",
    "SurgicalProgramacionDB",
    "classify_age_group",
    "normalize_upper",
    "classify_pathology_group",
    "classify_procedure_group",
    "get_cie11_from_patologia",
    "get_snomed_from_patologia",
    "get_cie9mc_from_procedimiento",
    "utcnow",
    "_compute_preventive_priority",
    "_estimate_cancelation_risk",
    "push_module_feedback",
    "registrar_evento_flujo_quirurgico",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing = []
    for symbol in _DYNAMIC_SYMBOLS:
        if symbol in module_globals:
            continue
        try:
            module_globals[symbol] = getattr(m, symbol)
        except Exception:
            missing.append(symbol)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def sync_quirofano_to_surgical_db(
    consulta: ConsultaDB,
    cirugia: QuirofanoDB,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Replica programación quirúrgica en una BD quirúrgica dedicada.
    Es aditivo: no modifica ni reemplaza tablas del código maestro.
    """
    _ensure_symbols()
    if consulta is None or cirugia is None:
        return
    sdb = _new_surgical_session(enable_dual_write=True)
    try:
        row = sdb.query(SurgicalProgramacionDB).filter(
            SurgicalProgramacionDB.quirofano_id == cirugia.id
        ).first()
        if row is None:
            row = SurgicalProgramacionDB(
                quirofano_id=cirugia.id,
                consulta_id=cirugia.consulta_id,
            )
            sdb.add(row)

        row.curp = consulta.curp
        row.nss = consulta.nss
        row.agregado_medico = getattr(consulta, "agregado_medico", None)
        row.paciente_nombre = consulta.nombre
        row.edad = consulta.edad
        row.edad_grupo = classify_age_group(consulta.edad)
        row.sexo = consulta.sexo
        row.grupo_sexo = normalize_upper(consulta.sexo)
        row.diagnostico_principal = consulta.diagnostico_principal
        row.patologia = consulta.diagnostico_principal
        row.grupo_patologia = classify_pathology_group(normalize_upper(consulta.diagnostico_principal))
        row.procedimiento = cirugia.procedimiento
        row.procedimiento_programado = cirugia.procedimiento
        row.grupo_procedimiento = classify_procedure_group(cirugia.procedimiento or "", "", "")
        row.cie11_codigo = get_cie11_from_patologia(row.patologia)
        row.snomed_codigo = get_snomed_from_patologia(row.patologia)
        row.cie9mc_codigo = get_cie9mc_from_procedimiento(row.procedimiento_programado)
        row.cirujano = cirugia.cirujano
        row.anestesiologo = cirugia.anestesiologo
        row.quirofano = cirugia.quirofano
        row.fecha_programada = cirugia.fecha_programada
        row.fecha_realizacion = cirugia.fecha_realizacion
        row.estatus = cirugia.estatus
        row.protocolo_completo = "SI" if (consulta.estatus_protocolo or "").lower() == "completo" else "NO"
        if row.fecha_ingreso_pendiente_programar is None:
            if consulta.fecha_registro:
                row.fecha_ingreso_pendiente_programar = datetime.combine(consulta.fecha_registro, datetime.min.time())
            else:
                row.fecha_ingreso_pendiente_programar = utcnow()
        if row.fecha_ingreso_pendiente_programar:
            base_date = row.fecha_ingreso_pendiente_programar.date()
            if row.estatus == "PROGRAMADA" and row.fecha_programada:
                row.dias_en_espera = max((row.fecha_programada - base_date).days, 0)
            elif row.estatus == "REALIZADA" and row.fecha_realizacion:
                row.dias_en_espera = max((row.fecha_realizacion - base_date).days, 0)
            else:
                row.dias_en_espera = max((date.today() - base_date).days, 0)
        prioridad, score_prev, motivo = _compute_preventive_priority(
            edad=row.edad,
            grupo_patologia=row.grupo_patologia,
            ecog=row.ecog,
            charlson=row.charlson,
            dias_espera=row.dias_en_espera,
            requiere_intermed=row.requiere_intermed,
        )
        row.prioridad_clinica = prioridad
        row.score_preventivo = score_prev
        row.motivo_prioridad = motivo
        row.riesgo_cancelacion_predicho = _estimate_cancelation_risk(
            edad=row.edad,
            ecog=row.ecog,
            charlson=row.charlson,
            dias_espera=row.dias_en_espera,
            requiere_intermed=row.requiere_intermed,
        )
        row.notas = cirugia.notas

        if extra_fields:
            for key, value in extra_fields.items():
                if hasattr(row, key):
                    setattr(row, key, value)
            if not row.grupo_patologia and row.patologia:
                row.grupo_patologia = classify_pathology_group(normalize_upper(row.patologia))
            if not row.edad_grupo and row.edad is not None:
                row.edad_grupo = classify_age_group(row.edad)
            if not row.grupo_procedimiento and row.procedimiento_programado:
                row.grupo_procedimiento = classify_procedure_group(
                    row.procedimiento_programado or "",
                    row.abordaje or "",
                    row.sistema_succion or "",
                )
            if not row.cie11_codigo:
                row.cie11_codigo = get_cie11_from_patologia(row.patologia)
            if not row.snomed_codigo:
                row.snomed_codigo = get_snomed_from_patologia(row.patologia)
            if not row.cie9mc_codigo:
                row.cie9mc_codigo = get_cie9mc_from_procedimiento(row.procedimiento_programado)
            if row.fecha_ingreso_pendiente_programar:
                base_date = row.fecha_ingreso_pendiente_programar.date()
                if row.estatus == "PROGRAMADA" and row.fecha_programada:
                    row.dias_en_espera = max((row.fecha_programada - base_date).days, 0)
                elif row.estatus == "REALIZADA" and row.fecha_realizacion:
                    row.dias_en_espera = max((row.fecha_realizacion - base_date).days, 0)
                else:
                    row.dias_en_espera = max((date.today() - base_date).days, 0)
            prioridad, score_prev, motivo = _compute_preventive_priority(
                edad=row.edad,
                grupo_patologia=row.grupo_patologia,
                ecog=row.ecog,
                charlson=row.charlson,
                dias_espera=row.dias_en_espera,
                requiere_intermed=row.requiere_intermed,
            )
            row.prioridad_clinica = prioridad
            row.score_preventivo = score_prev
            row.motivo_prioridad = motivo
            row.riesgo_cancelacion_predicho = _estimate_cancelation_risk(
                edad=row.edad,
                ecog=row.ecog,
                charlson=row.charlson,
                dias_espera=row.dias_en_espera,
                requiere_intermed=row.requiere_intermed,
            )

        sdb.commit()
        push_module_feedback(
            consulta_id=cirugia.consulta_id,
            modulo="quirofano",
            referencia_id=f"quirofano:{cirugia.id}",
            payload={
                "estatus": cirugia.estatus,
                "fecha_programada": str(cirugia.fecha_programada) if cirugia.fecha_programada else None,
                "procedimiento": cirugia.procedimiento,
            },
        )
        registrar_evento_flujo_quirurgico(
            consulta_id=cirugia.consulta_id,
            evento="PROGRAMADA" if (cirugia.estatus or "").upper() == "PROGRAMADA" else "ACTUALIZACION_QUIROFANO",
            estatus=cirugia.estatus,
            surgical_programacion_id=row.id,
            quirofano_id=cirugia.id,
            edad=row.edad,
            sexo=row.sexo,
            nss=row.nss,
            hgz=row.hgz,
            diagnostico=row.patologia or row.diagnostico_principal,
            procedimiento=row.procedimiento_programado or row.procedimiento,
            ecog=row.ecog,
            cirujano=row.cirujano,
        )
    except Exception:
        sdb.rollback()
    finally:
        sdb.close()
