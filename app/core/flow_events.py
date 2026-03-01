from __future__ import annotations

from typing import Any, Dict, Optional, Type


def emit_module_feedback(
    *,
    consulta_id: int,
    modulo: str,
    referencia_id: Optional[str],
    payload: Optional[Dict[str, Any]],
    new_surgical_session_fn,
    surgical_feedback_model: Type[Any],
    outbox_emit_fn,
    event_emit_fn,
) -> None:
    if not consulta_id:
        return
    sdb = new_surgical_session_fn(enable_dual_write=True)
    try:
        event_row = surgical_feedback_model(
            consulta_id=consulta_id,
            modulo=modulo,
            referencia_id=referencia_id,
            payload=payload or {},
        )
        sdb.add(event_row)
        sdb.commit()
        try:
            outbox_emit_fn(
                sdb,
                consulta_id=int(consulta_id),
                modulo=str(modulo or "").strip() or "modulo_desconocido",
                evento="FEEDBACK_SYNC",
                referencia_id=referencia_id,
                payload=payload or {},
                estado="DONE",
                commit=True,
            )
        except Exception:
            sdb.rollback()
        try:
            event_emit_fn(
                sdb,
                module=str(modulo or "").strip() or "modulo_desconocido",
                event_type="FEEDBACK_SYNC",
                entity="consulta",
                entity_id=str(int(consulta_id)),
                consulta_id=int(consulta_id),
                payload={"referencia_id": referencia_id, "payload": payload or {}},
                commit=True,
            )
        except Exception:
            sdb.rollback()
    except Exception:
        sdb.rollback()
    finally:
        sdb.close()


def emit_flujo_quirurgico_event(
    *,
    consulta_id: int,
    evento: str,
    estatus: Optional[str],
    surgical_programacion_id: Optional[int],
    quirofano_id: Optional[int],
    edad: Optional[int],
    sexo: Optional[str],
    nss: Optional[str],
    hgz: Optional[str],
    diagnostico: Optional[str],
    procedimiento: Optional[str],
    ecog: Optional[str],
    cirujano: Optional[str],
    sangrado_ml: Optional[float],
    metadata_json: Optional[Dict[str, Any]],
    new_surgical_session_fn,
    hecho_model: Type[Any],
    normalize_upper_fn,
    classify_age_group_fn,
    normalize_nss_fn,
    outbox_emit_fn,
    event_emit_fn,
) -> None:
    if not consulta_id or not evento:
        return
    sdb = new_surgical_session_fn(enable_dual_write=True)
    try:
        row = hecho_model(
            consulta_id=consulta_id,
            surgical_programacion_id=surgical_programacion_id,
            quirofano_id=quirofano_id,
            evento=normalize_upper_fn(evento),
            estatus=normalize_upper_fn(estatus),
            edad=edad,
            edad_grupo=classify_age_group_fn(edad) if edad is not None else None,
            sexo=normalize_upper_fn(sexo),
            nss=normalize_nss_fn(nss) if nss else None,
            hgz=(hgz or "").strip().upper() if hgz else None,
            diagnostico=(diagnostico or "").strip().upper() if diagnostico else None,
            procedimiento=(procedimiento or "").strip().upper() if procedimiento else None,
            ecog=(ecog or "").strip().upper() if ecog else None,
            cirujano=(cirujano or "").strip().upper() if cirujano else None,
            sangrado_ml=sangrado_ml,
            metadata_json=metadata_json or {},
        )
        sdb.add(row)
        sdb.commit()
        try:
            outbox_emit_fn(
                sdb,
                consulta_id=int(consulta_id),
                modulo="flujo_quirurgico",
                evento=normalize_upper_fn(evento),
                referencia_id=f"hecho_flujo:{getattr(row, 'id', None) or 'na'}",
                payload={
                    "estatus": normalize_upper_fn(estatus),
                    "nss": normalize_nss_fn(nss) if nss else None,
                    "hgz": (hgz or "").strip().upper() if hgz else None,
                    "diagnostico": (diagnostico or "").strip().upper() if diagnostico else None,
                    "procedimiento": (procedimiento or "").strip().upper() if procedimiento else None,
                    "cirujano": (cirujano or "").strip().upper() if cirujano else None,
                    "sangrado_ml": sangrado_ml,
                    "metadata_json": metadata_json or {},
                },
                estado="DONE",
                commit=True,
            )
        except Exception:
            sdb.rollback()
        try:
            event_emit_fn(
                sdb,
                module="flujo_quirurgico",
                event_type=normalize_upper_fn(evento),
                entity="consulta",
                entity_id=str(int(consulta_id)),
                consulta_id=int(consulta_id),
                payload={
                    "estatus": normalize_upper_fn(estatus),
                    "surgical_programacion_id": surgical_programacion_id,
                    "quirofano_id": quirofano_id,
                    "nss": normalize_nss_fn(nss) if nss else None,
                    "hgz": (hgz or "").strip().upper() if hgz else None,
                    "diagnostico": (diagnostico or "").strip().upper() if diagnostico else None,
                    "procedimiento": (procedimiento or "").strip().upper() if procedimiento else None,
                    "cirujano": (cirujano or "").strip().upper() if cirujano else None,
                    "sangrado_ml": sangrado_ml,
                    "metadata_json": metadata_json or {},
                },
                commit=True,
            )
        except Exception:
            sdb.rollback()
    except Exception:
        sdb.rollback()
    finally:
        sdb.close()
