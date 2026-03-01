from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def push_module_feedback_flow(
    *,
    consulta_id: int,
    modulo: str,
    referencia_id: Optional[str],
    payload: Optional[Dict[str, Any]],
    emit_module_feedback_fn: Callable[..., None],
    new_surgical_session_fn: Callable[..., Any],
    surgical_feedback_model: Any,
    outbox_emit_fn: Callable[..., None],
    event_emit_fn: Callable[..., None],
) -> None:
    emit_module_feedback_fn(
        consulta_id=consulta_id,
        modulo=modulo,
        referencia_id=referencia_id,
        payload=payload,
        new_surgical_session_fn=new_surgical_session_fn,
        surgical_feedback_model=surgical_feedback_model,
        outbox_emit_fn=outbox_emit_fn,
        event_emit_fn=event_emit_fn,
    )


def registrar_evento_flujo_quirurgico_flow(
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
    emit_flujo_quirurgico_event_fn: Callable[..., None],
    new_surgical_session_fn: Callable[..., Any],
    hecho_model: Any,
    normalize_upper_fn: Callable[[Optional[str]], Optional[str]],
    classify_age_group_fn: Callable[[Optional[int]], Optional[str]],
    normalize_nss_fn: Callable[[str], str],
    outbox_emit_fn: Callable[..., None],
    event_emit_fn: Callable[..., None],
) -> None:
    emit_flujo_quirurgico_event_fn(
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
        new_surgical_session_fn=new_surgical_session_fn,
        hecho_model=hecho_model,
        normalize_upper_fn=normalize_upper_fn,
        classify_age_group_fn=classify_age_group_fn,
        normalize_nss_fn=normalize_nss_fn,
        outbox_emit_fn=outbox_emit_fn,
        event_emit_fn=event_emit_fn,
    )

