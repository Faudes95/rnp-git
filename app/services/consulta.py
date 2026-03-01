"""Servicios de consulta clínica (extracción progresiva y aditiva)."""

from typing import Any, Callable, Dict, Iterable


def preparar_payload_consulta(
    raw_form: Dict[str, Any],
    *,
    validate_csrf_fn: Callable[[Dict[str, Any], Any], None],
    request: Any,
    normalize_form_data_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    apply_aliases_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    aplicar_derivaciones_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    expected_fields: Iterable[str],
) -> Dict[str, Any]:
    """Conserva el pipeline actual de normalización/derivación de consulta."""
    payload = {k: v for k, v in raw_form.items()}
    validate_csrf_fn(payload, request)
    payload.pop("csrf_token", None)
    payload = normalize_form_data_fn(payload)
    payload = apply_aliases_fn(payload)
    payload = aplicar_derivaciones_fn(payload)
    for field_name in expected_fields:
        payload.setdefault(field_name, None)
    return payload


def mensaje_estatus_consulta(estatus: str) -> str:
    """Mantiene el mismo criterio de texto de estatus usado por la vista."""
    if estatus == "completo":
        return "✅ PACIENTE AGREGADO A LISTA DE ESPERA QUIRÚRGICA"
    if estatus == "incompleto":
        return "⚠️ PROTOCOLO INCOMPLETO - PENDIENTE ESTUDIOS"
    return "🔵 SEGUIMIENTO CONSULTA"

