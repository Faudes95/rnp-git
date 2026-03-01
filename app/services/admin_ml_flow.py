from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


def actualizar_data_mart_payload(
    *,
    sdb: Any,
    actualizar_data_mart_fn: Callable[..., Dict[str, Any]],
) -> Tuple[int, Dict[str, Any]]:
    try:
        result = actualizar_data_mart_fn(sdb, incremental=True)
        return 200, {"status": "ok", "result": result}
    except Exception as exc:
        sdb.rollback()
        return 500, {"status": "error", "message": str(exc)}


def calidad_datos_payload(
    *,
    sdb: Any,
    check_data_quality_fn: Callable[[Any], List[Dict[str, Any]]],
) -> Tuple[int, Dict[str, Any]]:
    try:
        issues = check_data_quality_fn(sdb)
        return 200, {"status": "ok", "problemas_detectados": issues}
    except Exception as exc:
        sdb.rollback()
        return 500, {"status": "error", "message": str(exc)}


def entrenar_modelo_payload(
    *,
    sdb: Any,
    entrenar_fn: Callable[[Any], Dict[str, Any]],
) -> Tuple[int, Dict[str, Any]]:
    result = entrenar_fn(sdb)
    if not result.get("ok"):
        return 400, result
    return 200, result


def listar_modelos_ml_payload(
    *,
    sdb: Any,
    ensure_modelos_ml_schema_fn: Callable[[], None],
    modelo_ml_cls: Any,
) -> List[Dict[str, Any]]:
    ensure_modelos_ml_schema_fn()
    try:
        modelos = sdb.query(modelo_ml_cls).order_by(modelo_ml_cls.fecha_entrenamiento.desc()).all()
    except Exception:
        sdb.rollback()
        return []
    return [
        {
            "id": m.id,
            "nombre": m.nombre,
            "version": m.version,
            "auc": m.auc,
            "fecha": m.fecha_entrenamiento.isoformat() if m.fecha_entrenamiento else None,
            "features": m.features.split(",") if m.features else [],
            "path": m.path,
        }
        for m in modelos
    ]

