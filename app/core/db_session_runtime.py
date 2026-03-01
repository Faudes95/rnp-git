from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


def serialize_model_row(obj: Any) -> Tuple[Optional[type], Dict[str, Any], Dict[str, Any]]:
    if obj is None or not hasattr(obj, "__table__"):
        return None, {}, {}
    model_cls = obj.__class__
    table = getattr(model_cls, "__table__", None)
    if table is None:
        return None, {}, {}
    row: Dict[str, Any] = {}
    pk: Dict[str, Any] = {}
    for col in table.columns:
        col_name = col.name
        try:
            value = getattr(obj, col_name)
        except Exception:
            continue
        row[col_name] = value
        if col.primary_key:
            pk[col_name] = value
    return model_cls, row, pk


def is_model_for_base(obj: Any, model_base: Any) -> bool:
    try:
        return isinstance(obj, model_base)
    except Exception:
        return False


def capture_dual_write_ops(
    db: Any,
    *,
    model_base: Any,
    serialize_model_row_fn: Callable[[Any], Tuple[Optional[type], Dict[str, Any], Dict[str, Any]]],
    is_model_for_base_fn: Callable[[Any, Any], bool],
):
    inserts = []
    updates = []
    deletes = []
    for obj in list(db.new):
        if not is_model_for_base_fn(obj, model_base):
            continue
        cls, row, pk = serialize_model_row_fn(obj)
        if cls and row and pk:
            inserts.append((cls, row, pk))
    for obj in list(db.dirty):
        if not is_model_for_base_fn(obj, model_base):
            continue
        cls, row, pk = serialize_model_row_fn(obj)
        if cls and row and pk:
            updates.append((cls, row, pk))
    for obj in list(db.deleted):
        if not is_model_for_base_fn(obj, model_base):
            continue
        cls, _, pk = serialize_model_row_fn(obj)
        if cls and pk:
            deletes.append((cls, pk))
    return inserts, updates, deletes


def apply_dual_write_ops(
    *,
    shadow_factory: Any,
    inserts: list,
    updates: list,
    deletes: list,
    label: str,
    logger: Any,
) -> None:
    if shadow_factory is None:
        return
    shadow = shadow_factory()
    try:
        for cls, row, pk in inserts + updates:
            try:
                found = shadow.query(cls).filter_by(**pk).first()
                if found is None:
                    shadow.add(cls(**row))
                else:
                    for key, value in row.items():
                        setattr(found, key, value)
            except Exception:
                continue
        for cls, pk in deletes:
            try:
                found = shadow.query(cls).filter_by(**pk).first()
                if found is not None:
                    shadow.delete(found)
            except Exception:
                continue
        shadow.commit()
    except Exception as exc:
        shadow.rollback()
        logger.warning(
            {
                "event": "dual_write_shadow_failed",
                "label": label,
                "error": str(exc),
            }
        )
    finally:
        shadow.close()


def install_dual_write_commit_wrapper(
    db: Any,
    *,
    shadow_factory: Any,
    model_base: Any,
    label: str,
    capture_dual_write_ops_fn: Callable[[Any, Any], Any],
    apply_dual_write_ops_fn: Callable[..., None],
):
    if shadow_factory is None:
        return db
    if getattr(db, "_dual_write_wrapped", False):
        return db
    original_commit = db.commit

    def commit_with_dual_write():
        db.flush()
        inserts, updates, deletes = capture_dual_write_ops_fn(db, model_base)
        original_commit()
        apply_dual_write_ops_fn(
            shadow_factory=shadow_factory,
            inserts=inserts,
            updates=updates,
            deletes=deletes,
            label=label,
        )

    db.commit = commit_with_dual_write  # type: ignore[assignment]
    setattr(db, "_dual_write_wrapped", True)
    return db


def new_session_with_optional_dual_write(
    *,
    session_factory: Callable[[], Any],
    enable_dual_write: bool,
    dual_write_enabled: bool,
    shadow_session_factory: Any,
    model_base: Any,
    label: str,
    install_wrapper_fn: Callable[..., Any],
):
    db = session_factory()
    if enable_dual_write and dual_write_enabled and shadow_session_factory is not None:
        install_wrapper_fn(
            db,
            shadow_factory=shadow_session_factory,
            model_base=model_base,
            label=label,
        )
    return db


def sync_consulta_sensitive_encrypted(
    target: Any,
    *,
    enable_pii_encryption: bool,
    encrypt_sensitive_value_fn: Callable[[Any], Any],
) -> None:
    if not enable_pii_encryption:
        return
    target.curp_enc = encrypt_sensitive_value_fn(target.curp)
    target.nss_enc = encrypt_sensitive_value_fn(target.nss)
    target.nombre_enc = encrypt_sensitive_value_fn(target.nombre)
    target.telefono_enc = encrypt_sensitive_value_fn(target.telefono)
    target.email_enc = encrypt_sensitive_value_fn(target.email)


def sync_surgical_sensitive_encrypted(
    target: Any,
    *,
    enable_pii_encryption: bool,
    encrypt_sensitive_value_fn: Callable[[Any], Any],
) -> None:
    if not enable_pii_encryption:
        return
    target.curp_enc = encrypt_sensitive_value_fn(target.curp)
    target.nss_enc = encrypt_sensitive_value_fn(target.nss)
    target.paciente_nombre_enc = encrypt_sensitive_value_fn(target.paciente_nombre)
