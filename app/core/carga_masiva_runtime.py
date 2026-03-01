from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Set


@dataclass
class CargaMasivaTaskStatus:
    total: int = 0
    procesados: int = 0
    exitosos: int = 0
    errores: List[str] = field(default_factory=list)
    task_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "total": int(self.total),
            "procesados": int(self.procesados),
            "exitosos": int(self.exitosos),
            "errores": self.errores[:20],
        }


def prepare_excel_row(
    raw_row: Dict[str, Any],
    *,
    pd_module: Any,
    normalize_form_data_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    apply_aliases_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    aplicar_derivaciones_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    normalize_curp_fn: Callable[[str], str],
    normalize_nss_fn: Callable[[str], str],
) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key, value in raw_row.items():
        key_s = str(key).strip()
        if not key_s:
            continue
        cell = value
        if pd_module is not None:
            try:
                if pd_module.isna(cell):
                    cell = None
            except Exception:
                pass
        if hasattr(cell, "to_pydatetime"):
            try:
                cell = cell.to_pydatetime()
            except Exception:
                pass
        if isinstance(cell, datetime):
            cell = cell.date()
        if isinstance(cell, str):
            cell = cell.strip()
        data[key_s] = cell

    data = normalize_form_data_fn(data)
    data = apply_aliases_fn(data)
    data = aplicar_derivaciones_fn(data)
    if data.get("curp"):
        data["curp"] = normalize_curp_fn(str(data["curp"]))
    if data.get("nss"):
        data["nss"] = normalize_nss_fn(str(data["nss"]))
    return data


def process_massive_excel_dataframe(
    df: Any,
    *,
    required_columns: Set[str],
    progress_cb: Optional[Callable[[int, int], None]],
    prepare_excel_row_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    new_clinical_session_fn: Callable[..., Any],
    consulta_create_cls: Any,
    extraer_protocolo_detalles_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    detectar_inconsistencias_fn: Callable[[Dict[str, Any]], List[str]],
    generar_nota_soap_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    build_embedding_text_fn: Callable[[Dict[str, Any]], str],
    async_embeddings: bool,
    compute_embedding_fn: Callable[[str], Any],
    consulta_db_cls: Any,
    sync_consulta_embedding_vector_fn: Callable[..., Any],
    enqueue_embedding_fn: Callable[[int, str], Any],
    json_module: Any,
    today_fn: Callable[[], date],
) -> CargaMasivaTaskStatus:
    status = CargaMasivaTaskStatus()
    normalized_cols = {str(col).strip() for col in df.columns}
    missing = sorted(set(required_columns) - normalized_cols)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")

    status.total = int(len(df))
    if status.total <= 0:
        return status

    db = new_clinical_session_fn(enable_dual_write=True)
    try:
        for idx, row in df.iterrows():
            try:
                payload = prepare_excel_row_fn(row.to_dict())
                consulta_validada = consulta_create_cls(**payload)
                protocolo_detalles = extraer_protocolo_detalles_fn(payload)
                inconsistencias = detectar_inconsistencias_fn(payload)
                nota_soap = generar_nota_soap_fn(payload)
                note_text = build_embedding_text_fn(payload)
                embedding = None if async_embeddings else compute_embedding_fn(note_text)
                db_consulta = consulta_db_cls(
                    fecha_registro=today_fn(),
                    **consulta_validada.dict(exclude_unset=True),
                    protocolo_detalles=protocolo_detalles,
                    embedding_diagnostico=embedding,
                    nota_soap_auto=json_module.dumps(nota_soap, ensure_ascii=False),
                    inconsistencias="; ".join(inconsistencias) if inconsistencias else None,
                )
                db.add(db_consulta)
                db.commit()
                db.refresh(db_consulta)
                if embedding:
                    sync_consulta_embedding_vector_fn(
                        db,
                        consulta_id=int(db_consulta.id),
                        embedding=embedding,
                        commit=True,
                    )
                status.exitosos += 1
                if async_embeddings:
                    enqueue_embedding_fn(db_consulta.id, note_text)
            except Exception as exc:
                db.rollback()
                status.errores.append(f"Fila {int(idx) + 2}: {exc}")
            status.procesados += 1
            if progress_cb:
                try:
                    progress_cb(status.procesados, status.total)
                except Exception:
                    pass
    finally:
        db.close()
    return status
