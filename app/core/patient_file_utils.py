from __future__ import annotations

import mimetypes
import os
import re
from typing import Any, Dict, Optional, Type


def safe_filename(filename: str) -> str:
    base = os.path.basename(filename or "archivo")
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._")
    return cleaned or "archivo"


def extract_extension(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower().strip()


def detect_mime(upload: Any, ext: str) -> str:
    ctype = getattr(upload, "content_type", None)
    if ctype:
        return str(ctype)
    if ext == ".pgn":
        return "image/png"
    guessed = mimetypes.types_map.get(ext)
    if guessed:
        return guessed
    if ext in {".dcm", ".dicom"}:
        return "application/dicom"
    return "application/octet-stream"


def resolve_consulta_para_archivo(
    *,
    db: Any,
    consulta_id: Optional[int],
    curp: Optional[str],
    consulta_model: Type[Any],
    normalize_curp_fn,
) -> Optional[Any]:
    if consulta_id:
        return db.query(consulta_model).filter(consulta_model.id == int(consulta_id)).first()
    if curp:
        return (
            db.query(consulta_model)
            .filter(consulta_model.curp == normalize_curp_fn(curp))
            .order_by(consulta_model.id.desc())
            .first()
        )
    return None


def serialize_archivo_row(row: Any) -> Dict[str, Any]:
    fecha_subida = getattr(row, "fecha_subida", None)
    return {
        "id": getattr(row, "id", None),
        "consulta_id": getattr(row, "consulta_id", None),
        "nombre_original": getattr(row, "nombre_original", None),
        "extension": getattr(row, "extension", None),
        "mime_type": getattr(row, "mime_type", None),
        "tamano_bytes": getattr(row, "tamano_bytes", None),
        "descripcion": getattr(row, "descripcion", None),
        "subido_por": getattr(row, "subido_por", None),
        "fecha_subida": fecha_subida.isoformat() if fecha_subida else None,
    }
