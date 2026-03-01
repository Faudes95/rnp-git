"""Servicios de archivos clínicos extraídos progresivamente."""

from typing import Optional


def format_size(size_bytes: Optional[int]) -> str:
    if not size_bytes:
        return "0 B"
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size_bytes} B"

