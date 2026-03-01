from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """UTC naive para mantener compatibilidad con almacenamiento legacy."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

