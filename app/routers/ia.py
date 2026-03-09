from __future__ import annotations

from app.api.fau_bot import router as fau_bot_router
from app.api.fau_bot_core import router as fau_bot_core_router

__all__ = ["fau_bot_core_router", "fau_bot_router"]
