"""Fachada de servicios para ward round, smart expediente y command center."""

from app.services.command_center_flow import command_center_flow
from app.services.smart_expediente_flow import smart_expediente_flow
from app.services.ward_round_flow import (
    ward_round_autofill_vitals_flow,
    ward_round_dashboard_flow,
    ward_round_save_inline_note_flow,
)

__all__ = [
    "command_center_flow",
    "smart_expediente_flow",
    "ward_round_autofill_vitals_flow",
    "ward_round_dashboard_flow",
    "ward_round_save_inline_note_flow",
]
