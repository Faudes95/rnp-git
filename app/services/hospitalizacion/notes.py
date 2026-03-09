"""Fachada de servicios para episodios y notas diarias inpatient."""

from app.services.hospitalization_notes_flow import (
    close_episode,
    create_or_get_active_episode,
    get_active_episode_by_patient,
    get_daily_note,
    get_episode,
    list_daily_notes,
    list_patient_daily_notes,
    list_patient_episodes,
    summarize_patient_episodes,
    upsert_daily_note,
)

__all__ = [
    "close_episode",
    "create_or_get_active_episode",
    "get_active_episode_by_patient",
    "get_daily_note",
    "get_episode",
    "list_daily_notes",
    "list_patient_daily_notes",
    "list_patient_episodes",
    "summarize_patient_episodes",
    "upsert_daily_note",
]
