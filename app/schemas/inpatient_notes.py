from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EpisodeCreate(BaseModel):
    patient_id: str = Field(..., description="NSS canónico de 10 dígitos")
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    service: Optional[str] = None
    location: Optional[str] = None
    shift: Optional[str] = None
    author_user_id: Optional[str] = None
    started_on: Optional[date] = None
    source_route: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)


class EpisodeOut(BaseModel):
    id: int
    patient_id: str
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    service: Optional[str] = None
    location: Optional[str] = None
    shift: Optional[str] = None
    author_user_id: Optional[str] = None
    status: str
    started_on: str
    ended_on: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    source_route: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    active: bool = True


class InpatientDailyNoteCreate(BaseModel):
    note_date: Optional[date] = None
    note_type: str = "EVOLUCION"
    service: Optional[str] = None
    location: Optional[str] = None
    shift: Optional[str] = None
    author_user_id: Optional[str] = None
    cie10_codigo: Optional[str] = None
    diagnostico: Optional[str] = None
    vitals: Dict[str, Any] = Field(default_factory=dict)
    labs: Dict[str, Any] = Field(default_factory=dict)
    devices: Dict[str, Any] = Field(default_factory=dict)
    events: Dict[str, Any] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(default_factory=dict)
    note_text: Optional[str] = None
    status: str = "BORRADOR"
    mirror_legacy: bool = True


class InpatientDailyNoteUpdate(BaseModel):
    service: Optional[str] = None
    location: Optional[str] = None
    shift: Optional[str] = None
    cie10_codigo: Optional[str] = None
    diagnostico: Optional[str] = None
    vitals: Optional[Dict[str, Any]] = None
    labs: Optional[Dict[str, Any]] = None
    devices: Optional[Dict[str, Any]] = None
    events: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    note_text: Optional[str] = None
    status: Optional[str] = None


class InpatientDailyNoteOut(BaseModel):
    id: int
    episode_id: int
    patient_id: str
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    note_date: str
    note_type: str
    service: Optional[str] = None
    location: Optional[str] = None
    shift: Optional[str] = None
    author_user_id: Optional[str] = None
    cie10_codigo: Optional[str] = None
    diagnostico: Optional[str] = None
    vitals: Dict[str, Any] = Field(default_factory=dict)
    labs: Dict[str, Any] = Field(default_factory=dict)
    devices: Dict[str, Any] = Field(default_factory=dict)
    events: Dict[str, Any] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(default_factory=dict)
    note_text: Optional[str] = None
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class EpisodeNotesListOut(BaseModel):
    episode: EpisodeOut
    total_notes: int
    notes: List[InpatientDailyNoteOut] = Field(default_factory=list)

