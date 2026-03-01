from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class LabResultCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    collected_at: Optional[datetime] = None
    test_name: str
    value_num: Optional[float] = None
    value_text: Optional[str] = None
    unit: Optional[str] = None
    source: Optional[str] = None


class LabResultOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    collected_at: str
    test_name: str
    value_num: Optional[float] = None
    value_text: Optional[str] = None
    unit: Optional[str] = None
    source: Optional[str] = None
    created_at: Optional[str] = None


class InpatientDailyNoteCreate(BaseModel):
    hospitalizacion_id: Optional[int] = None
    consulta_id: Optional[int] = None
    note_date: date
    author_user_id: Optional[str] = None
    problem_list_json: Dict[str, Any] = Field(default_factory=dict)
    plan_by_problem_json: Dict[str, Any] = Field(default_factory=dict)
    devices_snapshot_json: Dict[str, Any] = Field(default_factory=dict)
    io_summary_json: Dict[str, Any] = Field(default_factory=dict)
    symptoms_json: Dict[str, Any] = Field(default_factory=dict)
    events_pending_json: Dict[str, Any] = Field(default_factory=dict)
    free_text: Optional[str] = None
    is_final: bool = False
    consulta_patient_id: Optional[str] = None
    note_type: Optional[str] = "EVOLUCION"


class InpatientDailyNoteOut(BaseModel):
    id: int
    hospitalizacion_id: Optional[int] = None
    consulta_id: Optional[int] = None
    note_date: str
    author_user_id: Optional[str] = None
    problem_list_json: Dict[str, Any] = Field(default_factory=dict)
    plan_by_problem_json: Dict[str, Any] = Field(default_factory=dict)
    devices_snapshot_json: Dict[str, Any] = Field(default_factory=dict)
    io_summary_json: Dict[str, Any] = Field(default_factory=dict)
    symptoms_json: Dict[str, Any] = Field(default_factory=dict)
    events_pending_json: Dict[str, Any] = Field(default_factory=dict)
    free_text: Optional[str] = None
    is_final: bool = False
    version: int = 1
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class AlertAckPayload(BaseModel):
    ack_by: Optional[str] = None


class AlertResolvePayload(BaseModel):
    resolved_by: Optional[str] = None
    resolution_reason: Optional[str] = None
    action_taken_json: Dict[str, Any] = Field(default_factory=dict)


class ClinicalTagCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    tag_type: str
    tag_value: str
    laterality: Optional[str] = None
    severity: Optional[str] = None


class ClinicalTagOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    tag_type: str
    tag_value: str
    laterality: Optional[str] = None
    severity: Optional[str] = None
    created_at: Optional[str] = None

