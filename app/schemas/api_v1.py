from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ApiV1Health(BaseModel):
    status: str = "ok"
    api_version: str = "v1"
    timestamp: str


class ApiV1EventItem(BaseModel):
    id: int
    correlation_id: Optional[str] = None
    actor: Optional[str] = None
    module: str
    event_type: str
    entity: Optional[str] = None
    entity_id: Optional[str] = None
    consulta_id: Optional[int] = None
    source_route: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None


class ApiV1EventSummary(BaseModel):
    total: int
    por_modulo: List[Dict[str, Any]] = Field(default_factory=list)
    por_event_type: List[Dict[str, Any]] = Field(default_factory=list)
    latest: List[ApiV1EventItem] = Field(default_factory=list)


class ApiV1JobItem(BaseModel):
    id: int
    job_name: str
    source: str
    task_id: Optional[str] = None
    triggered_by: Optional[str] = None
    status: str
    correlation_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    result: Dict[str, Any] = Field(default_factory=dict)
    error_text: Optional[str] = None
    duration_ms: Optional[int] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None


class ApiV1JobSummary(BaseModel):
    total: int
    por_estado: List[Dict[str, Any]] = Field(default_factory=list)
    por_job: List[Dict[str, Any]] = Field(default_factory=list)
    latencia_ms: Dict[str, Any] = Field(default_factory=dict)
    latest: List[ApiV1JobItem] = Field(default_factory=list)


class ApiV1FormsList(BaseModel):
    total: int
    forms: List[Dict[str, Any]] = Field(default_factory=list)


class ApiV1ValidationResult(BaseModel):
    ok: bool
    form_code: str
    validation: Dict[str, Any] = Field(default_factory=dict)
