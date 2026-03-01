from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class UrologyDeviceCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    device_type: str
    present: bool = True
    inserted_at: Optional[datetime] = None
    removed_at: Optional[datetime] = None
    side: Optional[str] = None
    location: Optional[str] = None
    size_fr: Optional[str] = None
    difficulty: Optional[str] = None
    irrigation: Optional[bool] = None
    planned_removal_at: Optional[datetime] = None
    planned_change_at: Optional[datetime] = None
    notes: Optional[str] = None


class UrologyDevicePatch(BaseModel):
    present: Optional[bool] = None
    removed_at: Optional[datetime] = None
    planned_removal_at: Optional[datetime] = None
    planned_change_at: Optional[datetime] = None
    notes: Optional[str] = None
    irrigation: Optional[bool] = None
    side: Optional[str] = None
    location: Optional[str] = None
    size_fr: Optional[str] = None
    difficulty: Optional[str] = None


class UrologyDeviceOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    device_type: str
    present: bool = True
    inserted_at: Optional[str] = None
    removed_at: Optional[str] = None
    side: Optional[str] = None
    location: Optional[str] = None
    size_fr: Optional[str] = None
    difficulty: Optional[str] = None
    irrigation: Optional[bool] = None
    planned_removal_at: Optional[str] = None
    planned_change_at: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None


class ClinicalEventCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    event_time: Optional[datetime] = None
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class ClinicalEventOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    event_time: str
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None

