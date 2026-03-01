from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class VitalsTSCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    recorded_at: Optional[datetime] = None
    heart_rate: Optional[int] = None
    sbp: Optional[int] = None
    dbp: Optional[int] = None
    map: Optional[float] = None
    temperature: Optional[float] = None
    spo2: Optional[float] = None
    resp_rate: Optional[int] = None
    mental_status_avpu: Optional[str] = None
    gcs: Optional[int] = None
    o2_device: Optional[str] = None
    o2_flow_lpm: Optional[float] = None
    pain_score_0_10: Optional[int] = None
    source: Optional[str] = None


class VitalsTSOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    recorded_at: str
    heart_rate: Optional[int] = None
    sbp: Optional[int] = None
    dbp: Optional[int] = None
    map: Optional[float] = None
    temperature: Optional[float] = None
    spo2: Optional[float] = None
    resp_rate: Optional[int] = None
    mental_status_avpu: Optional[str] = None
    gcs: Optional[int] = None
    o2_device: Optional[str] = None
    o2_flow_lpm: Optional[float] = None
    pain_score_0_10: Optional[int] = None
    source: Optional[str] = None
    created_at: Optional[str] = None


class IOBlockCreate(BaseModel):
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    interval_start: datetime
    interval_end: datetime
    urine_output_ml: Optional[float] = None
    intake_ml: Optional[float] = None
    net_balance_ml: Optional[float] = None
    weight_kg: Optional[float] = None
    height_cm: Optional[float] = None


class IOBlockOut(BaseModel):
    id: int
    consulta_id: Optional[int] = None
    hospitalizacion_id: Optional[int] = None
    interval_start: str
    interval_end: str
    urine_output_ml: Optional[float] = None
    intake_ml: Optional[float] = None
    net_balance_ml: Optional[float] = None
    weight_kg: Optional[float] = None
    height_cm: Optional[float] = None
    created_at: Optional[str] = None

