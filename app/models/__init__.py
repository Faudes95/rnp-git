"""Modelos SQLAlchemy (extracción aditiva progresiva)."""

from app.models.hospitalization_episode import (  # noqa: F401
    HOSPITALIZATION_EPISODES,
    HOSPITALIZATION_NOTES_METADATA,
    ensure_hospitalization_notes_schema,
)
from app.models.inpatient_daily_note import INPATIENT_DAILY_NOTES  # noqa: F401
from app.models.inpatient_ai_models import (  # noqa: F401
    ALERT_ACTION_METADATA,
    CLINICAL_TAGS,
    CLINICAL_EVENT_LOG,
    IO_BLOCKS,
    INPATIENT_TS_METADATA,
    LAB_RESULTS,
    UROLOGY_DEVICES,
    VITALS_TS,
    ensure_inpatient_time_series_schema,
)
from app.models.ehr_models import (  # noqa: F401
    EHR_METADATA,
    EHR_DOCUMENTS,
    EHR_TIMELINE_EVENTS,
    EHR_TAGS,
    EHR_DOCUMENT_TAGS,
    EHR_FEATURES_DAILY,
    EHR_PROBLEM_LIST,
    EHR_ALERT_LIFECYCLE,
    ensure_ehr_schema,
)
