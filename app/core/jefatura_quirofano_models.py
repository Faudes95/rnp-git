from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship


def define_jefatura_quirofano_models(
    *,
    base: Any,
    json_type: Any,
    utcnow_fn: Callable[[], datetime],
) -> SimpleNamespace:
    class JefaturaQuirofanoServiceLineDB(base):
        __tablename__ = "jefatura_quirofano_service_lines"

        id = Column(Integer, primary_key=True, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        code = Column(String(60), nullable=False, unique=True, index=True)
        nombre = Column(String(180), nullable=False, index=True)
        line_type = Column(String(40), nullable=False, default="CLINICO", index=True)
        activo = Column(Boolean, default=True, nullable=False, index=True)
        display_order = Column(Integer, default=0, nullable=False, index=True)
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        updated_at = Column(DateTime, default=utcnow_fn, onupdate=datetime.utcnow, nullable=False, index=True)

    class JefaturaQuirofanoTemplateVersionDB(base):
        __tablename__ = "jefatura_quirofano_template_versions"

        id = Column(Integer, primary_key=True, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        nombre = Column(String(180), nullable=False, index=True)
        version_label = Column(String(120), nullable=False, index=True)
        is_active = Column(Boolean, default=True, nullable=False, index=True)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        updated_at = Column(DateTime, default=utcnow_fn, onupdate=datetime.utcnow, nullable=False, index=True)
        slots = relationship("JefaturaQuirofanoTemplateSlotDB", back_populates="template_version", cascade="all, delete-orphan")

    class JefaturaQuirofanoTemplateSlotDB(base):
        __tablename__ = "jefatura_quirofano_template_slots"

        id = Column(Integer, primary_key=True, index=True)
        template_version_id = Column(Integer, ForeignKey("jefatura_quirofano_template_versions.id", ondelete="CASCADE"), nullable=False, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        day_of_week = Column(Integer, nullable=False, index=True)
        turno = Column(String(20), nullable=False, index=True)
        room_number = Column(Integer, nullable=False, index=True)
        room_code = Column(String(20), nullable=False, index=True)
        service_line_code = Column(String(60), nullable=False, index=True)
        notes = Column(Text)
        activo = Column(Boolean, default=True, nullable=False, index=True)
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        template_version = relationship("JefaturaQuirofanoTemplateVersionDB", back_populates="slots")

    class JefaturaQuirofanoImportBatchDB(base):
        __tablename__ = "jefatura_quirofano_import_batches"

        id = Column(Integer, primary_key=True, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        file_date = Column(Date, index=True)
        original_filename = Column(String(255), nullable=False)
        storage_path = Column(Text, nullable=False)
        parser_name = Column(String(80), default="pymupdf_find_tables", nullable=False, index=True)
        parser_version = Column(String(40), default="1", nullable=False)
        page_count = Column(Integer, default=0, nullable=False)
        extracted_rows_count = Column(Integer, default=0, nullable=False)
        status = Column(String(40), default="REVIEW", nullable=False, index=True)
        warnings_json = Column(json_type)
        errors_json = Column(json_type)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        reviewed_at = Column(DateTime, index=True)
        confirmed_at = Column(DateTime, index=True)
        rows = relationship("JefaturaQuirofanoImportRowDB", back_populates="batch", cascade="all, delete-orphan")

    class JefaturaQuirofanoDailyBlockDB(base):
        __tablename__ = "jefatura_quirofano_daily_blocks"

        id = Column(Integer, primary_key=True, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        fecha = Column(Date, nullable=False, index=True)
        turno = Column(String(20), nullable=False, index=True)
        room_number = Column(Integer, nullable=False, index=True)
        room_code = Column(String(20), nullable=False, index=True)
        service_line_code = Column(String(60), nullable=False, index=True)
        template_version_id = Column(Integer, ForeignKey("jefatura_quirofano_template_versions.id", ondelete="SET NULL"), index=True)
        import_batch_id = Column(Integer, ForeignKey("jefatura_quirofano_import_batches.id", ondelete="SET NULL"), index=True)
        block_status = Column(String(40), nullable=False, default="ACTIVO", index=True)
        notes = Column(Text)
        confirmed_by = Column(String(120))
        confirmed_at = Column(DateTime, index=True)
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        updated_at = Column(DateTime, default=utcnow_fn, onupdate=datetime.utcnow, nullable=False, index=True)
        cases = relationship("JefaturaQuirofanoCaseProgramacionDB", back_populates="daily_block", cascade="all, delete-orphan")

    class JefaturaQuirofanoImportRowDB(base):
        __tablename__ = "jefatura_quirofano_import_rows"

        id = Column(Integer, primary_key=True, index=True)
        batch_id = Column(Integer, ForeignKey("jefatura_quirofano_import_batches.id", ondelete="CASCADE"), nullable=False, index=True)
        page_number = Column(Integer, nullable=False, default=1, index=True)
        row_number = Column(Integer, nullable=False, default=1, index=True)
        review_status = Column(String(40), nullable=False, default="EXTRACTED", index=True)
        room_code = Column(String(20), index=True)
        turno = Column(String(20), index=True)
        hora_programada = Column(String(10), index=True)
        cama = Column(String(20), index=True)
        paciente_nombre = Column(String(220), index=True)
        nss = Column(String(20), index=True)
        agregado_medico = Column(String(80), index=True)
        edad = Column(Integer, index=True)
        diagnostico_preoperatorio = Column(String(240), index=True)
        operacion_proyectada = Column(String(240), index=True)
        cirujano = Column(String(180), index=True)
        anestesiologo = Column(String(180), index=True)
        tipo_anestesia = Column(String(120), index=True)
        enfermera_especialista = Column(String(180), index=True)
        specialty_guess = Column(String(60), index=True)
        discrepancy_flag = Column(Boolean, default=False, nullable=False, index=True)
        discrepancy_json = Column(json_type)
        raw_json = Column(json_type)
        normalized_json = Column(json_type)
        edited_json = Column(json_type)
        confirmed_case_id = Column(Integer, index=True)
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        updated_at = Column(DateTime, default=utcnow_fn, onupdate=datetime.utcnow, nullable=False, index=True)
        batch = relationship("JefaturaQuirofanoImportBatchDB", back_populates="rows")

    class JefaturaQuirofanoCaseProgramacionDB(base):
        __tablename__ = "jefatura_quirofano_case_programaciones"

        id = Column(Integer, primary_key=True, index=True)
        daily_block_id = Column(Integer, ForeignKey("jefatura_quirofano_daily_blocks.id", ondelete="CASCADE"), nullable=False, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        source_type = Column(String(40), nullable=False, default="MANUAL", index=True)
        import_batch_id = Column(Integer, ForeignKey("jefatura_quirofano_import_batches.id", ondelete="SET NULL"), index=True)
        import_row_id = Column(Integer, ForeignKey("jefatura_quirofano_import_rows.id", ondelete="SET NULL"), index=True)
        status = Column(String(40), nullable=False, default="PROGRAMADA", index=True)
        scheduled_time = Column(String(10), index=True)
        duracion_estimada_min = Column(Integer, default=60, index=True)
        cama = Column(String(20), index=True)
        patient_name = Column(String(220), index=True)
        nss = Column(String(20), index=True)
        agregado_medico = Column(String(80), index=True)
        edad = Column(Integer, index=True)
        diagnostico_preoperatorio = Column(String(240), index=True)
        operacion_proyectada = Column(String(240), index=True)
        cirujano = Column(String(180), index=True)
        anestesiologo = Column(String(180), index=True)
        enfermera_especialista = Column(String(180), index=True)
        tipo_anestesia = Column(String(120), index=True)
        notes = Column(Text)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        updated_at = Column(DateTime, default=utcnow_fn, onupdate=datetime.utcnow, nullable=False, index=True)
        daily_block = relationship("JefaturaQuirofanoDailyBlockDB", back_populates="cases")
        staff_assignments = relationship("JefaturaQuirofanoCaseStaffDB", back_populates="case", cascade="all, delete-orphan")
        events = relationship("JefaturaQuirofanoCaseEventDB", back_populates="case", cascade="all, delete-orphan")
        incidencias = relationship("JefaturaQuirofanoCaseIncidenciaDB", back_populates="case", cascade="all, delete-orphan")

    class JefaturaQuirofanoCaseStaffDB(base):
        __tablename__ = "jefatura_quirofano_case_staff"

        id = Column(Integer, primary_key=True, index=True)
        case_id = Column(Integer, ForeignKey("jefatura_quirofano_case_programaciones.id", ondelete="CASCADE"), nullable=False, index=True)
        staff_name = Column(String(180), nullable=False, index=True)
        staff_role = Column(String(60), nullable=False, index=True)
        notes = Column(Text)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        case = relationship("JefaturaQuirofanoCaseProgramacionDB", back_populates="staff_assignments")

    class JefaturaQuirofanoCaseEventDB(base):
        __tablename__ = "jefatura_quirofano_case_events"

        id = Column(Integer, primary_key=True, index=True)
        case_id = Column(Integer, ForeignKey("jefatura_quirofano_case_programaciones.id", ondelete="CASCADE"), nullable=False, index=True)
        event_type = Column(String(60), nullable=False, index=True)
        event_at = Column(DateTime, nullable=False, index=True)
        notes = Column(Text)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        case = relationship("JefaturaQuirofanoCaseProgramacionDB", back_populates="events")

    class JefaturaQuirofanoCaseIncidenciaDB(base):
        __tablename__ = "jefatura_quirofano_case_incidencias"

        id = Column(Integer, primary_key=True, index=True)
        case_id = Column(Integer, ForeignKey("jefatura_quirofano_case_programaciones.id", ondelete="CASCADE"), nullable=False, index=True)
        incidence_type = Column(String(80), nullable=False, index=True)
        status = Column(String(40), nullable=False, default="ABIERTA", index=True)
        description = Column(Text, nullable=False)
        event_at = Column(DateTime, nullable=False, index=True)
        created_by = Column(String(120))
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)
        case = relationship("JefaturaQuirofanoCaseProgramacionDB", back_populates="incidencias")

    class JefaturaQuirofanoAuditLogDB(base):
        __tablename__ = "jefatura_quirofano_audit_log"

        id = Column(Integer, primary_key=True, index=True)
        unidad_code = Column(String(80), nullable=False, default="HES_CMN_LA_RAZA", index=True)
        actor = Column(String(120), nullable=False, index=True)
        action = Column(String(80), nullable=False, index=True)
        entity_type = Column(String(80), nullable=False, index=True)
        entity_id = Column(Integer, index=True)
        payload = Column(json_type)
        created_at = Column(DateTime, default=utcnow_fn, nullable=False, index=True)

    return SimpleNamespace(
        JefaturaQuirofanoServiceLineDB=JefaturaQuirofanoServiceLineDB,
        JefaturaQuirofanoTemplateVersionDB=JefaturaQuirofanoTemplateVersionDB,
        JefaturaQuirofanoTemplateSlotDB=JefaturaQuirofanoTemplateSlotDB,
        JefaturaQuirofanoImportBatchDB=JefaturaQuirofanoImportBatchDB,
        JefaturaQuirofanoDailyBlockDB=JefaturaQuirofanoDailyBlockDB,
        JefaturaQuirofanoImportRowDB=JefaturaQuirofanoImportRowDB,
        JefaturaQuirofanoCaseProgramacionDB=JefaturaQuirofanoCaseProgramacionDB,
        JefaturaQuirofanoCaseStaffDB=JefaturaQuirofanoCaseStaffDB,
        JefaturaQuirofanoCaseEventDB=JefaturaQuirofanoCaseEventDB,
        JefaturaQuirofanoCaseIncidenciaDB=JefaturaQuirofanoCaseIncidenciaDB,
        JefaturaQuirofanoAuditLogDB=JefaturaQuirofanoAuditLogDB,
    )
