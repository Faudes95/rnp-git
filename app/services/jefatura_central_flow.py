from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.core.app_context import main_proxy as m
from app.services.jefatura_central_records_flow import serialize_case_row, serialize_incidence_row
from app.services.jefatura_central_shared import (
    CENTRAL_MODULE,
    CENTRAL_SUBMODULES,
    effective_exam_status,
    normalize_text,
    request_actor,
    resident_display,
    resident_selection_groups,
    resolve_assignment_targets,
    safe_date,
    safe_int,
    status_badge,
)


def build_central_overview(db: Session) -> Dict[str, Any]:
    active_templates = db.query(func.count(m.ResidentExamTemplateDB.id)).filter(m.ResidentExamTemplateDB.is_active.is_(True)).scalar() or 0
    pending_assignments = (
        db.query(func.count(m.ResidentExamAssignmentDB.id))
        .outerjoin(m.ResidentExamAttemptDB, m.ResidentExamAttemptDB.assignment_id == m.ResidentExamAssignmentDB.id)
        .filter(m.ResidentExamAttemptDB.id.is_(None))
        .scalar()
        or 0
    )
    active_cases = (
        db.query(func.count(m.ResidentCaseAssignmentDB.id))
        .filter(func.upper(m.ResidentCaseAssignmentDB.estado).notin_(["RESUELTO", "CERRADO"]))
        .scalar()
        or 0
    )
    active_incidences = (
        db.query(func.count(m.ResidentIncidenceDB.id))
        .filter(func.upper(m.ResidentIncidenceDB.estado).notin_(["RESUELTA", "CERRADA"]))
        .scalar()
        or 0
    )
    latest_assignments = (
        db.query(m.ResidentExamAssignmentDB)
        .options(joinedload(m.ResidentExamAssignmentDB.exam), joinedload(m.ResidentExamAssignmentDB.attempts))
        .order_by(m.ResidentExamAssignmentDB.assigned_at.desc(), m.ResidentExamAssignmentDB.id.desc())
        .limit(6)
        .all()
    )
    latest_cases = (
        db.query(m.ResidentCaseAssignmentDB)
        .order_by(m.ResidentCaseAssignmentDB.created_at.desc(), m.ResidentCaseAssignmentDB.id.desc())
        .limit(5)
        .all()
    )
    latest_incidences = (
        db.query(m.ResidentIncidenceDB)
        .order_by(m.ResidentIncidenceDB.fecha_evento.desc(), m.ResidentIncidenceDB.id.desc())
        .limit(5)
        .all()
    )
    return {
        "kpis": {
            "active_templates": int(active_templates),
            "pending_assignments": int(pending_assignments),
            "active_cases": int(active_cases),
            "active_incidences": int(active_incidences),
        },
        "latest_assignments": [_serialize_assignment_card(row) for row in latest_assignments],
        "latest_cases": [serialize_case_row(row) for row in latest_cases],
        "latest_incidences": [serialize_incidence_row(row) for row in latest_incidences],
    }


def _serialize_assignment_card(row: Any) -> Dict[str, Any]:
    resident = resident_display(getattr(row, "resident_code", ""))
    attempt = next((item for item in sorted(getattr(row, "attempts", []) or [], key=lambda item: item.id or 0, reverse=True) if item.submitted_at), None)
    status = effective_exam_status(row, getattr(attempt, "submitted_at", None))
    badge = status_badge(status)
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "resident_code": resident["code"],
        "resident_name": resident["name"],
        "resident_grade": resident["grade"],
        "exam_title": str(getattr(getattr(row, "exam", None), "title", None) or "Examen sin título"),
        "periodo_label": str(getattr(row, "periodo_label", "") or getattr(getattr(row, "exam", None), "period_label", "") or "Sin periodo"),
        "status": status,
        "status_label": badge["label"],
        "status_tone": badge["tone"],
        "score_pct": round(float(getattr(attempt, "score_pct", 0) or 0), 1) if attempt and getattr(attempt, "score_pct", None) is not None else None,
        "take_href": f"/jefatura-urologia/programa-academico/residentes/{resident['code']}/examenes/{int(getattr(row, 'id', 0) or 0)}",
        "assigned_at": getattr(row, "assigned_at", None),
    }


def _question_counts(db: Session) -> Dict[int, int]:
    rows = (
        db.query(
            m.ResidentExamQuestionDB.exam_template_id.label("exam_template_id"),
            func.count(m.ResidentExamQuestionDB.id).label("question_count"),
        )
        .group_by(m.ResidentExamQuestionDB.exam_template_id)
        .all()
    )
    return {int(row.exam_template_id): int(row.question_count or 0) for row in rows}


def _assignment_counts(db: Session) -> Tuple[Dict[int, int], Dict[int, int]]:
    assignment_rows = (
        db.query(
            m.ResidentExamAssignmentDB.exam_template_id.label("exam_template_id"),
            func.count(m.ResidentExamAssignmentDB.id).label("assignment_count"),
        )
        .group_by(m.ResidentExamAssignmentDB.exam_template_id)
        .all()
    )
    completion_rows = (
        db.query(
            m.ResidentExamAssignmentDB.exam_template_id.label("exam_template_id"),
            func.count(m.ResidentExamAttemptDB.id).label("completed_count"),
        )
        .join(m.ResidentExamAttemptDB, m.ResidentExamAttemptDB.assignment_id == m.ResidentExamAssignmentDB.id)
        .filter(m.ResidentExamAttemptDB.submitted_at.isnot(None))
        .group_by(m.ResidentExamAssignmentDB.exam_template_id)
        .all()
    )
    return (
        {int(row.exam_template_id): int(row.assignment_count or 0) for row in assignment_rows},
        {int(row.exam_template_id): int(row.completed_count or 0) for row in completion_rows},
    )


def _serialize_exam_template(row: Any, *, question_counts: Dict[int, int], assignment_counts: Dict[int, int], completion_counts: Dict[int, int]) -> Dict[str, Any]:
    exam_id = int(getattr(row, "id", 0) or 0)
    return {
        "id": exam_id,
        "title": str(getattr(row, "title", "") or "Examen sin título"),
        "description": str(getattr(row, "description", "") or ""),
        "period_label": str(getattr(row, "period_label", "") or "Sin periodo"),
        "question_count": int(question_counts.get(exam_id, 0)),
        "assignment_count": int(assignment_counts.get(exam_id, 0)),
        "completed_count": int(completion_counts.get(exam_id, 0)),
        "is_active": bool(getattr(row, "is_active", True)),
        "assign_href": f"/jefatura-urologia/central/examenes/{exam_id}/asignar",
    }


async def render_jefatura_central_home_flow(request: Any, db: Session, *, flash: Optional[Dict[str, str]] = None):
    overview = build_central_overview(db)
    resolved_flash = flash
    if resolved_flash is None and str(request.query_params.get("saved") or "") == "1":
        resolved_flash = {"kind": "success", "message": "Central actualizada correctamente."}
    return m.render_template(
        "jefatura_central_home.html",
        request=request,
        module=CENTRAL_MODULE,
        sections=CENTRAL_SUBMODULES,
        overview=overview,
        flash=resolved_flash,
    )


async def render_jefatura_central_exams_flow(request: Any, db: Session, *, flash: Optional[Dict[str, str]] = None, create_open: bool = False):
    question_counts = _question_counts(db)
    assignment_counts, completion_counts = _assignment_counts(db)
    exams = (
        db.query(m.ResidentExamTemplateDB)
        .order_by(m.ResidentExamTemplateDB.created_at.desc(), m.ResidentExamTemplateDB.id.desc())
        .all()
    )
    exam_rows = [
        _serialize_exam_template(row, question_counts=question_counts, assignment_counts=assignment_counts, completion_counts=completion_counts)
        for row in exams
    ]
    return m.render_template(
        "jefatura_central_examenes.html",
        request=request,
        module=CENTRAL_MODULE,
        exams=exam_rows,
        flash=flash,
        create_open=create_open,
        submodules=CENTRAL_SUBMODULES,
    )


def _parse_exam_questions(form: Any) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    prompts = form.getlist("question_prompt[]")
    explanations = form.getlist("question_explanation[]")
    option_a = form.getlist("question_option_a[]")
    option_b = form.getlist("question_option_b[]")
    option_c = form.getlist("question_option_c[]")
    option_d = form.getlist("question_option_d[]")
    correct_options = form.getlist("question_correct[]")
    questions: List[Dict[str, Any]] = []
    max_len = max(len(prompts), len(option_a), len(option_b), len(option_c), len(option_d), len(correct_options))
    for index in range(max_len):
        prompt = normalize_text(prompts[index] if index < len(prompts) else "", max_len=2000)
        if not prompt:
            continue
        options = [
            normalize_text(option_a[index] if index < len(option_a) else "", max_len=500),
            normalize_text(option_b[index] if index < len(option_b) else "", max_len=500),
            normalize_text(option_c[index] if index < len(option_c) else "", max_len=500),
            normalize_text(option_d[index] if index < len(option_d) else "", max_len=500),
        ]
        if any(item is None for item in options):
            return [], f"La pregunta {index + 1} debe incluir las cuatro opciones."
        correct = str(correct_options[index] if index < len(correct_options) else "0").strip()
        correct_idx = safe_int(correct)
        if correct_idx not in {0, 1, 2, 3}:
            return [], f"La pregunta {index + 1} debe indicar una respuesta correcta."
        questions.append(
            {
                "prompt": prompt,
                "explanation": normalize_text(explanations[index] if index < len(explanations) else "", max_len=2000),
                "options": options,
                "correct_index": correct_idx,
            }
        )
    if not questions:
        return [], "Debes capturar al menos una pregunta para crear el examen."
    return questions, None


async def create_central_exam_template_from_request(request: Any, db: Session) -> Dict[str, Any]:
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)

    title = normalize_text(form.get("title"), max_len=220)
    description = normalize_text(form.get("description"), max_len=4000)
    period_label = normalize_text(form.get("period_label"), max_len=120)
    is_active = str(form.get("is_active") or "0").strip() in {"1", "true", "on", "yes"}
    questions, error = _parse_exam_questions(form)
    if not title:
        return {"ok": False, "error": "El examen requiere un título."}
    if error:
        return {"ok": False, "error": error}

    try:
        exam = m.ResidentExamTemplateDB(
            title=title,
            description=description,
            period_label=period_label,
            created_by=request_actor(request),
            is_active=is_active,
        )
        db.add(exam)
        db.flush()
        for question_index, question in enumerate(questions, start=1):
            question_row = m.ResidentExamQuestionDB(
                exam_template_id=exam.id,
                prompt=question["prompt"],
                explanation=question["explanation"],
                sort_order=question_index,
            )
            db.add(question_row)
            db.flush()
            for option_index, option_text in enumerate(question["options"], start=1):
                db.add(
                    m.ResidentExamOptionDB(
                        question_id=question_row.id,
                        option_text=option_text,
                        sort_order=option_index,
                        is_correct=(option_index - 1) == int(question["correct_index"]),
                    )
                )
        db.commit()
        return {"ok": True, "exam_id": int(exam.id)}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible crear el examen semestral."}


def _load_exam_for_assignment(db: Session, exam_id: int) -> Optional[Any]:
    return (
        db.query(m.ResidentExamTemplateDB)
        .options(
            joinedload(m.ResidentExamTemplateDB.questions).joinedload(m.ResidentExamQuestionDB.options),
        )
        .filter(m.ResidentExamTemplateDB.id == int(exam_id))
        .first()
    )


async def render_jefatura_central_exam_assignment_flow(request: Any, db: Session, exam_id: int, *, flash: Optional[Dict[str, str]] = None):
    exam = _load_exam_for_assignment(db, exam_id)
    if exam is None:
        flash = {"kind": "error", "message": "El examen solicitado no existe."}
    assignments = (
        db.query(m.ResidentExamAssignmentDB)
        .options(joinedload(m.ResidentExamAssignmentDB.attempts))
        .filter(m.ResidentExamAssignmentDB.exam_template_id == int(exam_id))
        .order_by(m.ResidentExamAssignmentDB.assigned_at.desc(), m.ResidentExamAssignmentDB.id.desc())
        .all()
        if exam is not None
        else []
    )
    assignment_rows = [_serialize_assignment_card(row) for row in assignments]
    return m.render_template(
        "jefatura_central_asignar_examen.html",
        request=request,
        module=CENTRAL_MODULE,
        exam=exam,
        assignments=assignment_rows,
        flash=flash,
        resident_groups=resident_selection_groups(),
        grades=[group["grade"] for group in resident_selection_groups()],
    )


async def assign_central_exam_from_request(request: Any, db: Session, exam_id: int) -> Dict[str, Any]:
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    exam = _load_exam_for_assignment(db, exam_id)
    if exam is None:
        return {"ok": False, "error": "El examen solicitado no existe."}

    mode = str(form.get("assignment_mode") or "resident").strip().lower()
    resident_code = normalize_text(form.get("resident_code"), max_len=120)
    resident_grade = normalize_text(form.get("resident_grade"), max_len=10)
    periodo_label = normalize_text(form.get("periodo_label"), max_len=120) or normalize_text(getattr(exam, "period_label", None), max_len=120) or "Sin periodo"
    disponible_desde = safe_date(form.get("disponible_desde"))
    cierra_en = safe_date(form.get("cierra_en"))
    targets = resolve_assignment_targets(mode, resident_code, resident_grade)
    if not targets:
        return {"ok": False, "error": "Selecciona al menos un residente o un grado para asignar."}

    created_count = 0
    updated_count = 0
    skipped_count = 0
    actor = request_actor(request)
    try:
        for target in targets:
            resident_target_code = str(target["code"]).upper()
            existing = (
                db.query(m.ResidentExamAssignmentDB)
                .options(joinedload(m.ResidentExamAssignmentDB.attempts))
                .filter(
                    func.upper(m.ResidentExamAssignmentDB.resident_code) == resident_target_code,
                    m.ResidentExamAssignmentDB.exam_template_id == int(exam.id),
                    func.upper(m.ResidentExamAssignmentDB.periodo_label) == str(periodo_label or "").upper(),
                )
                .order_by(m.ResidentExamAssignmentDB.id.desc())
                .first()
            )
            submitted_attempt = None
            if existing is not None:
                submitted_attempt = next((item for item in existing.attempts if item.submitted_at), None)
            if submitted_attempt is not None:
                skipped_count += 1
                continue
            if existing is None:
                db.add(
                    m.ResidentExamAssignmentDB(
                        resident_code=resident_target_code,
                        exam_template_id=int(exam.id),
                        periodo_label=periodo_label,
                        disponible_desde=disponible_desde,
                        cierra_en=cierra_en,
                        estado="PENDIENTE",
                        assigned_by=actor,
                    )
                )
                created_count += 1
            else:
                existing.disponible_desde = disponible_desde
                existing.cierra_en = cierra_en
                existing.estado = "PENDIENTE"
                existing.assigned_by = actor
                updated_count += 1
        db.commit()
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible asignar el examen a los residentes seleccionados."}

    return {
        "ok": True,
        "message": f"Asignación completada: {created_count} nuevas, {updated_count} actualizadas, {skipped_count} omitidas por intento ya contestado.",
    }


def _load_assignment_for_resident(db: Session, resident_code: str, assignment_id: int) -> Optional[Any]:
    return (
        db.query(m.ResidentExamAssignmentDB)
        .options(
            joinedload(m.ResidentExamAssignmentDB.exam)
            .joinedload(m.ResidentExamTemplateDB.questions)
            .joinedload(m.ResidentExamQuestionDB.options),
            joinedload(m.ResidentExamAssignmentDB.attempts).joinedload(m.ResidentExamAttemptDB.answers),
        )
        .filter(
            m.ResidentExamAssignmentDB.id == int(assignment_id),
            func.upper(m.ResidentExamAssignmentDB.resident_code) == str(resident_code or "").strip().upper(),
        )
        .first()
    )


def _serialize_resident_exam(assignment: Any, resident_code: str) -> Dict[str, Any]:
    attempt = next((item for item in sorted(getattr(assignment, "attempts", []) or [], key=lambda row: row.id or 0, reverse=True) if item.submitted_at), None)
    status = effective_exam_status(assignment, getattr(attempt, "submitted_at", None))
    badge = status_badge(status)
    answer_map = {}
    if attempt is not None:
        for answer in getattr(attempt, "answers", []) or []:
            answer_map[int(answer.question_id)] = int(answer.selected_option_id)
    questions: List[Dict[str, Any]] = []
    exam = getattr(assignment, "exam", None)
    ordered_questions = sorted(getattr(exam, "questions", []) or [], key=lambda row: (row.sort_order or 0, row.id or 0))
    for question in ordered_questions:
        questions.append(
            {
                "id": int(question.id),
                "prompt": str(question.prompt or ""),
                "explanation": str(question.explanation or ""),
                "selected_option_id": answer_map.get(int(question.id)),
                "options": [
                    {
                        "id": int(option.id),
                        "text": str(option.option_text or ""),
                        "is_correct": bool(option.is_correct),
                    }
                    for option in sorted(getattr(question, "options", []) or [], key=lambda row: (row.sort_order or 0, row.id or 0))
                ],
            }
        )
    return {
        "assignment_id": int(getattr(assignment, "id", 0) or 0),
        "resident": resident_display(resident_code),
        "title": str(getattr(exam, "title", None) or "Examen semestral"),
        "description": str(getattr(exam, "description", None) or ""),
        "periodo_label": str(getattr(assignment, "periodo_label", None) or getattr(exam, "period_label", None) or "Sin periodo"),
        "available_from": getattr(assignment, "disponible_desde", None),
        "due_on": getattr(assignment, "cierra_en", None),
        "status": status,
        "status_label": badge["label"],
        "status_tone": badge["tone"],
        "has_attempt": attempt is not None,
        "attempt": {
            "submitted_at": getattr(attempt, "submitted_at", None),
            "score_pct": round(float(getattr(attempt, "score_pct", 0) or 0), 1) if attempt and getattr(attempt, "score_pct", None) is not None else None,
            "correct_answers": int(getattr(attempt, "correct_answers", 0) or 0) if attempt else None,
            "total_questions": int(getattr(attempt, "total_questions", 0) or 0) if attempt else None,
        },
        "questions": questions,
    }


async def render_resident_exam_flow(request: Any, db: Session, resident_code: str, assignment_id: int, *, flash: Optional[Dict[str, str]] = None):
    assignment = _load_assignment_for_resident(db, resident_code, assignment_id)
    if assignment is None:
        return m.render_template(
            "jefatura_urologia_residente_examen.html",
            request=request,
            flash={"kind": "error", "message": "La evaluación solicitada no existe para este residente."},
            exam_view=None,
        )
    exam_view = _serialize_resident_exam(assignment, resident_code)
    resolved_flash = flash
    if resolved_flash is None and str(request.query_params.get("saved") or "") == "1":
        resolved_flash = {"kind": "success", "message": "Examen contestado y calificado correctamente."}
    return m.render_template(
        "jefatura_urologia_residente_examen.html",
        request=request,
        exam_view=exam_view,
        flash=resolved_flash,
    )


async def submit_resident_exam_response(request: Any, db: Session, resident_code: str, assignment_id: int) -> Dict[str, Any]:
    assignment = _load_assignment_for_resident(db, resident_code, assignment_id)
    if assignment is None:
        return {"ok": False, "error": "La evaluación solicitada no existe para este residente."}
    form = await request.form()
    form_dict = dict(form)
    m.validate_csrf(form_dict, request)
    exam_view = _serialize_resident_exam(assignment, resident_code)
    if exam_view["has_attempt"]:
        return {"ok": False, "error": "Este examen ya fue contestado y no admite nuevos intentos."}
    if exam_view["status"] in {"PENDIENTE", "VENCIDA"}:
        return {"ok": False, "error": "El examen no está disponible para respuesta en este momento."}

    ordered_questions = sorted(getattr(getattr(assignment, "exam", None), "questions", []) or [], key=lambda row: (row.sort_order or 0, row.id or 0))
    if not ordered_questions:
        return {"ok": False, "error": "El examen no contiene preguntas válidas."}

    selected_map: Dict[int, int] = {}
    for question in ordered_questions:
        raw_value = form.get(f"answer_{int(question.id)}")
        option_id = safe_int(raw_value)
        valid_option_ids = {int(option.id) for option in getattr(question, "options", []) or []}
        if option_id is None or option_id not in valid_option_ids:
            return {"ok": False, "error": "Debes responder todas las preguntas antes de enviar el examen."}
        selected_map[int(question.id)] = option_id

    try:
        attempt = m.ResidentExamAttemptDB(
            assignment_id=int(assignment.id),
            started_at=datetime.utcnow(),
            submitted_at=datetime.utcnow(),
            correct_answers=0,
            total_questions=len(ordered_questions),
        )
        db.add(attempt)
        db.flush()
        correct_answers = 0
        for question in ordered_questions:
            correct_option = next((option for option in getattr(question, "options", []) or [] if option.is_correct), None)
            selected_option_id = int(selected_map[int(question.id)])
            is_correct = bool(correct_option is not None and int(correct_option.id) == selected_option_id)
            if is_correct:
                correct_answers += 1
            db.add(
                m.ResidentExamAnswerDB(
                    attempt_id=int(attempt.id),
                    question_id=int(question.id),
                    selected_option_id=selected_option_id,
                    is_correct=is_correct,
                )
            )
        attempt.correct_answers = correct_answers
        attempt.total_questions = len(ordered_questions)
        attempt.score_pct = round((correct_answers / len(ordered_questions)) * 100.0, 1) if ordered_questions else 0.0
        assignment.estado = "CONTESTADA"
        db.commit()
        return {"ok": True}
    except Exception:
        db.rollback()
        return {"ok": False, "error": "No fue posible guardar las respuestas del examen."}
