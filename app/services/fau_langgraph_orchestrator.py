from __future__ import annotations

from typing import Any, Dict, Optional, TypedDict

from sqlalchemy.orm import Session

from app.services.fau_central_brain import generate_patient_integral_report, search_knowledge


class PatientState(TypedDict, total=False):
    patient_id: int
    raw_data: Dict[str, Any]
    clinical_notes_summary: str
    risk_prediction: float
    relevant_guidelines: list
    final_report: str


def _fetch_data_node(state: PatientState, db: Session, sdb: Session, main_module: Any) -> PatientState:
    consulta_id = int(state.get("patient_id") or 0)
    result = generate_patient_integral_report(db, sdb, main_module, consulta_id=consulta_id)
    context = result.get("context") or {}
    state["raw_data"] = context
    pred = (context.get("prediccion") or {}).get("risk_score")
    state["risk_prediction"] = float(pred or 0.0)
    return state


def _nlp_summary_node(state: PatientState) -> PatientState:
    ctx = state.get("raw_data") or {}
    p = ctx.get("paciente") or {}
    cnt = ctx.get("conteos") or {}
    pred = ctx.get("prediccion") or {}
    state["clinical_notes_summary"] = (
        f"Paciente {p.get('nombre', 'N/A')} con diagnóstico {p.get('diagnostico_principal', 'N/A')}. "
        f"Hospitalizaciones: {cnt.get('hospitalizaciones', 0)}, QX programadas: {cnt.get('qx_programadas', 0)}, "
        f"QX urgencias: {cnt.get('qx_urgencias', 0)}. Riesgo integrado: {pred.get('risk_level', 'N/A')}"
    )
    return state


def _evidence_node(state: PatientState, db: Session) -> PatientState:
    ctx = state.get("raw_data") or {}
    p = ctx.get("paciente") or {}
    dx = str(p.get("diagnostico_principal") or "urologia")
    state["relevant_guidelines"] = search_knowledge(db, query_text=dx, area="UROLOGIA", limit=5)
    return state


def _central_node(state: PatientState) -> PatientState:
    ctx = state.get("raw_data") or {}
    pred = ctx.get("prediccion") or {}
    guidelines = state.get("relevant_guidelines") or []
    g_titles = ", ".join([str(g.get("titulo") or "") for g in guidelines[:3]]) or "sin evidencia específica indexada"
    state["final_report"] = (
        f"fau_BOT: riesgo estimado {round(float(pred.get('risk_score') or 0.0) * 100, 2)}%. "
        f"Factores: {', '.join(pred.get('factores') or []) if pred.get('factores') else 'N/A'}. "
        f"Resumen: {state.get('clinical_notes_summary')}. Evidencia relacionada: {g_titles}."
    )
    return state


def run_patient_pipeline(
    db: Session,
    sdb: Session,
    main_module: Any,
    *,
    patient_id: int,
) -> Dict[str, Any]:
    """
    Orquestador de fau_BOT. Si LangGraph está disponible, usa grafo;
    de lo contrario usa pipeline secuencial equivalente.
    """
    state: PatientState = {"patient_id": int(patient_id)}

    try:
        from langgraph.graph import END, StateGraph

        workflow = StateGraph(PatientState)

        def fetch_node(s: PatientState) -> PatientState:
            return _fetch_data_node(s, db, sdb, main_module)

        def nlp_node(s: PatientState) -> PatientState:
            return _nlp_summary_node(s)

        def evidence_node(s: PatientState) -> PatientState:
            return _evidence_node(s, db)

        def central_node(s: PatientState) -> PatientState:
            return _central_node(s)

        workflow.add_node("fetch_data", fetch_node)
        workflow.add_node("nlp", nlp_node)
        workflow.add_node("evidence", evidence_node)
        workflow.add_node("central", central_node)
        workflow.set_entry_point("fetch_data")
        workflow.add_edge("fetch_data", "nlp")
        workflow.add_edge("nlp", "evidence")
        workflow.add_edge("evidence", "central")
        workflow.add_edge("central", END)

        app = workflow.compile()
        out = app.invoke(state)
        return {"mode": "langgraph", "state": out}
    except Exception:
        state = _fetch_data_node(state, db, sdb, main_module)
        state = _nlp_summary_node(state)
        state = _evidence_node(state, db)
        state = _central_node(state)
        return {"mode": "sequential", "state": state}
