from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
import json
import re
import subprocess
import sys
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from app.core.time_utils import utcnow
from app.core.observability import automatic_alerts, metrics_snapshot

from app.services.fau_bot_flow import (
    ensure_default_connectors,
    ensure_fau_schema,
    get_dashboard_payload,
    ingest_external_event,
    latest_alerts,
    latest_external_events,
    latest_reports,
    list_connectors,
    list_action_proposals,
    list_runs,
    run_fau_bot_cycle,
    set_action_proposal_status,
    upsert_connector,
)
from app.services.fau_hospitalizacion_agent import (
    analyze_hospitalizacion_patients,
    get_patient_alerts,
    get_patient_predictions,
    get_patient_summaries,
    list_recent_hospital_alerts,
    summarize_hospital_alerts,
)
from app.services.fau_quirofano_agent import (
    analyze_quirofano_programacion,
    analyze_quirofano_programaciones,
    list_quirofano_predictions,
    list_recent_quirofano_alerts,
    summarize_quirofano_alerts,
)
from app.services.fau_central_brain import (
    analyze_runtime_log_and_suggest_pr,
    get_pr_suggestion,
    generate_patient_integral_report,
    list_engineering_issues,
    list_pr_suggestions,
    list_system_logs,
    search_knowledge,
    set_pr_suggestion_patch,
    set_pr_suggestion_spec,
    set_pr_suggestion_status,
    set_pr_suggestion_test_report,
    upsert_engineering_issue,
    upsert_knowledge_document,
)
from app.services.fau_langgraph_orchestrator import run_patient_pipeline
from app.services.job_registry_flow import list_jobs as list_background_jobs
from app.services.job_registry_flow import summary as jobs_summary
from app.services.inpatient_labs_notes_service import (
    ack_alert as ack_alert_metadata,
    list_alert_actions as list_alert_actions_metadata,
    resolve_alert as resolve_alert_metadata,
)
from fau_bot_core.service import SERVICE as FAU_CORE_SERVICE

router = APIRouter(tags=["fau-bot"])

PR_PATCH_ALLOWED_PREFIXES = ("app/ai_rules", "app/services", "app/api", "app/schemas")
PR_LOOP_STATUSES = {
    "OPEN",
    "SPEC_READY",
    "READY_FOR_PATCH",
    "PATCH_READY",
    "READY_FOR_TEST",
    "TEST_PASSED",
    "TEST_FAILED",
    "MERGED",
    "REJECTED",
}


class QuirofanoAlertsConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast_json(self, payload: Dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        for ws in self.active_connections:
            try:
                await ws.send_json(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.disconnect(ws)


_qx_ws_manager = QuirofanoAlertsConnectionManager()


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_surgical_db()


def _split_logic_bullets(raw: str, limit: int = 3) -> list[str]:
    parts = [x.strip(" -\t\r\n") for x in re.split(r"[\n;\.]+", str(raw or "")) if x.strip()]
    if not parts:
        return ["Aplicar ajuste aditivo respetando compatibilidad y comportamiento actual."]
    return parts[: max(1, min(limit, 10))]


def _to_rule_id(title: str) -> str:
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", str(title or "").upper()) if t]
    base = "_".join(tokens[:8]) or "AUTO_RULE"
    return f"{base}_V1"


def _infer_change_type(*, target_module: str, title: str) -> str:
    t = str(title or "").lower()
    module = str(target_module or "").lower()
    if "rule" in t or module.startswith("app/ai_rules"):
        return "add_rule_version"
    if "endpoint" in t or "api" in t:
        return "add_endpoint"
    if any(k in t for k in ("performance", "latency", "slow")):
        return "perf_fix"
    return "bug_fix"


def _build_proposal_json_from_suggestion(suggestion: Dict[str, Any]) -> Dict[str, Any]:
    titulo = str(suggestion.get("titulo_pr") or "Improve behavior").strip()
    explicacion = str(suggestion.get("explicacion") or "").strip()
    codigo = str(suggestion.get("codigo_sugerido") or "").strip()
    target_module = str(suggestion.get("archivo_objetivo") or "UNKNOWN").strip() or "UNKNOWN"
    goal = titulo
    if explicacion:
        brief = explicacion[:220]
        goal = f"{titulo}: {brief}"
    change_type = _infer_change_type(target_module=target_module, title=titulo)
    proposal: Dict[str, Any] = {
        "goal": goal,
        "constraints": [
            "Additive change only",
            "Do not delete existing fields or behavior",
            "Keep backwards compatibility",
            "Follow existing rule registry pattern",
        ],
        "change_type": change_type,
        "target_module": target_module,
        "new_artifacts": [],
        "tests": [
            {"type": "unit", "name": "test_imports_smoke"},
            {"type": "smoke", "name": "test_health_endpoint"},
        ],
        "rollback_plan": "Revert commit or disable feature flag for this change",
    }
    if change_type == "add_rule_version":
        proposal["rule_spec"] = {
            "rule_id": _to_rule_id(titulo),
            "inputs_required": [],
            "logic": _split_logic_bullets(codigo, limit=3),
            "severity": "MEDIUM",
            "actions": [],
        }
    return proposal


def _extract_diff_paths(patch_diff: str) -> list[str]:
    paths: list[str] = []
    for line in str(patch_diff or "").splitlines():
        m = re.match(r"^diff --git a/(.+?) b/(.+)$", line.strip())
        if not m:
            continue
        a_path = m.group(1).strip()
        b_path = m.group(2).strip()
        for p in (a_path, b_path):
            if p and p != "/dev/null" and p != "dev/null":
                paths.append(p)
    # preservar orden, sin duplicar
    out: list[str] = []
    seen = set()
    for p in paths:
        if p in seen:
            continue
        out.append(p)
        seen.add(p)
    return out


def _is_path_allowed(path_value: str) -> bool:
    p = str(path_value or "").strip().lstrip("./")
    return any(p == pref or p.startswith(pref + "/") for pref in PR_PATCH_ALLOWED_PREFIXES)


def _run_cmd(cmd: list[str], cwd: Path, timeout_sec: int = 600) -> Dict[str, Any]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "cmd": cmd,
            "returncode": int(proc.returncode),
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
            "ran": True,
        }
    except Exception as exc:
        return {
            "cmd": cmd,
            "returncode": -1,
            "stdout": "",
            "stderr": str(exc),
            "ran": False,
        }


def _pytest_skipped(run_result: Dict[str, Any]) -> bool:
    rc = int(run_result.get("returncode") or -1)
    raw = f"{run_result.get('stdout') or ''}\n{run_result.get('stderr') or ''}".lower()
    if rc == 0:
        return False
    skip_markers = (
        "no module named pytest",
        "collected 0 items",
        "no tests ran",
        "pytest skipped",
    )
    return (rc == 5) or any(m in raw for m in skip_markers)


def _generate_or_get_pr_spec(db: Session, suggestion_id: int, *, force: bool = False) -> Dict[str, Any]:
    row = get_pr_suggestion(db, suggestion_id)
    existing = row.get("proposal_json") or {}
    if existing and not force:
        return {"id": int(suggestion_id), "status": row.get("status") or "SPEC_READY", "proposal_json": existing}
    proposal = _build_proposal_json_from_suggestion(row)
    saved = set_pr_suggestion_spec(db, suggestion_id, proposal_json=proposal, status="SPEC_READY")
    return {"id": int(suggestion_id), "status": saved.get("status") or "SPEC_READY", "proposal_json": saved.get("proposal_json") or {}}


def _save_pr_patch(db: Session, suggestion_id: int, patch_diff: str) -> Dict[str, Any]:
    if not str(patch_diff or "").strip():
        raise HTTPException(status_code=400, detail="patch_diff requerido")
    if "diff --git" not in patch_diff:
        raise HTTPException(status_code=400, detail="patch_diff inválido: debe contener encabezados 'diff --git'")
    changed_paths = _extract_diff_paths(patch_diff)
    offending = [p for p in changed_paths if not _is_path_allowed(p)]
    if offending:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "patch_diff contiene rutas fuera del allowlist",
                "allowlist": list(PR_PATCH_ALLOWED_PREFIXES),
                "offending_paths": offending,
            },
        )
    get_pr_suggestion(db, suggestion_id)
    saved = set_pr_suggestion_patch(
        db,
        suggestion_id=suggestion_id,
        patch_diff=patch_diff,
        status="PATCH_READY",
        files_affected=changed_paths,
    )
    return {"id": int(suggestion_id), "status": saved.get("status") or "PATCH_READY"}


def _run_pr_tests(db: Session, suggestion_id: int) -> Dict[str, Any]:
    get_pr_suggestion(db, suggestion_id)
    set_pr_suggestion_status(db, suggestion_id=suggestion_id, status="READY_FOR_TEST")
    project_root = Path(__file__).resolve().parents[2]
    compile_run = _run_cmd([sys.executable, "-m", "compileall", "app", "fau_bot_core"], cwd=project_root)
    pytest_run = _run_cmd([sys.executable, "-m", "pytest", "-q"], cwd=project_root)
    compile_ok = int(compile_run.get("returncode") or -1) == 0
    pytest_ok = int(pytest_run.get("returncode") or -1) == 0
    pytest_skipped = _pytest_skipped(pytest_run)
    final_status = "TEST_PASSED" if (compile_ok and (pytest_ok or pytest_skipped)) else "TEST_FAILED"
    report = {
        "compileall": compile_run,
        "pytest": pytest_run,
        "compile_ok": compile_ok,
        "pytest_ok": pytest_ok,
        "pytest_skipped": pytest_skipped,
        "note": "pytest skipped/failed to run" if (compile_ok and not pytest_ok and pytest_skipped) else "",
        "final_status": final_status,
        "updated_en": utcnow().isoformat(),
    }
    saved = set_pr_suggestion_test_report(db, suggestion_id, test_report_json=report, status=final_status)
    return {"id": int(suggestion_id), "status": saved.get("status") or final_status, "test_report_json": saved.get("test_report_json") or report}


def _sort_local_pr_for_ops(items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    # Prioridad operativa: primero fallas (requieren atención inmediata),
    # luego items listos para parche/revisión y finalmente backlog.
    rank = {
        "TEST_FAILED": 0,
        "PATCH_READY": 1,
        "READY_FOR_TEST": 2,
        "READY_FOR_PATCH": 3,
        "SPEC_READY": 4,
        "OPEN": 5,
        "PENDING_REVIEW": 6,
        "TEST_PASSED": 7,
        "MERGED": 8,
        "REJECTED": 9,
    }
    ordered = sorted(
        list(items or []),
        key=lambda p: str(p.get("updated_en") or p.get("creado_en") or ""),
        reverse=True,
    )
    ordered = sorted(
        ordered,
        key=lambda p: rank.get(str(p.get("status") or "").upper(), 99),
    )
    return ordered


@router.get("/ai/fau-bot", response_class=HTMLResponse)
async def fau_bot_dashboard(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    ensure_default_connectors(db)
    payload = get_dashboard_payload(db)
    payload["jobs_summary"] = jobs_summary(sdb, limit=60)
    payload["jobs_recent"] = list_background_jobs(sdb, limit=40)
    return m.render_template("fau_bot_dashboard.html", request=request, payload=payload)


@router.post("/api/ai/fau-bot/run", response_class=JSONResponse)
async def api_fau_bot_run(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    window_days = 30
    triggered_by = request.headers.get("X-User", "manual")

    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        window_days = int(payload.get("window_days") or 30)
        triggered_by = str(payload.get("triggered_by") or triggered_by)
    else:
        form = await request.form()
        window_days = int(form.get("window_days") or 30)
        triggered_by = str(form.get("triggered_by") or triggered_by)

    result = run_fau_bot_cycle(
        db,
        sdb,
        window_days=max(1, min(window_days, 365)),
        triggered_by=triggered_by,
    )
    return JSONResponse(content=result)


@router.post("/api/ai/hospitalizacion/run", response_class=JSONResponse)
async def api_hospitalizacion_run(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    window_days = 30
    triggered_by = request.headers.get("X-User", "manual")
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        window_days = int(payload.get("window_days") or 30)
        triggered_by = str(payload.get("triggered_by") or triggered_by)
    else:
        form = await request.form()
        window_days = int(form.get("window_days") or 30)
        triggered_by = str(form.get("triggered_by") or triggered_by)

    result = analyze_hospitalizacion_patients(
        db,
        sdb,
        m,
        window_days=max(1, min(window_days, 365)),
        run_id=None,
        model_version="hospitalizacion_agent_v1_manual",
    )
    return JSONResponse(content={"triggered_by": triggered_by, **result})


@router.get("/api/ai/fau-bot/status", response_class=JSONResponse)
def api_fau_bot_status(db: Session = Depends(_get_db)):
    ensure_fau_schema(db)
    runs = list_runs(db, limit=1)
    reports = latest_reports(db, limit=10)
    alerts = latest_alerts(db, limit=10)
    return JSONResponse(
        content={
            "service": "fau_BOT",
            "active": True,
            "timestamp": utcnow().isoformat() + "Z",
            "latest_run": runs[0] if runs else None,
            "latest_reports": reports,
            "latest_alerts": alerts,
        }
    )


@router.get("/ai/quirofano", response_class=HTMLResponse)
async def ai_quirofano_dashboard(request: Request, sdb: Session = Depends(_get_surgical_db)):
    from app.core.app_context import main_proxy as m

    qx_alerts = list_recent_quirofano_alerts(sdb, limit=100, days=30)
    qx_resume = summarize_quirofano_alerts(qx_alerts)
    qx_predictions = list_quirofano_predictions(sdb, limit=50)
    return m.render_template(
        "quirofano_ai_dashboard.html",
        request=request,
        fecha=utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        resumen=qx_resume,
        qx_alerts=qx_alerts,
        qx_predictions=qx_predictions,
    )


@router.post("/api/ai/quirofano/analizar/{programacion_id}", response_class=JSONResponse)
async def api_quirofano_analizar_programacion(
    programacion_id: int,
    request: Request,
    async_mode: bool = True,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    pid = int(programacion_id)
    if async_mode and getattr(m, "celery_app", None) is not None:
        try:
            task = m.celery_app.send_task("clinical_ai.async_quirofano_programacion_analizar", args=[pid])
            return JSONResponse(
                content={
                    "status": "queued",
                    "task_id": str(task.id),
                    "programacion_id": pid,
                    "message": "Análisis encolado por fau_BOT.",
                }
            )
        except Exception:
            # fallback síncrono si Celery no está disponible en runtime.
            pass

    result = analyze_quirofano_programacion(
        db,
        sdb,
        m,
        programacion_id=pid,
        run_id=None,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error") or "Programación no encontrada")

    await _qx_ws_manager.broadcast_json(
        {
            "event": "quirofano_analysis_completed",
            "programacion_id": pid,
            "timestamp": utcnow().isoformat() + "Z",
            "result": result,
        }
    )
    return JSONResponse(content=result)


@router.post("/api/ai/quirofano/run", response_class=JSONResponse)
async def api_quirofano_run_window(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    window_days = 30
    limit = 400
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        window_days = int(payload.get("window_days") or 30)
        limit = int(payload.get("limit") or 400)
    else:
        form = await request.form()
        window_days = int(form.get("window_days") or 30)
        limit = int(form.get("limit") or 400)

    result = analyze_quirofano_programaciones(
        db,
        sdb,
        m,
        window_days=max(1, min(window_days, 365)),
        limit=max(1, min(limit, 3000)),
        run_id=None,
    )

    await _qx_ws_manager.broadcast_json(
        {
            "event": "quirofano_window_completed",
            "timestamp": utcnow().isoformat() + "Z",
            "metrics": result.get("metrics", {}),
            "insights": result.get("insights", []),
        }
    )
    return JSONResponse(content=result)


@router.get("/api/ai/quirofano/alertas", response_class=JSONResponse)
def api_quirofano_alertas(
    limit: int = 200,
    days: int = 30,
    level: Optional[str] = None,
    only_open: bool = False,
    sdb: Session = Depends(_get_surgical_db),
):
    rows = list_recent_quirofano_alerts(sdb, limit=limit, days=days, level=level, only_open=only_open)
    return JSONResponse(content={"resumen": summarize_quirofano_alerts(rows), "alertas": rows})


@router.get("/api/ai/quirofano/predicciones", response_class=JSONResponse)
def api_quirofano_predicciones(
    programacion_id: Optional[int] = None,
    consulta_id: Optional[int] = None,
    limit: int = 200,
    sdb: Session = Depends(_get_surgical_db),
):
    rows = list_quirofano_predictions(
        sdb,
        programacion_id=programacion_id,
        consulta_id=consulta_id,
        limit=limit,
    )
    return JSONResponse(content={"total": len(rows), "predicciones": rows})


@router.websocket("/ai/quirofano/ws/alertas")
async def websocket_quirofano_alertas(websocket: WebSocket):
    from app.core.app_context import main_proxy as m

    await _qx_ws_manager.connect(websocket)
    try:
        await websocket.send_json(
            {
                "event": "connected",
                "channel": "ai_quirofano_alertas",
                "timestamp": utcnow().isoformat() + "Z",
            }
        )
        while True:
            message = (await websocket.receive_text()).strip().lower()
            if message in {"ping", "keepalive", "heartbeat"}:
                await websocket.send_json({"event": "pong", "timestamp": utcnow().isoformat() + "Z"})
                continue
            if message.startswith("latest"):
                try:
                    parts = message.split(":")
                    req_limit = int(parts[1]) if len(parts) > 1 else 50
                except Exception:
                    req_limit = 50
                with m.SurgicalSessionLocal() as ws_sdb:
                    rows = list_recent_quirofano_alerts(ws_sdb, limit=req_limit, days=30)
                    await websocket.send_json(
                        {
                            "event": "latest_alerts",
                            "timestamp": utcnow().isoformat() + "Z",
                            "payload": {"resumen": summarize_quirofano_alerts(rows), "alertas": rows},
                        }
                    )
                continue
            await websocket.send_json(
                {
                    "event": "help",
                    "commands": ["ping", "latest", "latest:100"],
                    "timestamp": utcnow().isoformat() + "Z",
                }
            )
    except WebSocketDisconnect:
        _qx_ws_manager.disconnect(websocket)
    except Exception:
        _qx_ws_manager.disconnect(websocket)


@router.get("/api/ai/fau-bot/runs", response_class=JSONResponse)
def api_fau_bot_runs(limit: int = 30, db: Session = Depends(_get_db)):
    return JSONResponse(content=list_runs(db, limit=limit))


@router.get("/api/ai/fau-bot/reports", response_class=JSONResponse)
def api_fau_bot_reports(
    agent_name: Optional[str] = None,
    limit: int = 30,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content=latest_reports(db, agent_name=agent_name, limit=limit))


@router.get("/api/ai/fau-bot/alerts", response_class=JSONResponse)
def api_fau_bot_alerts(limit: int = 50, db: Session = Depends(_get_db)):
    return JSONResponse(content=latest_alerts(db, limit=limit))


@router.get("/api/ai/fau-bot/connectors", response_class=JSONResponse)
def api_fau_bot_connectors(db: Session = Depends(_get_db)):
    return JSONResponse(content=list_connectors(db))


@router.post("/api/ai/fau-bot/connectors", response_class=JSONResponse)
async def api_fau_bot_upsert_connector(request: Request, db: Session = Depends(_get_db)):
    payload = await request.json()
    result = upsert_connector(
        db,
        name=str(payload.get("name") or "").upper(),
        display_name=payload.get("display_name"),
        kind=str(payload.get("kind") or "api"),
        base_url=str(payload.get("base_url") or ""),
        db_dsn=str(payload.get("db_dsn") or ""),
        auth_mode=str(payload.get("auth_mode") or "none"),
        enabled=payload.get("enabled"),
        permissions_granted=payload.get("permissions_granted"),
        config=payload.get("config") or {},
    )
    return JSONResponse(content=result)


@router.post("/api/ai/fau-bot/connectors/{connector_name}/grant", response_class=JSONResponse)
async def api_fau_bot_connector_grant(
    connector_name: str,
    request: Request,
    db: Session = Depends(_get_db),
):
    payload = await request.json()
    grant = bool(payload.get("permissions_granted", True))
    enabled = bool(payload.get("enabled", grant))

    result = upsert_connector(
        db,
        name=connector_name,
        permissions_granted=grant,
        enabled=enabled,
        config=payload.get("config") or {},
    )
    return JSONResponse(content=result)


@router.post("/api/ai/fau-bot/connectors/{connector_name}/ingest", response_class=JSONResponse)
async def api_fau_bot_connector_ingest(
    connector_name: str,
    request: Request,
    db: Session = Depends(_get_db),
):
    payload = await request.json()

    # Permiso explícito para carga externa
    permission_header = (request.headers.get("X-Connector-Permission") or "").strip().lower()
    if permission_header not in {"granted", "true", "1"}:
        raise HTTPException(status_code=403, detail="Permiso de ingesta externa no otorgado")

    event_date = None
    if payload.get("event_date"):
        try:
            event_date = date.fromisoformat(str(payload.get("event_date")))
        except Exception:
            raise HTTPException(status_code=400, detail="event_date inválida (YYYY-MM-DD)")

    try:
        result = ingest_external_event(
            db,
            connector_name=connector_name,
            event_type=str(payload.get("event_type") or "EXTERNAL_EVENT"),
            payload=payload.get("payload") or payload,
            event_date=event_date,
            external_id=str(payload.get("external_id") or ""),
            patient_ref=str(payload.get("patient_ref") or ""),
        )
        return JSONResponse(content=result)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/api/ai/fau-bot/external-events", response_class=JSONResponse)
def api_fau_bot_external_events(
    connector_name: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content=latest_external_events(db, connector_name=connector_name, limit=limit))


@router.post("/api/ai/fau-bot/knowledge/upsert", response_class=JSONResponse)
async def api_fau_bot_knowledge_upsert(request: Request, db: Session = Depends(_get_db)):
    payload = await request.json()
    try:
        doc = upsert_knowledge_document(
            db,
            fuente=str(payload.get("fuente") or "MANUAL"),
            titulo=str(payload.get("titulo") or "Documento"),
            contenido=str(payload.get("contenido") or ""),
            area=str(payload.get("area") or "GENERAL"),
            tags=payload.get("tags") or [],
        )
        return JSONResponse(content={"status": "ok", "documento": doc})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/ai/fau-bot/knowledge/search", response_class=JSONResponse)
def api_fau_bot_knowledge_search(
    q: str,
    area: Optional[str] = None,
    limit: int = 10,
    db: Session = Depends(_get_db),
):
    rows = search_knowledge(db, query_text=q, area=area, limit=limit)
    return JSONResponse(content={"query": q, "results": rows})


@router.post("/api/ai/fau-bot/central/analyze/{consulta_id}", response_class=JSONResponse)
def api_fau_bot_central_analyze(
    consulta_id: int,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    try:
        report = generate_patient_integral_report(db, sdb, m, consulta_id=consulta_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=report)


@router.post("/api/ai/fau-bot/central/pipeline/{consulta_id}", response_class=JSONResponse)
def api_fau_bot_central_pipeline(
    consulta_id: int,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    try:
        out = run_patient_pipeline(db, sdb, m, patient_id=consulta_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=out)


@router.post("/api/ai/fau-bot/system/self-improvement", response_class=JSONResponse)
async def api_fau_bot_system_self_improvement(
    request: Request,
    db: Session = Depends(_get_db),
):
    source_file = "/tmp/rnp_uvicorn.err.log"
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        source_file = str(payload.get("source_file") or source_file)
    result = analyze_runtime_log_and_suggest_pr(db, source_file=source_file)
    return JSONResponse(content=result)


@router.get("/api/ai/fau-bot/system/logs", response_class=JSONResponse)
def api_fau_bot_system_logs(
    tipo_evento: Optional[str] = None,
    limit: int = 120,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content=list_system_logs(db, event_type=tipo_evento, limit=limit))


@router.get("/api/ai/fau-bot/system/pr-suggestions", response_class=JSONResponse)
def api_fau_bot_system_pr_suggestions(
    status: Optional[str] = None,
    limit: int = 120,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content=list_pr_suggestions(db, status=status, limit=limit))


@router.post("/api/ai/fau-bot/system/telemetry/issues/scan-local", response_class=JSONResponse)
def api_fau_bot_system_scan_local_engineering_issues(
    window_minutes: int = 60,
    db: Session = Depends(_get_db),
):
    mins = max(1, min(int(window_minutes or 60), 24 * 60))
    metrics = metrics_snapshot(window_minutes=mins)
    alerts = automatic_alerts(window_minutes=mins)
    created: list[Dict[str, Any]] = []

    error_rate = float(metrics.get("error_rate_pct") or 0.0)
    p95 = float((metrics.get("latency") or {}).get("p95_ms") or 0.0)
    if error_rate >= 1.0:
        created.append(
            upsert_engineering_issue(
                db,
                source="runtime_observability",
                issue_code="GLOBAL_5XX_RATE",
                title=f"Error 5xx global elevado ({error_rate:.2f}%)",
                category="runtime",
                severity="CRITICAL" if error_rate >= 5.0 else "WARNING",
                priority="P1" if error_rate >= 5.0 else "P2",
                evidence={"window_minutes": mins, "error_rate_pct": error_rate, "errors_5xx": metrics.get("errors_5xx")},
                status="OPEN",
            )
        )
    if p95 >= 800:
        created.append(
            upsert_engineering_issue(
                db,
                source="runtime_observability",
                issue_code="GLOBAL_P95_LATENCY",
                title=f"Latencia P95 alta ({p95:.1f} ms)",
                category="performance",
                severity="WARNING" if p95 < 2000 else "CRITICAL",
                priority="P2" if p95 < 2000 else "P1",
                evidence={"window_minutes": mins, "p95_ms": p95, "events": metrics.get("events")},
                status="OPEN",
            )
        )

    for route in (metrics.get("top_routes") or [])[:20]:
        route_name = str(route.get("route") or "").strip()
        if not route_name:
            continue
        total = int(route.get("total") or 0)
        err_pct = float(route.get("error_rate_pct") or 0.0)
        route_p95 = float(route.get("p95_ms") or 0.0)
        if total < 20:
            continue
        if err_pct < 10.0 and route_p95 < 1500:
            continue
        code = re.sub(r"[^A-Z0-9]+", "_", route_name.upper()).strip("_")
        issue_code = f"ROUTE_{code}"[:110]
        created.append(
            upsert_engineering_issue(
                db,
                source="runtime_observability",
                issue_code=issue_code,
                title=f"Ruta degradada {route_name}",
                category="endpoint",
                severity="WARNING",
                priority="P2" if err_pct < 20.0 else "P1",
                evidence={
                    "window_minutes": mins,
                    "route": route_name,
                    "total": total,
                    "error_rate_pct": err_pct,
                    "p95_ms": route_p95,
                },
                status="OPEN",
            )
        )

    return JSONResponse(
        content={
            "status": "ok",
            "window_minutes": mins,
            "alerts_detected": len(alerts),
            "issues_upserted": len(created),
            "issues": created,
            "metrics": metrics,
        }
    )


@router.get("/api/ai/fau-bot/system/telemetry/issues-local", response_class=JSONResponse)
def api_fau_bot_system_local_engineering_issues(
    status: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content=list_engineering_issues(db, status=status, limit=limit))


@router.post("/api/ai/fau-bot/system/pr-suggestions/{suggestion_id}/status", response_class=JSONResponse)
async def api_fau_bot_system_pr_suggestion_status(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    payload = await request.json()
    status = str(payload.get("status") or "PENDING_REVIEW")
    try:
        updated = set_pr_suggestion_status(db, suggestion_id=suggestion_id, status=status)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=updated)


@router.post("/api/ai/fau-bot/system/pr-suggestions/{suggestion_id}/spec", response_class=JSONResponse)
def api_fau_bot_system_pr_suggestion_spec(
    suggestion_id: int,
    force: int = 0,
    db: Session = Depends(_get_db),
):
    try:
        result = _generate_or_get_pr_spec(db, suggestion_id, force=bool(int(force or 0)))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/ai/fau-bot/system/pr-suggestions/{suggestion_id}/ready-for-patch", response_class=JSONResponse)
def api_fau_bot_system_pr_suggestion_ready_for_patch(
    suggestion_id: int,
    db: Session = Depends(_get_db),
):
    try:
        row = get_pr_suggestion(db, suggestion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    current_status = str(row.get("status") or "").upper()
    if current_status == "PATCH_READY":
        # Ya está en una fase posterior; respetar estado actual.
        return JSONResponse(content={"id": int(suggestion_id), "status": "PATCH_READY"})

    updated = set_pr_suggestion_status(db, suggestion_id=suggestion_id, status="READY_FOR_PATCH")
    return JSONResponse(content={"id": int(suggestion_id), "status": updated.get("status") or "READY_FOR_PATCH"})


@router.post("/api/ai/fau-bot/system/pr-suggestions/{suggestion_id}/patch", response_class=JSONResponse)
async def api_fau_bot_system_pr_suggestion_patch(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    ctype = (request.headers.get("content-type") or "").lower()
    payload: Dict[str, Any] = {}
    if "application/json" in ctype:
        payload = await request.json()
    else:
        form = await request.form()
        payload = {k: v for k, v in form.items()}

    patch_diff = str(payload.get("patch_diff") or "")
    if not patch_diff.strip():
        raise HTTPException(status_code=400, detail="patch_diff requerido")
    try:
        result = _save_pr_patch(db, suggestion_id, patch_diff)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/ai/fau-bot/system/pr-suggestions/{suggestion_id}/run-tests", response_class=JSONResponse)
def api_fau_bot_system_pr_suggestion_run_tests(
    suggestion_id: int,
    db: Session = Depends(_get_db),
):
    try:
        result = _run_pr_tests(db, suggestion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/api/ai/hospitalizacion/patient/{consulta_id}/predicciones", response_class=JSONResponse)
def api_hospitalizacion_patient_predictions(
    consulta_id: int,
    limit: int = 120,
    sdb: Session = Depends(_get_surgical_db),
):
    return JSONResponse(
        content={
            "consulta_id": consulta_id,
            "predicciones": get_patient_predictions(sdb, consulta_id=consulta_id, limit=limit),
        }
    )


@router.get("/api/ai/hospitalizacion/patient/{consulta_id}/alertas", response_class=JSONResponse)
def api_hospitalizacion_patient_alerts(
    consulta_id: int,
    limit: int = 120,
    only_open: bool = False,
    sdb: Session = Depends(_get_surgical_db),
):
    return JSONResponse(
        content={
            "consulta_id": consulta_id,
            "alertas": get_patient_alerts(sdb, consulta_id=consulta_id, limit=limit, only_open=only_open),
        }
    )


@router.get("/api/ai/hospitalizacion/patient/{consulta_id}/resumen", response_class=JSONResponse)
def api_hospitalizacion_patient_summary(
    consulta_id: int,
    limit: int = 60,
    sdb: Session = Depends(_get_surgical_db),
):
    return JSONResponse(
        content={
            "consulta_id": consulta_id,
            "resumenes": get_patient_summaries(sdb, consulta_id=consulta_id, limit=limit),
        }
    )


@router.get("/api/ai/hospitalizacion/alertas", response_class=JSONResponse)
def api_hospitalizacion_alertas(
    limit: int = 200,
    days: int = 30,
    only_open: bool = False,
    sdb: Session = Depends(_get_surgical_db),
):
    rows = list_recent_hospital_alerts(sdb, limit=limit, days=days, only_open=only_open)
    return JSONResponse(content={"resumen": summarize_hospital_alerts(rows), "alertas": rows})


@router.get("/api/ai/fau-bot/actions", response_class=JSONResponse)
def api_fau_bot_actions(
    status: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(_get_db),
):
    return JSONResponse(content={"actions": list_action_proposals(db, status=status, limit=limit)})


@router.post("/api/ai/fau-bot/actions/{action_id}/status", response_class=JSONResponse)
async def api_fau_bot_action_status(
    action_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    payload: Dict[str, Any] = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        payload = await request.json()
    else:
        form = await request.form()
        payload = {k: v for k, v in form.items()}
    status = str(payload.get("status") or "PENDING_REVIEW")
    reviewer = str(payload.get("reviewer") or request.headers.get("X-User") or "reviewer")
    reviewer_comment = str(payload.get("reviewer_comment") or "")
    try:
        updated = set_action_proposal_status(
            db,
            action_id=int(action_id),
            status=status,
            reviewer=reviewer,
            reviewer_comment=reviewer_comment,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=updated)


@router.post("/reporte/alertas/actions/{action_id}", response_class=HTMLResponse)
async def reporte_alertas_action_status(
    action_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")

    status = str(form_dict.get("status") or "PENDING_REVIEW")
    reviewer = str(form_dict.get("reviewer") or request.headers.get("X-User") or "reviewer")
    reviewer_comment = str(form_dict.get("reviewer_comment") or "")
    try:
        set_action_proposal_status(
            db,
            action_id=int(action_id),
            status=status,
            reviewer=reviewer,
            reviewer_comment=reviewer_comment,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return m.RedirectResponse(url="/reporte/alertas?updated=1", status_code=303)


@router.post("/reporte/alertas/central/{alert_id}/ack", response_class=HTMLResponse)
async def reporte_alertas_central_ack(
    alert_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
    ack_by = str(form_dict.get("ack_by") or request.headers.get("X-User") or "reviewer")
    try:
        ack_alert_metadata(db, alert_id=int(alert_id), ack_by=ack_by)
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"No fue posible hacer ACK: {exc}") from exc
    return m.RedirectResponse(url="/reporte/alertas?updated=central_ack", status_code=303)


@router.post("/reporte/alertas/central/{alert_id}/resolve", response_class=HTMLResponse)
async def reporte_alertas_central_resolve(
    alert_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")

    resolved_by = str(form_dict.get("resolved_by") or request.headers.get("X-User") or "reviewer")
    resolution_reason = str(form_dict.get("resolution_reason") or "handled")
    action_taken_txt = str(form_dict.get("action_taken_json") or "").strip()
    action_taken = {}
    if action_taken_txt:
        try:
            parsed = json.loads(action_taken_txt)
            if isinstance(parsed, dict):
                action_taken = parsed
            else:
                action_taken = {"texto": action_taken_txt}
        except Exception:
            action_taken = {"texto": action_taken_txt}
    try:
        resolve_alert_metadata(
            db,
            alert_id=int(alert_id),
            resolved_by=resolved_by,
            resolution_reason=resolution_reason,
            action_taken_json=action_taken,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"No fue posible resolver alerta central: {exc}") from exc
    return m.RedirectResponse(url="/reporte/alertas?updated=central_resolve", status_code=303)


@router.post("/reporte/alertas/pr-suggestions/{suggestion_id}/spec", response_class=HTMLResponse)
async def reporte_alertas_pr_spec(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
    force = bool(int(str(form_dict.get("force") or "0")))
    try:
        _generate_or_get_pr_spec(db, suggestion_id, force=force)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return m.RedirectResponse(url="/reporte/alertas?updated=pr_spec", status_code=303)


@router.post("/reporte/alertas/pr-suggestions/{suggestion_id}/ready-for-patch", response_class=HTMLResponse)
async def reporte_alertas_pr_ready_for_patch(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
    try:
        row = get_pr_suggestion(db, suggestion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    current = str(row.get("status") or "").upper()
    if current != "PATCH_READY":
        set_pr_suggestion_status(db, suggestion_id=suggestion_id, status="READY_FOR_PATCH")
    return m.RedirectResponse(url="/reporte/alertas?updated=pr_ready", status_code=303)


@router.post("/reporte/alertas/pr-suggestions/{suggestion_id}/patch", response_class=HTMLResponse)
async def reporte_alertas_pr_patch(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
    patch_diff = str(form_dict.get("patch_diff") or "")
    _save_pr_patch(db, suggestion_id, patch_diff)
    return m.RedirectResponse(url="/reporte/alertas?updated=pr_patch", status_code=303)


@router.post("/reporte/alertas/pr-suggestions/{suggestion_id}/run-tests", response_class=HTMLResponse)
async def reporte_alertas_pr_run_tests(
    suggestion_id: int,
    request: Request,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    form = await request.form()
    form_dict = {k: v for k, v in form.items()}
    try:
        m.validate_csrf(form_dict, request)
    except Exception:
        raise HTTPException(status_code=400, detail="CSRF token inválido")
    _run_pr_tests(db, suggestion_id)
    return m.RedirectResponse(url="/reporte/alertas?updated=pr_tests", status_code=303)


@router.get("/reporte/alertas", response_class=HTMLResponse)
def reporte_alertas_faubot(
    request: Request,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    severity_filter = str(request.query_params.get("severity") or "ALL").upper()
    module_filter = str(request.query_params.get("module") or "ALL").upper()
    status_filter = str(request.query_params.get("status") or "ALL").upper()
    # Operación por defecto: enfocar la bandeja en bloqueados.
    pr_queue_filter = str(request.query_params.get("pr_queue") or "BLOCKED").upper()
    limit = max(50, min(int(request.query_params.get("limit") or 400), 2000))

    central_alerts = latest_alerts(db, limit=200)
    central_alert_action_map = list_alert_actions_metadata(
        db,
        alert_ids=[int(a.get("id")) for a in central_alerts if a.get("id") is not None],
        limit=3000,
    )
    action_proposals = list_action_proposals(db, limit=200)
    hosp_alerts = list_recent_hospital_alerts(sdb, limit=200, days=30)
    qx_alerts = list_recent_quirofano_alerts(sdb, limit=200, days=30)
    local_pr_suggestions_all = _sort_local_pr_for_ops(list_pr_suggestions(db, limit=200))
    blocked_local_pr_statuses = {"TEST_FAILED", "PATCH_READY", "READY_FOR_TEST"}
    local_pr_suggestions = local_pr_suggestions_all
    if pr_queue_filter == "BLOCKED":
        local_pr_suggestions = [
            p
            for p in local_pr_suggestions_all
            if str(p.get("status") or "").upper() in blocked_local_pr_statuses
        ]
    core_pr_suggestions = FAU_CORE_SERVICE.list_pr_suggestions(limit=200)
    core_issues = FAU_CORE_SERVICE.list_engineering_issues(limit=200)
    runtime_kpis = FAU_CORE_SERVICE.runtime_kpis(window_minutes=60)

    def _sev_bucket(v: str) -> str:
        raw = str(v or "").upper()
        if raw in {"CRITICAL", "HIGH", "ALTA", "P0", "P1", "WARNING"}:
            return "ALTA"
        if raw in {"MEDIUM", "MEDIA", "P2"}:
            return "MEDIA"
        if raw in {"LOW", "BAJA", "P3", "INFO"}:
            return "BAJA"
        return "INFO"

    unified_feed = []
    for a in central_alerts:
        unified_feed.append(
            {
                "module": "FAU_CENTRAL",
                "source": "FAU_CENTRAL",
                "date": a.get("created_at"),
                "severity": _sev_bucket(str(a.get("severity") or "")),
                "status": "OPEN",
                "title": a.get("title") or "Alerta central",
                "description": a.get("description") or "",
                "ref": f"ALERT-{a.get('id')}",
            }
        )
    for a in hosp_alerts:
        unified_feed.append(
            {
                "module": "HOSPITALIZACION",
                "source": "AI_HOSPITALIZACION",
                "date": a.get("alert_ts") or a.get("created_at"),
                "severity": _sev_bucket(str(a.get("severity") or "")),
                "status": "RESOLVED" if bool(a.get("resolved")) else "OPEN",
                "title": a.get("alert_type") or "Alerta hospitalización",
                "description": a.get("message") or "",
                "ref": f"HOSP-{a.get('id')}",
            }
        )
    for a in qx_alerts:
        unified_feed.append(
            {
                "module": "QUIROFANO",
                "source": "AI_QUIROFANO",
                "date": a.get("fecha_creacion"),
                "severity": _sev_bucket(str(a.get("nivel") or "")),
                "status": "RESOLVED" if bool(a.get("resuelta")) else "OPEN",
                "title": str((a.get("detalles") or {}).get("tipo_insight") or "Alerta quirófano"),
                "description": a.get("mensaje") or "",
                "ref": f"QX-{a.get('id')}",
            }
        )
    for a in action_proposals:
        unified_feed.append(
            {
                "module": "FAU_ACTIONS",
                "source": "FAU_ACTION",
                "date": a.get("created_at"),
                "severity": _sev_bucket(str(a.get("priority") or "")),
                "status": str(a.get("status") or "PENDING_REVIEW").upper(),
                "title": a.get("title") or "Action proposal",
                "description": a.get("description") or "",
                "ref": f"ACT-{a.get('id')}",
            }
        )
    for p in core_pr_suggestions:
        unified_feed.append(
            {
                "module": "DEVOPS_PR",
                "source": "FAU_PR",
                "date": p.get("updated_at") or p.get("created_at"),
                "severity": _sev_bucket(str(p.get("priority") or "")),
                "status": str(p.get("status") or "OPEN").upper(),
                "title": p.get("title") or "PR suggestion",
                "description": str((p.get("proposal_json") or {}).get("goal") or ""),
                "ref": f"PR-{p.get('id')}",
            }
        )
    for issue in core_issues:
        unified_feed.append(
            {
                "module": "DEVOPS_ISSUE",
                "source": "FAU_ISSUE",
                "date": issue.get("last_seen") or issue.get("first_seen"),
                "severity": _sev_bucket(str(issue.get("severity") or "")),
                "status": str(issue.get("status") or "OPEN").upper(),
                "title": issue.get("title") or "Engineering issue",
                "description": issue.get("issue_code") or "",
                "ref": f"ISS-{issue.get('id')}",
            }
        )

    filtered_feed = []
    for row in unified_feed:
        if severity_filter != "ALL" and str(row.get("severity") or "").upper() != severity_filter:
            continue
        if module_filter != "ALL" and str(row.get("module") or "").upper() != module_filter:
            continue
        if status_filter != "ALL" and str(row.get("status") or "").upper() != status_filter:
            continue
        filtered_feed.append(row)
    filtered_feed = sorted(filtered_feed, key=lambda x: str(x.get("date") or ""), reverse=True)[:limit]

    unified_summary = {
        "total": len(filtered_feed),
        "total_raw": len(unified_feed),
        "por_modulo": dict(Counter(str(x.get("module") or "N/A") for x in filtered_feed)),
        "por_severidad": dict(Counter(str(x.get("severity") or "INFO") for x in filtered_feed)),
        "por_estado": dict(Counter(str(x.get("status") or "OPEN") for x in filtered_feed)),
    }

    resumen = {
        "total_central": len(central_alerts),
        "total_actions": len(action_proposals),
        "total_hospitalizacion": len(hosp_alerts),
        "total_quirofano": len(qx_alerts),
        "hospitalizacion": summarize_hospital_alerts(hosp_alerts),
        "quirofano": summarize_quirofano_alerts(qx_alerts),
        "total_core_pr_suggestions": len(core_pr_suggestions),
        "total_local_pr_suggestions": len(local_pr_suggestions_all),
        "total_local_pr_suggestions_filtered": len(local_pr_suggestions),
        "total_local_pr_blocked": sum(
            1 for p in local_pr_suggestions_all if str(p.get("status") or "").upper() in blocked_local_pr_statuses
        ),
        "total_core_issues": len(core_issues),
        "runtime_kpis": runtime_kpis,
        "unified": unified_summary,
    }
    return m.render_template(
        "reporte_alertas.html",
        request=request,
        fecha=utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        resumen=resumen,
        central_alerts=central_alerts,
        central_alert_action_map=central_alert_action_map,
        action_proposals=action_proposals,
        local_pr_suggestions=local_pr_suggestions,
        hospital_alerts=hosp_alerts,
        qx_alerts=qx_alerts,
        unified_feed=filtered_feed,
        severity_filter=severity_filter,
        module_filter=module_filter,
        status_filter=status_filter,
        pr_queue_filter=pr_queue_filter,
        limit_filter=limit,
        module_options=sorted({str(x.get("module") or "N/A") for x in unified_feed}),
        status_options=sorted({str(x.get("status") or "OPEN") for x in unified_feed}),
        updated=request.query_params.get("updated"),
    )
