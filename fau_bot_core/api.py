from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .service import SERVICE


router = APIRouter(tags=["fau-bot-core"])


@router.get("/status", response_class=JSONResponse)
def core_status():
    return JSONResponse(content=SERVICE.status())


@router.post("/run", response_class=JSONResponse)
async def core_run(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    result = SERVICE.run_cycle(
        window_days=int(payload.get("window_days") or 30),
        triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
    )
    return JSONResponse(content=result)


@router.get("/runs", response_class=JSONResponse)
def core_runs(limit: int = 40):
    return JSONResponse(content=SERVICE.list_runs(limit=limit))


@router.get("/alerts", response_class=JSONResponse)
def core_alerts(limit: int = 120, only_open: bool = False):
    return JSONResponse(content=SERVICE.list_alerts(limit=limit, only_open=only_open))


@router.post("/knowledge/load-default", response_class=JSONResponse)
def core_load_default_knowledge():
    return JSONResponse(content=SERVICE.load_default_knowledge())


@router.get("/knowledge/search", response_class=JSONResponse)
def core_knowledge_search(q: str, area: Optional[str] = None, limit: int = 10):
    return JSONResponse(content={"query": q, "results": SERVICE.knowledge_search(q, area=area, limit=limit)})


@router.get("/hitl/suggestions", response_class=JSONResponse)
def core_hitl_list(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_hitl(status=status, limit=limit))


@router.post("/dev/scan", response_class=JSONResponse)
async def core_dev_scan(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.run_dev_collaboration_scan(
            source_root=payload.get("source_root"),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
            max_files=int(payload.get("max_files") or 350),
            max_file_size_kb=int(payload.get("max_file_size_kb") or 900),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/dev/suggestions", response_class=JSONResponse)
def core_dev_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_dev_suggestions(status=status, limit=limit))


@router.get("/architect/rules", response_class=JSONResponse)
def core_architect_rules():
    return JSONResponse(content=SERVICE.architect_rules())


@router.post("/architect/scan", response_class=JSONResponse)
async def core_architect_scan(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.run_architect_scan(
            source_root=payload.get("source_root"),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
            max_files=int(payload.get("max_files") or 350),
            max_file_size_kb=int(payload.get("max_file_size_kb") or 900),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/architect/suggestions", response_class=JSONResponse)
def core_architect_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_architect_suggestions(status=status, limit=limit))


@router.post("/hitl/suggestions/{suggestion_id}/status", response_class=JSONResponse)
async def core_hitl_update(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        out = SERVICE.set_hitl_status(
            int(suggestion_id),
            status=str(payload.get("status") or "PENDING_REVIEW"),
            reviewer=str(payload.get("reviewer") or request.headers.get("X-User") or "reviewer"),
            reviewer_comment=str(payload.get("reviewer_comment") or ""),
        )
        return JSONResponse(content=out)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/hitl/audit", response_class=JSONResponse)
def core_hitl_audit(limit: int = 200):
    return JSONResponse(content=SERVICE.list_audit(limit=limit))


@router.post("/dev/telemetry/scan", response_class=JSONResponse)
async def core_telemetry_scan(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.run_engineering_telemetry_scan(
            source_root=payload.get("source_root"),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
            max_files=int(payload.get("max_files") or 400),
            max_file_size_kb=int(payload.get("max_file_size_kb") or 900),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/dev/telemetry/issues", response_class=JSONResponse)
def core_telemetry_issues(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_engineering_issues(status=status, limit=limit))


@router.get("/dev/telemetry/runtime-kpis", response_class=JSONResponse)
def core_telemetry_runtime_kpis(window_minutes: int = 60):
    return JSONResponse(content=SERVICE.runtime_kpis(window_minutes=window_minutes))


@router.post("/dev/pr-suggestions/generate", response_class=JSONResponse)
async def core_generate_pr_suggestions(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    out = SERVICE.generate_pr_suggestions(
        triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        limit=int(payload.get("limit") or 10),
    )
    return JSONResponse(content=out)


@router.get("/dev/pr-suggestions", response_class=JSONResponse)
def core_list_pr_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_pr_suggestions(status=status, limit=limit))


@router.post("/dev/pr-suggestions/{suggestion_id}/spec", response_class=JSONResponse)
async def core_pr_spec(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        out = SERVICE.build_pr_suggestion_spec(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=out)


@router.post("/dev/pr-suggestions/{suggestion_id}/build-patch", response_class=JSONResponse)
async def core_pr_patch(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        out = SERVICE.build_patch_from_suggestion(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=out)


@router.post("/dev/pr-suggestions/{suggestion_id}/run-tests", response_class=JSONResponse)
async def core_pr_tests(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    commands = payload.get("commands")
    if commands is not None and not isinstance(commands, list):
        raise HTTPException(status_code=400, detail="commands debe ser lista.")
    try:
        out = SERVICE.run_patch_verification(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
            commands=commands,
            timeout_sec=int(payload.get("timeout_sec") or 240),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=out)


@router.post("/dev/pr-suggestions/{suggestion_id}/mark-merged", response_class=JSONResponse)
async def core_pr_merged(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        out = SERVICE.mark_pr_merged(
            int(suggestion_id),
            reviewer=str(payload.get("reviewer") or request.headers.get("X-User") or "manual"),
            reviewer_comment=str(payload.get("reviewer_comment") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=out)


@router.get("/panel", response_class=HTMLResponse)
def core_panel():
    status = SERVICE.status()
    alerts = SERVICE.list_alerts(limit=50, only_open=False)
    hitl = SERVICE.list_hitl(limit=50)
    dev = SERVICE.list_dev_suggestions(limit=50)
    architect = SERVICE.list_architect_suggestions(limit=50)

    html = [
        "<!doctype html><html><head><meta charset='utf-8'><title>fau_BOT Core</title>",
        "<style>body{font-family:Montserrat,Arial;background:#f4f6f9;padding:20px}"
        ".box{background:#fff;border-radius:10px;padding:16px;margin-bottom:14px}"
        "table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #ddd;padding:8px;text-align:left}"
        "th{background:#13322B;color:#fff}.tag{padding:2px 8px;border-radius:6px;background:#e9ecef}</style>",
        "</head><body>",
        "<div class='box'><h2>fau_BOT Core (Microservicio)</h2>",
        f"<p>Schema: <b>{status.get('schema')}</b> | LLM: <b>{status.get('llm_provider')}</b> / <b>{status.get('llm_model')}</b></p>",
        "<p><a href='/knowledge/load-default'>Cargar corpus EAU/AUA/NCCN (endpoint)</a> | "
        "<a href='#' onclick=\"event.preventDefault();fetch('/dev/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({max_files:350,triggered_by:'panel'})}).then(()=>location.reload())\">Ejecutar escaneo Dev-HITL</a> | "
        "<a href='#' onclick=\"event.preventDefault();fetch('/architect/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({max_files:350,triggered_by:'panel'})}).then(()=>location.reload())\">Ejecutar Agente Arquitecto</a></p>",
        "</div>",
        "<div class='box'><h3>Alertas recientes</h3><table><tr><th>ID</th><th>Severidad</th><th>Título</th><th>Categoría</th></tr>",
    ]
    for a in alerts[:20]:
        html.append(f"<tr><td>{a['id']}</td><td><span class='tag'>{a['severity']}</span></td><td>{a['title']}</td><td>{a['category']}</td></tr>")
    html.append("</table></div>")

    html.append("<div class='box'><h3>Cola HITL</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Reviewer</th></tr>")
    for s in hitl[:20]:
        html.append(
            f"<tr><td>{s['id']}</td><td><span class='tag'>{s['status']}</span></td><td>{s['title']}</td><td>{s.get('reviewer') or '-'}</td></tr>"
        )
    html.append("</table></div>")

    html.append("<div class='box'><h3>Dev suggestions (CODE_IMPROVEMENT)</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Recomendación</th></tr>")
    for s in dev[:20]:
        html.append(
            f"<tr><td>{s['id']}</td><td><span class='tag'>{s['status']}</span></td><td>{s['title']}</td><td>{s['recommendation']}</td></tr>"
        )
    html.append("</table></div>")

    html.append("<div class='box'><h3>Agente Arquitecto (ARCHITECT_REVIEW)</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Recomendación</th></tr>")
    for s in architect[:20]:
        html.append(
            f"<tr><td>{s['id']}</td><td><span class='tag'>{s['status']}</span></td><td>{s['title']}</td><td>{s['recommendation']}</td></tr>"
        )
    html.append("</table></div></body></html>")
    return HTMLResponse("".join(html))
