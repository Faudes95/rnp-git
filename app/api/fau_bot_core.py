from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.integrations.fau_bot_core.boundary import build_fau_bot_core_boundary_status
from fau_bot_core.service import SERVICE


router = APIRouter(tags=["fau-bot-core-bridge"])


@router.get("/ai/fau-bot-core", response_class=HTMLResponse)
def fau_bot_core_panel():
    SERVICE.bootstrap()
    return HTMLResponse(SERVICE_PANEL_HTML())


def SERVICE_PANEL_HTML() -> str:
    status = SERVICE.status()
    alerts = SERVICE.list_alerts(limit=30)
    hitl = SERVICE.list_hitl(limit=30)
    dev = SERVICE.list_dev_suggestions(limit=30)
    architect = SERVICE.list_architect_suggestions(limit=30)
    issues = SERVICE.list_engineering_issues(limit=30)
    prs = SERVICE.list_pr_suggestions(limit=30)
    html = [
        "<!doctype html><html><head><meta charset='utf-8'><title>fau_BOT Core</title>",
        "<style>body{font-family:Montserrat,Arial;background:#f4f6f9;padding:20px}.box{background:#fff;border-radius:10px;padding:16px;margin-bottom:14px}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #ddd;padding:8px;text-align:left}th{background:#13322B;color:#fff}.btn{display:inline-block;padding:8px 12px;background:#13322B;color:#fff;text-decoration:none;border-radius:6px;margin-right:6px}</style>",
        "</head><body>",
        "<div class='box'><h2>fau_BOT Core (Bridge)</h2>",
        f"<p>Schema: <b>{status.get('schema')}</b> | LLM: <b>{status.get('llm_provider')}</b> / <b>{status.get('llm_model')}</b></p>",
        "<a class='btn' href='/api/ai/fau-bot-core/run' onclick=\"event.preventDefault();fetch('/api/ai/fau-bot-core/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({window_days:30,triggered_by:'ui'})}).then(()=>location.reload())\">Ejecutar ciclo</a>",
        "<a class='btn' href='/api/v1/dev/telemetry/scan' onclick=\"event.preventDefault();fetch('/api/v1/dev/telemetry/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({max_files:350,triggered_by:'ui'})}).then(()=>location.reload())\">Telemetry scan</a>",
        "<a class='btn' href='/api/v1/dev/telemetry/runtime-kpis' target='_blank'>Runtime KPIs JSON</a>",
        "<a class='btn' href='/api/v1/dev/pr-suggestions/generate' onclick=\"event.preventDefault();fetch('/api/v1/dev/pr-suggestions/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({limit:10,triggered_by:'ui'})}).then(()=>location.reload())\">Generar PR suggestions</a>",
        "<a class='btn' href='/api/ai/fau-bot-core/dev/scan' onclick=\"event.preventDefault();fetch('/api/ai/fau-bot-core/dev/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({max_files:350,triggered_by:'ui'})}).then(()=>location.reload())\">Escaneo Dev-HITL</a>",
        "<a class='btn' href='/api/ai/fau-bot-core/architect/scan' onclick=\"event.preventDefault();fetch('/api/ai/fau-bot-core/architect/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({max_files:350,triggered_by:'ui'})}).then(()=>location.reload())\">Agente Arquitecto</a>",
        "<a class='btn' href='/api/ai/fau-bot-core/knowledge/load-default'>Cargar corpus EAU/AUA/NCCN</a>",
        "<a class='btn' href='/reporte/alertas'>Volver a ALERTAS</a>",
        "</div>",
        "<div class='box'><h3>Alertas</h3><table><tr><th>ID</th><th>Severidad</th><th>Título</th><th>Categoría</th></tr>",
    ]
    for a in alerts:
        html.append(f"<tr><td>{a['id']}</td><td>{a['severity']}</td><td>{a['title']}</td><td>{a['category']}</td></tr>")
    html.append("</table></div>")
    html.append("<div class='box'><h3>HITL</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Reviewer</th></tr>")
    for s in hitl:
        html.append(f"<tr><td>{s['id']}</td><td>{s['status']}</td><td>{s['title']}</td><td>{s.get('reviewer') or '-'}</td></tr>")
    html.append("</table></div>")
    html.append("<div class='box'><h3>Dev suggestions (CODE_IMPROVEMENT)</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Recomendación</th></tr>")
    for s in dev:
        html.append(f"<tr><td>{s['id']}</td><td>{s['status']}</td><td>{s['title']}</td><td>{s['recommendation']}</td></tr>")
    html.append("</table></div>")
    html.append("<div class='box'><h3>Agente Arquitecto (ARCHITECT_REVIEW)</h3><table><tr><th>ID</th><th>Status</th><th>Título</th><th>Recomendación</th></tr>")
    for s in architect:
        html.append(f"<tr><td>{s['id']}</td><td>{s['status']}</td><td>{s['title']}</td><td>{s['recommendation']}</td></tr>")
    html.append("</table></div>")
    html.append("<div class='box'><h3>Engineering Issues</h3><table><tr><th>ID</th><th>Priority</th><th>Status</th><th>Issue</th><th>Category</th></tr>")
    for i in issues:
        html.append(f"<tr><td>{i['id']}</td><td>{i['priority']}</td><td>{i['status']}</td><td>{i['title']}</td><td>{i['category']}</td></tr>")
    html.append("</table></div>")
    html.append("<div class='box'><h3>PR Suggestions Board</h3><table><tr><th>ID</th><th>Priority</th><th>Status</th><th>Título</th><th>Acciones</th></tr>")
    for p in prs:
        sid = int(p["id"])
        html.append(
            "<tr>"
            f"<td>{sid}</td><td>{p.get('priority') or 'P3'}</td><td>{p.get('status') or 'OPEN'}</td><td>{p.get('title') or ''}</td>"
            "<td>"
            f"<a class='btn' href='#' onclick=\"event.preventDefault();fetch('/api/v1/dev/pr-suggestions/{sid}/spec',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{triggered_by:'ui'}})}}).then(()=>location.reload())\">Spec</a>"
            f"<a class='btn' href='#' onclick=\"event.preventDefault();fetch('/api/v1/dev/pr-suggestions/{sid}/build-patch',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{triggered_by:'ui'}})}}).then(()=>location.reload())\">Patch</a>"
            f"<a class='btn' href='#' onclick=\"event.preventDefault();fetch('/api/v1/dev/pr-suggestions/{sid}/run-tests',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{triggered_by:'ui'}})}}).then(()=>location.reload())\">Tests</a>"
            f"<a class='btn' href='#' onclick=\"event.preventDefault();fetch('/api/v1/dev/pr-suggestions/{sid}/mark-merged',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{reviewer:'ui'}})}}).then(()=>location.reload())\">Merge</a>"
            "</td></tr>"
        )
    html.append("</table></div></body></html>")
    return "".join(html)


@router.get("/api/ai/fau-bot-core/status", response_class=JSONResponse)
def fau_bot_core_status():
    return JSONResponse(content=build_fau_bot_core_boundary_status(SERVICE.status()))


@router.post("/api/ai/fau-bot-core/run", response_class=JSONResponse)
async def fau_bot_core_run(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    result = SERVICE.run_cycle(
        window_days=int(payload.get("window_days") or 30),
        triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
    )
    return JSONResponse(content=result)


@router.get("/api/ai/fau-bot-core/runs", response_class=JSONResponse)
def fau_bot_core_runs(limit: int = 40):
    return JSONResponse(content=SERVICE.list_runs(limit=limit))


@router.get("/api/ai/fau-bot-core/alerts", response_class=JSONResponse)
def fau_bot_core_alerts(limit: int = 120, only_open: bool = False):
    return JSONResponse(content=SERVICE.list_alerts(limit=limit, only_open=only_open))


@router.post("/api/ai/fau-bot-core/knowledge/load-default", response_class=JSONResponse)
def fau_bot_core_load_default_knowledge():
    return JSONResponse(content=SERVICE.load_default_knowledge())


@router.get("/api/ai/fau-bot-core/knowledge/search", response_class=JSONResponse)
def fau_bot_core_knowledge_search(q: str, area: Optional[str] = None, limit: int = 10):
    return JSONResponse(content={"query": q, "results": SERVICE.knowledge_search(q, area=area, limit=limit)})


@router.get("/api/ai/fau-bot-core/knowledge/eval-offline", response_class=JSONResponse)
def fau_bot_core_knowledge_eval_offline(limit: int = 5):
    benchmark = [
        {"query": "litiasis obstructiva con sepsis", "area": "LITIASIS"},
        {"query": "prostatectomia riesgo ecog charlson", "area": "ONCOLOGIA"},
        {"query": "hematuria evaluacion basada en riesgo", "area": "DIAGNOSTICO"},
        {"query": "infeccion urinaria complicada y cultivos", "area": "INFECCIOSO"},
        {"query": "creatinina hemoglobina leucocitos plaquetas", "area": "EPIDEMIOLOGIA"},
    ]
    lim = max(1, min(int(limit or 5), 20))
    hits = 0
    mrr = 0.0
    details = []
    for case in benchmark:
        rows = SERVICE.knowledge_search(case["query"], limit=lim)
        rank = None
        for idx, row in enumerate(rows, start=1):
            if str(row.get("area") or "").upper() == case["area"]:
                rank = idx
                break
        if rank is not None:
            hits += 1
            mrr += 1.0 / float(rank)
        details.append(
            {
                "query": case["query"],
                "expected_area": case["area"],
                "rank": rank,
                "hit": bool(rank is not None),
            }
        )
    total = len(benchmark)
    return JSONResponse(
        content={
            "total": total,
            "limit": lim,
            "hit_rate": round(hits / float(total or 1), 4),
            "mrr": round(mrr / float(total or 1), 4),
            "details": details,
        }
    )


@router.get("/api/ai/fau-bot-core/hitl/suggestions", response_class=JSONResponse)
def fau_bot_core_hitl_list(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_hitl(status=status, limit=limit))


@router.post("/api/ai/fau-bot-core/dev/scan", response_class=JSONResponse)
async def fau_bot_core_dev_scan(request: Request):
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


@router.get("/api/ai/fau-bot-core/dev/suggestions", response_class=JSONResponse)
def fau_bot_core_dev_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_dev_suggestions(status=status, limit=limit))


@router.get("/api/ai/fau-bot-core/architect/rules", response_class=JSONResponse)
def fau_bot_core_architect_rules():
    return JSONResponse(content=SERVICE.architect_rules())


@router.post("/api/ai/fau-bot-core/architect/scan", response_class=JSONResponse)
async def fau_bot_core_architect_scan(request: Request):
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


@router.get("/api/ai/fau-bot-core/architect/suggestions", response_class=JSONResponse)
def fau_bot_core_architect_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_architect_suggestions(status=status, limit=limit))


@router.post("/api/ai/fau-bot-core/hitl/suggestions/{suggestion_id}/status", response_class=JSONResponse)
async def fau_bot_core_hitl_update(suggestion_id: int, request: Request):
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
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse(content=out)


@router.get("/api/ai/fau-bot-core/hitl/audit", response_class=JSONResponse)
def fau_bot_core_hitl_audit(limit: int = 200):
    return JSONResponse(content=SERVICE.list_audit(limit=limit))


@router.post("/api/v1/dev/telemetry/scan", response_class=JSONResponse)
async def devops_ai_telemetry_scan(request: Request):
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


@router.get("/api/v1/dev/telemetry/issues", response_class=JSONResponse)
def devops_ai_telemetry_issues(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_engineering_issues(status=status, limit=limit))


@router.get("/api/v1/dev/telemetry/runtime-kpis", response_class=JSONResponse)
def devops_ai_telemetry_runtime_kpis(window_minutes: int = 60):
    return JSONResponse(content=SERVICE.runtime_kpis(window_minutes=window_minutes))


@router.post("/api/v1/dev/pr-suggestions/generate", response_class=JSONResponse)
async def devops_ai_generate_pr_suggestions(request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    result = SERVICE.generate_pr_suggestions(
        triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        limit=int(payload.get("limit") or 10),
    )
    return JSONResponse(content=result)


@router.get("/api/v1/dev/pr-suggestions", response_class=JSONResponse)
def devops_ai_list_pr_suggestions(status: Optional[str] = None, limit: int = 200):
    return JSONResponse(content=SERVICE.list_pr_suggestions(status=status, limit=limit))


@router.post("/api/v1/dev/pr-suggestions/{suggestion_id}/spec", response_class=JSONResponse)
async def devops_ai_pr_spec(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.build_pr_suggestion_spec(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/v1/dev/pr-suggestions/{suggestion_id}/build-patch", response_class=JSONResponse)
async def devops_ai_pr_build_patch(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.build_patch_from_suggestion(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/v1/dev/codex/patch-from-suggestion/{suggestion_id}", response_class=JSONResponse)
async def devops_ai_codex_patch_alias(suggestion_id: int, request: Request):
    # Alias explícito solicitado para integración futura endpoint->Codex.
    return await devops_ai_pr_build_patch(suggestion_id, request)


@router.post("/api/v1/dev/pr-suggestions/{suggestion_id}/run-tests", response_class=JSONResponse)
async def devops_ai_pr_run_tests(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    commands = payload.get("commands")
    if commands is not None and not isinstance(commands, list):
        raise HTTPException(status_code=400, detail="commands debe ser lista de comandos.")
    try:
        result = SERVICE.run_patch_verification(
            int(suggestion_id),
            triggered_by=str(payload.get("triggered_by") or request.headers.get("X-User") or "manual"),
            commands=commands,
            timeout_sec=int(payload.get("timeout_sec") or 240),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/v1/dev/pr-suggestions/{suggestion_id}/mark-merged", response_class=JSONResponse)
async def devops_ai_pr_mark_merged(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.mark_pr_merged(
            int(suggestion_id),
            reviewer=str(payload.get("reviewer") or request.headers.get("X-User") or "manual"),
            reviewer_comment=str(payload.get("reviewer_comment") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/api/v1/dev/pr-suggestions/{suggestion_id}/status", response_class=JSONResponse)
async def devops_ai_pr_set_status(suggestion_id: int, request: Request):
    payload = {}
    if "application/json" in (request.headers.get("content-type") or "").lower():
        payload = await request.json()
    try:
        result = SERVICE.set_pr_suggestion_status(
            int(suggestion_id),
            status=str(payload.get("status") or "").upper(),
            actor=str(payload.get("actor") or request.headers.get("X-User") or "manual"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)
