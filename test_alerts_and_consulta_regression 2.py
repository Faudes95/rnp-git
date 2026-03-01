import os

from fastapi.testclient import TestClient

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"
os.environ["FAU_CORE_USE_FALLBACK_EMBEDDING"] = "true"

from main import app  # noqa: E402


client = TestClient(app)


def test_reporte_alertas_unified_panel_html_snapshot_key():
    res = client.get("/reporte/alertas")
    assert res.status_code == 200
    assert "Panel Central Unificado" in res.text
    assert "Aplicar filtros" in res.text
    assert "PR suggestions (DevOps)" in res.text
    assert "Runtime P95 ms (60m)" in res.text


def test_reporte_alertas_unified_filters_html_snapshot_key():
    res = client.get("/reporte/alertas?severity=ALTA&module=QUIROFANO&status=OPEN&limit=150")
    assert res.status_code == 200
    assert "Panel Central Unificado" in res.text
    assert "QUIROFANO" in res.text


def test_devops_pr_and_telemetry_endpoints_online():
    issues = client.get("/api/v1/dev/telemetry/issues")
    assert issues.status_code == 200
    payload_issues = issues.json()
    assert isinstance(payload_issues, list)

    runtime = client.get("/api/v1/dev/telemetry/runtime-kpis")
    assert runtime.status_code == 200
    payload_runtime = runtime.json()
    assert "profile" in payload_runtime
    assert "thresholds" in payload_runtime
    assert "latency" in payload_runtime
    assert "p95_ms" in (payload_runtime.get("latency") or {})

    prs = client.get("/api/v1/dev/pr-suggestions")
    assert prs.status_code == 200
    payload_prs = prs.json()
    assert isinstance(payload_prs, list)


def test_consulta_externa_submodules_html_snapshot_keys():
    home = client.get("/consulta_externa")
    assert home.status_code == 200
    assert "Consulta Externa - Urología" in home.text

    uro = client.get("/consulta_externa/uroendoscopia")
    assert uro.status_code == 200
    assert "Uroendoscopia - Consulta Externa" in uro.text

    leoch = client.get("/consulta_externa/leoch")
    assert leoch.status_code == 200
    assert "LEOCH - Consulta Externa" in leoch.text

    recetas = client.get("/consulta_externa/recetas")
    assert recetas.status_code == 200
    assert "Recetas - Consulta Externa" in recetas.text


def test_inpatient_notes_patient_routes_nss_10_validation():
    ok = client.get("/api/v1/hospitalization/patients/0000000001/episodes")
    assert ok.status_code == 200
    body_ok = ok.json()
    assert body_ok.get("patient_id") == "0000000001"

    bad = client.get("/api/v1/hospitalization/patients/000000001/episodes")
    assert bad.status_code == 422
