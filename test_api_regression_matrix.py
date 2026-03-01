import os

from fastapi.testclient import TestClient

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"
os.environ["FAU_CORE_USE_FALLBACK_EMBEDDING"] = "true"

from main import app  # noqa: E402


client = TestClient(app)


def test_regression_html_alertas():
    res = client.get("/reporte/alertas")
    assert res.status_code == 200
    assert "ALERTAS" in res.text


def test_regression_dashboard_html():
    res = client.get("/dashboard")
    assert res.status_code == 200
    assert "DASHBOARD EPIDEMIOLOGICO" in res.text


def test_regression_fau_bot_legacy_status():
    res = client.get("/api/ai/fau-bot/status")
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("service") in ("fau_BOT", "fau_bot")


def test_regression_fau_bot_core_status():
    res = client.get("/api/ai/fau-bot-core/status")
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("service") == "fau_bot_core"
    assert isinstance(payload.get("connectivity"), dict)


def test_regression_fau_bot_core_collections():
    for path in (
        "/api/ai/fau-bot-core/runs",
        "/api/ai/fau-bot-core/alerts",
        "/api/ai/fau-bot-core/hitl/suggestions",
        "/api/ai/fau-bot-core/hitl/audit",
    ):
        res = client.get(path)
        assert res.status_code == 200, path
        assert isinstance(res.json(), list), path


def test_regression_fau_bot_core_knowledge_search():
    res = client.get("/api/ai/fau-bot-core/knowledge/search", params={"q": "prostata", "limit": 3})
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("query") == "prostata"
    assert isinstance(payload.get("results"), list)


def test_regression_fau_bot_core_knowledge_eval_offline():
    res = client.get("/api/ai/fau-bot-core/knowledge/eval-offline")
    assert res.status_code == 200
    payload = res.json()
    assert isinstance(payload.get("hit_rate"), float)
    assert isinstance(payload.get("mrr"), float)


def test_regression_fau_bot_core_run_cycle():
    res = client.post(
        "/api/ai/fau-bot-core/run",
        json={"window_days": 7, "triggered_by": "pytest-regression"},
    )
    assert res.status_code == 200
    payload = res.json()
    assert isinstance(payload.get("run_id"), int)
    assert isinstance(payload.get("alerts_count"), int)
    assert isinstance(payload.get("module_reports"), dict)


def test_regression_admin_model_cache_status():
    res = client.get("/admin/models/cache")
    assert res.status_code == 200
    payload = res.json()
    assert "cache" in payload


def test_regression_admin_observability_health():
    res = client.get("/admin/observability/health")
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("status") in ("ok", "degraded")
    assert isinstance(payload.get("metrics"), dict)


def test_regression_reporte_stats_legacy_aliases():
    for path in (
        "/api/stats/pendientes-programar",
        "/api/stats/cirugias-realizadas",
        "/api/stats/sangrado",
        "/api/stats/cola-preventiva",
        "/api/stats/hemoderivados",
        "/api/stats/cohortes",
        "/api/stats/urgencias",
        "/api/stats/quirofano/cancelaciones",
    ):
        res = client.get(path)
        assert res.status_code == 200, path
