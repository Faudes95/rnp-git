import os

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"

from fau_bot_core.service import SERVICE  # noqa: E402


def test_runtime_kpis_profile_dev(monkeypatch):
    monkeypatch.setenv("FAU_CORE_RUNTIME_PROFILE", "dev")
    out = SERVICE.runtime_kpis(window_minutes=30)
    assert out.get("profile") == "dev"
    thresholds = out.get("thresholds") or {}
    assert "api_p95_ms_max" in thresholds
    assert "error_rate_pct_max" in thresholds
    assert "response_time_ms_max" in thresholds


def test_runtime_kpis_profile_override_thresholds(monkeypatch):
    monkeypatch.setenv("FAU_CORE_RUNTIME_PROFILE", "staging")
    monkeypatch.setenv("FAU_CORE_KPI_STAGING_P95_MAX", "777")
    monkeypatch.setenv("FAU_CORE_KPI_STAGING_ERROR_RATE_MAX", "3.25")
    monkeypatch.setenv("FAU_CORE_KPI_STAGING_RESPONSE_MAX", "888")
    out = SERVICE.runtime_kpis(window_minutes=30)
    assert out.get("profile") == "staging"
    thresholds = out.get("thresholds") or {}
    assert float(thresholds.get("api_p95_ms_max") or 0.0) == 777.0
    assert float(thresholds.get("error_rate_pct_max") or 0.0) == 3.25
    assert float(thresholds.get("response_time_ms_max") or 0.0) == 888.0
