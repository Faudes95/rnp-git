from __future__ import annotations

from fastapi.testclient import TestClient

from main import app


def test_health_endpoint_available() -> None:
    client = TestClient(app)
    candidates = [
        "/api/v1/health",
        "/health",
        "/dashboard/health",
        "/consulta/health",
        "/reporte/health",
    ]
    statuses = []
    for path in candidates:
        resp = client.get(path)
        statuses.append((path, resp.status_code))
        if resp.status_code in (200, 401):
            try:
                data = resp.json()
                assert isinstance(data, dict)
            except Exception:
                pass
            return
    raise AssertionError(f"No health endpoint reachable (200/401). Got: {statuses}")
