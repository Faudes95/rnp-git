from __future__ import annotations

from fastapi.testclient import TestClient

from main import app


def test_ui_shell_injected_on_html_routes() -> None:
    client = TestClient(app)
    resp = client.get("/hospitalizacion")
    assert resp.status_code in (200, 401)
    if resp.status_code == 200:
        assert 'id="rnp-ui-shell"' in resp.text
        assert "RNP Navegación Clínica" in resp.text


def test_ui_nav_event_endpoints_reachable() -> None:
    client = TestClient(app)
    payload = {
        "event_type": "PAGE_VIEW",
        "path": "/hospitalizacion",
        "referrer": "",
        "stage": "page_load",
        "context": {"consulta_id": "1", "hospitalizacion_id": "1", "has_nss": True},
    }
    r_post = client.post("/api/v1/ui/nav-event", json=payload)
    assert r_post.status_code in (200, 401, 422)
    r_get = client.get("/api/v1/ui/nav-summary")
    assert r_get.status_code in (200, 401)

