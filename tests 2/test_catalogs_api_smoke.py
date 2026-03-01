from __future__ import annotations

import unittest

try:
    from fastapi.testclient import TestClient
except Exception:  # pragma: no cover
    TestClient = None

from main import app


@unittest.skipUnless(TestClient is not None, "fastapi.testclient/httpx no disponible")
def test_catalogs_api_smoke() -> None:
    client = TestClient(app)
    res = client.get("/api/catalogs")
    assert res.status_code == 200
    payload = res.json()
    assert isinstance(payload, dict)
    assert "items" in payload
    assert "cie11" in [str(x).lower() for x in payload.get("items") or []]

    cie11 = client.get("/api/catalogs/cie11")
    assert cie11.status_code == 200
    body = cie11.json()
    assert int(body.get("count") or 0) >= 1

    validation = client.post("/api/catalogs/cie11/validate", json={"code": "2C82.0"})
    assert validation.status_code == 200
    out = validation.json()
    assert out.get("valid") is True
