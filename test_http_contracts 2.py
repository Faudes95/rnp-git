import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_ENABLED", "false")
os.environ.setdefault("ASYNC_EMBEDDINGS", "true")
os.environ.setdefault("STARTUP_INTERCONEXION_MODE", "off")
os.environ.setdefault("AI_WARMUP_MODE", "off")

from main import app  # noqa: E402

CONTRACT_PATH = Path("snapshots/http_contracts.json")


def _load_contracts():
    data = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    return data.get("routes") or []


def test_http_contracts_snapshot_exists():
    assert CONTRACT_PATH.exists(), "Falta snapshots/http_contracts.json"


def test_http_contracts():
    client = TestClient(app)
    for contract in _load_contracts():
        path = contract["path"]
        expected_status = int(contract.get("status", 200))
        kind = contract.get("kind", "json")

        response = client.get(path)
        assert response.status_code == expected_status, f"Ruta {path} devolvió {response.status_code}"

        if kind == "json":
            payload = response.json()
            for key in contract.get("keys") or []:
                assert key in payload, f"Ruta {path} no contiene clave {key}"
        elif kind == "html":
            text = response.text
            for marker in contract.get("contains") or []:
                assert marker in text, f"Ruta {path} no contiene marcador '{marker}'"
