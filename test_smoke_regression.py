import os

from fastapi.testclient import TestClient

os.environ["AUTH_ENABLED"] = "false"

from main import app  # noqa: E402

client = TestClient(app)


def test_smoke_menu():
    res = client.get("/")
    assert res.status_code == 200
    assert "Bienvenido a UROMED" in res.text


def test_smoke_consulta_form():
    res = client.get("/consulta")
    assert res.status_code == 200
    assert ("Expediente Clínico Único" in res.text) or ("Consulta Externa" in res.text)


def test_smoke_dependencies_endpoint():
    res = client.get("/admin/dependencies")
    assert res.status_code == 200
    data = res.json()
    assert "dependencies" in data
    assert "fastapi" in data["dependencies"]


def test_smoke_consulta_router_health():
    res = client.get("/api/consulta/health")
    assert res.status_code == 200
    assert res.json().get("status") == "ok"


def test_smoke_reporte_router_resumen():
    res = client.get("/api/reporte/resumen")
    assert res.status_code == 200
    data = res.json()
    assert "total" in data
    assert "fecha" in data


def test_smoke_quirofano_router_health():
    res = client.get("/api/quirofano/health")
    assert res.status_code == 200
    assert res.json().get("status") == "ok"


def test_smoke_fhir_router_health():
    res = client.get("/api/fhir/health")
    assert res.status_code == 200
    assert res.json().get("status") == "ok"


def test_smoke_dashboard_router_health():
    res = client.get("/api/dashboard/health")
    assert res.status_code == 200
    assert res.json().get("status") == "ok"


def test_smoke_quirofano_resumen_programadas():
    res = client.get("/api/quirofano/resumen-programadas")
    assert res.status_code == 200
    data = res.json()
    assert "total" in data
    assert "patologia_counts" in data


def test_smoke_fhir_metadata():
    res = client.get("/api/fhir/metadata")
    assert res.status_code == 200
    data = res.json()
    assert data.get("resourceType") == "CapabilityStatement"


def test_smoke_dashboard_resumen():
    res = client.get("/api/dashboard/resumen")
    assert res.status_code == 200
    data = res.json()
    assert "total_procedimientos" in data


def test_smoke_carga_archivos_form():
    res = client.get("/analisis/cargar-archivos")
    assert res.status_code == 200
    assert "Cargar Archivos" in res.text


def test_smoke_hospitalizacion_alta_form():
    res = client.get("/hospitalizacion/alta")
    assert res.status_code == 200
    assert "ALTA HOSPITALARIA" in res.text


def test_smoke_hospitalizacion_egresos_reporte():
    res = client.get("/hospitalizacion/reporte/egresos")
    assert res.status_code == 200
    assert "Egresos Hospitalarios" in res.text


def test_smoke_api_hospitalizacion_egresos():
    res = client.get("/api/hospitalizacion/egresos")
    assert res.status_code == 200
    data = res.json()
    assert "totales" in data
    assert "rows" in data
