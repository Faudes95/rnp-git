import os

from fastapi.testclient import TestClient

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"
os.environ["FAU_CORE_USE_FALLBACK_EMBEDDING"] = "true"

from main import app  # noqa: E402


client = TestClient(app)


def test_regression_hospitalizacion_guardia_html_snapshot_key():
    res = client.get("/hospitalizacion/guardia")
    assert res.status_code == 200
    assert "Hospitalizacion / Guardia" in res.text
    assert "--imss-verde" in res.text


def test_regression_hospitalizacion_importar_html():
    res = client.get("/hospitalizacion/importar")
    assert res.status_code == 200
    assert "Importar" in res.text


def test_regression_quirofano_urgencias_html_snapshot_key():
    res = client.get("/quirofano/urgencias")
    assert res.status_code == 200
    assert "Cirugía de Urgencia" in res.text
    assert "realizar solicitud de cirugía de urgencia" in res.text.lower()


def test_regression_reporte_pendientes_programar_html_snapshot_key():
    res = client.get("/reporte/pendientes-programar")
    assert res.status_code == 200
    assert "Pendientes de Programar" in res.text
    assert "Reporte Estadístico" in res.text


def test_regression_api_hospitalizacion_guardia_reporte_json():
    res = client.get("/api/hospitalizacion/guardia/reporte")
    assert res.status_code == 200
    payload = res.json()
    assert isinstance(payload, dict)
    assert "fecha_desde" in payload
    assert "fecha_hasta" in payload


def test_regression_api_stats_urgencias_resumen_json():
    res = client.get("/api/stats/urgencias/resumen")
    assert res.status_code == 200
    payload = res.json()
    assert "total_urgencias_programadas" in payload
    assert "oncologicos" in payload
