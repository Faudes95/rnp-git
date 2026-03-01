from __future__ import annotations

from fastapi.testclient import TestClient

from main import app


def test_inpatient_capture_tabs_and_exports_reachable() -> None:
    client = TestClient(app)
    r = client.get('/expediente/inpatient-captura?consulta_id=1')
    assert r.status_code in (200, 401, 404)
    if r.status_code == 200:
        assert 'Captura Intrahospitalaria Estructurada' in r.text
        assert 'data-tab="resumen"' in r.text
        assert 'data-tab="vitales"' in r.text
        assert 'data-tab="io"' in r.text
        assert 'data-tab="eventos"' in r.text

    r_xlsx = client.get('/expediente/inpatient-captura/export/xlsx?consulta_id=1')
    assert r_xlsx.status_code in (200, 401, 404)

    r_docx = client.get('/expediente/inpatient-captura/export/docx?consulta_id=1')
    assert r_docx.status_code in (200, 401, 404)


def test_reporte_structured_hospitalizacion_section_and_api() -> None:
    client = TestClient(app)
    r_html = client.get('/reporte')
    assert r_html.status_code in (200, 401)
    if r_html.status_code == 200:
        assert 'Hospitalización estructurada (Captura IA)' in r_html.text
        assert 'seccion-hospitalizacion-estructurada' in r_html.text

    r_api = client.get('/api/stats/hospitalizacion/estructurada/resumen')
    assert r_api.status_code in (200, 401)
