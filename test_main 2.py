import os
import re
from datetime import date
from typing import Any, Dict, get_args, get_origin

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Configuración de entorno de pruebas antes de importar la app.
BASE_DIR = os.path.dirname(__file__)
TEST_DB_PATH = os.path.join(BASE_DIR, "test_urologia.db")
TEST_SURGICAL_DB_PATH = os.path.join(BASE_DIR, "test_urologia_quirurgico.db")

os.environ["AUTH_ENABLED"] = "false"
os.environ["ASYNC_EMBEDDINGS"] = "true"
os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"
os.environ["SURGICAL_DATABASE_URL"] = f"sqlite:///{TEST_SURGICAL_DB_PATH}"

import main as main_module  # noqa: E402

from main import (  # noqa: E402
    app,
    Base,
    SurgicalBase,
    ConsultaDB,
    ConsultaCreate,
    calcular_digito_verificador_curp,
    get_db,
    get_surgical_db,
)

# Evita encolado Celery/Redis durante pruebas.
main_module.celery_app = None


TEST_DATABASE_URL = f"sqlite:///{TEST_DB_PATH}"
TEST_SURGICAL_URL = f"sqlite:///{TEST_SURGICAL_DB_PATH}"

engine_test = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_test)

surgical_engine_test = create_engine(TEST_SURGICAL_URL, connect_args={"check_same_thread": False})
SurgicalTestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=surgical_engine_test)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


def override_get_surgical_db():
    db = SurgicalTestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_surgical_db] = override_get_surgical_db

Base.metadata.drop_all(bind=engine_test)
SurgicalBase.metadata.drop_all(bind=surgical_engine_test)
Base.metadata.create_all(bind=engine_test)
SurgicalBase.metadata.create_all(bind=surgical_engine_test)

client = TestClient(app)


def _extract_csrf(html: str) -> str:
    m = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert m, "No se encontró csrf_token en la vista"
    return m.group(1)


def _valid_curp_from_base17(curp17: str) -> str:
    assert len(curp17) == 17
    return f"{curp17}{calcular_digito_verificador_curp(curp17)}"


def _resolve_base_type(annotation: Any):
    if annotation is None:
        return str
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [a for a in get_args(annotation) if a is not type(None)]
    if not args:
        return str
    return args[0]


def _model_fields(model_cls):
    fields = getattr(model_cls, "model_fields", None)
    if fields:
        return fields
    return getattr(model_cls, "__fields__", {})


def _build_full_consulta_payload(
    *,
    curp: str,
    nss: str,
    nombre: str,
    sexo: str,
    diagnostico: str,
    estatus_protocolo: str = "incompleto",
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    fields = _model_fields(ConsultaCreate)

    for field_name, field in fields.items():
        annotation = getattr(field, "annotation", None) or getattr(field, "outer_type_", None)
        base_type = _resolve_base_type(annotation)
        if base_type is int:
            payload[field_name] = 1
        elif base_type is float:
            payload[field_name] = 1.0
        elif base_type is date:
            payload[field_name] = "2000-01-01"
        else:
            payload[field_name] = "NO_APLICA"

    # Overrides para pasar validadores clínicos y formato.
    payload["curp"] = curp
    payload["nss"] = nss
    payload["nombre"] = nombre
    payload["edad"] = 45
    payload["sexo"] = sexo
    payload["diagnostico_principal"] = diagnostico
    payload["estatus_protocolo"] = estatus_protocolo
    payload["fecha_nacimiento"] = "1980-01-01"
    payload["telefono"] = "5512345678"
    payload["email"] = "test@example.com"
    payload["peso"] = 70.0
    payload["talla"] = 170.0
    payload["fc"] = 70
    payload["temp"] = 36.5
    payload["hosp_dias"] = 1
    payload["hosp_dias_uci"] = 0
    payload["cigarros_dia"] = 0
    payload["anios_fumando"] = 0
    # Se omite IMC en el form; el backend lo calcula desde peso/talla.
    payload.pop("imc", None)
    return payload


def _crear_consulta(curp: str, nss: str, nombre: str, sexo: str, diagnostico: str) -> int:
    res_form = client.get("/consulta")
    assert res_form.status_code == 200
    token = _extract_csrf(res_form.text)
    data = _build_full_consulta_payload(
        curp=curp,
        nss=nss,
        nombre=nombre,
        sexo=sexo,
        diagnostico=diagnostico,
        estatus_protocolo="incompleto",
    )
    data["csrf_token"] = token
    res_save = client.post("/guardar_consulta_completa", data=data)
    assert res_save.status_code == 200
    assert "REGISTRO GUARDADO" in res_save.text

    with TestingSessionLocal() as db:
        row = (
            db.query(ConsultaDB)
            .filter(ConsultaDB.curp == curp)
            .order_by(ConsultaDB.id.desc())
            .first()
        )
        assert row is not None
        return int(row.id)


def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert "REGISTRO NACIONAL DE PACIENTES" in response.text


def test_consulta_creacion():
    consulta_id = _crear_consulta(
        curp=_valid_curp_from_base17("ABCD001122HDFRRS0"),
        nss="12345678901",
        nombre="PRUEBA TEST",
        sexo="Masculino",
        diagnostico="litiasis",
    )
    assert consulta_id > 0


def test_quirofano_programar_cirugia():
    consulta_id = _crear_consulta(
        curp=_valid_curp_from_base17("EFGH001122MDFRRS0"),
        nss="10987654321",
        nombre="PACIENTE QUIRURGICO",
        sexo="Femenino",
        diagnostico="litiasis",
    )

    response_form = client.get("/quirofano/nuevo")
    assert response_form.status_code == 200
    token = _extract_csrf(response_form.text)

    data_cirugia = {
        "csrf_token": token,
        "consulta_id": str(consulta_id),
        "nss": "10987654321",
        "agregado_medico": "DR. TEST",
        "nombre_completo": "PACIENTE QUIRURGICO",
        "edad": "45",
        "sexo": "FEMENINO",
        "patologia": "CALCULO DEL URETER",
        "procedimiento_programado": "URETEROLITOTRICIA LASER FLEXIBLE",
        "insumos_solicitados_list": "ENDOURO (INTERMED)",
        "hgz": "HGZ 27",
        "estatus": "PENDIENTE",
        "fecha_programada": "2026-03-15",
    }
    response = client.post("/quirofano/nuevo", data=data_cirugia)
    assert response.status_code == 200
    assert "Cirugía programada exitosamente" in response.text


def test_api_stats_oncology():
    response = client.get("/api/stats/oncology")
    assert response.status_code == 200
    data = response.json()
    assert "total_oncologicos_programados" in data


def test_api_forecast_surgery():
    response = client.get("/api/forecast/surgery?dias=10")
    assert response.status_code in (200, 400)


def teardown_module(_module):
    for path in (TEST_DB_PATH, TEST_SURGICAL_DB_PATH):
        if os.path.exists(path):
            os.unlink(path)
