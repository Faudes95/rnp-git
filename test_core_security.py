import os

from app.core.config import env_bool, env_int, env_str
from app.core.dependencies import detect_dependencies, is_available
from app.core.logging import redact_dict, redact_value


def test_redact_value_masks_patterns():
    text = "CURP ABCD010203HDFRRS08 NSS 12345678901 correo a@b.com tel 5512345678"
    redacted = redact_value(text)
    assert "***CURP***" in redacted
    assert "***NSS***" in redacted
    assert "***EMAIL***" in redacted
    assert "***TEL***" in redacted


def test_redact_dict_masks_sensitive_keys():
    payload = {
        "curp": "ABCD010203HDFRRS08",
        "nss": "12345678901",
        "nombre": "PACIENTE DEMO",
        "diagnostico": "litiasis",
    }
    redacted = redact_dict(payload)
    assert redacted["curp"] == "***REDACTED***"
    assert redacted["nss"] == "***REDACTED***"
    assert redacted["nombre"] == "***REDACTED***"
    assert redacted["diagnostico"] == "litiasis"


def test_config_env_helpers(monkeypatch):
    monkeypatch.setenv("BOOL_ON", "true")
    monkeypatch.setenv("INT_VAL", "42")
    monkeypatch.setenv("STR_VAL", "abc")
    assert env_bool("BOOL_ON", False) is True
    assert env_int("INT_VAL", 0) == 42
    assert env_str("STR_VAL", "") == "abc"
    assert env_bool("MISSING_BOOL", True) is True
    assert env_int("MISSING_INT", 7) == 7


def test_dependencies_endpoint_helpers_available():
    deps = detect_dependencies(force_refresh=True)
    assert isinstance(deps, dict)
    assert "fastapi" in deps
    assert "available" in deps["fastapi"]
    # fastapi es esencial para el servicio; debe resolverse como disponible.
    assert is_available("fastapi") is True

