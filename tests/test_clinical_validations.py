"""
Tests unitarios para validaciones clínicas — Item #10.

Cubre:
- validate_demographics
- validate_vitals
- validate_urology_protocol
- validate_surgical_programming
- validate_all
"""
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.clinical_validations import (
    ValidationResult,
    validate_demographics,
    validate_vitals,
    validate_urology_protocol,
    validate_surgical_programming,
    validate_all,
)


class TestValidationResult:
    def test_empty_is_valid(self):
        r = ValidationResult()
        assert r.is_valid is True
        assert r.to_dict()["valid"] is True
        assert r.to_dict()["total_issues"] == 0

    def test_with_error_is_invalid(self):
        r = ValidationResult()
        r.errors.append("Test error")
        assert r.is_valid is False
        assert r.to_dict()["valid"] is False
        assert r.to_dict()["total_issues"] == 1

    def test_warnings_dont_invalidate(self):
        r = ValidationResult()
        r.warnings.append("Test warning")
        assert r.is_valid is True
        assert r.to_dict()["total_issues"] == 1


class TestValidateDemographics:
    def test_valid_demographics(self):
        data = {
            "nss": "1234567890",
            "nombre": "JUAN PEREZ LOPEZ",
            "edad": 45,
            "sexo": "MASCULINO",
        }
        r = validate_demographics(data)
        assert r.is_valid
        assert len(r.errors) == 0

    def test_invalid_nss_short(self):
        data = {"nss": "12345", "nombre": "JUAN PEREZ", "edad": 45, "sexo": "MASCULINO"}
        r = validate_demographics(data)
        assert not r.is_valid
        assert any("10 dígitos" in e for e in r.errors)

    def test_missing_nss(self):
        data = {"nombre": "JUAN PEREZ", "edad": 45, "sexo": "MASCULINO"}
        r = validate_demographics(data)
        assert not r.is_valid

    def test_invalid_edad(self):
        data = {"nss": "1234567890", "nombre": "JUAN PEREZ", "edad": 150, "sexo": "MASCULINO"}
        r = validate_demographics(data)
        assert not r.is_valid
        assert any("Edad" in e for e in r.errors)

    def test_missing_nombre(self):
        data = {"nss": "1234567890", "nombre": "", "edad": 45, "sexo": "MASCULINO"}
        r = validate_demographics(data)
        assert not r.is_valid

    def test_invalid_sexo(self):
        data = {"nss": "1234567890", "nombre": "JUAN PEREZ", "edad": 45, "sexo": "OTRO"}
        r = validate_demographics(data)
        assert not r.is_valid
        assert any("Sexo" in e for e in r.errors)

    def test_nombre_completo_alias(self):
        data = {"nss": "1234567890", "nombre_completo": "MARIA GARCIA HERNANDEZ", "edad": 30, "sexo": "FEMENINO"}
        r = validate_demographics(data)
        assert r.is_valid


class TestValidateVitals:
    def test_normal_vitals(self):
        data = {"peso": 75, "talla": 1.70, "ta": "120/80", "fc": 72, "temp": 36.5}
        r = validate_vitals(data)
        assert r.is_valid
        assert len(r.warnings) == 0

    def test_extreme_peso(self):
        data = {"peso": 350}
        r = validate_vitals(data)
        assert any("Peso" in w for w in r.warnings)

    def test_low_peso(self):
        data = {"peso": 15}
        r = validate_vitals(data)
        assert any("Peso" in w for w in r.warnings)

    def test_ta_sistolica_lt_diastolica(self):
        data = {"ta": "60/120"}
        r = validate_vitals(data)
        assert not r.is_valid
        assert any("sistólica" in e.lower() for e in r.errors)

    def test_bradicardia(self):
        data = {"fc": 25}
        r = validate_vitals(data)
        assert any("Bradicardia" in w for w in r.warnings)

    def test_taquicardia(self):
        data = {"fc": 200}
        r = validate_vitals(data)
        assert any("Taquicardia" in w for w in r.warnings)

    def test_hipotermia(self):
        data = {"temp": 33}
        r = validate_vitals(data)
        assert any("Hipotermia" in w for w in r.warnings)

    def test_fiebre(self):
        data = {"temp": 41}
        r = validate_vitals(data)
        assert any("Fiebre" in w for w in r.warnings)

    def test_talla_cm_conversion(self):
        data = {"talla": 170}  # Should be converted to 1.70m
        r = validate_vitals(data)
        assert r.is_valid  # 1.70m is within range

    def test_imc_extremes(self):
        data = {"imc": 10}
        r = validate_vitals(data)
        assert any("IMC" in w and "bajo" in w for w in r.warnings)


class TestValidateUrologyProtocol:
    def test_cancer_prostata_valid(self):
        data = {
            "diagnostico_principal": "CANCER DE PROSTATA",
            "pros_ape_act": 8.5,
            "pros_gleason": "3+4=7",
            "pros_ecog": 1,
            "pros_tnm": "T2N0M0",
        }
        r = validate_urology_protocol(data)
        assert r.is_valid

    def test_cancer_prostata_negative_ape(self):
        data = {"diagnostico_principal": "CANCER DE PROSTATA", "pros_ape_act": -1}
        r = validate_urology_protocol(data)
        assert not r.is_valid
        assert any("APE" in e for e in r.errors)

    def test_cancer_prostata_high_ecog(self):
        data = {"diagnostico_principal": "CANCER DE PROSTATA", "pros_ecog": 4}
        r = validate_urology_protocol(data)
        assert any("ECOG" in w for w in r.warnings)

    def test_litiasis_high_uh(self):
        data = {"diagnostico_principal": "CALCULO DEL RIÑON", "lit_densidad_uh": 5000}
        r = validate_urology_protocol(data)
        assert any("UH" in w for w in r.warnings)

    def test_hpb_ipss_out_of_range(self):
        data = {"diagnostico_principal": "HIPERPLASIA PROSTATICA BENIGNA", "hpb_ipss": 40}
        r = validate_urology_protocol(data)
        assert not r.is_valid
        assert any("IPSS" in e for e in r.errors)

    def test_hpb_valid_ipss(self):
        data = {"diagnostico_principal": "HPB", "hpb_ipss": 15}
        r = validate_urology_protocol(data)
        assert r.is_valid

    def test_hpb_severe_ipss(self):
        data = {"diagnostico_principal": "HPB", "hpb_ipss": 25}
        r = validate_urology_protocol(data)
        assert any("severa" in i for i in r.info)


class TestValidateSurgicalProgramming:
    def test_normal_surgical(self):
        data = {"edad": 60, "charlson": 2, "asa": "ASA 2"}
        r = validate_surgical_programming(data)
        assert r.is_valid

    def test_elderly_patient(self):
        data = {"edad": 90}
        r = validate_surgical_programming(data)
        assert any("edad" in w for w in r.warnings)

    def test_high_charlson(self):
        data = {"charlson": 7}
        r = validate_surgical_programming(data)
        assert any("Charlson" in w for w in r.warnings)

    def test_high_asa(self):
        data = {"asa": "ASA 4"}
        r = validate_surgical_programming(data)
        assert any("ASA" in w for w in r.warnings)

    def test_hemoderivados_sin_unidades(self):
        data = {"solicita_hemoderivados": "SI"}
        r = validate_surgical_programming(data)
        assert not r.is_valid
        assert any("hemoderivados" in e.lower() for e in r.errors)


class TestValidateAll:
    def test_consulta_context(self):
        data = {
            "nss": "1234567890",
            "nombre": "JUAN PEREZ LOPEZ",
            "edad": 45,
            "sexo": "MASCULINO",
        }
        r = validate_all(data, context="consulta")
        assert r.is_valid

    def test_quirofano_includes_surgical(self):
        data = {
            "nss": "1234567890",
            "nombre": "JUAN PEREZ LOPEZ",
            "edad": 92,
            "sexo": "MASCULINO",
        }
        r = validate_all(data, context="quirofano")
        assert any("edad" in w for w in r.warnings)

    def test_consulta_excludes_surgical(self):
        data = {
            "nss": "1234567890",
            "nombre": "JUAN PEREZ LOPEZ",
            "edad": 92,
            "sexo": "MASCULINO",
        }
        r = validate_all(data, context="consulta")
        # Should NOT have surgical warnings in consulta context
        assert not any("riesgo quirúrgico" in w for w in r.warnings)

    def test_with_vitals(self):
        data = {
            "nss": "1234567890",
            "nombre": "JUAN PEREZ LOPEZ",
            "edad": 45,
            "sexo": "MASCULINO",
            "peso": 75,
            "ta": "120/80",
            "fc": 72,
        }
        r = validate_all(data, context="consulta")
        assert r.is_valid

    def test_invalid_everything(self):
        data = {
            "nss": "123",
            "nombre": "AB",
            "edad": 200,
            "sexo": "X",
            "peso": 500,
            "ta": "50/200",
        }
        r = validate_all(data, context="consulta")
        assert not r.is_valid
        assert len(r.errors) >= 3


class TestSharedHelpers:
    def test_safe_text(self):
        from app.core.shared_helpers import safe_text
        assert safe_text(None) == ""
        assert safe_text(123) == "123"
        assert safe_text("  hello  ") == "hello"

    def test_safe_int(self):
        from app.core.shared_helpers import safe_int
        assert safe_int("42") == 42
        assert safe_int("abc") is None
        assert safe_int(None) is None
        assert safe_int(3.7) == 3

    def test_safe_float(self):
        from app.core.shared_helpers import safe_float
        assert safe_float("3.14") == pytest.approx(3.14)
        assert safe_float("abc") is None
        assert safe_float(None) is None

    def test_normalize_nss(self):
        from app.core.shared_helpers import normalize_nss
        assert normalize_nss("  1234567890  ") == "1234567890"
        assert normalize_nss("12345") == "12345"

    def test_normalize_upper(self):
        from app.core.shared_helpers import normalize_upper
        assert normalize_upper("hello world") == "HELLO WORLD"
        assert normalize_upper(None) == ""


class TestRBAC:
    def test_role_hierarchy(self):
        from app.core.rbac import ROLE_HIERARCHY
        assert ROLE_HIERARCHY["admin"] > ROLE_HIERARCHY["jefe_servicio"]
        assert ROLE_HIERARCHY["jefe_servicio"] > ROLE_HIERARCHY["medico_adscrito"]
        assert ROLE_HIERARCHY["medico_adscrito"] > ROLE_HIERARCHY["residente"]
        assert ROLE_HIERARCHY["residente"] > ROLE_HIERARCHY["enfermeria"]
        assert ROLE_HIERARCHY["enfermeria"] > ROLE_HIERARCHY["capturista"]
        assert ROLE_HIERARCHY["capturista"] > ROLE_HIERARCHY["readonly"]

    def test_module_permissions(self):
        from app.core.rbac import MODULE_PERMISSIONS
        assert "admin" in MODULE_PERMISSIONS["gobernanza"]
        assert "jefe_servicio" in MODULE_PERMISSIONS["gobernanza"]
        assert "residente" not in MODULE_PERMISSIONS["gobernanza"]
        assert "enfermeria" in MODULE_PERMISSIONS["enfermeria_write"]
        assert "readonly" not in MODULE_PERMISSIONS["firma_write"]


class TestGovernanceModels:
    def test_tables_exist(self):
        from app.models.governance_models import (
            GOV_USERS, GOV_SESSIONS, GOV_ACCESS_LOG,
            GOV_CONSENT_FORMS, GOV_CLINICAL_ALERTS,
        )
        assert GOV_USERS.name == "gov_users"
        assert GOV_SESSIONS.name == "gov_sessions"
        assert GOV_ACCESS_LOG.name == "gov_access_log"
        assert GOV_CONSENT_FORMS.name == "gov_consent_forms"
        assert GOV_CLINICAL_ALERTS.name == "gov_clinical_alerts"

    def test_gov_users_columns(self):
        from app.models.governance_models import GOV_USERS
        col_names = {c.name for c in GOV_USERS.columns}
        assert "username" in col_names
        assert "password_hash" in col_names
        assert "rol" in col_names
        assert "activo" in col_names

    def test_gov_access_log_columns(self):
        from app.models.governance_models import GOV_ACCESS_LOG
        col_names = {c.name for c in GOV_ACCESS_LOG.columns}
        assert "method" in col_names
        assert "path" in col_names
        assert "status_code" in col_names
        assert "username" in col_names
        assert "duracion_ms" in col_names


class TestFirmaModel:
    def test_firma_table(self):
        from app.api.firma_electronica import FIRMAS_ELECTRONICAS
        assert FIRMAS_ELECTRONICAS.name == "firmas_electronicas"
        col_names = {c.name for c in FIRMAS_ELECTRONICAS.columns}
        assert "firma_hash" in col_names
        assert "firma_imagen_base64" in col_names
        assert "firmante_username" in col_names
        assert "nss" in col_names


class TestEnfermeriaModel:
    def test_hoja_enfermeria_table(self):
        from app.api.enfermeria import HOJA_ENFERMERIA
        assert HOJA_ENFERMERIA.name == "hoja_enfermeria"
        col_names = {c.name for c in HOJA_ENFERMERIA.columns}
        assert "nss" in col_names
        assert "turno" in col_names
        assert "ta_sistolica" in col_names
        assert "balance_hidrico_ml" in col_names
        assert "medicamentos_json" in col_names
        assert "enfermera_nombre" in col_names


class TestNotificacionesModel:
    def test_notificaciones_table(self):
        from app.api.notificaciones import NOTIFICACIONES
        assert NOTIFICACIONES.name == "notificaciones"
        col_names = {c.name for c in NOTIFICACIONES.columns}
        assert "titulo" in col_names
        assert "tipo" in col_names
        assert "severidad" in col_names
        assert "leida" in col_names
        assert "destinatario_username" in col_names


class TestInterconsultasModel:
    def test_interconsultas_table(self):
        from app.api.interconsultas import INTERCONSULTAS, REFERENCIAS
        assert INTERCONSULTAS.name == "interconsultas"
        assert REFERENCIAS.name == "referencias_contrarreferencias"
        ic_cols = {c.name for c in INTERCONSULTAS.columns}
        assert "nss" in ic_cols
        assert "servicio_destino" in ic_cols
        assert "motivo" in ic_cols
        assert "estatus" in ic_cols
        ref_cols = {c.name for c in REFERENCIAS.columns}
        assert "tipo" in ref_cols
        assert "unidad_destino" in ref_cols


class TestAuditMiddleware:
    def test_detect_module(self):
        from app.core.audit_middleware import _detect_module
        assert _detect_module("/consulta/metadata") == "CONSULTA"
        assert _detect_module("/hospitalizacion/nuevo") == "HOSPITALIZACION"
        assert _detect_module("/quirofano/123") == "QUIROFANO"
        assert _detect_module("/api/governance/users") == "GOBERNANZA"
        assert _detect_module("/interconsultas") == "INTERCONSULTAS"
        assert _detect_module("/expediente/integrado") == "EXPEDIENTE"
        assert _detect_module("/") == "GENERAL"

    def test_detect_operation(self):
        from app.core.audit_middleware import _detect_operation
        assert _detect_operation("GET", "/api/test") == "READ"
        assert _detect_operation("POST", "/api/test") == "CREATE"
        assert _detect_operation("PUT", "/api/test") == "UPDATE"
        assert _detect_operation("DELETE", "/api/test") == "DELETE"

    def test_extract_nss(self):
        from app.core.audit_middleware import _extract_nss_from_path
        assert _extract_nss_from_path("/api/patient/1234567890/data") == "1234567890"
        assert _extract_nss_from_path("/api/test") == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
