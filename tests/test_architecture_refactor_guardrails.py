from __future__ import annotations

import json
import pathlib
import re
import unittest

from app.core.boot_profile import list_known_boot_profiles
from app.core.profile_manifest import get_profile_manifest, list_profile_manifests
from app.routers.module_catalog import ALL_ROUTER_MODULE_IDS, ROUTER_MODULE_INDEX


ROOT = pathlib.Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = ROOT / "snapshots" / "architecture_import_guardrails.json"

FORBIDDEN_IMPORT_PATTERNS = {
    "main_proxy": re.compile(r"from\s+app\.core\.app_context\s+import\s+main_proxy|import\s+app\.core\.app_context"),
    "main_full": re.compile(r"from\s+main_full\s+import|import\s+main_full\b"),
}

STRICT_LAYER_ROOTS = (
    ROOT / "app" / "domain",
    ROOT / "app" / "infra",
    ROOT / "app" / "routers",
    ROOT / "app" / "integrations",
)


def _iter_python_files(base: pathlib.Path):
    if not base.exists():
        return
    for path in base.rglob("*.py"):
        if "__pycache__" not in path.parts:
            yield path


def _scan_forbidden_imports() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {key: [] for key in FORBIDDEN_IMPORT_PATTERNS}
    for path in _iter_python_files(ROOT / "app"):
        relpath = str(path.relative_to(ROOT))
        text = path.read_text(encoding="utf-8")
        for key, pattern in FORBIDDEN_IMPORT_PATTERNS.items():
            if pattern.search(text):
                found[key].append(relpath)
    for key in found:
        found[key].sort()
    return found


class ArchitectureRefactorGuardrailsTest(unittest.TestCase):
    def test_known_boot_profiles_have_manifests(self):
        known_profiles = set(list_known_boot_profiles())
        manifest_profiles = {manifest.profile for manifest in list_profile_manifests()}
        self.assertEqual(known_profiles, manifest_profiles)

    def test_manifests_only_reference_known_router_modules(self):
        known_modules = set(ALL_ROUTER_MODULE_IDS)
        for manifest in list_profile_manifests():
            unknown = set(manifest.active_modules) - known_modules
            self.assertFalse(unknown, f"Perfil {manifest.profile} referencia módulos desconocidos: {sorted(unknown)}")
            self.assertTrue(manifest.active_modules, f"Perfil {manifest.profile} quedó sin módulos activos")

    def test_internal_profiles_route_through_main_full_for_now(self):
        for profile in (
            "consulta",
            "hospitalizacion",
            "quirofano",
            "expediente",
            "investigacion",
            "jefatura_urologia",
            "residentes_urologia",
            "pilot_urologia",
        ):
            manifest = get_profile_manifest(profile)
            self.assertEqual(manifest.entrypoint_module, "main_full")

    def test_first_wave_domains_are_registered_via_router_wrappers(self):
        self.assertEqual(
            ROUTER_MODULE_INDEX["jefaturas"].specs[0].module_name,
            "app.routers.jefaturas",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["jefatura_quirofano"].specs[0].module_name,
            "app.routers.quirofano",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["quirofano"].specs[0].module_name,
            "app.routers.quirofano",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["urgencias"].specs[0].module_name,
            "app.routers.quirofano",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["hospitalizacion"].specs[0].module_name,
            "app.routers.hospitalizacion",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["inpatient_notes"].specs[0].module_name,
            "app.routers.hospitalizacion",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["ward_smart"].specs[0].module_name,
            "app.routers.hospitalizacion",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["consulta"].specs[0].module_name,
            "app.routers.consulta",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["interconsultas"].specs[0].module_name,
            "app.routers.consulta",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["expediente_plus"].specs[0].module_name,
            "app.routers.expediente",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["ehr_integrado"].specs[0].module_name,
            "app.routers.expediente",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["patient_autofill"].specs[0].module_name,
            "app.routers.expediente",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["fhir"].specs[0].module_name,
            "app.routers.fhir",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["ai_fau_bot"].specs[0].module_name,
            "app.routers.ia",
        )
        self.assertEqual(
            ROUTER_MODULE_INDEX["ai_fau_bot_core"].specs[0].module_name,
            "app.routers.ia",
        )

    def test_legacy_composition_imports_do_not_grow(self):
        expected = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        current = _scan_forbidden_imports()
        for key, expected_paths in expected.items():
            unexpected = sorted(set(current.get(key, [])) - set(expected_paths))
            self.assertFalse(
                unexpected,
                f"Se detectaron nuevos acoplamientos hacia {key}: {unexpected}",
            )

    def test_new_architecture_layers_do_not_depend_on_legacy_composition(self):
        offenders: list[str] = []
        for base in STRICT_LAYER_ROOTS:
            for path in _iter_python_files(base):
                text = path.read_text(encoding="utf-8")
                for key, pattern in FORBIDDEN_IMPORT_PATTERNS.items():
                    if pattern.search(text):
                        offenders.append(f"{path.relative_to(ROOT)} -> {key}")
        self.assertFalse(offenders, f"Las capas nuevas no deben depender del composition root legacy: {offenders}")


if __name__ == "__main__":
    unittest.main()
