from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class ArchitectRule:
    rule_id: str
    tier: str
    severity: str
    title: str
    category: str
    languages: Tuple[str, ...]
    recommendation: str
    pattern: Optional[str] = None
    mode: str = "regex"  # regex | threshold
    threshold: Optional[int] = None
    field: Optional[str] = None


class ArchitectAgent:
    """Agente de calidad/safety para revisión técnica aditiva (HITL)."""

    LANGUAGE_BY_SUFFIX = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".java": "java",
        ".go": "go",
        ".sql": "sql",
        ".html": "html",
        ".css": "css",
        ".md": "markdown",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".toml": "toml",
        ".ini": "ini",
        ".sh": "shell",
    }

    EXCLUDED_DIRS = {
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        "venv",
        ".venv",
        "dist",
        "build",
    }

    RULES: Tuple[ArchitectRule, ...] = (
        # P0 - críticos de seguridad/estabilidad
        ArchitectRule(
            rule_id="P0-001",
            tier="P0",
            severity="CRITICAL",
            title="Uso de eval() con entrada potencialmente no confiable",
            category="SEGURIDAD",
            languages=("python", "javascript", "typescript"),
            pattern=r"\beval\s*\(",
            recommendation="Eliminar eval() o restringir con parser seguro/whitelist.",
        ),
        ArchitectRule(
            rule_id="P0-002",
            tier="P0",
            severity="CRITICAL",
            title="Excepción silenciosa con pass",
            category="FIABILIDAD",
            languages=("python",),
            pattern=r"except[^\n:]*:\s*(?:#.*\n\s*)*pass\b",
            recommendation="Reemplazar `pass` por logging estructurado y manejo explícito.",
        ),
        ArchitectRule(
            rule_id="P0-003",
            tier="P0",
            severity="CRITICAL",
            title="Captura bare except",
            category="FIABILIDAD",
            languages=("python",),
            pattern=r"(?m)^\s*except\s*:\s*$",
            recommendation="Capturar excepciones específicas y registrar contexto.",
        ),
        ArchitectRule(
            rule_id="P0-004",
            tier="P0",
            severity="ALTA",
            title="Posible SQL concatenado no parametrizado",
            category="SEGURIDAD",
            languages=("python", "javascript", "typescript"),
            pattern=r"(SELECT|INSERT|UPDATE|DELETE)[^\\n]*(\+|%s|f\")",
            recommendation="Usar consultas parametrizadas/ORM; evitar concatenación de SQL.",
        ),
        ArchitectRule(
            rule_id="P0-005",
            tier="P0",
            severity="ALTA",
            title="Posible secreto hardcodeado",
            category="SEGURIDAD",
            languages=("python", "javascript", "typescript", "yaml", "toml", "ini"),
            pattern=r"(API_KEY|SECRET|PASSWORD|TOKEN)\s*[:=]\s*[\"'][^\"']+[\"']",
            recommendation="Mover secretos a variables de entorno/secret manager y rotar credenciales.",
        ),
        # P1 - alta prioridad técnica
        ArchitectRule(
            rule_id="P1-001",
            tier="P1",
            severity="ALTA",
            title="Uso de except Exception genérico",
            category="FIABILIDAD",
            languages=("python",),
            pattern=r"except\s+Exception(?:\s+as\s+\w+)?\s*:",
            recommendation="Reducir catch genérico y clasificar errores por tipo.",
        ),
        ArchitectRule(
            rule_id="P1-002",
            tier="P1",
            severity="MEDIA",
            title="URL local hardcodeada",
            category="CONFIG",
            languages=("python", "javascript", "typescript", "yaml", "toml", "ini", "markdown"),
            pattern=r"https?://(?:127\.0\.0\.1|localhost)",
            recommendation="Externalizar endpoints por ambiente en configuración.",
        ),
        ArchitectRule(
            rule_id="P1-003",
            tier="P1",
            severity="MEDIA",
            title="Ruta absoluta hardcodeada",
            category="CONFIG",
            languages=("python", "javascript", "typescript", "yaml", "toml", "ini", "shell"),
            pattern=r"/Users/|[A-Za-z]:\\\\",
            recommendation="Mover rutas a configuración/env para portabilidad.",
        ),
        ArchitectRule(
            rule_id="P1-004",
            tier="P1",
            severity="MEDIA",
            title="Uso de print en código de aplicación",
            category="OBSERVABILIDAD",
            languages=("python",),
            pattern=r"(?m)^\s*print\(",
            recommendation="Migrar a logging estructurado con redacción PHI/PII.",
        ),
        ArchitectRule(
            rule_id="P1-005",
            tier="P1",
            severity="MEDIA",
            title="Mutable default argument en Python",
            category="CORRECTITUD",
            languages=("python",),
            pattern=r"def\s+\w+\([^)]*=\s*(\[\]|\{\}|set\(\))",
            recommendation="Usar `None` y crear el contenedor dentro de la función.",
        ),
        ArchitectRule(
            rule_id="P1-006",
            tier="P1",
            severity="MEDIA",
            title="Comparación directa de flotantes con ==",
            category="CORRECTITUD",
            languages=("python", "javascript", "typescript", "java", "go"),
            pattern=r"==\s*\d+\.\d+|\d+\.\d+\s*==",
            recommendation="Usar tolerancia/epsilon para comparaciones de float.",
        ),
        ArchitectRule(
            rule_id="P1-007",
            tier="P1",
            severity="MEDIA",
            title="Posible falta de context manager en open()",
            category="RECURSOS",
            languages=("python",),
            pattern=r"(?m)^\s*(?!with\s)open\s*\(",
            recommendation="Usar `with open(...)` para liberar recursos correctamente.",
        ),
        ArchitectRule(
            rule_id="P1-008",
            tier="P1",
            severity="MEDIA",
            title="Archivo demasiado grande (deuda técnica)",
            category="ARQUITECTURA",
            languages=("python", "javascript", "typescript", "java", "go", "html"),
            mode="threshold",
            field="line_count",
            threshold=1200,
            recommendation="Extraer módulos por dominio sin alterar rutas/contratos actuales.",
        ),
        ArchitectRule(
            rule_id="P1-009",
            tier="P1",
            severity="MEDIA",
            title="TODO/FIXME pendiente",
            category="MANTENIBILIDAD",
            languages=("python", "javascript", "typescript", "java", "go", "sql", "html", "markdown"),
            pattern=r"\b(TODO|FIXME|XXX|HACK)\b",
            recommendation="Convertir deuda en tareas trazables con prioridad clínica-operativa.",
        ),
        # P2 - legibilidad/consistencia
        ArchitectRule(
            rule_id="P2-001",
            tier="P2",
            severity="BAJA",
            title="Nombres de variable poco descriptivos",
            category="LEGIBILIDAD",
            languages=("python",),
            pattern=r"(?m)^\s*([abijxyz]|tmp)\s*=",
            recommendation="Usar nombres semánticos para reducir errores de mantenimiento.",
        ),
        ArchitectRule(
            rule_id="P2-002",
            tier="P2",
            severity="BAJA",
            title="Posible número mágico hardcodeado",
            category="LEGIBILIDAD",
            languages=("python", "javascript", "typescript", "java", "go"),
            pattern=r"(?<![A-Za-z_])(?:3650|9999|86400|100000)(?![A-Za-z_])",
            recommendation="Reemplazar por constantes nombradas con contexto de dominio.",
        ),
        ArchitectRule(
            rule_id="P2-003",
            tier="P2",
            severity="BAJA",
            title="Comentario redundante",
            category="LEGIBILIDAD",
            languages=("python", "javascript", "typescript", "java", "go"),
            pattern=r"(?i)#\s*(incrementa|decrementa|asigna)\b|//\s*(incrementa|decrementa|asigna)\b",
            recommendation="Conservar comentarios explicativos de intención, no triviales.",
        ),
    )

    def list_rules(self) -> List[Dict[str, Any]]:
        return [
            {
                "rule_id": r.rule_id,
                "tier": r.tier,
                "severity": r.severity,
                "title": r.title,
                "category": r.category,
                "languages": list(r.languages),
                "mode": r.mode,
                "recommendation": r.recommendation,
            }
            for r in self.RULES
        ]

    def scan(
        self,
        *,
        source_root: str,
        max_files: int = 350,
        max_file_size_kb: int = 900,
    ) -> Dict[str, Any]:
        root = Path(source_root).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise ValueError(f"source_root inválido: {root}")

        max_files = max(20, min(int(max_files or 350), 5000))
        max_bytes = max(64 * 1024, min(int(max_file_size_kb or 900) * 1024, 6 * 1024 * 1024))

        findings: List[Dict[str, Any]] = []
        scanned_files = 0
        skipped_large = 0
        skipped_decode = 0
        files_with_findings = 0

        for path in self._iter_files(root):
            if scanned_files >= max_files:
                break
            try:
                size = int(path.stat().st_size or 0)
            except Exception:
                continue
            if size > max_bytes:
                skipped_large += 1
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                skipped_decode += 1
                continue
            except Exception:
                try:
                    content = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    skipped_decode += 1
                    continue

            scanned_files += 1
            language = self._detect_language(path)
            line_count = content.count("\n") + 1 if content else 0

            file_findings = self._scan_file(path, language, content, line_count)
            if file_findings:
                files_with_findings += 1
                findings.extend(file_findings)

        by_tier = {"P0": 0, "P1": 0, "P2": 0}
        by_severity: Dict[str, int] = {}
        for f in findings:
            by_tier[str(f["tier"])] = int(by_tier.get(str(f["tier"]), 0)) + int(f["count"])
            sev = str(f["severity"])
            by_severity[sev] = int(by_severity.get(sev, 0)) + int(f["count"])

        findings_sorted = sorted(
            findings,
            key=lambda x: (
                {"P0": 0, "P1": 1, "P2": 2}.get(str(x["tier"]), 9),
                -int(x["count"]),
                str(x["file"]),
            ),
        )

        return {
            "source_root": str(root),
            "scanned_files": scanned_files,
            "files_with_findings": files_with_findings,
            "skipped_large_files": skipped_large,
            "skipped_decode_files": skipped_decode,
            "catalog_rules_total": len(self.RULES),
            "summary": {
                "by_tier": by_tier,
                "by_severity": by_severity,
                "total_findings": int(sum(int(f["count"]) for f in findings)),
                "unique_rule_hits": len({str(f["rule_id"]) for f in findings}),
            },
            "findings": findings_sorted,
        }

    def _scan_file(
        self,
        path: Path,
        language: str,
        content: str,
        line_count: int,
    ) -> List[Dict[str, Any]]:
        findings: List[Dict[str, Any]] = []
        rel_path = self._safe_rel_path(path)

        for rule in self.RULES:
            if language not in rule.languages:
                continue
            if rule.mode == "regex" and rule.pattern:
                compiled = re.compile(rule.pattern, flags=re.IGNORECASE | re.MULTILINE)
                matches = list(compiled.finditer(content))
                if not matches:
                    continue
                lines = sorted({self._line_for_offset(content, m.start()) for m in matches})[:5]
                findings.append(
                    {
                        "rule_id": rule.rule_id,
                        "tier": rule.tier,
                        "severity": rule.severity,
                        "title": rule.title,
                        "category": rule.category,
                        "file": rel_path,
                        "language": language,
                        "count": len(matches),
                        "line_numbers": lines,
                        "recommendation": rule.recommendation,
                    }
                )
                continue

            if rule.mode == "threshold" and rule.field == "line_count" and rule.threshold is not None:
                if int(line_count) < int(rule.threshold):
                    continue
                findings.append(
                    {
                        "rule_id": rule.rule_id,
                        "tier": rule.tier,
                        "severity": rule.severity,
                        "title": rule.title,
                        "category": rule.category,
                        "file": rel_path,
                        "language": language,
                        "count": 1,
                        "metric_value": int(line_count),
                        "line_numbers": [],
                        "recommendation": rule.recommendation,
                    }
                )
        return findings

    def _iter_files(self, root: Path) -> Iterable[Path]:
        allowed = set(self.LANGUAGE_BY_SUFFIX.keys())
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in self.EXCLUDED_DIRS for part in path.parts):
                continue
            if path.suffix.lower() not in allowed:
                continue
            yield path

    def _detect_language(self, path: Path) -> str:
        return str(self.LANGUAGE_BY_SUFFIX.get(path.suffix.lower(), "generic"))

    def _line_for_offset(self, content: str, offset: int) -> int:
        if offset <= 0:
            return 1
        return int(content.count("\n", 0, offset) + 1)

    def _safe_rel_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(Path.cwd()))
        except Exception:
            return str(path)
