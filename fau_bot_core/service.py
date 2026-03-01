from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from .architect_agent import ArchitectAgent
from .config import get_config
from .db import CLINICAL_RO_ENGINE, SURGICAL_RO_ENGINE, OUTPUT_ENGINE, clinical_ro_conn, output_conn, surgical_ro_conn
from .local_llm import LocalLLMClient
from .schema import ensure_output_schema, schema_name
from .vector_knowledge import load_default_corpus, search_knowledge
from .time_utils import utcnow


def _dump(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _safe_upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _extract_float(text_value: Any) -> Optional[float]:
    raw = str(text_value or "").strip().replace(",", "")
    if not raw:
        return None
    buf = ""
    dot = False
    minus = False
    for ch in raw:
        if ch.isdigit():
            buf += ch
        elif ch == "." and not dot:
            buf += ch
            dot = True
        elif ch == "-" and not minus and not buf:
            buf += ch
            minus = True
    if buf in {"", "-", ".", "-."}:
        return None
    try:
        return float(buf)
    except Exception:
        return None


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((float(part) / float(total)) * 100.0, 2)


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


PR_WORKFLOW_STATUSES = {
    "OPEN",
    "TRIAGED",
    "READY_FOR_SPEC",
    "SPEC_READY",
    "READY_FOR_PATCH",
    "PATCH_READY",
    "READY_FOR_TEST",
    "TEST_PASSED",
    "TEST_FAILED",
    "READY_FOR_REVIEW",
    "MERGED",
    "REJECTED",
}


PR_STATUS_TRANSITIONS: Dict[str, set[str]] = {
    "OPEN": {"TRIAGED", "READY_FOR_SPEC", "REJECTED"},
    "TRIAGED": {"READY_FOR_SPEC", "REJECTED"},
    "READY_FOR_SPEC": {"SPEC_READY", "REJECTED"},
    "SPEC_READY": {"READY_FOR_PATCH", "PATCH_READY", "REJECTED"},
    "READY_FOR_PATCH": {"PATCH_READY", "REJECTED"},
    "PATCH_READY": {"READY_FOR_TEST", "TEST_FAILED", "REJECTED"},
    "READY_FOR_TEST": {"TEST_PASSED", "TEST_FAILED", "REJECTED"},
    "TEST_FAILED": {"READY_FOR_PATCH", "PATCH_READY", "REJECTED"},
    "TEST_PASSED": {"READY_FOR_REVIEW", "MERGED", "REJECTED"},
    "READY_FOR_REVIEW": {"MERGED", "REJECTED"},
    "MERGED": set(),
    "REJECTED": set(),
}


def _priority_rank(priority: str) -> int:
    return {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(str(priority or "P3").upper(), 9)


_DEFAULT_PATCH_ALLOWLIST = ["app/ai_rules", "app/services", "app/api", "app/schemas", "app/core"]
ENGINEERING_PATCH_ALLOWLIST = tuple(
    p.strip().strip("/")
    for p in (os.getenv("FAU_CORE_PATCH_ALLOWLIST", ",".join(_DEFAULT_PATCH_ALLOWLIST)) or "").split(",")
    if p.strip()
)
ENGINEERING_MAX_PATCH_DIFF_LINES = max(80, min(int(os.getenv("FAU_CORE_MAX_PATCH_DIFF_LINES", "1200") or 1200), 8000))

_MIN_TESTS_BY_CHANGE_TYPE: Dict[str, List[str]] = {
    "bug_fix": ["unit", "smoke"],
    "perf_fix": ["unit", "smoke"],
    "ops_fix": ["smoke"],
    "refactor": ["unit", "smoke"],
    "tech_debt": ["unit"],
    "security_fix": ["unit", "smoke"],
    "add_rule_version": ["unit", "smoke"],
}


def _canonical_allowed_paths(paths: Any) -> List[str]:
    out: List[str] = []
    raw = paths if isinstance(paths, list) else []
    for item in raw:
        p = str(item or "").strip().strip("/")
        if not p:
            continue
        if any(p == root or p.startswith(root + "/") for root in ENGINEERING_PATCH_ALLOWLIST):
            out.append(p)
    if not out:
        out = list(ENGINEERING_PATCH_ALLOWLIST)
    dedup: List[str] = []
    seen = set()
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        dedup.append(p)
    return dedup


def _ensure_tests_contract(change_type: str, tests: Any) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    if isinstance(tests, list):
        for item in tests:
            if not isinstance(item, dict):
                continue
            ttype = str(item.get("type") or "").strip().lower()
            name = str(item.get("name") or "").strip()
            if not ttype:
                continue
            if not name:
                name = f"test_{ttype}_required"
            normalized.append({"type": ttype, "name": name})
    required = _MIN_TESTS_BY_CHANGE_TYPE.get(str(change_type or "bug_fix").lower(), ["unit"])
    existing = {x["type"] for x in normalized}
    for ttype in required:
        if ttype in existing:
            continue
        normalized.append({"type": ttype, "name": f"test_{ttype}_mandatory"})
    return normalized


def _extract_patch_paths(patch_diff: str) -> List[str]:
    out: List[str] = []
    for line in str(patch_diff or "").splitlines():
        if line.startswith("+++ b/"):
            p = line.replace("+++ b/", "", 1).strip().strip("/")
            if p:
                out.append(p)
    dedup: List[str] = []
    seen = set()
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        dedup.append(p)
    return dedup


def _count_patch_diff_lines(patch_diff: str) -> int:
    return len([ln for ln in str(patch_diff or "").splitlines() if ln.strip()])


def _default_kpi_contract(issue: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ev = (issue or {}).get("evidence") or {}
    runtime = _runtime_observability_metrics(window_minutes=60)
    profile = _runtime_env_profile()
    thresholds = _kpi_thresholds_for_profile(profile)
    latency_runtime = runtime.get("latency") or {}
    baseline_p95 = latency_runtime.get("p95_ms")
    baseline_avg = latency_runtime.get("avg_ms")
    baseline_error = runtime.get("error_rate_pct")
    return {
        "api_p95_ms": {
            "baseline": ev.get("api_p95_ms", baseline_p95),
            "target_max": float(thresholds.get("api_p95_ms") or 350.0),
            "source": "telemetry_or_runtime",
        },
        "error_rate_pct": {
            "baseline": ev.get("error_rate_pct", baseline_error),
            "target_max": float(thresholds.get("error_rate_pct") or 1.0),
            "source": "telemetry_or_runtime",
        },
        "response_time_ms": {
            "baseline": ev.get("response_time_ms", baseline_avg),
            "target_max": float(thresholds.get("response_time_ms") or 500.0),
            "source": "telemetry_or_runtime",
        },
        "regressions_zero": {
            "baseline": None,
            "target_exact": int(thresholds.get("regressions_zero") or 0),
            "source": "verifier_tests",
        },
        "_profile": profile,
    }


def _validate_patch_policy(proposal: Dict[str, Any], patch_diff: str) -> Dict[str, Any]:
    policy = proposal.get("policy") or {}
    allowed_paths = _canonical_allowed_paths(policy.get("allowed_paths") or proposal.get("target_paths") or [])
    max_lines = int(policy.get("max_patch_diff_lines") or ENGINEERING_MAX_PATCH_DIFF_LINES)
    changed_paths = _extract_patch_paths(patch_diff)
    patch_lines = _count_patch_diff_lines(patch_diff)
    violations: List[str] = []
    disallowed = [
        p for p in changed_paths if not any(p == root or p.startswith(root + "/") for root in allowed_paths)
    ]
    if disallowed:
        violations.append(f"paths_no_permitidos={disallowed}")
    if patch_lines > max_lines:
        violations.append(f"patch_excede_lineas({patch_lines}>{max_lines})")
    return {
        "ok": len(violations) == 0,
        "allowed_paths": allowed_paths,
        "max_patch_diff_lines": max_lines,
        "changed_paths": changed_paths,
        "patch_lines": patch_lines,
        "violations": violations,
    }


def _runtime_observability_metrics(window_minutes: int = 60) -> Dict[str, Any]:
    try:
        from app.core.observability import metrics_snapshot

        data = metrics_snapshot(window_minutes=max(1, min(int(window_minutes or 60), 24 * 60)))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _runtime_env_profile() -> str:
    raw = (
        os.getenv("FAU_CORE_RUNTIME_PROFILE")
        or os.getenv("FAU_CORE_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or "prod"
    )
    profile = str(raw or "prod").strip().lower()
    if profile in {"production", "prod"}:
        return "prod"
    if profile in {"staging", "stage", "preprod", "qa"}:
        return "staging"
    return "dev"


def _kpi_thresholds_for_profile(profile: str) -> Dict[str, Any]:
    p = str(profile or "prod").strip().lower()
    defaults = {
        "dev": {"api_p95_ms": 1400.0, "error_rate_pct": 8.0, "response_time_ms": 1000.0, "regressions_zero": 0},
        "staging": {"api_p95_ms": 700.0, "error_rate_pct": 2.5, "response_time_ms": 700.0, "regressions_zero": 0},
        "prod": {"api_p95_ms": 350.0, "error_rate_pct": 1.0, "response_time_ms": 500.0, "regressions_zero": 0},
    }
    base = dict(defaults.get(p, defaults["prod"]))
    env_map = {
        "api_p95_ms": f"FAU_CORE_KPI_{p.upper()}_P95_MAX",
        "error_rate_pct": f"FAU_CORE_KPI_{p.upper()}_ERROR_RATE_MAX",
        "response_time_ms": f"FAU_CORE_KPI_{p.upper()}_RESPONSE_MAX",
        "regressions_zero": f"FAU_CORE_KPI_{p.upper()}_REGRESSIONS_TARGET",
    }
    for key, env_key in env_map.items():
        raw = os.getenv(env_key)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            if key == "regressions_zero":
                base[key] = int(raw)
            else:
                base[key] = float(raw)
        except Exception:
            continue
    return base


def _eval_numeric_kpi(value: Optional[float], *, target_max: Optional[float] = None, target_min: Optional[float] = None) -> Dict[str, Any]:
    if value is None:
        return {"status": "NOT_MEASURED", "value": None}
    if target_max is not None and float(value) > float(target_max):
        return {"status": "FAIL", "value": float(value), "target_max": float(target_max)}
    if target_min is not None and float(value) < float(target_min):
        return {"status": "FAIL", "value": float(value), "target_min": float(target_min)}
    payload: Dict[str, Any] = {"status": "PASS", "value": float(value)}
    if target_max is not None:
        payload["target_max"] = float(target_max)
    if target_min is not None:
        payload["target_min"] = float(target_min)
    return payload


def _evaluate_runtime_kpis(proposal: Dict[str, Any], *, runtime_metrics: Dict[str, Any], regressions_count: int) -> Dict[str, Any]:
    criteria = proposal.get("acceptance_criteria") or {}
    kpis = criteria.get("kpis") or {}
    latency = (runtime_metrics or {}).get("latency") or {}
    profile = _runtime_env_profile()
    thresholds = _kpi_thresholds_for_profile(profile)

    api_p95_value = latency.get("p95_ms")
    api_p95_rule = kpis.get("api_p95_ms") or {}
    api_p95 = _eval_numeric_kpi(api_p95_value, target_max=api_p95_rule.get("target_max"))
    api_p95["baseline"] = api_p95_rule.get("baseline")

    err_value = (runtime_metrics or {}).get("error_rate_pct")
    err_rule = kpis.get("error_rate_pct") or {}
    err = _eval_numeric_kpi(err_value, target_max=err_rule.get("target_max"))
    err["baseline"] = err_rule.get("baseline")

    avg_value = latency.get("avg_ms")
    avg_rule = kpis.get("response_time_ms") or {}
    avg = _eval_numeric_kpi(avg_value, target_max=avg_rule.get("target_max"))
    avg["baseline"] = avg_rule.get("baseline")

    reg_rule = kpis.get("regressions_zero") or {}
    regressions = {
        "status": "PASS" if int(regressions_count or 0) == int(reg_rule.get("target_exact") or 0) else "FAIL",
        "value": int(regressions_count or 0),
        "target_exact": int(reg_rule.get("target_exact") or 0),
    }

    return {
        "profile": profile,
        "thresholds": {
            "api_p95_ms_max": float(thresholds.get("api_p95_ms") or 0.0),
            "error_rate_pct_max": float(thresholds.get("error_rate_pct") or 0.0),
            "response_time_ms_max": float(thresholds.get("response_time_ms") or 0.0),
            "regressions_target": int(thresholds.get("regressions_zero") or 0),
        },
        "api_p95_ms": api_p95,
        "error_rate_pct": err,
        "response_time_ms": avg,
        "regressions_zero": regressions,
        "runtime_metrics_window": {
            "events": int((runtime_metrics or {}).get("events") or 0),
            "window_minutes": int((runtime_metrics or {}).get("window_minutes") or 60),
        },
    }


class FauBotCoreService:
    def __init__(self) -> None:
        self.cfg = get_config()
        self.schema = schema_name()
        self.llm = LocalLLMClient()
        self.architect = ArchitectAgent()

    def bootstrap(self) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            self._ensure_default_connectors(conn)
        return self.status()

    def _ensure_default_connectors(self, conn) -> None:
        now = utcnow()
        defaults = [
            ("PHEDS", "PHEDS", "ecosystem", False, False),
            ("ECE", "ECE", "ecosystem", False, False),
            ("LOCAL_RNP", "RNP Local", "internal", True, True),
        ]
        for name, display, kind, enabled, granted in defaults:
            exists = conn.execute(
                text(f"SELECT id FROM {self.schema}.connectors WHERE name=:n LIMIT 1"),
                {"n": name},
            ).scalar()
            if exists:
                continue
            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.connectors (
                        name, display_name, kind, base_url, auth_mode,
                        permissions_granted, enabled, config_json,
                        created_at, updated_at
                    ) VALUES (
                        :name, :display, :kind, '', 'token',
                        :granted, :enabled, '{{}}',
                        :created, :updated
                    )
                    """
                ),
                {
                    "name": name,
                    "display": display,
                    "kind": kind,
                    "granted": granted,
                    "enabled": enabled,
                    "created": now,
                    "updated": now,
                },
            )

    def status(self) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        status: Dict[str, Any] = {
            "service": "fau_bot_core",
            "schema": self.schema,
            "llm_provider": self.cfg.llm_provider,
            "llm_model": self.cfg.llm_model,
            "timestamp": utcnow().isoformat() + "Z",
            "dialects": {
                "clinical": str(CLINICAL_RO_ENGINE.dialect.name),
                "surgical": str(SURGICAL_RO_ENGINE.dialect.name),
                "output": str(OUTPUT_ENGINE.dialect.name),
            },
            "connectivity": {},
            "tables": {},
        }
        try:
            with clinical_ro_conn() as c:
                status["connectivity"]["clinical_ro"] = bool(c.execute(text("SELECT 1")).scalar())
        except Exception as exc:
            status["connectivity"]["clinical_ro"] = f"error: {exc}"
        try:
            with surgical_ro_conn() as c:
                status["connectivity"]["surgical_ro"] = bool(c.execute(text("SELECT 1")).scalar())
        except Exception as exc:
            status["connectivity"]["surgical_ro"] = f"error: {exc}"
        try:
            with output_conn() as c:
                status["connectivity"]["output_rw"] = bool(c.execute(text("SELECT 1")).scalar())
                for t in [
                    "runs",
                    "reports",
                    "alerts",
                    "messages",
                    "connectors",
                    "external_events",
                    "knowledge_documents",
                    "hitl_suggestions",
                    "audit_log",
                    "fau_engineering_issues",
                    "fau_pr_suggestions",
                    "fau_patch_runs",
                    "fau_test_runs",
                ]:
                    status["tables"][t] = int(c.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{t}")).scalar() or 0)
        except Exception as exc:
            status["connectivity"]["output_rw"] = f"error: {exc}"
        return status

    def load_default_knowledge(self) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            result = load_default_corpus(conn)
            self._audit(conn, actor="SYSTEM", action="LOAD_DEFAULT_CORPUS", target_type="knowledge", target_id="seed", details=result)
        return result

    def knowledge_search(self, query: str, area: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            return search_knowledge(conn, query=query, area=area, limit=limit)

    def run_cycle(self, *, window_days: int = 30, triggered_by: str = "manual") -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        window_days = max(1, min(int(window_days or 30), 365))
        since_date = date.today() - timedelta(days=window_days)

        consulta_metrics = self._consulta_metrics(since_date)
        hosp_metrics = self._hospital_metrics(since_date)
        qx_prog_metrics = self._qx_program_metrics(since_date)
        qx_urg_metrics = self._qx_urg_metrics(since_date)
        lab_metrics = self._lab_metrics(since_date)

        module_reports = {
            "AI_CONSULTA": consulta_metrics,
            "AI_HOSPITALIZACION": hosp_metrics,
            "AI_QUIROFANO_PROGRAMADO": qx_prog_metrics,
            "AI_QUIROFANO_URGENCIAS": qx_urg_metrics,
            "AI_LABORATORIOS": lab_metrics,
        }

        alerts = self._build_alerts(module_reports)
        llm_summary = self.llm.summarize(_dump(module_reports), task="fau_bot_core_run_summary")

        with output_conn() as conn:
            run_id = int(
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {self.schema}.runs(triggered_by, window_days, status, started_at, ended_at, summary_json)
                        VALUES(:tr, :wd, 'COMPLETED', :st, :en, :sum)
                        RETURNING id
                        """
                    ),
                    {
                        "tr": str(triggered_by or "manual"),
                        "wd": window_days,
                        "st": utcnow(),
                        "en": utcnow(),
                        "sum": _dump(
                            {
                                "window_days": window_days,
                                "module_reports": module_reports,
                                "alerts_count": len(alerts),
                                "llm": {
                                    "provider": llm_summary.provider,
                                    "model": llm_summary.model,
                                    "used_remote": llm_summary.used_remote,
                                    "guardrails": llm_summary.guardrails,
                                },
                                "text_summary": llm_summary.text,
                            }
                        ),
                    },
                ).scalar_one()
            )

            for agent_name, metrics in module_reports.items():
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {self.schema}.reports(
                            run_id, agent_name, scope, report_date, metrics_json, insights_json, created_at
                        ) VALUES(:run_id, :agent, :scope, :rd, :metrics, :insights, :created)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "agent": agent_name,
                        "scope": agent_name.lower(),
                        "rd": date.today(),
                        "metrics": _dump(metrics),
                        "insights": _dump([]),
                        "created": utcnow(),
                    },
                )

            for a in alerts:
                alert_id = int(
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.alerts(
                                run_id, title, severity, category, description, recommendation,
                                payload_json, created_at, acknowledged, resolved
                            ) VALUES(
                                :run_id, :title, :severity, :category, :description, :recommendation,
                                :payload, :created, FALSE, FALSE
                            ) RETURNING id
                            """
                        ),
                        {
                            "run_id": run_id,
                            "title": a["title"],
                            "severity": a["severity"],
                            "category": a["category"],
                            "description": a["description"],
                            "recommendation": a["recommendation"],
                            "payload": _dump(a.get("payload") or {}),
                            "created": utcnow(),
                        },
                    ).scalar_one()
                )
                # HITL queue automática para eventos de mayor impacto
                if _safe_upper(a["severity"]) in {"ALTA", "CRITICAL"}:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.hitl_suggestions(
                                run_id, suggestion_type, title, recommendation, evidence_json,
                                status, reviewer, reviewer_comment, created_at
                            ) VALUES(
                                :run_id, :stype, :title, :rec, :ev,
                                'PENDING_REVIEW', NULL, NULL, :created
                            )
                            """
                        ),
                        {
                            "run_id": run_id,
                            "stype": "CLINICAL_ALERT",
                            "title": f"Validar alerta #{alert_id}: {a['title']}",
                            "rec": a["recommendation"],
                            "ev": _dump({"alert_id": alert_id, "payload": a.get("payload")}),
                            "created": utcnow(),
                        },
                    )

            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.messages(
                        run_id, from_agent, to_agent, message_type, severity, payload_json, created_at
                    ) VALUES(:run_id, 'FAU_CORE', 'HITL_PANEL', 'RUN_SUMMARY', 'INFO', :payload, :created)
                    """
                ),
                {
                    "run_id": run_id,
                    "payload": _dump({"summary": llm_summary.text, "guardrails": llm_summary.guardrails}),
                    "created": utcnow(),
                },
            )

            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="RUN_CYCLE",
                target_type="run",
                target_id=str(run_id),
                details={"window_days": window_days, "alerts": len(alerts)},
            )

        return {
            "run_id": run_id,
            "window_days": window_days,
            "alerts_count": len(alerts),
            "module_reports": module_reports,
            "llm_summary": {
                "text": llm_summary.text,
                "provider": llm_summary.provider,
                "model": llm_summary.model,
                "used_remote": llm_summary.used_remote,
                "guardrails": llm_summary.guardrails,
            },
        }

    def list_runs(self, limit: int = 40) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            rows = conn.execute(
                text(
                    f"SELECT id, triggered_by, window_days, status, started_at, ended_at, summary_json "
                    f"FROM {self.schema}.runs ORDER BY id DESC LIMIT :lim"
                ),
                {"lim": max(1, min(int(limit or 40), 500))},
            ).mappings().all()
            out = []
            for r in rows:
                out.append(
                    {
                        "id": int(r["id"]),
                        "triggered_by": r["triggered_by"],
                        "window_days": r["window_days"],
                        "status": r["status"],
                        "started_at": _to_iso(r["started_at"]),
                        "ended_at": _to_iso(r["ended_at"]),
                        "summary": json.loads(r["summary_json"] or "{}"),
                    }
                )
            return out

    def list_alerts(self, limit: int = 120, only_open: bool = False) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            where = ""
            if only_open:
                where = "WHERE resolved = FALSE"
            rows = conn.execute(
                text(
                    f"SELECT id, run_id, title, severity, category, description, recommendation, payload_json, created_at, acknowledged, resolved "
                    f"FROM {self.schema}.alerts {where} ORDER BY id DESC LIMIT :lim"
                ),
                {"lim": max(1, min(int(limit or 120), 1000))},
            ).mappings().all()
            return [
                {
                    "id": int(r["id"]),
                    "run_id": r["run_id"],
                    "title": r["title"],
                    "severity": r["severity"],
                    "category": r["category"],
                    "description": r["description"],
                    "recommendation": r["recommendation"],
                    "payload": json.loads(r["payload_json"] or "{}"),
                    "created_at": _to_iso(r["created_at"]),
                    "acknowledged": bool(r["acknowledged"]),
                    "resolved": bool(r["resolved"]),
                }
                for r in rows
            ]

    def architect_rules(self) -> List[Dict[str, Any]]:
        return self.architect.list_rules()

    def run_architect_scan(
        self,
        *,
        source_root: Optional[str] = None,
        triggered_by: str = "manual",
        max_files: int = 350,
        max_file_size_kb: int = 900,
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        root = Path(source_root or os.getenv("FAU_CORE_SOURCE_ROOT", ".")).expanduser().resolve()
        scan = self.architect.scan(
            source_root=str(root),
            max_files=max_files,
            max_file_size_kb=max_file_size_kb,
        )
        findings = scan.get("findings") or []

        grouped: Dict[str, Dict[str, Any]] = {}
        for f in findings:
            rid = str(f.get("rule_id") or "")
            if not rid:
                continue
            g = grouped.setdefault(
                rid,
                {
                    "rule_id": rid,
                    "title": str(f.get("title") or ""),
                    "tier": str(f.get("tier") or "P2"),
                    "severity": str(f.get("severity") or "BAJA"),
                    "category": str(f.get("category") or "MANTENIBILIDAD"),
                    "recommendation": str(f.get("recommendation") or ""),
                    "total_hits": 0,
                    "files": {},
                },
            )
            hits = int(f.get("count") or 0)
            g["total_hits"] = int(g["total_hits"]) + hits
            fp = str(f.get("file") or "desconocido")
            g["files"][fp] = int(g["files"].get(fp, 0)) + hits

        ranked = sorted(
            grouped.values(),
            key=lambda x: (
                {"P0": 0, "P1": 1, "P2": 2}.get(str(x.get("tier")), 9),
                -int(x.get("total_hits") or 0),
            ),
        )

        created_ids: List[int] = []
        skipped_duplicates = 0
        with output_conn() as conn:
            now = utcnow()
            recent_cutoff = now - timedelta(days=7)
            for g in ranked[:20]:
                top_files = sorted(g["files"].items(), key=lambda x: int(x[1]), reverse=True)[:8]
                title = f"[{g['tier']}] {g['title']}"
                exists = conn.execute(
                    text(
                        f"""
                        SELECT id FROM {self.schema}.hitl_suggestions
                        WHERE suggestion_type='ARCHITECT_REVIEW'
                          AND title=:title
                          AND status IN ('PENDING_REVIEW', 'APPROVED')
                          AND created_at >= :cutoff
                        LIMIT 1
                        """
                    ),
                    {"title": title, "cutoff": recent_cutoff},
                ).scalar()
                if exists:
                    skipped_duplicates += 1
                    continue

                new_id = int(
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.hitl_suggestions(
                                run_id, suggestion_type, title, recommendation, evidence_json,
                                status, reviewer, reviewer_comment, created_at
                            ) VALUES(
                                NULL, 'ARCHITECT_REVIEW', :title, :rec, :ev,
                                'PENDING_REVIEW', NULL, NULL, :created
                            ) RETURNING id
                            """
                        ),
                        {
                            "title": title,
                            "rec": str(g.get("recommendation") or ""),
                            "ev": _dump(
                                {
                                    "rule_id": g["rule_id"],
                                    "severity": g["severity"],
                                    "category": g["category"],
                                    "total_hits": int(g["total_hits"]),
                                    "top_files": [{"file": f, "hits": int(h)} for f, h in top_files],
                                }
                            ),
                            "created": now,
                        },
                    ).scalar_one()
                )
                created_ids.append(new_id)

            llm_summary = self.llm.summarize(
                _dump(
                    {
                        "agent": "AGENTE_ARQUITECTO",
                        "scan": scan,
                        "top_rule_groups": [
                            {
                                "rule_id": r["rule_id"],
                                "title": r["title"],
                                "tier": r["tier"],
                                "severity": r["severity"],
                                "total_hits": int(r["total_hits"]),
                            }
                            for r in ranked[:10]
                        ],
                    }
                ),
                task="architect_agent_summary",
            )

            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.messages(
                        run_id, from_agent, to_agent, message_type, severity, payload_json, created_at
                    ) VALUES(
                        NULL, 'AGENTE_ARQUITECTO', 'HITL_PANEL', 'ARCHITECT_SCAN', 'INFO', :payload, :created
                    )
                    """
                ),
                {
                    "payload": _dump(
                        {
                            "summary": llm_summary.text,
                            "scanned_files": scan.get("scanned_files"),
                            "hitl_created": len(created_ids),
                            "hitl_skipped_duplicates": skipped_duplicates,
                        }
                    ),
                    "created": now,
                },
            )

            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="ARCHITECT_SCAN",
                target_type="source_root",
                target_id=str(root),
                details={
                    "scanned_files": scan.get("scanned_files"),
                    "findings_total": (scan.get("summary") or {}).get("total_findings"),
                    "hitl_created": len(created_ids),
                    "hitl_skipped_duplicates": skipped_duplicates,
                },
            )

        scan["hitl_created"] = len(created_ids)
        scan["hitl_skipped_duplicates"] = skipped_duplicates
        scan["hitl_suggestion_ids"] = created_ids
        scan["suggestions_generated"] = len(ranked[:20])
        return scan

    def list_architect_suggestions(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return self.list_hitl(status=status, limit=limit, suggestion_type="ARCHITECT_REVIEW")

    def list_hitl(
        self,
        status: Optional[str] = None,
        limit: int = 200,
        suggestion_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            params = {"lim": max(1, min(int(limit or 200), 2000))}
            where_clauses: List[str] = []
            if status:
                where_clauses.append("status = :st")
                params["st"] = str(status).upper()
            if suggestion_type:
                where_clauses.append("suggestion_type = :stype")
                params["stype"] = str(suggestion_type).upper()
            where = ""
            if where_clauses:
                where = "WHERE " + " AND ".join(where_clauses)
            rows = conn.execute(
                text(
                    f"SELECT id, run_id, suggestion_type, title, recommendation, evidence_json, status, reviewer, reviewer_comment, created_at, reviewed_at "
                    f"FROM {self.schema}.hitl_suggestions {where} ORDER BY id DESC LIMIT :lim"
                ),
                params,
            ).mappings().all()
            return [
                {
                    "id": int(r["id"]),
                    "run_id": r["run_id"],
                    "suggestion_type": r["suggestion_type"],
                    "title": r["title"],
                    "recommendation": r["recommendation"],
                    "evidence": json.loads(r["evidence_json"] or "{}"),
                    "status": r["status"],
                    "reviewer": r["reviewer"],
                    "reviewer_comment": r["reviewer_comment"],
                    "created_at": _to_iso(r["created_at"]),
                    "reviewed_at": _to_iso(r["reviewed_at"]),
                }
                for r in rows
            ]

    def set_hitl_status(
        self,
        suggestion_id: int,
        *,
        status: str,
        reviewer: str,
        reviewer_comment: str = "",
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        status = str(status or "PENDING_REVIEW").upper()
        if status not in {"PENDING_REVIEW", "APPROVED", "REJECTED", "APPLIED"}:
            raise ValueError("status inválido")

        with output_conn() as conn:
            exists = conn.execute(
                text(f"SELECT id FROM {self.schema}.hitl_suggestions WHERE id=:id LIMIT 1"),
                {"id": int(suggestion_id)},
            ).scalar()
            if not exists:
                raise ValueError("sugerencia no encontrada")

            conn.execute(
                text(
                    f"""
                    UPDATE {self.schema}.hitl_suggestions
                    SET status=:st, reviewer=:rev, reviewer_comment=:rc, reviewed_at=:rt
                    WHERE id=:id
                    """
                ),
                {
                    "st": status,
                    "rev": reviewer or "SYSTEM",
                    "rc": reviewer_comment,
                    "rt": utcnow(),
                    "id": int(suggestion_id),
                },
            )
            self._audit(
                conn,
                actor=reviewer or "SYSTEM",
                action="HITL_STATUS_UPDATE",
                target_type="hitl_suggestion",
                target_id=str(int(suggestion_id)),
                details={"status": status, "reviewer_comment": reviewer_comment},
            )

            row = conn.execute(
                text(
                    f"SELECT id, status, reviewer, reviewer_comment, reviewed_at "
                    f"FROM {self.schema}.hitl_suggestions WHERE id=:id"
                ),
                {"id": int(suggestion_id)},
            ).mappings().first()
            return {
                "id": int(row["id"]),
                "status": row["status"],
                "reviewer": row["reviewer"],
                "reviewer_comment": row["reviewer_comment"],
                "reviewed_at": _to_iso(row["reviewed_at"]),
            }

    def list_audit(self, limit: int = 200) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            rows = conn.execute(
                text(
                    f"SELECT id, actor, action, target_type, target_id, details_json, created_at "
                    f"FROM {self.schema}.audit_log ORDER BY id DESC LIMIT :lim"
                ),
                {"lim": max(1, min(int(limit or 200), 5000))},
            ).mappings().all()
            return [
                {
                    "id": int(r["id"]),
                    "actor": r["actor"],
                    "action": r["action"],
                    "target_type": r["target_type"],
                    "target_id": r["target_id"],
                    "details": json.loads(r["details_json"] or "{}"),
                    "created_at": _to_iso(r["created_at"]),
                }
                for r in rows
            ]

    def run_dev_collaboration_scan(
        self,
        *,
        source_root: Optional[str] = None,
        triggered_by: str = "manual",
        max_files: int = 350,
        max_file_size_kb: int = 900,
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        root = Path(source_root or os.getenv("FAU_CORE_SOURCE_ROOT", ".")).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise ValueError(f"source_root inválido: {root}")

        max_files = max(20, min(int(max_files or 350), 3000))
        max_bytes = max(32 * 1024, min(int(max_file_size_kb or 900) * 1024, 5 * 1024 * 1024))

        scanned_files = 0
        skipped_large = 0
        skipped_decode = 0
        findings: List[Dict[str, Any]] = []

        for path in self._iter_source_files(root):
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
                text_body = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                skipped_decode += 1
                continue
            except Exception:
                try:
                    text_body = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    skipped_decode += 1
                    continue

            scanned_files += 1
            metrics = self._analyze_source_text(path, text_body)
            if int(metrics.get("risk_score") or 0) > 0:
                findings.append(metrics)

        findings_sorted = sorted(findings, key=lambda x: int(x.get("risk_score") or 0), reverse=True)
        top_findings = findings_sorted[:20]

        total_bare_except = sum(int(x.get("bare_except") or 0) for x in findings_sorted)
        total_generic_except = sum(int(x.get("generic_except") or 0) for x in findings_sorted)
        total_except_pass = sum(int(x.get("except_with_pass") or 0) for x in findings_sorted)
        total_todos = sum(int(x.get("todo_fixme") or 0) for x in findings_sorted)
        total_hardcoded_paths = sum(int(x.get("hardcoded_paths") or 0) for x in findings_sorted)
        total_local_urls = sum(int(x.get("local_urls") or 0) for x in findings_sorted)
        total_prints = sum(int(x.get("debug_prints") or 0) for x in findings_sorted)

        suggestions: List[Dict[str, Any]] = []

        def _top_files(field: str, limit: int = 8) -> List[Dict[str, Any]]:
            ranked = sorted(
                [x for x in findings_sorted if int(x.get(field) or 0) > 0],
                key=lambda x: int(x.get(field) or 0),
                reverse=True,
            )[:limit]
            return [{"file": x["file"], field: int(x.get(field) or 0)} for x in ranked]

        if total_bare_except + total_generic_except > 0:
            suggestions.append(
                {
                    "title": "Reducir manejo de excepciones amplias",
                    "recommendation": (
                        "Priorizar reemplazo de `except:` y `except Exception` por excepciones específicas en los archivos críticos. "
                        "Esto reduce falsos positivos y facilita diagnóstico clínico-operativo."
                    ),
                    "evidence": {
                        "bare_except_total": total_bare_except,
                        "generic_except_total": total_generic_except,
                        "top_files_bare_except": _top_files("bare_except"),
                        "top_files_generic_except": _top_files("generic_except"),
                    },
                }
            )

        if total_except_pass > 0:
            suggestions.append(
                {
                    "title": "Eliminar silenciamiento de errores con `pass`",
                    "recommendation": (
                        "Sustituir bloques `except ...: pass` por logging estructurado y errores controlados. "
                        "Mantener trazabilidad en captura clínica, exportaciones y colas asíncronas."
                    ),
                    "evidence": {
                        "except_with_pass_total": total_except_pass,
                        "top_files_except_pass": _top_files("except_with_pass"),
                    },
                }
            )

        large_files = [x for x in findings_sorted if bool(x.get("is_large_file"))]
        if large_files:
            suggestions.append(
                {
                    "title": "Segmentar archivos grandes para reducir deuda técnica",
                    "recommendation": (
                        "Extraer bloques por dominio a `app/services` y `app/api` manteniendo wrappers/rutas actuales. "
                        "Objetivo: mejorar mantenibilidad sin cambiar comportamiento."
                    ),
                    "evidence": {
                        "large_files_count": len(large_files),
                        "top_large_files": [
                            {"file": x["file"], "line_count": int(x.get("line_count") or 0)}
                            for x in sorted(large_files, key=lambda y: int(y.get("line_count") or 0), reverse=True)[:10]
                        ],
                    },
                }
            )

        if total_hardcoded_paths + total_local_urls > 0:
            suggestions.append(
                {
                    "title": "Reducir rutas/URLs hardcodeadas",
                    "recommendation": (
                        "Mover rutas absolutas y URLs locales a variables de entorno/configuración centralizada "
                        "para estabilidad entre entornos y despliegues."
                    ),
                    "evidence": {
                        "hardcoded_paths_total": total_hardcoded_paths,
                        "local_urls_total": total_local_urls,
                        "top_files_hardcoded_paths": _top_files("hardcoded_paths"),
                        "top_files_local_urls": _top_files("local_urls"),
                    },
                }
            )

        if total_todos > 0:
            suggestions.append(
                {
                    "title": "Cerrar backlog técnico de TODO/FIXME",
                    "recommendation": (
                        "Convertir TODO/FIXME críticos en tareas HITL priorizadas por impacto clínico, "
                        "comenzando por hospitalización, urgencias y reporte estadístico."
                    ),
                    "evidence": {
                        "todo_fixme_total": total_todos,
                        "top_files_todo_fixme": _top_files("todo_fixme"),
                    },
                }
            )

        if total_prints > 0:
            suggestions.append(
                {
                    "title": "Migrar `print` a logging estructurado",
                    "recommendation": (
                        "Reemplazar `print(...)` por logging JSON con redacción de PHI/PII. "
                        "Facilita observabilidad y análisis forense."
                    ),
                    "evidence": {
                        "debug_prints_total": total_prints,
                        "top_files_debug_prints": _top_files("debug_prints"),
                    },
                }
            )

        if not suggestions:
            suggestions.append(
                {
                    "title": "No se detectaron riesgos estructurales críticos",
                    "recommendation": (
                        "Mantener ciclo preventivo: pruebas de regresión, monitoreo de latencia y revisión de cambios por HITL."
                    ),
                    "evidence": {"scanned_files": scanned_files},
                }
            )

        llm_summary = self.llm.summarize(
            _dump(
                {
                    "source_root": str(root),
                    "scanned_files": scanned_files,
                    "skipped_large": skipped_large,
                    "skipped_decode": skipped_decode,
                    "totals": {
                        "bare_except": total_bare_except,
                        "generic_except": total_generic_except,
                        "except_with_pass": total_except_pass,
                        "todo_fixme": total_todos,
                        "hardcoded_paths": total_hardcoded_paths,
                        "local_urls": total_local_urls,
                        "debug_prints": total_prints,
                    },
                    "suggestions": suggestions,
                }
            ),
            task="fau_bot_dev_collaboration_scan",
        )

        created_ids: List[int] = []
        skipped_duplicates = 0
        with output_conn() as conn:
            now = utcnow()
            recent_cutoff = now - timedelta(days=7)

            for s in suggestions:
                title = str(s.get("title") or "").strip()
                if not title:
                    continue
                exists = conn.execute(
                    text(
                        f"""
                        SELECT id FROM {self.schema}.hitl_suggestions
                        WHERE suggestion_type='CODE_IMPROVEMENT'
                          AND title=:title
                          AND status IN ('PENDING_REVIEW', 'APPROVED')
                          AND created_at >= :cutoff
                        LIMIT 1
                        """
                    ),
                    {"title": title, "cutoff": recent_cutoff},
                ).scalar()
                if exists:
                    skipped_duplicates += 1
                    continue

                new_id = int(
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.hitl_suggestions(
                                run_id, suggestion_type, title, recommendation, evidence_json,
                                status, reviewer, reviewer_comment, created_at
                            ) VALUES(
                                NULL, 'CODE_IMPROVEMENT', :title, :rec, :ev,
                                'PENDING_REVIEW', NULL, NULL, :created
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "title": title,
                            "rec": str(s.get("recommendation") or ""),
                            "ev": _dump(s.get("evidence") or {}),
                            "created": now,
                        },
                    ).scalar_one()
                )
                created_ids.append(new_id)

            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.messages(
                        run_id, from_agent, to_agent, message_type, severity, payload_json, created_at
                    ) VALUES(
                        NULL, 'FAU_BOT_DEV', 'HITL_PANEL', 'DEV_SCAN', 'INFO', :payload, :created
                    )
                    """
                ),
                {
                    "payload": _dump(
                        {
                            "source_root": str(root),
                            "created_suggestions": len(created_ids),
                            "skipped_duplicates": skipped_duplicates,
                            "summary": llm_summary.text,
                        }
                    ),
                    "created": now,
                },
            )

            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="DEV_COLLAB_SCAN",
                target_type="source_root",
                target_id=str(root),
                details={
                    "scanned_files": scanned_files,
                    "created_suggestions": len(created_ids),
                    "skipped_duplicates": skipped_duplicates,
                },
            )

        return {
            "source_root": str(root),
            "scanned_files": scanned_files,
            "skipped_large_files": skipped_large,
            "skipped_decode_files": skipped_decode,
            "totals": {
                "bare_except": total_bare_except,
                "generic_except": total_generic_except,
                "except_with_pass": total_except_pass,
                "todo_fixme": total_todos,
                "hardcoded_paths": total_hardcoded_paths,
                "local_urls": total_local_urls,
                "debug_prints": total_prints,
            },
            "top_findings": top_findings,
            "suggestions_generated": len(suggestions),
            "hitl_created": len(created_ids),
            "hitl_skipped_duplicates": skipped_duplicates,
            "hitl_suggestion_ids": created_ids,
            "llm_summary": {
                "text": llm_summary.text,
                "provider": llm_summary.provider,
                "model": llm_summary.model,
                "used_remote": llm_summary.used_remote,
                "guardrails": llm_summary.guardrails,
            },
        }

    def list_dev_suggestions(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return self.list_hitl(status=status, limit=limit, suggestion_type="CODE_IMPROVEMENT")

    def _agent_message(
        self,
        conn,
        *,
        from_agent: str,
        to_agent: str,
        message_type: str,
        severity: str,
        payload: Dict[str, Any],
    ) -> None:
        conn.execute(
            text(
                f"""
                INSERT INTO {self.schema}.messages(
                    run_id, from_agent, to_agent, message_type, severity, payload_json, created_at
                ) VALUES(
                    NULL, :from_agent, :to_agent, :message_type, :severity, :payload, :created
                )
                """
            ),
            {
                "from_agent": str(from_agent or "AGENT").upper(),
                "to_agent": str(to_agent or "AGENT").upper(),
                "message_type": str(message_type or "INFO").upper(),
                "severity": str(severity or "INFO").upper(),
                "payload": _dump(payload or {}),
                "created": utcnow(),
            },
        )

    def _insert_engineering_issue(
        self,
        conn,
        *,
        source: str,
        issue_code: str,
        title: str,
        category: str,
        severity: str,
        priority: str,
        evidence: Dict[str, Any],
    ) -> int:
        now = utcnow()
        existing = conn.execute(
            text(
                f"""
                SELECT id FROM {self.schema}.fau_engineering_issues
                WHERE issue_code=:issue_code
                  AND status IN ('OPEN','ACK')
                  AND last_seen >= :cutoff
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"issue_code": issue_code, "cutoff": now - timedelta(days=7)},
        ).scalar()
        if existing:
            conn.execute(
                text(
                    f"""
                    UPDATE {self.schema}.fau_engineering_issues
                    SET last_seen=:last_seen, evidence_json=:ev
                    WHERE id=:id
                    """
                ),
                {
                    "id": int(existing),
                    "last_seen": now,
                    "ev": _dump(evidence),
                },
            )
            return int(existing)
        return int(
            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.fau_engineering_issues(
                        source, issue_code, title, category, severity, priority,
                        evidence_json, first_seen, last_seen, status
                    ) VALUES(
                        :source, :issue_code, :title, :category, :severity, :priority,
                        :ev, :first_seen, :last_seen, 'OPEN'
                    ) RETURNING id
                    """
                ),
                {
                    "source": source,
                    "issue_code": issue_code,
                    "title": title,
                    "category": category,
                    "severity": severity,
                    "priority": priority,
                    "ev": _dump(evidence),
                    "first_seen": now,
                    "last_seen": now,
                },
            ).scalar_one()
        )

    def run_engineering_telemetry_scan(
        self,
        *,
        source_root: Optional[str] = None,
        triggered_by: str = "manual",
        max_files: int = 400,
        max_file_size_kb: int = 900,
    ) -> Dict[str, Any]:
        """Telemetry Agent: observa y prioriza problemas de ingeniería."""
        ensure_output_schema(OUTPUT_ENGINE)
        root = Path(source_root or os.getenv("FAU_CORE_SOURCE_ROOT", ".")).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise ValueError(f"source_root inválido: {root}")

        max_files = max(20, min(int(max_files or 400), 3000))
        max_bytes = max(32 * 1024, min(int(max_file_size_kb or 900) * 1024, 5 * 1024 * 1024))

        scanned_files = 0
        skipped_large = 0
        findings: List[Dict[str, Any]] = []
        for path in self._iter_source_files(root):
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
                body = path.read_text(encoding="utf-8")
            except Exception:
                continue
            scanned_files += 1
            findings.append(self._analyze_source_text(path, body))

        totals = {
            "bare_except": sum(int(x.get("bare_except") or 0) for x in findings),
            "generic_except": sum(int(x.get("generic_except") or 0) for x in findings),
            "except_with_pass": sum(int(x.get("except_with_pass") or 0) for x in findings),
            "todo_fixme": sum(int(x.get("todo_fixme") or 0) for x in findings),
            "hardcoded_paths": sum(int(x.get("hardcoded_paths") or 0) for x in findings),
            "local_urls": sum(int(x.get("local_urls") or 0) for x in findings),
            "debug_prints": sum(int(x.get("debug_prints") or 0) for x in findings),
            "large_files": sum(1 for x in findings if bool(x.get("is_large_file"))),
        }
        runtime_metrics = _runtime_observability_metrics(window_minutes=60)
        runtime_error_rate = float(runtime_metrics.get("error_rate_pct") or 0.0) if runtime_metrics else 0.0
        runtime_p95 = float((runtime_metrics.get("latency") or {}).get("p95_ms") or 0.0) if runtime_metrics else 0.0
        runtime_avg = float((runtime_metrics.get("latency") or {}).get("avg_ms") or 0.0) if runtime_metrics else 0.0
        runtime_events = int(runtime_metrics.get("events") or 0) if runtime_metrics else 0

        critical_tokens = ["hospitalizacion", "urgencias", "quirofano", "consulta", "expediente", "reporte"]
        critical_findings: List[Dict[str, Any]] = []
        for item in findings:
            path_lower = str(item.get("file") or "").lower()
            if not any(tok in path_lower for tok in critical_tokens):
                continue
            noise = int(item.get("bare_except") or 0) + int(item.get("generic_except") or 0) + int(
                item.get("except_with_pass") or 0
            )
            debt = int(item.get("todo_fixme") or 0)
            if noise + debt <= 0:
                continue
            critical_findings.append(
                {
                    "file": item.get("file"),
                    "noise": noise,
                    "todo_fixme": debt,
                    "line_count": int(item.get("line_count") or 0),
                }
            )
        critical_findings = sorted(critical_findings, key=lambda x: (int(x.get("noise") or 0), int(x.get("todo_fixme") or 0)), reverse=True)

        issues: List[Dict[str, Any]] = []
        if critical_findings:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "CLINICAL_FLOW_CODE_RISK",
                    "title": "Riesgo técnico en flujos clínicos críticos (hospitalización/urgencias/quirofano/consulta)",
                    "category": "clinical_reliability",
                    "severity": "CRITICAL",
                    "priority": "P0",
                    "evidence": {
                        "critical_files": critical_findings[:15],
                        "critical_files_count": len(critical_findings),
                        "totals": totals,
                    },
                }
            )
        if totals["bare_except"] + totals["generic_except"] > 0:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "EXCEPTION_NOISE",
                    "title": "Manejo de excepciones amplio en código crítico",
                    "category": "reliability",
                    "severity": "HIGH",
                    "priority": "P1",
                    "evidence": {
                        "totals": {
                            "bare_except": totals["bare_except"],
                            "generic_except": totals["generic_except"],
                        },
                        "top_files": sorted(
                            [
                                {
                                    "file": x["file"],
                                    "bare_except": int(x.get("bare_except") or 0),
                                    "generic_except": int(x.get("generic_except") or 0),
                                }
                                for x in findings
                                if int(x.get("bare_except") or 0) + int(x.get("generic_except") or 0) > 0
                            ],
                            key=lambda y: int(y["bare_except"]) + int(y["generic_except"]),
                            reverse=True,
                        )[:12],
                    },
                }
            )
        if totals["except_with_pass"] > 0:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "SILENT_EXCEPT_PASS",
                    "title": "Bloques except silenciosos detectados",
                    "category": "bug_risk",
                    "severity": "HIGH",
                    "priority": "P1",
                    "evidence": {
                        "except_with_pass_total": totals["except_with_pass"],
                    },
                }
            )
        if totals["large_files"] > 0:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "MONOLITH_LARGE_FILES",
                    "title": "Archivos monolíticos con alto riesgo de regresión",
                    "category": "maintainability",
                    "severity": "MEDIUM",
                    "priority": "P2",
                    "evidence": {
                        "large_files_count": totals["large_files"],
                        "top_large_files": sorted(
                            [
                                {"file": x["file"], "line_count": int(x.get("line_count") or 0)}
                                for x in findings
                                if bool(x.get("is_large_file"))
                            ],
                            key=lambda y: int(y["line_count"]),
                            reverse=True,
                        )[:10],
                    },
                }
            )
        if totals["hardcoded_paths"] + totals["local_urls"] > 0:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "HARDCODED_PATHS_URLS",
                    "title": "Rutas/URLs hardcodeadas reducen portabilidad",
                    "category": "portability",
                    "severity": "MEDIUM",
                    "priority": "P2",
                    "evidence": {
                        "hardcoded_paths": totals["hardcoded_paths"],
                        "local_urls": totals["local_urls"],
                    },
                }
            )
        if totals["todo_fixme"] > 0:
            issues.append(
                {
                    "source": "code_scan",
                    "issue_code": "TECH_DEBT_TODO_FIXME",
                    "title": "Backlog TODO/FIXME acumulado",
                    "category": "tech_debt",
                    "severity": "LOW",
                    "priority": "P3",
                    "evidence": {"todo_fixme_total": totals["todo_fixme"]},
                }
            )
        if runtime_events >= 20 and runtime_error_rate >= 3.0:
            issues.append(
                {
                    "source": "runtime",
                    "issue_code": "RUNTIME_ERROR_RATE_HIGH",
                    "title": "Tasa de error 5xx elevada en ventana operativa",
                    "category": "observability",
                    "severity": "HIGH",
                    "priority": "P1",
                    "evidence": {
                        "error_rate_pct": round(runtime_error_rate, 3),
                        "events": runtime_events,
                        "window_minutes": int(runtime_metrics.get("window_minutes") or 60),
                    },
                }
            )
        if runtime_events >= 20 and runtime_p95 >= 900.0:
            issues.append(
                {
                    "source": "runtime",
                    "issue_code": "RUNTIME_P95_HIGH",
                    "title": "Latencia P95 elevada en endpoints",
                    "category": "performance",
                    "severity": "MEDIUM",
                    "priority": "P2",
                    "evidence": {
                        "api_p95_ms": round(runtime_p95, 3),
                        "response_time_ms": round(runtime_avg, 3),
                        "events": runtime_events,
                        "window_minutes": int(runtime_metrics.get("window_minutes") or 60),
                    },
                }
            )

        # Señal operativa de alertas abiertas acumuladas.
        with output_conn() as conn:
            open_alerts = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.schema}.alerts WHERE resolved = FALSE")
                ).scalar()
                or 0
            )
            if open_alerts >= 20:
                issues.append(
                    {
                        "source": "runtime",
                        "issue_code": "OPEN_ALERT_BACKLOG",
                        "title": "Acumulación de alertas abiertas en bandeja",
                        "category": "operations",
                        "severity": "MEDIUM",
                        "priority": "P2",
                        "evidence": {"open_alerts": open_alerts},
                    }
                )

            try:
                high_sev_open = int(
                    conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*) FROM {self.schema}.alerts
                            WHERE resolved = FALSE
                              AND UPPER(COALESCE(severity,'')) IN ('CRITICAL','HIGH','ALTA')
                            """
                        )
                    ).scalar()
                    or 0
                )
            except Exception:
                high_sev_open = 0
            if high_sev_open >= 8:
                issues.append(
                    {
                        "source": "runtime",
                        "issue_code": "CLINICAL_ALERT_SATURATION",
                        "title": "Saturación de alertas clínicas de alta severidad sin resolver",
                        "category": "clinical_operations",
                        "severity": "CRITICAL",
                        "priority": "P0",
                        "evidence": {"open_alerts_high_severity": high_sev_open, "window": "current"},
                    }
                )

            issue_ids: List[int] = []
            for issue in issues:
                issue_id = self._insert_engineering_issue(
                    conn,
                    source=issue["source"],
                    issue_code=issue["issue_code"],
                    title=issue["title"],
                    category=issue["category"],
                    severity=issue["severity"],
                    priority=issue["priority"],
                    evidence=issue.get("evidence") or {},
                )
                issue_ids.append(issue_id)

            self._agent_message(
                conn,
                from_agent="TelemetryAgent",
                to_agent="RootCauseAgent",
                message_type="ISSUE_BATCH",
                severity="INFO",
                payload={
                    "source_root": str(root),
                    "scanned_files": scanned_files,
                    "issues_created_or_updated": len(issue_ids),
                    "issue_ids": issue_ids,
                    "totals": totals,
                    "runtime_metrics": {
                        "events": runtime_events,
                        "error_rate_pct": round(runtime_error_rate, 3),
                        "api_p95_ms": round(runtime_p95, 3),
                        "response_time_ms": round(runtime_avg, 3),
                    },
                },
            )
            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="ENGINEERING_TELEMETRY_SCAN",
                target_type="source_root",
                target_id=str(root),
                details={
                    "issues": len(issue_ids),
                    "scanned_files": scanned_files,
                    "skipped_large_files": skipped_large,
                },
            )

        return {
            "source_root": str(root),
            "scanned_files": scanned_files,
            "skipped_large_files": skipped_large,
            "totals": totals,
            "runtime_metrics": {
                "events": runtime_events,
                "error_rate_pct": round(runtime_error_rate, 3),
                "api_p95_ms": round(runtime_p95, 3),
                "response_time_ms": round(runtime_avg, 3),
                "window_minutes": int(runtime_metrics.get("window_minutes") or 60) if runtime_metrics else 60,
            },
            "issues": issues,
            "issues_count": len(issues),
        }

    def list_engineering_issues(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        lim = max(1, min(int(limit or 200), 2000))
        params: Dict[str, Any] = {"lim": lim}
        where = ""
        if status:
            where = "WHERE status = :status"
            params["status"] = str(status).upper()
        with output_conn() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT id, source, issue_code, title, category, severity, priority, evidence_json,
                           first_seen, last_seen, status
                    FROM {self.schema}.fau_engineering_issues
                    {where}
                    ORDER BY
                        CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,
                        id DESC
                    LIMIT :lim
                    """
                ),
                params,
            ).mappings().all()
        return [
            {
                "id": int(r["id"]),
                "source": r["source"],
                "issue_code": r["issue_code"],
                "title": r["title"],
                "category": r["category"],
                "severity": r["severity"],
                "priority": r["priority"],
                "evidence": json.loads(r["evidence_json"] or "{}"),
                "first_seen": _to_iso(r["first_seen"]),
                "last_seen": _to_iso(r["last_seen"]),
                "status": r["status"],
            }
            for r in rows
        ]

    def runtime_kpis(self, *, window_minutes: int = 60) -> Dict[str, Any]:
        metrics = _runtime_observability_metrics(window_minutes=window_minutes)
        profile = _runtime_env_profile()
        thresholds = _kpi_thresholds_for_profile(profile)
        latency = (metrics or {}).get("latency") or {}
        p95 = float(latency.get("p95_ms") or 0.0)
        avg = float(latency.get("avg_ms") or 0.0)
        error_rate = float((metrics or {}).get("error_rate_pct") or 0.0)
        return {
            "timestamp": utcnow().isoformat() + "Z",
            "profile": profile,
            "thresholds": {
                "api_p95_ms_max": float(thresholds.get("api_p95_ms") or 0.0),
                "error_rate_pct_max": float(thresholds.get("error_rate_pct") or 0.0),
                "response_time_ms_max": float(thresholds.get("response_time_ms") or 0.0),
                "regressions_target": int(thresholds.get("regressions_zero") or 0),
            },
            "window_minutes": int((metrics or {}).get("window_minutes") or max(1, int(window_minutes or 60))),
            "events": int((metrics or {}).get("events") or 0),
            "error_rate_pct": error_rate,
            "latency": {
                "avg_ms": avg,
                "p50_ms": float(latency.get("p50_ms") or 0.0),
                "p95_ms": p95,
                "p99_ms": float(latency.get("p99_ms") or 0.0),
            },
            "kpi_status": {
                "api_p95_ms": "PASS" if p95 <= float(thresholds.get("api_p95_ms") or 0.0) else "FAIL",
                "error_rate_pct": "PASS" if error_rate <= float(thresholds.get("error_rate_pct") or 0.0) else "FAIL",
                "response_time_ms": "PASS" if avg <= float(thresholds.get("response_time_ms") or 0.0) else "FAIL",
            },
            "top_routes": (metrics or {}).get("top_routes") or [],
        }

    def _root_cause_from_issue(self, issue_code: str) -> Dict[str, Any]:
        code = str(issue_code or "").upper()
        mapping = {
            "CLINICAL_FLOW_CODE_RISK": {
                "hypothesis": "Excepciones amplias/deuda técnica en rutas clínicas críticas elevan riesgo de incidentes y regresiones.",
                "change_type": "bug_fix",
                "target_module": "app/services/hospitalizacion_flow.py",
                "risk_level": "high",
            },
            "CLINICAL_ALERT_SATURATION": {
                "hypothesis": "Acumulación de alertas clínicas de severidad alta sin resolver sugiere fatiga operativa y reglas sin cierre.",
                "change_type": "ops_fix",
                "target_module": "app/api/fau_bot.py",
                "risk_level": "high",
            },
            "RUNTIME_ERROR_RATE_HIGH": {
                "hypothesis": "Error rate 5xx alto indica fallas repetitivas en rutas de producción o manejo incompleto de excepciones.",
                "change_type": "bug_fix",
                "target_module": "app/api",
                "risk_level": "high",
            },
            "RUNTIME_P95_HIGH": {
                "hypothesis": "P95 elevado sugiere cuellos de botella en consultas, serialización o dependencias no cacheadas.",
                "change_type": "perf_fix",
                "target_module": "app/services",
                "risk_level": "medium",
            },
            "EXCEPTION_NOISE": {
                "hypothesis": "Manejo genérico de excepciones oculta errores raíz y vuelve frágil la trazabilidad.",
                "change_type": "bug_fix",
                "target_module": "app/services",
                "risk_level": "medium",
            },
            "SILENT_EXCEPT_PASS": {
                "hypothesis": "Bloques silenciosos evitan observabilidad y permiten datos inconsistentes en flujos críticos.",
                "change_type": "bug_fix",
                "target_module": "app/services",
                "risk_level": "medium",
            },
            "MONOLITH_LARGE_FILES": {
                "hypothesis": "Archivos grandes elevan acoplamiento y riesgo de regresión en cambios pequeños.",
                "change_type": "refactor",
                "target_module": "app/services",
                "risk_level": "low",
            },
            "HARDCODED_PATHS_URLS": {
                "hypothesis": "Rutas hardcodeadas rompen despliegues y dificultan operación offline/online.",
                "change_type": "refactor",
                "target_module": "app/core",
                "risk_level": "low",
            },
            "TECH_DEBT_TODO_FIXME": {
                "hypothesis": "Backlog técnico sin priorización acumula deuda y retrasa mejoras clínicas.",
                "change_type": "tech_debt",
                "target_module": "app/services",
                "risk_level": "low",
            },
            "OPEN_ALERT_BACKLOG": {
                "hypothesis": "Falta de workflow operativo de resolución de alertas produce cola creciente y fatiga del sistema.",
                "change_type": "ops_fix",
                "target_module": "app/api",
                "risk_level": "medium",
            },
        }
        return mapping.get(
            code,
            {
                "hypothesis": "Causa técnica por confirmar mediante análisis de archivo y ejecución controlada.",
                "change_type": "bug_fix",
                "target_module": "app/services",
                "risk_level": "medium",
            },
        )

    def _proposal_for_issue(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        root = self._root_cause_from_issue(str(issue.get("issue_code") or ""))
        issue_code = str(issue.get("issue_code") or "UNKNOWN")
        top_files = (issue.get("evidence") or {}).get("top_files") or []
        files_to_modify = [str(x.get("file")) for x in top_files if str(x.get("file") or "").strip()]
        if not files_to_modify:
            files_to_modify = [str(root.get("target_module") or "app/services") + "/__init__.py"]
        target_module = files_to_modify[0]
        change_type = str(root.get("change_type") or "bug_fix")
        tests = _ensure_tests_contract(change_type, [])
        target_paths = _canonical_allowed_paths(["app/ai_rules", "app/services", "app/api", "app/schemas"])
        if not any(
            str(target_module).strip("/").startswith(p + "/") or str(target_module).strip("/") == p
            for p in target_paths
        ):
            target_module = f"{target_paths[0]}/__init__.py"
        return {
            "goal": f"Resolver issue {issue_code}: {issue.get('title')}",
            "constraints": [
                "Additive change only",
                "Do not delete existing fields or behavior",
                "Keep backwards compatibility",
                "Follow existing rule registry pattern",
            ],
            "change_type": change_type,
            "target_module": target_module,
            "target_paths": target_paths,
            "new_artifacts": [],
            "root_cause": {
                "hypothesis": root.get("hypothesis"),
                "evidence": issue.get("evidence") or {},
            },
            "proposal_steps": [
                "Aislar condición raíz en módulo objetivo",
                "Agregar guardas de seguridad y logging estructurado",
                "Mantener rutas y payloads preexistentes sin breaking changes",
            ],
            "acceptance_criteria": {
                "no_breaking_routes": True,
                "no_removed_fields": True,
                "tests_required": sorted({str(t.get("type") or "").lower() for t in tests if str(t.get("type") or "")}),
                "kpis": _default_kpi_contract(issue),
            },
            "tests": _ensure_tests_contract(
                change_type,
                [
                    {"type": "unit", "name": f"test_{issue_code.lower()}_unit"},
                    {"type": "smoke", "name": "test_smoke_regression_core_routes"},
                ],
            ),
            "policy": {
                "allowed_paths": target_paths,
                "max_patch_diff_lines": ENGINEERING_MAX_PATCH_DIFF_LINES,
                "human_review_required": True,
            },
            "rollback_plan": "Revert commit relacionado y restaurar estado previo de feature flag.",
        }

    def generate_pr_suggestions(
        self,
        *,
        triggered_by: str = "manual",
        limit: int = 10,
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        lim = max(1, min(int(limit or 10), 200))
        with output_conn() as conn:
            issues = conn.execute(
                text(
                    f"""
                    SELECT id, source, issue_code, title, category, severity, priority, evidence_json, status
                    FROM {self.schema}.fau_engineering_issues
                    WHERE status IN ('OPEN','ACK')
                    ORDER BY
                        CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,
                        id DESC
                    LIMIT :lim
                    """
                ),
                {"lim": lim},
            ).mappings().all()
            created: List[int] = []
            for row in issues:
                issue = dict(row)
                issue["evidence"] = json.loads(issue.get("evidence_json") or "{}")
                existing = conn.execute(
                    text(
                        f"""
                        SELECT id FROM {self.schema}.fau_pr_suggestions
                        WHERE issue_id=:issue_id
                          AND status IN ('OPEN','TRIAGED','READY_FOR_SPEC','SPEC_READY','READY_FOR_PATCH','PATCH_READY','READY_FOR_TEST','TEST_FAILED','TEST_PASSED','READY_FOR_REVIEW')
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"issue_id": int(issue["id"])},
                ).scalar()
                if existing:
                    continue
                proposal = self._proposal_for_issue(issue)
                now = utcnow()
                suggestion_id = int(
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.fau_pr_suggestions(
                                log_id, titulo_pr, explicacion, codigo_sugerido, archivo_objetivo,
                                issue_id, title, category, priority, risk_level, status,
                                proposal_json, patch_diff, files_affected_json, tests_required_json,
                                test_report_json, builder, creado_en, aplicado_en, created_at, updated_at
                            ) VALUES(
                                NULL, :titulo_pr, :explicacion, '', :archivo_objetivo,
                                :issue_id, :title, :category, :priority, :risk_level, 'TRIAGED',
                                :proposal_json, '', :files_affected_json, :tests_required_json,
                                '{{}}', '', :creado_en, NULL, :created_at, :updated_at
                            ) RETURNING id
                            """
                        ),
                        {
                            "issue_id": int(issue["id"]),
                            "titulo_pr": str(issue.get("title") or "PR suggestion"),
                            "explicacion": str(self._root_cause_from_issue(issue.get("issue_code")).get("hypothesis") or ""),
                            "archivo_objetivo": str((proposal.get("target_module") or "")),
                            "title": str(issue.get("title") or "PR suggestion"),
                            "category": str(issue.get("category") or "general"),
                            "priority": str(issue.get("priority") or "P3"),
                            "risk_level": str(self._root_cause_from_issue(issue.get("issue_code")).get("risk_level") or "medium"),
                            "proposal_json": _dump(proposal),
                            "files_affected_json": _dump((issue.get("evidence") or {}).get("top_files") or []),
                            "tests_required_json": _dump(proposal.get("tests") or []),
                            "creado_en": now,
                            "created_at": now,
                            "updated_at": now,
                        },
                    ).scalar_one()
                )
                created.append(suggestion_id)
                conn.execute(
                    text(f"UPDATE {self.schema}.fau_engineering_issues SET status='ACK', last_seen=:now WHERE id=:id"),
                    {"id": int(issue["id"]), "now": now},
                )

            self._agent_message(
                conn,
                from_agent="RootCauseAgent",
                to_agent="PRSuggestionAgent",
                message_type="PR_SUGGESTIONS_CREATED",
                severity="INFO",
                payload={"created_count": len(created), "suggestion_ids": created},
            )
            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="GENERATE_PR_SUGGESTIONS",
                target_type="engineering_issues",
                target_id="batch",
                details={"created_count": len(created)},
            )
        return {"created_count": len(created), "suggestion_ids": created}

    def list_pr_suggestions(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        ensure_output_schema(OUTPUT_ENGINE)
        lim = max(1, min(int(limit or 200), 2000))
        params: Dict[str, Any] = {"lim": lim}
        where = ""
        if status:
            where = "WHERE status = :status"
            params["status"] = str(status).upper()
        with output_conn() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT id, issue_id, title, category, priority, risk_level, status,
                           proposal_json, patch_diff, files_affected_json, tests_required_json,
                           test_report_json, builder, created_at, updated_at
                    FROM {self.schema}.fau_pr_suggestions
                    {where}
                    ORDER BY
                        CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,
                        id DESC
                    LIMIT :lim
                    """
                ),
                params,
            ).mappings().all()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "issue_id": r["issue_id"],
                    "title": r["title"],
                    "category": r["category"],
                    "priority": r["priority"],
                    "risk_level": r["risk_level"],
                    "status": r["status"],
                    "proposal_json": json.loads(r["proposal_json"] or "{}"),
                    "patch_diff": str(r["patch_diff"] or ""),
                    "files_affected": json.loads(r["files_affected_json"] or "[]"),
                    "tests_required": json.loads(r["tests_required_json"] or "[]"),
                    "test_report": json.loads(r["test_report_json"] or "{}"),
                    "builder": r["builder"],
                    "created_at": _to_iso(r["created_at"]),
                    "updated_at": _to_iso(r["updated_at"]),
                }
            )
        return out

    def _get_pr_suggestion(self, conn, suggestion_id: int) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            text(
                f"""
                SELECT id, issue_id, title, category, priority, risk_level, status,
                       proposal_json, patch_diff, files_affected_json, tests_required_json,
                       test_report_json, builder, created_at, updated_at
                FROM {self.schema}.fau_pr_suggestions
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": int(suggestion_id)},
        ).mappings().first()
        if not row:
            return None
        return dict(row)

    def _transition_pr_status(self, conn, suggestion_id: int, to_status: str) -> Dict[str, Any]:
        to_status = str(to_status or "").upper()
        if to_status not in PR_WORKFLOW_STATUSES:
            raise ValueError(f"status inválido: {to_status}")
        row = self._get_pr_suggestion(conn, suggestion_id)
        if not row:
            raise ValueError("sugerencia no encontrada")
        current = str(row.get("status") or "OPEN").upper()
        if current == to_status:
            return {"id": int(suggestion_id), "status": current}
        allowed = PR_STATUS_TRANSITIONS.get(current, set())
        if to_status not in allowed:
            raise ValueError(f"transición inválida: {current} -> {to_status}")
        conn.execute(
            text(
                f"UPDATE {self.schema}.fau_pr_suggestions SET status=:st, updated_at=:now WHERE id=:id"
            ),
            {"st": to_status, "now": utcnow(), "id": int(suggestion_id)},
        )
        return {"id": int(suggestion_id), "status": to_status, "previous_status": current}

    def build_pr_suggestion_spec(self, suggestion_id: int, *, triggered_by: str = "manual") -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            row = self._get_pr_suggestion(conn, suggestion_id)
            if not row:
                raise ValueError("sugerencia no encontrada")
            status = str(row.get("status") or "OPEN").upper()
            if status in {"OPEN", "TRIAGED"}:
                self._transition_pr_status(conn, suggestion_id, "READY_FOR_SPEC")
            proposal = json.loads(row.get("proposal_json") or "{}")
            if not proposal:
                issue_row = conn.execute(
                    text(
                        f"SELECT id, source, issue_code, title, category, severity, priority, evidence_json "
                        f"FROM {self.schema}.fau_engineering_issues WHERE id=:id LIMIT 1"
                    ),
                    {"id": int(row.get("issue_id") or 0)},
                ).mappings().first()
                if not issue_row:
                    raise ValueError("issue asociado no encontrado para generar spec")
                issue = dict(issue_row)
                issue["evidence"] = json.loads(issue.get("evidence_json") or "{}")
                proposal = self._proposal_for_issue(issue)
            change_type = str(proposal.get("change_type") or "bug_fix")
            tests = _ensure_tests_contract(change_type, proposal.get("tests") or [])
            proposal["tests"] = tests
            proposal["target_paths"] = _canonical_allowed_paths(proposal.get("target_paths") or [])
            policy = proposal.get("policy") or {}
            proposal["policy"] = {
                "allowed_paths": _canonical_allowed_paths(policy.get("allowed_paths") or proposal.get("target_paths") or []),
                "max_patch_diff_lines": int(policy.get("max_patch_diff_lines") or ENGINEERING_MAX_PATCH_DIFF_LINES),
                "human_review_required": True,
            }
            acceptance = proposal.get("acceptance_criteria") or {}
            acceptance["tests_required"] = sorted(
                {str(t.get("type") or "").lower() for t in tests if str(t.get("type") or "").strip()}
            )
            acceptance["kpis"] = acceptance.get("kpis") or _default_kpi_contract(
                {"evidence": (proposal.get("root_cause") or {}).get("evidence") or {}}
            )
            proposal["acceptance_criteria"] = acceptance
            conn.execute(
                text(
                    f"""
                    UPDATE {self.schema}.fau_pr_suggestions
                    SET proposal_json=:proposal_json,
                        tests_required_json=:tests_required_json,
                        status='SPEC_READY',
                        updated_at=:updated_at
                    WHERE id=:id
                    """
                ),
                {
                    "proposal_json": _dump(proposal),
                    "tests_required_json": _dump(proposal.get("tests") or []),
                    "updated_at": utcnow(),
                    "id": int(suggestion_id),
                },
            )
            self._agent_message(
                conn,
                from_agent="PRSuggestionAgent",
                to_agent="PatchBuilderAgent",
                message_type="SPEC_READY",
                severity="INFO",
                payload={"suggestion_id": int(suggestion_id)},
            )
            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="PR_SPEC_READY",
                target_type="fau_pr_suggestion",
                target_id=str(int(suggestion_id)),
                details={"status": "SPEC_READY"},
            )
        return {"id": int(suggestion_id), "status": "SPEC_READY", "proposal_json": proposal}

    def build_patch_from_suggestion(self, suggestion_id: int, *, triggered_by: str = "manual") -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            row = self._get_pr_suggestion(conn, suggestion_id)
            if not row:
                raise ValueError("sugerencia no encontrada")
            status = str(row.get("status") or "OPEN").upper()
            if status in {"OPEN", "TRIAGED", "READY_FOR_SPEC"}:
                self.build_pr_suggestion_spec(suggestion_id, triggered_by=triggered_by)
                row = self._get_pr_suggestion(conn, suggestion_id) or row
                status = str(row.get("status") or "SPEC_READY").upper()
            if status not in {"SPEC_READY", "READY_FOR_PATCH", "TEST_FAILED", "PATCH_READY"}:
                raise ValueError(f"estado inválido para generar patch: {status}")
            if status == "SPEC_READY":
                self._transition_pr_status(conn, suggestion_id, "READY_FOR_PATCH")
            proposal = json.loads(row.get("proposal_json") or "{}")
            policy = proposal.get("policy") or {}
            allowed_paths = _canonical_allowed_paths(policy.get("allowed_paths") or proposal.get("target_paths") or [])
            proposal["target_paths"] = allowed_paths
            proposal["policy"] = {
                "allowed_paths": allowed_paths,
                "max_patch_diff_lines": int(policy.get("max_patch_diff_lines") or ENGINEERING_MAX_PATCH_DIFF_LINES),
                "human_review_required": True,
            }
            target = str(proposal.get("target_module") or "app/services/__init__.py")
            codex_prompt = {
                "instruction": "Output ONLY unified diff. Additive-only. No breaking routes/fields.",
                "proposal_json": proposal,
                "allowed_paths": allowed_paths,
            }
            patch_diff = (
                f"# CODEx_PATCH_REQUEST suggestion={int(suggestion_id)}\n"
                f"# Use proposal_json below and return unified diff only.\n"
                f"{json.dumps(codex_prompt, ensure_ascii=False, indent=2)}\n\n"
                f"diff --git a/{target} b/{target}\n"
                f"--- a/{target}\n"
                f"+++ b/{target}\n"
                f"@@\n"
                f"+# TODO({int(suggestion_id)}): aplicar parche aditivo según proposal_json.\n"
            )
            started_at = utcnow()
            patch_run_id = int(
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {self.schema}.fau_patch_runs(
                            pr_suggestion_id, builder, started_at, ended_at, result, error_json
                        ) VALUES(:sid, :builder, :st, :en, :result, :error_json)
                        RETURNING id
                        """
                    ),
                    {
                        "sid": int(suggestion_id),
                        "builder": "codex_bridge",
                        "st": started_at,
                        "en": utcnow(),
                        "result": "OK",
                        "error_json": "{}",
                    },
                ).scalar_one()
            )
            conn.execute(
                text(
                    f"""
                    UPDATE {self.schema}.fau_pr_suggestions
                    SET patch_diff=:patch_diff,
                        builder='codex_bridge',
                        status='PATCH_READY',
                        updated_at=:updated_at
                    WHERE id=:id
                    """
                ),
                {"patch_diff": patch_diff, "updated_at": utcnow(), "id": int(suggestion_id)},
            )
            self._agent_message(
                conn,
                from_agent="PatchBuilderAgent",
                to_agent="VerifierAgent",
                message_type="PATCH_READY",
                severity="INFO",
                payload={"suggestion_id": int(suggestion_id), "patch_run_id": patch_run_id},
            )
            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="PATCH_GENERATED",
                target_type="fau_pr_suggestion",
                target_id=str(int(suggestion_id)),
                details={"patch_run_id": patch_run_id},
            )
        return {"id": int(suggestion_id), "status": "PATCH_READY", "patch_run_id": patch_run_id, "patch_diff": patch_diff}

    def run_patch_verification(
        self,
        suggestion_id: int,
        *,
        triggered_by: str = "manual",
        commands: Optional[List[str]] = None,
        timeout_sec: int = 240,
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            row = self._get_pr_suggestion(conn, suggestion_id)
            if not row:
                raise ValueError("sugerencia no encontrada")
            status = str(row.get("status") or "OPEN").upper()
            if status not in {"PATCH_READY", "READY_FOR_TEST", "TEST_FAILED"}:
                raise ValueError(f"estado inválido para verificación: {status}")
            if status == "PATCH_READY":
                self._transition_pr_status(conn, suggestion_id, "READY_FOR_TEST")
            proposal = json.loads(row.get("proposal_json") or "{}")
            patch_diff = str(row.get("patch_diff") or "")
            policy_report = _validate_patch_policy(proposal, patch_diff)

        # Prefer the active interpreter (venv) to avoid missing pytest in system python.
        python_exec = os.getenv("FAU_CORE_PYTHON") or sys.executable or "python3"
        command_set = commands or [
            f"{python_exec} -m compileall app fau_bot_core",
            f"{python_exec} -m pytest -q --no-cov test_route_snapshot.py",
        ]
        timeout_sec = max(30, min(int(timeout_sec or 240), 1200))
        started = utcnow()
        command_reports: List[Dict[str, Any]] = []
        all_ok = bool(policy_report.get("ok"))
        if not all_ok:
            command_reports.append(
                {
                    "command": "policy_guardrails",
                    "returncode": 2,
                    "stdout": "",
                    "stderr": "; ".join(policy_report.get("violations") or ["policy_check_failed"]),
                    "ok": False,
                }
            )
        for cmd in command_set:
            try:
                proc = subprocess.run(
                    shlex.split(cmd),
                    cwd=str(Path.cwd()),
                    capture_output=True,
                    text=True,
                    timeout=timeout_sec,
                )
                ok = int(proc.returncode or 0) == 0
                if not ok:
                    all_ok = False
                command_reports.append(
                    {
                        "command": cmd,
                        "returncode": int(proc.returncode or 0),
                        "stdout": str(proc.stdout or "")[-20000:],
                        "stderr": str(proc.stderr or "")[-20000:],
                        "ok": ok,
                    }
                )
            except Exception as exc:
                all_ok = False
                command_reports.append(
                    {
                        "command": cmd,
                        "returncode": -1,
                        "stdout": "",
                        "stderr": str(exc),
                        "ok": False,
                    }
                )
        regression_count = 0
        if not bool(policy_report.get("ok")):
            regression_count += 1
        regression_count += sum(1 for item in command_reports if not bool(item.get("ok")))
        runtime_metrics = _runtime_observability_metrics(window_minutes=60)
        kpi_results = _evaluate_runtime_kpis(
            proposal,
            runtime_metrics=runtime_metrics,
            regressions_count=regression_count,
        )
        kpi_failures = [k for k, v in (kpi_results or {}).items() if isinstance(v, dict) and str(v.get("status") or "") == "FAIL"]
        if kpi_failures:
            all_ok = False
        result_status = "TEST_PASSED" if all_ok else "TEST_FAILED"
        ended = utcnow()

        with output_conn() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {self.schema}.fau_test_runs(
                        pr_suggestion_id, command_set, stdout, stderr, result, started_at, ended_at
                    ) VALUES(
                        :sid, :command_set, :stdout, :stderr, :result, :started_at, :ended_at
                    )
                    """
                ),
                {
                    "sid": int(suggestion_id),
                    "command_set": _dump(command_set),
                    "stdout": _dump(command_reports),
                    "stderr": "",
                    "result": result_status,
                    "started_at": started,
                    "ended_at": ended,
                },
            )
            next_status = "READY_FOR_REVIEW" if all_ok else "TEST_FAILED"
            conn.execute(
                text(
                    f"""
                    UPDATE {self.schema}.fau_pr_suggestions
                    SET status=:status,
                        test_report_json=:test_report,
                        updated_at=:updated_at
                    WHERE id=:id
                    """
                ),
                {
                    "status": next_status,
                    "test_report": _dump(
                        {
                            "result": result_status,
                            "policy_guardrails": policy_report,
                            "kpi_results": kpi_results,
                            "kpi_failures": kpi_failures,
                            "commands": command_reports,
                            "started_at": _to_iso(started),
                            "ended_at": _to_iso(ended),
                        }
                    ),
                    "updated_at": utcnow(),
                    "id": int(suggestion_id),
                },
            )
            self._agent_message(
                conn,
                from_agent="VerifierAgent",
                to_agent="HUMAN_REVIEW",
                message_type="TEST_RESULT",
                severity="INFO" if all_ok else "WARNING",
                payload={
                    "suggestion_id": int(suggestion_id),
                    "result": result_status,
                    "next_status": next_status,
                    "kpi_failures": kpi_failures,
                    "runtime_events": (kpi_results.get("runtime_metrics_window") or {}).get("events"),
                },
            )
            self._audit(
                conn,
                actor=str(triggered_by or "manual"),
                action="PATCH_VERIFICATION",
                target_type="fau_pr_suggestion",
                target_id=str(int(suggestion_id)),
                details={"result": result_status, "next_status": next_status, "kpi_failures": kpi_failures},
            )
        return {
            "id": int(suggestion_id),
            "result": result_status,
            "status": "READY_FOR_REVIEW" if all_ok else "TEST_FAILED",
            "commands": command_reports,
        }

    def mark_pr_merged(
        self,
        suggestion_id: int,
        *,
        reviewer: str = "manual",
        reviewer_comment: str = "",
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            row = self._get_pr_suggestion(conn, suggestion_id)
            if not row:
                raise ValueError("sugerencia no encontrada")
            status = str(row.get("status") or "").upper()
            if status == "TEST_PASSED":
                self._transition_pr_status(conn, suggestion_id, "READY_FOR_REVIEW")
                status = "READY_FOR_REVIEW"
            if status not in {"READY_FOR_REVIEW"}:
                raise ValueError(f"estado inválido para merge: {status}")
            self._transition_pr_status(conn, suggestion_id, "MERGED")
            self._agent_message(
                conn,
                from_agent="HUMAN_REVIEW",
                to_agent="TelemetryAgent",
                message_type="MERGED",
                severity="INFO",
                payload={
                    "suggestion_id": int(suggestion_id),
                    "reviewer": reviewer,
                    "reviewer_comment": reviewer_comment,
                },
            )
            self._audit(
                conn,
                actor=reviewer or "manual",
                action="PR_SUGGESTION_MERGED",
                target_type="fau_pr_suggestion",
                target_id=str(int(suggestion_id)),
                details={"reviewer_comment": reviewer_comment},
            )
        return {"id": int(suggestion_id), "status": "MERGED", "reviewer": reviewer, "reviewer_comment": reviewer_comment}

    def set_pr_suggestion_status(
        self,
        suggestion_id: int,
        *,
        status: str,
        actor: str = "manual",
    ) -> Dict[str, Any]:
        ensure_output_schema(OUTPUT_ENGINE)
        with output_conn() as conn:
            out = self._transition_pr_status(conn, int(suggestion_id), str(status).upper())
            self._audit(
                conn,
                actor=actor or "manual",
                action="PR_SUGGESTION_STATUS_UPDATE",
                target_type="fau_pr_suggestion",
                target_id=str(int(suggestion_id)),
                details={"status": out.get("status"), "previous_status": out.get("previous_status")},
            )
            return out

    def _iter_source_files(self, root: Path):
        excluded_dirs = {
            ".git",
            "__pycache__",
            ".pytest_cache",
            "node_modules",
            ".venv",
            "venv",
            "dist",
            "build",
            ".mypy_cache",
        }
        allowed_suffixes = {
            ".py",
            ".html",
            ".js",
            ".ts",
            ".css",
            ".md",
            ".sql",
            ".toml",
            ".yml",
            ".yaml",
            ".ini",
        }
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in excluded_dirs for part in path.parts):
                continue
            if path.suffix.lower() not in allowed_suffixes:
                continue
            yield path

    def _analyze_source_text(self, path: Path, content: str) -> Dict[str, Any]:
        rel_path = str(path)
        try:
            rel_path = str(path.relative_to(Path.cwd()))
        except Exception:
            pass

        line_count = int((content.count("\n") + 1) if content else 0)
        bare_except = len(re.findall(r"(?m)^\s*except\s*:\s*$", content))
        generic_except = len(re.findall(r"except\s+Exception(?:\s+as\s+\w+)?\s*:", content))
        except_with_pass = len(re.findall(r"except[^\n:]*:\s*(?:#.*\n\s*)*pass\b", content))
        todo_fixme = len(re.findall(r"\b(TODO|FIXME|XXX|HACK)\b", content, flags=re.IGNORECASE))
        hardcoded_paths = len(re.findall(r"/Users/|file://|[A-Za-z]:\\\\", content))
        local_urls = len(re.findall(r"https?://(?:127\.0\.0\.1|localhost)", content, flags=re.IGNORECASE))
        debug_prints = 0
        if path.suffix.lower() == ".py":
            debug_prints = len(re.findall(r"(?m)^\s*print\(", content))

        is_large_file = bool(line_count >= 1200)
        risk_score = (
            (bare_except * 5)
            + (generic_except * 3)
            + (except_with_pass * 4)
            + (todo_fixme * 1)
            + (hardcoded_paths * 2)
            + (local_urls * 2)
            + (debug_prints * 1)
            + (3 if is_large_file else 0)
        )

        return {
            "file": rel_path,
            "line_count": line_count,
            "bare_except": bare_except,
            "generic_except": generic_except,
            "except_with_pass": except_with_pass,
            "todo_fixme": todo_fixme,
            "hardcoded_paths": hardcoded_paths,
            "local_urls": local_urls,
            "debug_prints": debug_prints,
            "is_large_file": is_large_file,
            "risk_score": int(risk_score),
        }

    def _audit(self, conn, *, actor: str, action: str, target_type: str, target_id: str, details: Dict[str, Any]) -> None:
        conn.execute(
            text(
                f"""
                INSERT INTO {self.schema}.audit_log(actor, action, target_type, target_id, details_json, created_at)
                VALUES(:actor, :action, :target_type, :target_id, :details, :created)
                """
            ),
            {
                "actor": actor,
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "details": _dump(details),
                "created": utcnow(),
            },
        )

    def _consulta_metrics(self, since_date: date) -> Dict[str, Any]:
        with clinical_ro_conn() as conn:
            total = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM consultas WHERE fecha_registro >= :d"),
                    {"d": since_date},
                ).scalar()
                or 0
            )
            by_status = {
                _safe_upper(r[0] or "NO_REGISTRADO"): int(r[1])
                for r in conn.execute(
                    text(
                        """
                        SELECT estatus_protocolo, COUNT(*)
                        FROM consultas
                        WHERE fecha_registro >= :d
                        GROUP BY estatus_protocolo
                        """
                    ),
                    {"d": since_date},
                ).fetchall()
            }
            by_dx = {
                str(r[0] or "NO_REGISTRADO"): int(r[1])
                for r in conn.execute(
                    text(
                        """
                        SELECT diagnostico_principal, COUNT(*)
                        FROM consultas
                        WHERE fecha_registro >= :d
                        GROUP BY diagnostico_principal
                        ORDER BY COUNT(*) DESC
                        LIMIT 15
                        """
                    ),
                    {"d": since_date},
                ).fetchall()
            }
        return {
            "period_start": since_date.isoformat(),
            "period_end": date.today().isoformat(),
            "total_consultas": total,
            "por_estatus_protocolo": by_status,
            "top_diagnosticos": by_dx,
        }

    def _hospital_metrics(self, since_date: date) -> Dict[str, Any]:
        with clinical_ro_conn() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT ingreso_tipo,
                           diagnostico,
                           dias_hospitalizacion,
                           fecha_ingreso,
                           fecha_egreso
                    FROM hospitalizaciones
                    WHERE fecha_ingreso >= :d
                    """
                ),
                {"d": since_date},
            ).fetchall()
        total = len(rows)
        by_ing = Counter(_safe_upper(r[0] or "NO_REGISTRADO") for r in rows)
        by_dx = Counter(str(r[1] or "NO_REGISTRADO") for r in rows)
        stays: List[int] = []
        for r in rows:
            if r[2] is not None:
                stays.append(int(r[2]))
                continue
            fi = r[3]
            fe = r[4] or date.today()
            if isinstance(fi, datetime):
                fi = fi.date()
            if isinstance(fe, datetime):
                fe = fe.date()
            if isinstance(fi, date) and isinstance(fe, date):
                stays.append(max(0, (fe - fi).days))
            else:
                stays.append(0)
        prolonged = sum(1 for d in stays if d > 5)
        return {
            "period_start": since_date.isoformat(),
            "period_end": date.today().isoformat(),
            "total_ingresos": total,
            "por_tipo_ingreso": dict(by_ing),
            "top_diagnosticos": dict(by_dx.most_common(15)),
            "estancia_media_dias": round((sum(stays) / len(stays)), 2) if stays else None,
            "indice_estancia_prolongada_pct": _pct(prolonged, len(stays)),
        }

    def _qx_program_metrics(self, since_date: date) -> Dict[str, Any]:
        with surgical_ro_conn() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT estatus, procedimiento_programado, grupo_patologia
                    FROM surgical_programaciones
                    WHERE fecha_programada >= :d
                      AND (modulo_origen IS NULL OR modulo_origen <> 'QUIROFANO_URGENCIA')
                    """
                ),
                {"d": since_date},
            ).fetchall()
        total = len(rows)
        by_status = Counter(_safe_upper(r[0] or "NO_REGISTRADO") for r in rows)
        by_proc = Counter(str(r[1] or "NO_REGISTRADO") for r in rows)
        by_pat = Counter(_safe_upper(r[2] or "NO_REGISTRADO") for r in rows)
        return {
            "period_start": since_date.isoformat(),
            "period_end": date.today().isoformat(),
            "total": total,
            "por_estatus": dict(by_status),
            "tasa_cancelacion_pct": _pct(by_status.get("CANCELADA", 0), total),
            "top_procedimientos": dict(by_proc.most_common(15)),
            "por_grupo_patologia": dict(by_pat),
        }

    def _qx_urg_metrics(self, since_date: date) -> Dict[str, Any]:
        with surgical_ro_conn() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT estatus, patologia, procedimiento_programado,
                           COALESCE(hemoderivados_pg_solicitados,0) + COALESCE(hemoderivados_pfc_solicitados,0) + COALESCE(hemoderivados_cp_solicitados,0) AS hemo
                    FROM surgical_urgencias_programaciones
                    WHERE fecha_urgencia >= :d
                    """
                ),
                {"d": since_date},
            ).fetchall()
        total = len(rows)
        by_status = Counter(_safe_upper(r[0] or "NO_REGISTRADO") for r in rows)
        by_dx = Counter(str(r[1] or "NO_REGISTRADO") for r in rows)
        by_proc = Counter(str(r[2] or "NO_REGISTRADO") for r in rows)
        hemo_total = sum(int(r[3] or 0) for r in rows)
        return {
            "period_start": since_date.isoformat(),
            "period_end": date.today().isoformat(),
            "total": total,
            "por_estatus": dict(by_status),
            "top_diagnosticos": dict(by_dx.most_common(15)),
            "top_procedimientos": dict(by_proc.most_common(15)),
            "hemoderivados_solicitados_total_unidades": hemo_total,
        }

    def _lab_metrics(self, since_date: date) -> Dict[str, Any]:
        since_dt = datetime.combine(since_date, datetime.min.time())
        with clinical_ro_conn() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT test_code, test_name, value
                    FROM labs
                    WHERE timestamp >= :d
                    """
                ),
                {"d": since_dt},
            ).fetchall()

        markers = {
            "creatinina": [],
            "hemoglobina": [],
            "leucocitos": [],
            "plaquetas": [],
            "sodio": [],
            "potasio": [],
        }

        for code, name, raw in rows:
            joined = f"{str(name or '').lower()} {str(code or '').lower()}"
            val = _extract_float(raw)
            if val is None:
                continue
            if "creatin" in joined or joined.strip() == "cr":
                markers["creatinina"].append(val)
            elif "hemoglob" in joined or "hgb" in joined or joined.strip().startswith("hb"):
                markers["hemoglobina"].append(val)
            elif "leuco" in joined or "wbc" in joined:
                markers["leucocitos"].append(val)
            elif "plaquet" in joined or "plt" in joined:
                markers["plaquetas"].append(val)
            elif "sodio" in joined or joined.strip() == "na":
                markers["sodio"].append(val)
            elif "potasio" in joined or joined.strip() == "k":
                markers["potasio"].append(val)

        incid = {
            "aki_cr_ge_2": sum(1 for x in markers["creatinina"] if x >= 2.0),
            "hb_lt_8": sum(1 for x in markers["hemoglobina"] if x < 8.0),
            "leuco_gt_10000": sum(1 for x in markers["leucocitos"] if x > 10000),
            "plt_lt_150": sum(1 for x in markers["plaquetas"] if x < 150),
            "disnatremia": sum(1 for x in markers["sodio"] if x < 135 or x > 145),
            "dispotasemia": sum(1 for x in markers["potasio"] if x < 3.5 or x > 5.0),
        }

        return {
            "period_start": since_date.isoformat(),
            "period_end": date.today().isoformat(),
            "total_registros_laboratorio": len(rows),
            "analitos_detectados": {k: len(v) for k, v in markers.items()},
            "incidencias": incid,
        }

    def _build_alerts(self, reports: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        alerts: List[Dict[str, Any]] = []
        qx = reports.get("AI_QUIROFANO_PROGRAMADO", {})
        hosp = reports.get("AI_HOSPITALIZACION", {})
        labs = reports.get("AI_LABORATORIOS", {})
        urg = reports.get("AI_QUIROFANO_URGENCIAS", {})

        cancel_pct = float(qx.get("tasa_cancelacion_pct") or 0.0)
        if cancel_pct >= 15:
            alerts.append(
                {
                    "title": "Cancelación quirúrgica elevada",
                    "severity": "ALTA",
                    "category": "QUIROFANO",
                    "description": f"Tasa de cancelación en programación: {cancel_pct}%.",
                    "recommendation": "Revisar protocolo preoperatorio y causas de suspensión por procedimiento.",
                    "payload": {"tasa_cancelacion_pct": cancel_pct},
                }
            )

        prolonged = float(hosp.get("indice_estancia_prolongada_pct") or 0.0)
        if prolonged >= 25:
            alerts.append(
                {
                    "title": "Estancia prolongada alta",
                    "severity": "ALTA",
                    "category": "HOSPITALIZACION",
                    "description": f"Índice de estancia prolongada: {prolonged}%.",
                    "recommendation": "Activar ruta de revisión de altas y barreras de egreso por diagnóstico.",
                    "payload": {"indice_estancia_prolongada_pct": prolonged},
                }
            )

        incid = labs.get("incidencias") or {}
        if int(incid.get("aki_cr_ge_2") or 0) > 0:
            alerts.append(
                {
                    "title": "Señal renal AKI",
                    "severity": "ALTA",
                    "category": "LABORATORIO",
                    "description": f"Eventos con creatinina >=2.0 detectados: {int(incid.get('aki_cr_ge_2') or 0)}.",
                    "recommendation": "Priorizar cohortes con daño renal y vigilancia de nefrotoxicidad.",
                    "payload": incid,
                }
            )

        hemo = int(urg.get("hemoderivados_solicitados_total_unidades") or 0)
        if hemo > 0:
            alerts.append(
                {
                    "title": "Demanda de hemoderivados en urgencias",
                    "severity": "MEDIA",
                    "category": "URGENCIAS",
                    "description": f"Unidades solicitadas en urgencias: {hemo}.",
                    "recommendation": "Verificar disponibilidad de banco y planificación por turnos.",
                    "payload": {"hemoderivados_solicitados_total_unidades": hemo},
                }
            )

        if not alerts:
            alerts.append(
                {
                    "title": "Sin alertas críticas",
                    "severity": "INFO",
                    "category": "CENTRAL",
                    "description": "No se detectaron umbrales críticos en la ventana analizada.",
                    "recommendation": "Continuar monitoreo rutinario.",
                    "payload": {},
                }
            )
        return alerts


SERVICE = FauBotCoreService()
