from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List

from app.core.time_utils import utcnow

_LOCK = threading.Lock()
_MAX_EVENTS = max(200, int(os.getenv("OBS_MAX_EVENTS", "8000") or 8000))
_EVENTS: Deque[Dict[str, Any]] = deque(maxlen=_MAX_EVENTS)

_ERR_RATE_ALERT_PCT = float(os.getenv("OBS_ALERT_ERROR_RATE_PCT", "5.0") or 5.0)
_P95_ALERT_MS = float(os.getenv("OBS_ALERT_P95_MS", "2000") or 2000)
_ROUTE_ERR_ALERT_PCT = float(os.getenv("OBS_ALERT_ROUTE_ERROR_RATE_PCT", "12.0") or 12.0)
_ROUTE_MIN_SAMPLES = max(5, int(os.getenv("OBS_ALERT_ROUTE_MIN_SAMPLES", "20") or 20))


def observe_request(*, method: str, path: str, status_code: int, latency_ms: float, error: bool = False) -> None:
    event = {
        "ts": time.time(),
        "method": str(method or "").upper(),
        "path": str(path or ""),
        "status_code": int(status_code or 0),
        "latency_ms": float(max(0.0, latency_ms or 0.0)),
        "error": bool(error or int(status_code or 0) >= 500),
    }
    with _LOCK:
        _EVENTS.append(event)


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    idx = int(round((len(ordered) - 1) * (float(p) / 100.0)))
    idx = max(0, min(idx, len(ordered) - 1))
    return float(ordered[idx])


def metrics_snapshot(window_minutes: int = 60) -> Dict[str, Any]:
    mins = max(1, min(int(window_minutes or 60), 24 * 60))
    cutoff = time.time() - (mins * 60)
    with _LOCK:
        sample = [e for e in list(_EVENTS) if float(e.get("ts") or 0) >= cutoff]

    total = len(sample)
    errors_5xx = sum(1 for e in sample if int(e.get("status_code") or 0) >= 500)
    errors_4xx = sum(1 for e in sample if 400 <= int(e.get("status_code") or 0) < 500)
    latencies = [float(e.get("latency_ms") or 0.0) for e in sample]

    by_route: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"total": 0, "errors_5xx": 0, "latencies": []})
    for e in sample:
        key = f"{e.get('method')} {e.get('path')}"
        node = by_route[key]
        node["total"] += 1
        if int(e.get("status_code") or 0) >= 500:
            node["errors_5xx"] += 1
        node["latencies"].append(float(e.get("latency_ms") or 0.0))

    by_route_out: List[Dict[str, Any]] = []
    for route, node in by_route.items():
        total_r = int(node["total"] or 0)
        err_r = int(node["errors_5xx"] or 0)
        p95_r = _percentile(node["latencies"], 95)
        by_route_out.append(
            {
                "route": route,
                "total": total_r,
                "errors_5xx": err_r,
                "error_rate_pct": round((err_r / float(total_r)) * 100.0, 2) if total_r else 0.0,
                "p95_ms": round(p95_r, 3),
                "avg_ms": round(sum(node["latencies"]) / float(total_r), 3) if total_r else 0.0,
            }
        )
    by_route_out.sort(key=lambda x: (-x["errors_5xx"], -x["p95_ms"], -x["total"]))

    snapshot = {
        "timestamp": utcnow().isoformat() + "Z",
        "window_minutes": mins,
        "events": total,
        "errors_5xx": errors_5xx,
        "errors_4xx": errors_4xx,
        "error_rate_pct": round((errors_5xx / float(total)) * 100.0, 2) if total else 0.0,
        "latency": {
            "avg_ms": round(sum(latencies) / float(total), 3) if total else 0.0,
            "p50_ms": round(_percentile(latencies, 50), 3),
            "p95_ms": round(_percentile(latencies, 95), 3),
            "p99_ms": round(_percentile(latencies, 99), 3),
        },
        "top_routes": by_route_out[:50],
    }
    return snapshot


def automatic_alerts(window_minutes: int = 60) -> List[Dict[str, Any]]:
    metrics = metrics_snapshot(window_minutes=window_minutes)
    alerts: List[Dict[str, Any]] = []

    if float(metrics.get("error_rate_pct") or 0.0) >= _ERR_RATE_ALERT_PCT:
        alerts.append(
            {
                "type": "HIGH_ERROR_RATE",
                "severity": "CRITICAL",
                "message": f"Error 5xx global alto ({metrics['error_rate_pct']}%).",
                "threshold": _ERR_RATE_ALERT_PCT,
            }
        )

    if float(metrics.get("latency", {}).get("p95_ms") or 0.0) >= _P95_ALERT_MS:
        alerts.append(
            {
                "type": "HIGH_LATENCY_P95",
                "severity": "WARNING",
                "message": f"Latencia P95 alta ({metrics['latency']['p95_ms']} ms).",
                "threshold": _P95_ALERT_MS,
            }
        )

    for route in metrics.get("top_routes", []):
        if int(route.get("total") or 0) < _ROUTE_MIN_SAMPLES:
            continue
        if float(route.get("error_rate_pct") or 0.0) >= _ROUTE_ERR_ALERT_PCT:
            alerts.append(
                {
                    "type": "ROUTE_ERROR_RATE",
                    "severity": "WARNING",
                    "message": f"Error alto en {route.get('route')} ({route.get('error_rate_pct')}%).",
                    "threshold": _ROUTE_ERR_ALERT_PCT,
                    "route": route,
                }
            )

    return alerts


def observability_health(window_minutes: int = 60) -> Dict[str, Any]:
    metrics = metrics_snapshot(window_minutes=window_minutes)
    alerts = automatic_alerts(window_minutes=window_minutes)
    return {
        "metrics": metrics,
        "alerts": alerts,
        "status": "ok" if not alerts else "degraded",
    }
