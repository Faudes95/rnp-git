from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text, func, select, update
from sqlalchemy.orm import Session

from app.core.request_context import get_correlation_id
from app.core.time_utils import utcnow


JOB_METADATA = MetaData()

BACKGROUND_JOB_RUNS = Table(
    "background_job_runs",
    JOB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_name", String(140), nullable=False, index=True),
    Column("source", String(40), nullable=False, default="celery", index=True),
    Column("task_id", String(120), nullable=True, index=True),
    Column("triggered_by", String(120), nullable=True, index=True),
    Column("status", String(30), nullable=False, default="RUNNING", index=True),
    Column("correlation_id", String(64), nullable=True, index=True),
    Column("payload_json", Text, nullable=True),
    Column("result_json", Text, nullable=True),
    Column("error_text", Text, nullable=True),
    Column("duration_ms", Integer, nullable=True),
    Column("started_at", DateTime, default=utcnow, nullable=False, index=True),
    Column("ended_at", DateTime, nullable=True, index=True),
)


def ensure_job_registry_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    JOB_METADATA.create_all(bind=bind, checkfirst=True)


def _dump(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, type(default)):
                return obj
        except Exception:
            return default
    return default


def start_job(
    sdb: Session,
    *,
    job_name: str,
    source: str = "celery",
    task_id: Optional[str] = None,
    triggered_by: str = "system",
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    commit: bool = True,
) -> int:
    ensure_job_registry_schema(sdb)
    started = utcnow()
    result = sdb.execute(
        BACKGROUND_JOB_RUNS.insert().values(
            job_name=str(job_name or "").strip() or "unknown_job",
            source=str(source or "celery").strip() or "celery",
            task_id=str(task_id or "").strip() or None,
            triggered_by=str(triggered_by or "system").strip() or "system",
            status="RUNNING",
            correlation_id=(str(correlation_id or "").strip() or get_correlation_id(default="") or None),
            payload_json=_dump(payload or {}),
            started_at=started,
        )
    )
    run_id = int(getattr(result, "inserted_primary_key", [0])[0] or 0)
    if commit:
        sdb.commit()
    return run_id


def finish_job(
    sdb: Session,
    *,
    run_id: int,
    ok: bool,
    result_payload: Optional[Dict[str, Any]] = None,
    error_text: str = "",
    commit: bool = True,
) -> bool:
    ensure_job_registry_schema(sdb)
    rid = int(run_id or 0)
    if rid <= 0:
        return False

    row = sdb.execute(
        select(BACKGROUND_JOB_RUNS.c.started_at).where(BACKGROUND_JOB_RUNS.c.id == rid).limit(1)
    ).first()
    started_at = row[0] if row else None
    ended = utcnow()
    duration_ms = None
    if started_at is not None:
        try:
            duration_ms = max(0, int((ended - started_at).total_seconds() * 1000))
        except Exception:
            duration_ms = None

    sdb.execute(
        update(BACKGROUND_JOB_RUNS)
        .where(BACKGROUND_JOB_RUNS.c.id == rid)
        .values(
            status="DONE" if ok else "ERROR",
            ended_at=ended,
            duration_ms=duration_ms,
            result_json=_dump(result_payload or {}),
            error_text=str(error_text or "")[:4000] or None,
        )
    )
    if commit:
        sdb.commit()
    return True


def run_job(
    sdb: Session,
    *,
    job_name: str,
    source: str = "celery",
    task_id: Optional[str] = None,
    triggered_by: str = "system",
    payload: Optional[Dict[str, Any]] = None,
    fn,
) -> Dict[str, Any]:
    run_id = start_job(
        sdb,
        job_name=job_name,
        source=source,
        task_id=task_id,
        triggered_by=triggered_by,
        payload=payload,
        commit=True,
    )
    try:
        result = fn()
        finish_job(
            sdb,
            run_id=run_id,
            ok=True,
            result_payload=result if isinstance(result, dict) else {"result": result},
            commit=True,
        )
        return {
            "job_run_id": run_id,
            "ok": True,
            "result": result,
        }
    except Exception as exc:
        finish_job(
            sdb,
            run_id=run_id,
            ok=False,
            result_payload={},
            error_text=str(exc),
            commit=True,
        )
        raise


def list_jobs(
    sdb: Session,
    *,
    limit: int = 200,
    status: str = "",
    job_name: str = "",
) -> List[Dict[str, Any]]:
    ensure_job_registry_schema(sdb)
    q = select(BACKGROUND_JOB_RUNS)
    if str(status or "").strip():
        q = q.where(BACKGROUND_JOB_RUNS.c.status == str(status).strip().upper())
    if str(job_name or "").strip():
        q = q.where(BACKGROUND_JOB_RUNS.c.job_name == str(job_name).strip())

    rows = sdb.execute(q.order_by(BACKGROUND_JOB_RUNS.c.id.desc()).limit(max(1, min(int(limit or 200), 4000)))).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "job_name": r["job_name"],
                "source": r["source"],
                "task_id": r["task_id"],
                "triggered_by": r["triggered_by"],
                "status": r["status"],
                "correlation_id": r["correlation_id"],
                "payload": _load(r["payload_json"], {}),
                "result": _load(r["result_json"], {}),
                "error_text": r["error_text"],
                "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
            }
        )
    return out


def summary(sdb: Session, *, limit: int = 120) -> Dict[str, Any]:
    ensure_job_registry_schema(sdb)
    total = int(sdb.execute(select(func.count()).select_from(BACKGROUND_JOB_RUNS)).scalar() or 0)
    by_status = sdb.execute(
        select(BACKGROUND_JOB_RUNS.c.status, func.count())
        .group_by(BACKGROUND_JOB_RUNS.c.status)
        .order_by(func.count().desc())
    ).all()
    by_job = sdb.execute(
        select(BACKGROUND_JOB_RUNS.c.job_name, func.count())
        .group_by(BACKGROUND_JOB_RUNS.c.job_name)
        .order_by(func.count().desc())
    ).all()

    latest = list_jobs(sdb, limit=limit)
    durations = [int(r.get("duration_ms") or 0) for r in latest if r.get("duration_ms") is not None]
    avg_duration = round(sum(durations) / float(len(durations)), 2) if durations else 0.0
    p95_duration = 0
    if durations:
        ordered = sorted(durations)
        idx = int(round((len(ordered) - 1) * 0.95))
        p95_duration = int(ordered[max(0, min(idx, len(ordered) - 1))])

    return {
        "total": total,
        "por_estado": [{"status": str(k or ""), "total": int(v)} for k, v in by_status],
        "por_job": [{"job_name": str(k or ""), "total": int(v)} for k, v in by_job],
        "latencia_ms": {"avg": avg_duration, "p95": p95_duration},
        "latest": latest,
    }
