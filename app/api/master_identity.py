from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.services.master_identity_flow import (
    PATIENT_IDENTITY_LINKS,
    PATIENT_IDENTITY_EVENTS,
    PATIENT_MASTER_IDENTITY,
    backfill_master_identity,
    ensure_master_identity_schema,
    get_master_identity_journey,
    get_master_identity_snapshot,
    list_master_identity_conflicts,
    master_identity_operational_stats,
    resolve_master_identity_conflict,
)


router = APIRouter(tags=["master-identity"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _get_surgical_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_surgical_db()


@router.get("/api/master-identity/{nss}", response_class=JSONResponse)
def api_master_identity_get(
    nss: str,
    include_links: bool = True,
    links_limit: int = 200,
    db: Session = Depends(_get_db),
):
    ensure_master_identity_schema(db)
    payload = get_master_identity_snapshot(
        db,
        nss=nss,
        include_links=bool(include_links),
        links_limit=max(1, min(int(links_limit), 2000)),
    )
    status_code = 200 if payload.get("ok") else 404
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/master-identity/stats/resumen", response_class=JSONResponse)
def api_master_identity_stats(db: Session = Depends(_get_db)):
    ensure_master_identity_schema(db)
    total_master = int(db.execute(select(func.count(PATIENT_MASTER_IDENTITY.c.id))).scalar() or 0)
    total_links = int(db.execute(select(func.count(PATIENT_IDENTITY_LINKS.c.id))).scalar() or 0)
    total_events = int(db.execute(select(func.count(PATIENT_IDENTITY_EVENTS.c.id))).scalar() or 0)
    conflictos = int(
        db.execute(
            select(func.count(PATIENT_MASTER_IDENTITY.c.id)).where(
                PATIENT_MASTER_IDENTITY.c.conflicto_identidad.is_(True)
            )
        ).scalar()
        or 0
    )
    return JSONResponse(
        content={
            "status": "ok",
            "total_master_identity": total_master,
            "total_links": total_links,
            "total_events": total_events,
            "conflictos_identidad": conflictos,
        }
    )


@router.get("/api/master-identity/{nss}/journey", response_class=JSONResponse)
def api_master_identity_journey(
    nss: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 1000,
    db: Session = Depends(_get_db),
):
    ensure_master_identity_schema(db)
    payload = get_master_identity_journey(
        db,
        nss=nss,
        from_date=from_date,
        to_date=to_date,
        limit=max(1, min(int(limit), 10000)),
    )
    status_code = 200 if payload.get("ok") else 404
    return JSONResponse(status_code=status_code, content=payload)


@router.get("/api/master-identity/stats/operativo", response_class=JSONResponse)
def api_master_identity_stats_operativo(
    months: int = 24,
    top_n: int = 20,
    db: Session = Depends(_get_db),
):
    ensure_master_identity_schema(db)
    payload = master_identity_operational_stats(
        db,
        months=max(1, min(int(months), 120)),
        top_n=max(1, min(int(top_n), 200)),
    )
    return JSONResponse(content=payload)


@router.get("/admin/master-identity/conflictos", response_class=JSONResponse)
def admin_master_identity_conflictos(
    limit: int = 200,
    db: Session = Depends(_get_db),
):
    ensure_master_identity_schema(db)
    payload = list_master_identity_conflicts(
        db,
        limit=max(1, min(int(limit), 5000)),
    )
    return JSONResponse(content=payload)


@router.post("/admin/master-identity/conflictos/{master_id}/resolver", response_class=JSONResponse)
def admin_master_identity_conflicto_resolver(
    master_id: int,
    resolver: str = "system",
    nota: str = "",
    db: Session = Depends(_get_db),
):
    ensure_master_identity_schema(db)
    payload = resolve_master_identity_conflict(
        db,
        master_id=int(master_id),
        resolver=resolver,
        nota=nota,
    )
    status = 200 if payload.get("ok") else 404
    return JSONResponse(status_code=status, content=payload)


@router.post("/admin/master-identity/backfill", response_class=JSONResponse)
def admin_master_identity_backfill(
    limit_per_source: Optional[int] = None,
    db: Session = Depends(_get_db),
    sdb: Session = Depends(_get_surgical_db),
):
    from app.core.app_context import main_proxy as m

    limit = int(limit_per_source) if limit_per_source is not None else 200000
    limit = max(100, min(limit, 500000))
    summary = backfill_master_identity(
        db,
        m,
        sdb=sdb,
        limit_per_source=limit,
    )
    return JSONResponse(content=summary)
