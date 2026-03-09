from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.core.validators import normalize_nss_10
from app.schemas.inpatient_notes import (
    EpisodeCreate,
    InpatientDailyNoteCreate,
)
from app.services.hospitalizacion.notes import (
    close_episode,
    create_or_get_active_episode,
    get_active_episode_by_patient,
    get_daily_note,
    get_episode,
    list_daily_notes,
    list_patient_episodes,
    list_patient_daily_notes,
    summarize_patient_episodes,
    upsert_daily_note,
)

router = APIRouter(prefix="/api/v1/hospitalization", tags=["hospitalization-notes"])


def _get_db():
    from app.core.app_context import main_proxy as m

    yield from m.get_db()


def _normalize_nss_or_422(value: str) -> str:
    nss = normalize_nss_10(value)
    if len(nss) != 10:
        raise HTTPException(status_code=422, detail="NSS inválido: se requieren 10 dígitos.")
    return nss


@router.post("/episodes", response_class=JSONResponse)
def api_hospitalization_create_episode(payload: EpisodeCreate, db: Session = Depends(_get_db)):
    from app.core.app_context import main_proxy as m

    nss = _normalize_nss_or_422(payload.patient_id)
    try:
        episode = create_or_get_active_episode(
            db,
            m,
            patient_id=nss,
            consulta_id=payload.consulta_id,
            hospitalizacion_id=payload.hospitalizacion_id,
            service=payload.service,
            location=payload.location,
            shift=payload.shift,
            author_user_id=payload.author_user_id or "api_v1",
            started_on=payload.started_on,
            source_route=payload.source_route or "/api/v1/hospitalization/episodes",
            metrics=payload.metrics or {},
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "episode": episode})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible crear episodio: {exc}") from exc


@router.get("/episodes/{episode_id}", response_class=JSONResponse)
def api_hospitalization_get_episode(episode_id: int, db: Session = Depends(_get_db)):
    episode = get_episode(db, episode_id=episode_id)
    if not episode:
        raise HTTPException(status_code=404, detail="Episodio no encontrado.")
    return JSONResponse(content={"episode": episode})


@router.post("/episodes/{episode_id}/close", response_class=JSONResponse)
def api_hospitalization_close_episode(
    episode_id: int,
    ended_on: Optional[date] = None,
    actor: str = "api_v1",
    db: Session = Depends(_get_db),
):
    try:
        episode = close_episode(
            db,
            episode_id=episode_id,
            ended_on=ended_on,
            author_user_id=actor or "api_v1",
        )
        if episode is None:
            raise HTTPException(status_code=404, detail="No se encontró episodio activo para cerrar.")
        db.commit()
        return JSONResponse(content={"status": "ok", "episode": episode})
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible cerrar episodio: {exc}") from exc


@router.get("/patients/{patient_id}/active-episode", response_class=JSONResponse)
def api_hospitalization_active_episode(patient_id: str, db: Session = Depends(_get_db)):
    nss = _normalize_nss_or_422(patient_id)
    episode = get_active_episode_by_patient(db, patient_id=nss)
    return JSONResponse(content={"active_episode": episode})


@router.get("/patients/{patient_id}/episodes", response_class=JSONResponse)
def api_hospitalization_patient_episodes(
    patient_id: str,
    include_summary: bool = True,
    limit: int = 300,
    db: Session = Depends(_get_db),
):
    from app.core.app_context import main_proxy as m

    nss = _normalize_nss_or_422(patient_id)
    episodes = list_patient_episodes(db, patient_id=nss, limit=limit)
    payload = {"patient_id": nss, "episodes": episodes}
    if include_summary:
        payload["summary"] = summarize_patient_episodes(db, m, patient_id=nss, episodes=episodes)
    return JSONResponse(content=payload)


@router.post("/episodes/{episode_id}/daily-notes", response_class=JSONResponse)
def api_hospitalization_upsert_daily_note(
    episode_id: int,
    payload: InpatientDailyNoteCreate,
    db: Session = Depends(_get_db),
):
    try:
        note = upsert_daily_note(
            db,
            episode_id=episode_id,
            note_date=payload.note_date,
            note_type=payload.note_type,
            service=payload.service or "UROLOGIA",
            location=payload.location or "",
            shift=payload.shift or "",
            author_user_id=payload.author_user_id or "api_v1",
            cie10_codigo=payload.cie10_codigo or "",
            diagnostico=payload.diagnostico or "",
            vitals=payload.vitals or {},
            labs=payload.labs or {},
            devices=payload.devices or {},
            events=payload.events or {},
            payload=payload.payload or {},
            note_text=payload.note_text or "",
            status=payload.status,
            source_route="/api/v1/hospitalization/episodes/{episode_id}/daily-notes",
            mirror_legacy=bool(payload.mirror_legacy),
        )
        db.commit()
        return JSONResponse(content={"status": "ok", "note": note})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"No fue posible guardar la nota: {exc}") from exc


@router.get("/episodes/{episode_id}/daily-notes", response_class=JSONResponse)
def api_hospitalization_list_daily_notes(
    episode_id: int,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 500,
    db: Session = Depends(_get_db),
):
    episode = get_episode(db, episode_id=episode_id)
    if not episode:
        raise HTTPException(status_code=404, detail="Episodio no encontrado.")
    notes = list_daily_notes(db, episode_id=episode_id, date_from=date_from, date_to=date_to, limit=limit)
    return JSONResponse(content={"episode": episode, "total_notes": len(notes), "notes": notes})


@router.get("/daily-notes/{note_id}", response_class=JSONResponse)
def api_hospitalization_get_daily_note(note_id: int, db: Session = Depends(_get_db)):
    note = get_daily_note(db, note_id=note_id)
    if not note:
        raise HTTPException(status_code=404, detail="Nota no encontrada.")
    return JSONResponse(content={"note": note})


@router.get("/patients/{patient_id}/daily-notes", response_class=JSONResponse)
def api_hospitalization_patient_daily_notes(
    patient_id: str,
    limit: int = 1000,
    db: Session = Depends(_get_db),
):
    nss = _normalize_nss_or_422(patient_id)
    notes = list_patient_daily_notes(db, patient_id=nss, limit=limit)
    return JSONResponse(content={"patient_id": nss, "total_notes": len(notes), "notes": notes})
