from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import random

from fastapi.testclient import TestClient

from app.db.sessions import clinical_session
from app.services.inpatient_labs_notes_service import (
    DailyNoteConflictError,
    ack_alert,
    add_lab,
    add_tag,
    list_daily_notes,
    list_labs,
    list_tags,
    resolve_alert,
    upsert_daily_note,
)
from main import app


def test_inpatient_labs_notes_actions_service_roundtrip() -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    uniq = int(now.timestamp()) + random.randint(1000, 9999)
    consulta_id = 990000 + (uniq % 900000)
    hospitalizacion_id = 880000 + (uniq % 900000)
    with clinical_session() as db:
        lab = add_lab(
            db,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            collected_at=now,
            test_name="creatinine",
            value_num=1.42,
            unit="mg/dL",
        )
        assert lab["test_name"] == "creatinine"
        labs = list_labs(db, hospitalizacion_id=hospitalizacion_id)
        assert any(int(x["id"]) == int(lab["id"]) for x in labs)

        note = upsert_daily_note(
            db,
            hospitalizacion_id=hospitalizacion_id,
            consulta_id=consulta_id,
            note_date=date.today(),
            author_user_id="pytest",
            problem_list_json={"dx": ["LITIASIS"]},
            plan_by_problem_json={"dx": "manejo"},
            devices_snapshot_json={"jj": "si"},
            io_summary_json={"uop_ml": 1200},
            symptoms_json={"fiebre": False, "hematuria": False},
            events_pending_json={"labs": ["urocultivo"]},
            free_text="Evolución estable",
            is_final=False,
            upsert=False,
            consulta_patient_id="1234567890",
            note_type="EVOLUCION",
        )
        assert int(note["version"]) == 1

        conflict = False
        try:
            upsert_daily_note(
                db,
                hospitalizacion_id=hospitalizacion_id,
                consulta_id=consulta_id,
                note_date=date.today(),
                author_user_id="pytest",
                problem_list_json={},
                plan_by_problem_json={},
                devices_snapshot_json={},
                io_summary_json={},
                symptoms_json={},
                events_pending_json={},
                free_text="conflict",
                is_final=False,
                upsert=False,
                consulta_patient_id="1234567890",
                note_type="EVOLUCION",
            )
        except DailyNoteConflictError:
            conflict = True
        assert conflict is True

        note_v2 = upsert_daily_note(
            db,
            hospitalizacion_id=hospitalizacion_id,
            consulta_id=consulta_id,
            note_date=date.today(),
            author_user_id="pytest",
            problem_list_json={"dx": ["LITIASIS"]},
            plan_by_problem_json={"dx": "alta probable"},
            devices_snapshot_json={"jj": "si"},
            io_summary_json={"uop_ml": 1400},
            symptoms_json={"fiebre": False, "hematuria": False},
            events_pending_json={},
            free_text="Actualización",
            is_final=True,
            upsert=True,
            consulta_patient_id="1234567890",
            note_type="EVOLUCION",
        )
        assert int(note_v2["version"]) >= 2
        rows = list_daily_notes(db, hospitalizacion_id=hospitalizacion_id)
        assert len(rows) >= 1

        ack = ack_alert(db, alert_id=77, ack_by="pytest")
        assert int(ack["alert_id"]) == 77
        resolved = resolve_alert(
            db,
            alert_id=77,
            resolved_by="pytest",
            resolution_reason="handled",
            action_taken_json={"accion": "reevaluado"},
        )
        assert resolved["resolution_reason"] == "handled"

        tag = add_tag(
            db,
            consulta_id=consulta_id,
            hospitalizacion_id=hospitalizacion_id,
            tag_type="DX",
            tag_value="LITIASIS",
            laterality="L",
            severity="MODERADA",
        )
        assert tag["tag_type"] == "DX"
        tags = list_tags(db, hospitalizacion_id=hospitalizacion_id)
        assert any(int(x["id"]) == int(tag["id"]) for x in tags)
        db.commit()


def test_inpatient_labs_notes_actions_api_routes_reachable() -> None:
    client = TestClient(app)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    consulta_id = 993002
    hosp_id = 993002

    r_lab = client.post(
        "/api/inpatient/labs",
        json={
            "consulta_id": consulta_id,
            "hospitalizacion_id": hosp_id,
            "collected_at": now.isoformat(),
            "test_name": "hb",
            "value_num": 12.4,
            "unit": "g/dL",
        },
    )
    assert r_lab.status_code in (200, 401, 422)

    r_labs = client.get(
        "/api/inpatient/labs",
        params={
            "consulta_id": consulta_id,
            "hospitalizacion_id": hosp_id,
            "from": (now - timedelta(hours=2)).isoformat(),
            "to": (now + timedelta(hours=2)).isoformat(),
        },
    )
    assert r_labs.status_code in (200, 401)

    r_note = client.post(
        "/api/inpatient/daily-note?upsert=1",
        json={
            "hospitalizacion_id": hosp_id,
            "consulta_id": consulta_id,
            "note_date": date.today().isoformat(),
            "author_user_id": "pytest",
            "problem_list_json": {"dx": ["LITIASIS"]},
            "plan_by_problem_json": {"dx": "control"},
            "devices_snapshot_json": {},
            "io_summary_json": {},
            "symptoms_json": {"fiebre": False},
            "events_pending_json": {},
            "free_text": "API smoke",
            "is_final": False,
            "consulta_patient_id": "1234567890",
        },
    )
    assert r_note.status_code in (200, 401, 422)

    r_notes = client.get("/api/inpatient/daily-notes", params={"hospitalizacion_id": hosp_id})
    assert r_notes.status_code in (200, 401)

    r_ack = client.post(f"/api/inpatient/alerts/{88}/ack", json={"ack_by": "pytest"})
    assert r_ack.status_code in (200, 401, 422)

    r_res = client.post(
        f"/api/inpatient/alerts/{88}/resolve",
        json={"resolved_by": "pytest", "resolution_reason": "handled", "action_taken_json": {"ok": True}},
    )
    assert r_res.status_code in (200, 401, 422)

    r_tag = client.post(
        "/api/inpatient/tags",
        json={
            "consulta_id": consulta_id,
            "hospitalizacion_id": hosp_id,
            "tag_type": "DX",
            "tag_value": "LITIASIS",
            "laterality": "L",
            "severity": "MODERADA",
        },
    )
    assert r_tag.status_code in (200, 401, 422)

    r_tags = client.get("/api/inpatient/tags", params={"hospitalizacion_id": hosp_id})
    assert r_tags.status_code in (200, 401)
