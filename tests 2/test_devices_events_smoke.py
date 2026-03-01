from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.db.sessions import clinical_session
from app.services.inpatient_devices_events_service import (
    add_device,
    add_event,
    list_devices,
    list_events,
    update_device,
)
from main import app


def test_devices_events_service_roundtrip() -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    consulta_id = 992001
    with clinical_session() as db:
        device = add_device(
            db,
            consulta_id=consulta_id,
            device_type="JJ_STENT",
            present=True,
            inserted_at=now,
            side="L",
            planned_removal_at=now + timedelta(days=21),
            notes="pytest device",
        )
        assert device["device_type"] == "JJ_STENT"

        patched = update_device(
            db,
            device_id=int(device["id"]),
            notes="updated",
            irrigation=True,
            difficulty="MODERATE",
        )
        assert patched["notes"] == "updated"

        devices = list_devices(db, consulta_id=consulta_id)
        assert any(int(x["id"]) == int(device["id"]) for x in devices)

        event = add_event(
            db,
            consulta_id=consulta_id,
            event_time=now,
            event_type="ABX_STARTED",
            payload={"abx_name": "ceftriaxona", "indication": "urosepsis"},
        )
        assert event["event_type"] == "ABX_STARTED"
        events = list_events(db, consulta_id=consulta_id, date_from=now - timedelta(minutes=1), date_to=now + timedelta(minutes=1))
        assert any(int(x["id"]) == int(event["id"]) for x in events)
        db.commit()


def test_devices_events_api_routes_reachable() -> None:
    client = TestClient(app)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    consulta_id = 992002

    r_dev = client.post(
        "/api/inpatient/devices",
        json={
            "consulta_id": consulta_id,
            "device_type": "FOLEY",
            "inserted_at": now.isoformat(),
            "side": "NA",
        },
    )
    assert r_dev.status_code in (200, 401, 422)

    r_dev_list = client.get("/api/inpatient/devices", params={"consulta_id": consulta_id})
    assert r_dev_list.status_code in (200, 401)

    r_evt = client.post(
        "/api/inpatient/events",
        json={
            "consulta_id": consulta_id,
            "event_time": now.isoformat(),
            "event_type": "ABX_STARTED",
            "payload": {"abx_name": "ceftriaxona"},
        },
    )
    assert r_evt.status_code in (200, 401, 422)

    r_evt_list = client.get(
        "/api/inpatient/events",
        params={"consulta_id": consulta_id, "from": (now - timedelta(hours=1)).isoformat(), "to": (now + timedelta(hours=1)).isoformat()},
    )
    assert r_evt_list.status_code in (200, 401)

