from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.db.sessions import clinical_session
from app.services.inpatient_time_series_service import (
    add_io_block,
    add_vitals_ts,
    list_io_blocks,
    list_vitals,
)
from main import app


def test_inpatient_vitals_io_service_roundtrip() -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    consulta_id = 991001
    with clinical_session() as db:
        vitals = add_vitals_ts(
            db,
            consulta_id=consulta_id,
            recorded_at=now,
            heart_rate=88,
            sbp=120,
            dbp=78,
            spo2=97.0,
            resp_rate=18,
            o2_flow_lpm=2.0,
            pain_score_0_10=4,
            source="pytest",
        )
        assert vitals["consulta_id"] == consulta_id
        assert vitals["map"] is not None

        listed_vitals = list_vitals(db, consulta_id=consulta_id, date_from=now - timedelta(minutes=1), date_to=now + timedelta(minutes=1))
        assert any(int(x["id"]) == int(vitals["id"]) for x in listed_vitals)

        block = add_io_block(
            db,
            consulta_id=consulta_id,
            interval_start=now,
            interval_end=now + timedelta(hours=8),
            intake_ml=1500.0,
            urine_output_ml=900.0,
            weight_kg=80.5,
            height_cm=173.0,
        )
        assert round(float(block["net_balance_ml"]), 2) == 600.0

        listed_blocks = list_io_blocks(
            db,
            consulta_id=consulta_id,
            date_from=now - timedelta(minutes=1),
            date_to=now + timedelta(hours=9),
        )
        assert any(int(x["id"]) == int(block["id"]) for x in listed_blocks)
        db.commit()


def test_inpatient_vitals_io_api_routes_reachable() -> None:
    client = TestClient(app)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    consulta_id = 991002

    r_vitals = client.post(
        "/api/inpatient/vitals",
        json={
            "consulta_id": consulta_id,
            "recorded_at": now.isoformat(),
            "heart_rate": 82,
            "sbp": 118,
            "dbp": 76,
            "pain_score_0_10": 3,
        },
    )
    assert r_vitals.status_code in (200, 401, 422)

    r_vitals_list = client.get("/api/inpatient/vitals", params={"consulta_id": consulta_id})
    assert r_vitals_list.status_code in (200, 401)

    r_io = client.post(
        "/api/inpatient/io-block",
        json={
            "consulta_id": consulta_id,
            "interval_start": now.isoformat(),
            "interval_end": (now + timedelta(hours=6)).isoformat(),
            "intake_ml": 1200,
            "urine_output_ml": 700,
        },
    )
    assert r_io.status_code in (200, 401, 422)

    r_io_list = client.get("/api/inpatient/io-blocks", params={"consulta_id": consulta_id})
    assert r_io_list.status_code in (200, 401)
