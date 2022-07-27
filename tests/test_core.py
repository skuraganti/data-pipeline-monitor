from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app.engine import Engine
from app.main import create_app


class Clock:
    def __init__(self):
        self.now = 1_700_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def source_fixture(path, rows=None):
    path.mkdir()
    rows = rows or [
        {
            "lpep_pickup_datetime": datetime(2024, 1, 2, 10),
            "lpep_dropoff_datetime": datetime(2024, 1, 2, 10, 5),
            "PULocationID": 1,
            "DOLocationID": 2,
            "trip_distance": 2.0,
            "total_amount": 10.005,
            "passenger_count": 1,
        },
        {
            "lpep_pickup_datetime": datetime(2024, 1, 2, 11),
            "lpep_dropoff_datetime": datetime(2024, 1, 2, 11),
            "PULocationID": 1,
            "DOLocationID": 2,
            "trip_distance": 0.0,
            "total_amount": -1.235,
            "passenger_count": None,
        },
        {
            "lpep_pickup_datetime": datetime(2023, 12, 31, 23),
            "lpep_dropoff_datetime": datetime(2024, 1, 1),
            "PULocationID": 1,
            "DOLocationID": 2,
            "trip_distance": 1.0,
            "total_amount": 7.0,
            "passenger_count": 2,
        },
    ]
    trip = path / "green_2024-01.parquet"
    lookup = path / "taxi_zone_lookup.csv"
    pq.write_table(pa.Table.from_pylist(rows), trip)
    with lookup.open("w", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        writer.writerows([[1, "Manhattan", "A", "A"], [2, "Queens", "B", "B"]])
    manifest = []
    for file in (trip, lookup):
        content = file.read_bytes()
        manifest.append(
            {
                "name": file.name,
                "url": f"https://example.invalid/{file.name}",
                "retrieved_at": "2026-10-16T00:00:00+00:00",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    (path / "manifest.json").write_text(json.dumps(manifest))
    return path


def test_scan_idempotency_source_fidelity_and_quality(tmp_path):
    source = source_fixture(tmp_path / "raw")
    clock = Clock()
    engine = Engine(tmp_path / "relay.sqlite", source, clock=clock, batch_size=2)
    first = engine.scan()
    second = engine.scan()
    assert first["created"] is True
    assert second == {"run_id": first["run_id"], "created": False, "reason": "source unchanged"}
    assert engine.process_once()
    run = engine.run(first["run_id"])
    assert (run["state"], run["accepted"], run["rejected"], run["warning_rows"]) == (
        "succeeded",
        2,
        1,
        1,
    )
    daily = engine.db.row("SELECT * FROM staging_daily WHERE run_id=?", (run["id"],))
    assert daily["trips"] == 2
    assert daily["net_cents"] == 877
    assert (
        engine.db.row("SELECT count(*) AS n FROM examples WHERE run_id=?", (run["id"],))["n"] == 2
    )
    source_rows = engine.db.rows("SELECT name,sha256,retrieved_at FROM sources ORDER BY name")
    assert source_rows[0]["retrieved_at"] == "2026-10-16T00:00:00+00:00"


def test_retry_resume_and_last_good_publication(tmp_path):
    source = source_fixture(tmp_path / "raw")
    clock = Clock()
    engine = Engine(tmp_path / "relay.sqlite", source, clock=clock, batch_size=2, retry_seconds=1)
    good = engine.scan()["run_id"]
    engine.process_once()
    experiment = engine.create_experiment("once_after_checkpoint")
    engine.process_once()
    failed_once = engine.run(experiment["id"])
    assert failed_once["state"] == "retry_wait"
    assert failed_once["cursor"] == 2
    assert engine.db.row("SELECT run_id FROM publications WHERE singleton=1")["run_id"] == good
    clock.advance(1)
    engine.process_once()
    resumed = engine.run(experiment["id"])
    assert resumed["state"] == "succeeded"
    assert resumed["accepted"] == 2
    assert resumed["rejected"] == 1
    assert resumed["attempts"] == 2


def test_restart_recovers_running_cursor_without_duplicate_output(tmp_path):
    source = source_fixture(tmp_path / "raw")
    clock = Clock()
    db_path = tmp_path / "relay.sqlite"
    first = Engine(db_path, source, clock=clock, batch_size=2)
    run_id = first.scan()["run_id"]
    with first.db.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE runs SET state='running',cursor=2,accepted=2,attempts=1 WHERE id=?", (run_id,)
        )
        connection.execute(
            "INSERT INTO staging_daily VALUES (?,?,?,?,?,?)",
            (run_id, "2024-01-02", "Manhattan", 2, 877, 1),
        )
        connection.execute(
            "INSERT INTO attempts(run_id,cycle,number,started_at,finished_at,s"
            "tate,error,resume_cursor) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (run_id, 0, 1, clock(), None, "running", None, 0),
        )
    restarted = Engine(db_path, source, clock=clock, batch_size=2)
    restarted.process_once()
    run = restarted.run(run_id)
    assert run["state"] == "succeeded"
    assert run["accepted"] == 2
    assert run["rejected"] == 1
    assert (
        restarted.db.row("SELECT trips FROM staging_daily WHERE run_id=?", (run_id,))["trips"] == 2
    )


def test_api_shapes_and_incomplete_metrics_hidden(tmp_path):
    source = source_fixture(tmp_path / "raw")
    app = create_app(
        tmp_path / "relay.sqlite", source, auto_start=False, clock=Clock(), batch_size=2
    )
    with TestClient(app, base_url="http://localhost") as client:
        scanned = client.post("/api/scan", json={}).json()
        assert client.get("/health").status_code == 200
        assert client.get(f"/api/daily?run_id={scanned['run_id']}").status_code == 409
        app.state.engine.process_once()
        state = client.get("/api/state").json()
        assert state["published"]["accepted"] == 2
        detail = client.get(f"/api/runs/{scanned['run_id']}").json()
        assert detail["quality"]
        assert len(detail["examples"]) == 2
        lineage = client.get(f"/api/runs/{scanned['run_id']}/lineage").json()
        assert lineage["nodes"][-1]["state"] == "published"
