"""Independent regressions for reproduced reliability defects, with synthetic faults."""

import hashlib
import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.engine import Engine, PermanentSourceError
from app.main import create_app
from app.quality import cents, evaluate
from tests.test_core import Clock, source_fixture


def rehash(source):
    path = source / "manifest.json"
    entries = json.loads(path.read_text())
    for entry in entries:
        body = (source / entry["name"]).read_bytes()
        entry["sha256"] = hashlib.sha256(body).hexdigest()
        entry["bytes"] = len(body)
    path.write_text(json.dumps(entries))


@pytest.fixture
def context(tmp_path):
    source = source_fixture(tmp_path / "raw")
    clock = Clock()
    engine = Engine(tmp_path / "case.sqlite", source, clock=clock, batch_size=2, retry_seconds=1)
    yield engine, source, clock
    engine.stop()


def test_completed_fault_experiment_cannot_replace_normal_publication(context):
    engine, _, clock = context
    normal = engine.scan()["run_id"]
    engine.process_once()
    experiment = engine.create_experiment("once_after_checkpoint")["id"]
    engine.process_once()
    assert engine.run(experiment)["state"] == "retry_wait"
    clock.advance(1)
    engine.process_once()
    run = engine.run(experiment)
    assert run["state"] == "succeeded" and run["cursor"] == 3
    assert (run["accepted"], run["rejected"], run["warning_rows"]) == (2, 1, 1)
    assert not run["published"]
    assert engine.db.row("SELECT run_id FROM publications")["run_id"] == normal
    assert (
        engine.db.row("SELECT sum(net_cents) n FROM staging_daily WHERE run_id=?", (experiment,))[
            "n"
        ]
        == 877
    )


def test_persistent_fault_exhausts_even_after_single_batch_eof(tmp_path):
    source = source_fixture(tmp_path / "raw")
    clock = Clock()
    engine = Engine(tmp_path / "case.sqlite", source, clock=clock, batch_size=100, retry_seconds=1)
    try:
        identifier = engine.create_experiment("always_after_checkpoint")["id"]
        for delta in [0, 1, 2]:
            clock.advance(delta)
            assert engine.process_once()
        run = engine.run(identifier)
        assert run["state"] == "failed" and run["attempts"] == 3
        assert run["cursor"] == 3 and not run["published"]
        clock.advance(100)
        assert not engine.process_once()
        assert engine.db.row("SELECT count(*) n FROM publications")["n"] == 0
    finally:
        engine.stop()


def test_recovered_exhausted_attempt_never_becomes_attempt_four(context):
    engine, _, clock = context
    identifier = engine.scan()["run_id"]
    with engine.db.transaction(immediate=True) as db:
        db.execute("UPDATE runs SET state='running',attempts=3 WHERE id=?", (identifier,))
        for attempt in [1, 2, 3]:
            db.execute(
                "INSERT INTO attempts(run_id,cycle,number,started_at,state,resume_cursor) "
                "VALUES (?,?,?,?,?,?)",
                (identifier, 0, attempt, clock(), "running" if attempt == 3 else "interrupted", 0),
            )
    engine.process_once()
    run = engine.run(identifier)
    assert run["state"] == "failed" and run["attempts"] == 3
    assert engine.db.row("SELECT max(number) n FROM attempts")["n"] == 3
    assert engine.db.row("SELECT count(*) n FROM alerts")["n"] >= 1


def test_stop_does_not_release_live_worker_lock(context):
    engine, source, _ = context
    assert engine._acquire_lock()

    class StillRunning:
        def join(self, timeout=None):
            pass

        def is_alive(self):
            return True

    engine._thread = StillRunning()
    second = Engine(engine.db.path, source)
    try:
        try:
            engine.stop()
        except (RuntimeError, TimeoutError):
            pass
        assert second._acquire_lock() is False
    finally:
        engine._thread = None
        engine.stop()
        second.stop()


def test_corrupt_parquet_is_permanent_not_worker_crash(context):
    engine, source, _ = context
    (source / "green_2024-01.parquet").write_bytes(b"Not a Parquet file; synthetic corruption")
    rehash(source)
    with pytest.raises(PermanentSourceError):
        engine.scan()
    assert engine.db.row("SELECT count(*) n FROM runs")["n"] == 0


def test_wrong_required_column_types_fail_before_publication(context):
    engine, source, _ = context
    path = source / "green_2024-01.parquet"
    rows = pq.read_table(path).to_pylist()
    for row in rows:
        row["lpep_pickup_datetime"] = row["lpep_pickup_datetime"].isoformat()
    pq.write_table(pa.Table.from_pylist(rows), path)
    rehash(source)
    with pytest.raises(PermanentSourceError):
        engine.scan()
    assert engine.db.row("SELECT count(*) n FROM publications")["n"] == 0


def test_symlinked_source_rejected(context, tmp_path):
    engine, source, _ = context
    original = source / "green_2024-01.parquet"
    target = tmp_path / "external.parquet"
    original.rename(target)
    original.symlink_to(target)
    with pytest.raises(PermanentSourceError):
        engine.scan()


def test_saved_schedule_controls_restart_and_coalesces(context):
    engine, source, clock = context
    engine.db.set_values({"scan_interval": "5", "next_scan": str(clock() + 5)})
    reopened = Engine(engine.db.path, source, clock=clock, scan_seconds=300)
    try:
        assert reopened.scan_seconds == 5
        clock.advance(10000)
        reopened.tick()
        assert float(reopened.db.value("next_scan")) == clock() + 5
        assert reopened.db.row("SELECT count(*) n FROM runs")["n"] == 1
        clock.advance(10000)
        reopened.tick()
        assert reopened.db.row("SELECT count(*) n FROM runs")["n"] == 1
        assert int(reopened.db.value("unchanged_scans")) == 1
    finally:
        reopened.stop()


def test_schedule_reanchors_due_and_rejects_coercion(context):
    engine, source, clock = context
    app = create_app(engine.db.path, source, auto_start=False, clock=clock)
    with TestClient(app, base_url="http://localhost") as client:
        assert (
            client.post(
                "/api/schedule", json={"enabled": "true", "interval_seconds": 5}
            ).status_code
            == 422
        )
        assert (
            client.post("/api/schedule", json={"enabled": True, "interval_seconds": 5}).status_code
            == 200
        )
        schedule = client.get("/api/state").json()["schedule"]
        assert schedule["next_scan"] == clock() + 5


def test_quarantine_retains_all_rows_beyond_display_limit(tmp_path):
    rows = [
        {
            "lpep_pickup_datetime": datetime(2023, 12, 1),
            "lpep_dropoff_datetime": datetime(2023, 12, 1, 0, 2),
            "PULocationID": 1,
            "DOLocationID": 2,
            "trip_distance": 1.0,
            "total_amount": 3.0,
            "passenger_count": 1,
        }
        for _ in range(80)
    ]
    source = source_fixture(tmp_path / "raw", rows)
    engine = Engine(tmp_path / "quarantine.sqlite", source, batch_size=17)
    try:
        identifier = engine.scan()["run_id"]
        engine.process_once()
        assert engine.run(identifier)["rejected"] == 80
        assert engine.db.row("SELECT count(*) n FROM quarantine")["n"] == 80
        assert engine.db.row("SELECT count(DISTINCT row_index) n FROM quarantine")["n"] == 80
    finally:
        engine.stop()


def test_source_change_during_processing_cannot_publish(context):
    engine, source, _ = context
    engine.scan()
    original = engine._commit_batch

    def change(*args, **kwargs):
        original(*args, **kwargs)
        path = source / "taxi_zone_lookup.csv"
        path.write_text(path.read_text().replace("Manhattan", "Brooklyn"))

    engine._commit_batch = change
    engine.process_once()
    assert engine.run(1)["state"] == "failed"
    assert engine.db.row("SELECT count(*) n FROM publications")["n"] == 0


def test_nonfinite_and_huge_amounts_have_inspectable_rejections():
    row = {
        "lpep_pickup_datetime": datetime(2024, 1, 1),
        "lpep_dropoff_datetime": datetime(2024, 1, 1, 0, 1),
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_distance": float("nan"),
        "total_amount": float("inf"),
        "passenger_count": None,
    }
    rejects, _, detail = evaluate(row, {1: "Manhattan", 2: "Queens"})
    assert len(rejects) >= 2
    json.dumps(detail["observed"], allow_nan=False)
    with pytest.raises(ValueError):
        cents(1e308)
    assert cents(10.005) == 1001 and cents(-1.235) == -124


def test_all_http_reads_have_host_and_mutations_have_size_origin_bounds(context):
    engine, source, _ = context
    app = create_app(engine.db.path, source, auto_start=False)
    with TestClient(app, base_url="http://localhost") as client:
        assert client.get("/api/state", headers={"host": "attacker.example"}).status_code == 400
        assert (
            client.post("/api/scan", headers={"origin": "https://attacker.example"}).status_code
            == 403
        )
        assert (
            client.post("/api/scan", content=iter([b"x" * 40000, b"x" * 40000])).status_code == 413
        )


def test_manual_retry_cycle_preserves_and_exposes_attempt_history(context):
    engine, source, clock = context
    engine.batch_size = 100
    identifier = engine.create_experiment("always_after_checkpoint")["id"]
    for delta in [0, 1, 2]:
        clock.advance(delta)
        engine.process_once()
    assert engine.run(identifier)["state"] == "failed"
    retried = engine.manual_retry(identifier)
    assert retried["retry_cycle"] == 1 and retried["attempts"] == 0
    engine.process_once()
    app = create_app(engine.db.path, source, auto_start=False, clock=clock)
    with TestClient(app, base_url="http://localhost") as client:
        history = client.get(f"/api/runs/{identifier}").json()["attempts"]
        assert [x["cycle"] for x in history] == [0, 0, 0, 1]
        assert [x["number"] for x in history] == [1, 2, 3, 1]


def test_invalid_auto_start_flag_rejected_before_db_creation(tmp_path, monkeypatch):
    monkeypatch.setenv("RELAY_AUTO_START", "not-a-boolean")
    path = tmp_path / "never.sqlite"
    with pytest.raises(ValueError):
        create_app(path, tmp_path, auto_start=None)
    assert not path.exists()


def test_import_does_not_create_state(tmp_path):
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    path = tmp_path / "import-must-not-create.sqlite"
    env["RELAY_DB"] = str(path)
    env["RELAY_AUTO_START"] = "0"
    completed = subprocess.run(
        [sys.executable, "-c", "import app.main"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    assert not path.exists()


def test_forward_schema_upgrade_preserves_existing_publication(context):
    engine, source, clock = context
    engine.scan()
    engine.process_once()
    before = engine.db.rows("SELECT * FROM staging_daily ORDER BY day,borough")
    engine.stop()
    # Explicit synthetic version1 prototype: no quarantine table existed there.
    with engine.db.transaction(immediate=True) as db:
        db.execute("DROP TABLE IF EXISTS quarantine")
        db.execute("UPDATE schema_version SET version=1")
    upgraded = Engine(engine.db.path, source, clock=clock)
    try:
        assert upgraded.db.row("SELECT version FROM schema_version")["version"] >= 2
        assert upgraded.db.rows("SELECT * FROM staging_daily ORDER BY day,borough") == before
        assert upgraded.db.row("SELECT run_id FROM publications")["run_id"] == 1
        assert upgraded.db.row("SELECT count(*) n FROM quarantine")["n"] == 1
        assert upgraded.db.row("SELECT row_index FROM quarantine")["row_index"] == 2
    finally:
        upgraded.stop()


def test_transient_checkpoint_write_enters_bounded_retry(context, monkeypatch):
    import sqlite3

    engine, _, clock = context
    identifier = engine.scan()["run_id"]
    original = engine._commit_batch

    def fail_once(*args, **kwargs):
        monkeypatch.setattr(engine, "_commit_batch", original)
        raise sqlite3.OperationalError("synthetic transient checkpoint write failure")

    monkeypatch.setattr(engine, "_commit_batch", fail_once)
    assert engine.process_once()
    assert engine.run(identifier)["state"] == "retry_wait"
    assert engine.run(identifier)["cursor"] == 0
    clock.advance(1)
    assert engine.process_once()
    assert engine.run(identifier)["state"] == "succeeded"
    assert engine.run(identifier)["accepted"] == 2
