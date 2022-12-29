"""Owned-process kill/resume protocol on a small synthetic fixture (not the TLC source)."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from tests.test_core import source_fixture

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_recovery.py"
spec = importlib.util.spec_from_file_location("verify_recovery", SCRIPT)
verify_recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify_recovery)


def _rows():
    start = datetime(2024, 1, 1, 8)
    rows = []
    for index in range(40):
        pickup = start + timedelta(days=index % 5, minutes=index)
        rows.append(
            {
                "lpep_pickup_datetime": pickup if index != 33 else datetime(2023, 12, 31, 23),
                "lpep_dropoff_datetime": pickup + timedelta(minutes=0 if index % 9 == 0 else 7),
                "PULocationID": 1 + index % 2,
                "DOLocationID": 2,
                "trip_distance": 0.0 if index % 11 == 0 else 1.5,
                "total_amount": -3.25 if index % 7 == 0 else 12.5,
                "passenger_count": None if index % 6 == 0 else 1,
            }
        )
    return rows


def test_sigkill_after_committed_checkpoint_resumes_to_identical_output(tmp_path):
    source = source_fixture(tmp_path / "raw", _rows())
    report = tmp_path / "verification" / "recovery.json"
    result = verify_recovery.verify(source, report, batch_size=7, require_pinned=False)
    assert json.loads(report.read_text()) == result
    assert result["killed_after_checkpoint_cursor"] == 7
    assert result["checks"] == {
        "child_killed_by_sigkill": True,
        "matches_uninterrupted_baseline": True,
        "checkpoint_inside_source": True,
        "row_count_conserved": True,
        "attempt_history_retained": True,
        "resumed_from_committed_cursor": True,
        "published_once": True,
    }
    assert result["counters"]["rejected"] == 1 and result["counters"]["warning_rows"] > 0
    assert result["attempts"][0][3] == "worker process interrupted"
    assert result["pinned_tlc_source"] is False and "frozen_tlc_counts" not in result["checks"]


def test_real_source_mode_refuses_unpinned_input_before_spawning(tmp_path):
    source = source_fixture(tmp_path / "raw")
    with pytest.raises(SystemExit, match="pinned TLC hashes"):
        verify_recovery.verify(source, tmp_path / "report.json")
    assert not (tmp_path / "report.json").exists()


def test_source_hash_refuses_symlinks_and_oversized_files(tmp_path):
    original = tmp_path / "original"
    original.write_bytes(b"bounded source")
    link = tmp_path / "link"
    link.symlink_to(original)
    assert verify_recovery._bounded_hash(link) is None
    with original.open("wb") as stream:
        stream.truncate(verify_recovery.MAX_BYTES + 1)
    assert verify_recovery._bounded_hash(original) is None
