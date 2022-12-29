"""Intentional controlled-kill recovery check over the acquired, read-only TLC source.

The verifier starts its own child worker, SIGKILLs only that child after its first committed
checkpoint, resumes on the same temporary database and compares with an uninterrupted baseline.
It never touches the application database, containers or processes it did not start.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.engine import LOOKUP_NAME, MAX_BYTES, TRIP_NAME, Engine  # noqa: E402

LABEL = "intentional controlled kill of the verifier's own child; not a spontaneous incident"
PINNED = {
    TRIP_NAME: "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    LOOKUP_NAME: "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
}
FROZEN = dict(total_rows=56551, accepted=56549, rejected=2, warning_rows=6640, groups=182)
COMPARED = "identity state published counters daily quality quarantine examples".split()
DEADLINE, HOLD = 300.0, 120.0  # seconds: wait for the checkpoint; child self-exit bound
TABLES = {  # per-run evidence compared between the baseline and the recovered run
    "daily": "day,borough,trips,net_cents,warning_rows FROM staging_daily WHERE run_id=? "
    "ORDER BY day,borough",
    "quality": "code,severity,count FROM quality WHERE run_id=? ORDER BY code",
    "quarantine": "row_index,codes,observed FROM quarantine WHERE run_id=? ORDER BY row_index",
    "examples": "row_index,codes,severity,observed FROM examples WHERE run_id=? ORDER BY row_index",
    "attempts": "number,state,resume_cursor,error FROM attempts WHERE run_id=? ORDER BY number",
}


class PauseAfterFirstCheckpoint(Engine):
    """Child-only wrapper: after the first committed nonzero checkpoint, mark it and wait."""

    marker: Path
    hold_seconds: float

    def _commit_batch(self, run_id, first_index, rows, zones):
        super()._commit_batch(run_id, first_index, rows, zones)
        cursor = self.db.row("SELECT cursor FROM runs WHERE id=?", (run_id,))["cursor"]
        if cursor > 0 and not self.marker.exists():
            staged = self.marker.with_suffix(".tmp")
            staged.write_text(str(cursor))
            os.replace(staged, self.marker)
            time.sleep(self.hold_seconds)  # the parent kills this process here
            raise SystemExit("child was not killed within its hold window")


def _child(db: str, source: str, marker: str, batch_size: int) -> None:
    engine = PauseAfterFirstCheckpoint(db, source, batch_size=batch_size)
    engine.marker, engine.hold_seconds = Path(marker), HOLD
    engine.scan()
    engine.process_once()


def _drain(engine: Engine) -> None:
    for _ in range(5):  # bounded: one normal run, at most a few claims
        if not engine.process_once():
            return
    raise RuntimeError("processing did not settle")


def _snapshot(db: Path) -> dict:
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    try:
        run = connection.execute("SELECT * FROM runs WHERE simulated=0").fetchone()
        rid = run["id"]

        def rows(sql):
            return [list(row) for row in connection.execute(sql, (rid,))]

        published = connection.execute("SELECT run_id FROM publications").fetchall()
        return {
            "identity": run["identity"],
            "state": run["state"],
            "published": bool(run["published"]) and [r[0] for r in published] == [rid],
            "counters": {
                k: run[k] for k in ("cursor", "total_rows", "accepted", "rejected", "warning_rows")
            },
            **{key: rows(f"SELECT {sql}") for key, sql in TABLES.items()},
            "published_events": connection.execute(
                "SELECT count(*) FROM events WHERE run_id=? AND kind='published'", (rid,)
            ).fetchone()[0],
            "recovered_cursors": [
                r[0] for r in rows("SELECT cursor FROM events WHERE run_id=? AND kind='recovered'")
            ],
        }
    finally:
        connection.close()


def _interrupted(db: Path, source: Path, batch_size: int):
    marker, log = db.with_suffix(".checkpoint"), db.with_suffix(".child.log")
    with log.open("wb") as output:
        child = subprocess.Popen(
            [sys.executable, __file__, "--child", str(db), str(source), str(marker)]
            + ["--batch-size", str(batch_size)],
            cwd=ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            end = time.monotonic() + DEADLINE
            while not marker.exists():
                if child.poll() is not None:
                    raise RuntimeError(f"child exited before a checkpoint ({child.returncode})")
                if time.monotonic() > end:
                    raise TimeoutError("no committed checkpoint before the deadline")
                time.sleep(0.05)
            child.kill()  # SIGKILL to this Popen child only
            child.wait(timeout=30)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=30)
    return int(marker.read_text()), child.returncode


def _bounded_hash(path: Path) -> str | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES:
            return None
        digest, total = hashlib.sha256(), 0
        while block := stream.read(65536):
            total += len(block)
            if total > MAX_BYTES:
                return None
            digest.update(block)
        return digest.hexdigest()


def verify(source: Path, report: Path, *, batch_size=5000, require_pinned=True, retries=2):
    source = source.resolve()
    hashes = {}
    for name in PINNED:
        path = source / name
        hashes[name] = _bounded_hash(path)
    pinned = hashes == PINNED
    if require_pinned and not pinned:
        raise SystemExit(f"source does not match the pinned TLC hashes: {hashes}")
    failures = []
    for attempt in range(1, retries + 1):
        work = Path(tempfile.mkdtemp(prefix="relay-recovery-"))
        try:
            baseline_db, recovered_db = work / "baseline.sqlite", work / "recovered.sqlite"
            baseline = Engine(baseline_db, source, batch_size=batch_size)
            baseline.scan()
            _drain(baseline)
            checkpoint, exit_code = _interrupted(recovered_db, source, batch_size)
            _drain(Engine(recovered_db, source, batch_size=batch_size))
            expected, actual = _snapshot(baseline_db), _snapshot(recovered_db)
            break
        except (RuntimeError, TimeoutError, subprocess.TimeoutExpired) as error:
            failures.append(f"attempt {attempt}: {error}")
        finally:
            shutil.rmtree(work, ignore_errors=True)
    else:
        raise SystemExit("recovery verification did not complete: " + "; ".join(failures))
    counters = actual["counters"]
    checks = {
        "child_killed_by_sigkill": exit_code == -signal.SIGKILL,
        "matches_uninterrupted_baseline": all(expected[k] == actual[k] for k in COMPARED),
        "checkpoint_inside_source": 0 < checkpoint < counters["total_rows"],
        "row_count_conserved": sum(r[2] for r in actual["daily"]) + counters["rejected"]
        == counters["total_rows"]
        == counters["cursor"]
        and len(actual["quarantine"]) == counters["rejected"],
        "attempt_history_retained": [a[:3] for a in actual["attempts"]]
        == [[1, "interrupted", 0], [2, "succeeded", checkpoint]],
        "resumed_from_committed_cursor": actual["recovered_cursors"] == [checkpoint],
        "published_once": actual["state"] == "succeeded"
        and actual["published"]
        and actual["published_events"] == 1,
    }
    if pinned:
        observed = {k: counters.get(k, len(actual["daily"])) for k in FROZEN}  # groups: daily
        checks["frozen_tlc_counts"] = observed == FROZEN
    result = {
        "label": LABEL,
        "source_sha256": hashes,
        "pinned_tlc_source": pinned,
        "batch_size": batch_size,
        "killed_after_checkpoint_cursor": checkpoint,
        "counters": counters,
        "daily_groups": len(actual["daily"]),
        "attempts": actual["attempts"],
        "failed_attempts_before_success": failures,
        "checks": checks,
        "passed": all(checks.values()),
    }
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", nargs=3, metavar=("DB", "SOURCE", "MARKER"))
    parser.add_argument("--source", type=Path, default=ROOT / "data/raw")
    parser.add_argument("--report", type=Path, default=ROOT / "data/verification/recovery.json")
    parser.add_argument("--batch-size", type=int, default=5000)
    arguments = parser.parse_args()
    if arguments.child:
        _child(*arguments.child, arguments.batch_size)
        raise SystemExit(0)
    outcome = verify(arguments.source, arguments.report, batch_size=arguments.batch_size)
    print(json.dumps(outcome["checks"], indent=2))
    raise SystemExit(0 if outcome["passed"] else 1)
