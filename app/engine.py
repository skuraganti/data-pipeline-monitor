"""Single-worker, checkpointed Relay processing engine."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import stat
import threading
from collections import Counter, defaultdict
from pathlib import Path
from time import time
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from app.db import Database
from app.quality import PIPELINE_VERSION, REQUIRED_COLUMNS, RULES_VERSION, evaluate

TRIP_NAME = "green_2024-01.parquet"
LOOKUP_NAME = "taxi_zone_lookup.csv"
MANIFEST_NAME = "manifest.json"
MAX_BYTES = 10 * 1024 * 1024
MAX_ROWS = 100_000


class PermanentSourceError(Exception):
    """A source defect that must not be automatically retried."""


class SimulatedOperationalError(Exception):
    """A deliberately injected retryable operational failure."""


class WorkerStopping(Exception):
    """The worker stopped cooperatively after a committed checkpoint."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Engine:
    def __init__(
        self,
        db_path: str | Path,
        source_dir: str | Path,
        *,
        clock: Callable[[], float] | None = None,
        batch_size: int = 5000,
        scan_seconds: int = 300,
        retry_seconds: int = 5,
    ):
        if not 5 <= scan_seconds <= 86400:
            raise ValueError("scan interval must be 5..86400 seconds")
        if not 1 <= retry_seconds <= 300:
            raise ValueError("retry interval must be 1..300 seconds")
        self.db = Database(db_path)
        self.source_dir = Path(source_dir).resolve()
        self.clock = clock or time
        if type(batch_size) is not int or not 1 <= batch_size <= 10000:
            raise ValueError("batch_size must be 1..10000")
        self.batch_size = batch_size
        self.retry_seconds = retry_seconds
        self.scan_seconds = scan_seconds
        if self.db.value("schedule_initialized") == "0":
            self.db.set_values({"scan_interval": str(scan_seconds), "schedule_initialized": "1"})
        self.scan_seconds = int(self.db.value("scan_interval"))
        self.busy = False
        self.last_error: str | None = None
        self.unexpected_death = False
        self._stop = threading.Event()
        self._mutex = threading.Lock()
        self._lifecycle = threading.Condition()
        self._thread: threading.Thread | None = None
        self._lock_file = None
        self._active_operations = 0

    @property
    def alive(self) -> bool:
        with self._lifecycle:
            return bool(self._thread and self._thread.is_alive())

    def _release_lock_locked(self) -> None:
        if self._lock_file is None or self._thread is not None or self._active_operations:
            return
        fcntl.flock(self._lock_file, fcntl.LOCK_UN)
        self._lock_file.close()
        self._lock_file = None

    def _acquire_lock_locked(self) -> bool:
        if self._lock_file is not None:
            return True
        lock_path = Path(f"{self.db.path}.worker.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return False
        self._lock_file = handle
        try:
            self._recover()
        except BaseException:
            self._release_lock_locked()
            raise
        return True

    def _acquire_lock(self) -> bool:
        with self._lifecycle:
            return self._acquire_lock_locked()

    def _enter_operation(self) -> bool:
        with self._lifecycle:
            already_owned = self._lock_file is not None
            if not self._acquire_lock_locked():
                return False
            self._active_operations += 1
            return not already_owned

    def _leave_operation(self, local_owner: bool) -> None:
        with self._lifecycle:
            self._active_operations -= 1
            if local_owner:
                self._release_lock_locked()
            self._lifecycle.notify_all()

    def _recover(self) -> None:
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            running = connection.execute(
                "SELECT * FROM runs WHERE state='running' ORDER BY id"
            ).fetchall()
            for run in running:
                if run["attempts"] >= run["max_attempts"]:
                    self._exhaust_locked(
                        connection, run, now, "retry attempts exhausted during recovery"
                    )
                    continue
                connection.execute(
                    "UPDATE runs SET state='queued',error=? WHERE id=?",
                    ("worker restarted after interrupted attempt", run["id"]),
                )
                connection.execute(
                    "UPDATE attempts SET state='interrupted',finished_at=?,error=? "
                    "WHERE run_id=? AND cycle=? AND number=? AND state='running'",
                    (
                        now,
                        "worker process interrupted",
                        run["id"],
                        run["retry_cycle"],
                        run["attempts"],
                    ),
                )
                connection.execute(
                    "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
                    (run["id"], now, "recovered", "recovered interrupted run", run["cursor"]),
                )
            exhausted = connection.execute(
                "SELECT * FROM runs WHERE state IN ('queued','retry_wait') AND att"
                "empts>=max_attempts"
            ).fetchall()
            for run in exhausted:
                self._exhaust_locked(
                    connection, run, now, "retry attempts exhausted during recovery"
                )

    @staticmethod
    def _exhaust_locked(connection, run, now: float, message: str) -> None:
        connection.execute(
            "UPDATE runs SET state='failed',finished_at=?,next_retry=NULL,error=? WHERE id=?",
            (now, message, run["id"]),
        )
        connection.execute(
            "UPDATE attempts SET state='failed',finished_at=?,error=? "
            "WHERE run_id=? AND state IN ('running','retry_wait','queued')",
            (now, message, run["id"]),
        )
        connection.execute(
            "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
            (run["id"], now, "failed", message, run["cursor"]),
        )
        connection.execute(
            "INSERT INTO alerts(run_id,message,created_at,simulated) VALUES (?,?,?,?)",
            (run["id"], message, now, run["simulated"]),
        )

    def start(self) -> None:
        with self._lifecycle:
            if self._thread is not None:
                if self._thread.is_alive():
                    return
                self._thread = None
            if not self._acquire_lock_locked():
                raise RuntimeError("another Relay worker owns the database lock")
            self._stop.clear()
            self.unexpected_death = False
            self._thread = threading.Thread(target=self._loop, name="relay-engine", daemon=False)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lifecycle:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._lifecycle:
            while self._active_operations:
                self._lifecycle.wait()
            if self._thread is None:
                self._release_lock_locked()

    def _loop(self) -> None:
        try:
            while not self._stop.wait(0.2):
                self.tick()
        except BaseException as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.unexpected_death = True
        finally:
            with self._lifecycle:
                if self._thread is threading.current_thread():
                    self._thread = None
                self._lifecycle.notify_all()
                self._release_lock_locked()

    def _claim_scan(self, now: float) -> bool:
        with self.db.transaction(immediate=True) as connection:
            enabled = connection.execute(
                "SELECT value FROM settings WHERE key='schedule_enabled'"
            ).fetchone()["value"]
            if enabled != "1":
                return False
            interval = int(
                connection.execute(
                    "SELECT value FROM settings WHERE key='scan_interval'"
                ).fetchone()["value"]
            )
            raw_next = connection.execute(
                "SELECT value FROM settings WHERE key='next_scan'"
            ).fetchone()["value"]
            if raw_next and now < float(raw_next):
                return False
            connection.executemany(
                "UPDATE settings SET value=? WHERE key=?",
                ((str(now), "last_scan"), (str(now + interval), "next_scan")),
            )
            self.scan_seconds = interval
            return True

    def tick(self) -> None:
        now = self.clock()
        if self._claim_scan(now):
            try:
                self.scan()
                self.db.set_values({"last_scan_error": ""})
                self.last_error = None
            except PermanentSourceError as exc:
                self.last_error = str(exc)
                self.db.set_values({"last_scan_error": str(exc)})
            except Exception as exc:
                self.last_error = f"scheduled scan failed: {exc}"
                self.db.set_values({"last_scan_error": self.last_error})
        self.process_once()

    def _validated_sources(self) -> dict:
        paths = {name: self.source_dir / name for name in (TRIP_NAME, LOOKUP_NAME, MANIFEST_NAME)}
        if any(
            not path.is_file()
            or path.is_symlink()
            or not stat.S_ISREG(path.stat().st_mode)
            or path.parent != self.source_dir
            for path in paths.values()
        ):
            raise PermanentSourceError("source snapshot unavailable")
        for path in paths.values():
            try:
                if path.stat().st_size > MAX_BYTES:
                    raise PermanentSourceError(f"source file exceeds 10MiB: {path.name}")
            except OSError as exc:
                raise PermanentSourceError(f"unable to inspect source file: {path.name}") from exc
        try:
            manifest_items = json.loads(paths[MANIFEST_NAME].read_text(encoding="utf-8"))
            if not isinstance(manifest_items, list) or len(manifest_items) != 2:
                raise ValueError("manifest must list two source files")
            if any(not isinstance(item, dict) for item in manifest_items):
                raise ValueError("manifest entries must be objects")
            names = [item.get("name") for item in manifest_items]
            if set(names) != {TRIP_NAME, LOOKUP_NAME} or len(set(names)) != 2:
                raise ValueError("manifest contains unexpected source names")
            manifest = {item["name"]: item for item in manifest_items}
        except (OSError, UnicodeError, ValueError, TypeError, KeyError) as exc:
            raise PermanentSourceError("invalid source manifest") from exc
        hashes = {name: _sha256(paths[name]) for name in (TRIP_NAME, LOOKUP_NAME)}
        for name, digest in hashes.items():
            item = manifest[name]
            if item.get("sha256") != digest or item.get("bytes") != paths[name].stat().st_size:
                raise PermanentSourceError(f"checksum or size mismatch: {name}")
        parquet = self._validate_parquet(paths[TRIP_NAME])
        zones = self._read_zones(paths[LOOKUP_NAME])
        return {
            "paths": paths,
            "manifest": manifest,
            "hashes": hashes,
            "rows": parquet.metadata.num_rows,
            "zones": zones,
        }

    @staticmethod
    def _validate_parquet(path: Path):
        try:
            parquet = pq.ParquetFile(path)
            schema = parquet.schema_arrow
            if parquet.metadata.num_rows > MAX_ROWS:
                raise PermanentSourceError("trip source exceeds row limit")
            missing = REQUIRED_COLUMNS - set(schema.names)
            if missing:
                raise PermanentSourceError(
                    f"trip schema missing columns: {', '.join(sorted(missing))}"
                )

            def integer(field):
                return pa.types.is_integer(field)

            def numeric(field):
                return pa.types.is_integer(field) or pa.types.is_floating(field)

            expected = {
                "lpep_pickup_datetime": lambda field: (
                    pa.types.is_timestamp(field) and field.tz is None
                ),
                "lpep_dropoff_datetime": lambda field: (
                    pa.types.is_timestamp(field) and field.tz is None
                ),
                "PULocationID": integer,
                "DOLocationID": integer,
                "passenger_count": integer,
                "trip_distance": numeric,
                "total_amount": numeric,
            }
            if any(not predicate(schema.field(name).type) for name, predicate in expected.items()):
                raise PermanentSourceError("trip schema has unsupported physical types")
            return parquet
        except PermanentSourceError:
            raise
        except Exception as exc:
            raise PermanentSourceError("invalid parquet source") from exc

    @staticmethod
    def _read_zones(path: Path) -> dict[int, str]:
        zones: dict[int, str] = {}
        try:
            with path.open(newline="", encoding="utf-8-sig") as source:
                reader = csv.DictReader(source)
                if not reader.fieldnames or not {"LocationID", "Borough"}.issubset(
                    reader.fieldnames
                ):
                    raise PermanentSourceError("zone lookup missing required columns")
                for row in reader:
                    location = int(row["LocationID"])
                    if location in zones:
                        raise PermanentSourceError("duplicate lookup LocationID")
                    borough = row["Borough"]
                    if not isinstance(borough, str) or not borough:
                        raise PermanentSourceError("invalid lookup borough")
                    zones[location] = borough
        except PermanentSourceError:
            raise
        except (OSError, UnicodeError, csv.Error, ValueError, KeyError, TypeError) as exc:
            raise PermanentSourceError("invalid zone lookup") from exc
        if len(zones) > MAX_ROWS:
            raise PermanentSourceError("lookup exceeds row limit")
        return zones

    def scan(self) -> dict:
        data = self._validated_sources()
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            for name in (TRIP_NAME, LOOKUP_NAME):
                item = data["manifest"][name]
                rows = data["rows"] if name == TRIP_NAME else len(data["zones"])
                connection.execute(
                    "INSERT OR IGNORE INTO sources(name,sha256,bytes,rows,url,retrieve"
                    "d_at,registered_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        name,
                        data["hashes"][name],
                        item["bytes"],
                        rows,
                        item.get("url"),
                        item.get("retrieved_at"),
                        now,
                    ),
                )
            identity = ":".join(
                (
                    data["hashes"][TRIP_NAME],
                    data["hashes"][LOOKUP_NAME],
                    PIPELINE_VERSION,
                    RULES_VERSION,
                    "2024-01",
                )
            )
            old = connection.execute(
                "SELECT id FROM runs WHERE identity=? AND simulated=0", (identity,)
            ).fetchone()
            if old:
                setting = connection.execute(
                    "SELECT value FROM settings WHERE key='unchanged_scans'"
                ).fetchone()
                unchanged = int(setting["value"]) + 1
                connection.execute(
                    "UPDATE settings SET value=? WHERE key='unchanged_scans'", (str(unchanged),)
                )
                return {"run_id": old["id"], "created": False, "reason": "source unchanged"}
            cursor = connection.execute(
                "INSERT INTO runs(state,trigger,simulated,fault_mode,source_sha,lo"
                "okup_sha,partition,identity,created_at,total_rows) "
                "VALUES ('queued','scan',0,NULL,?,?,?,?,?,?)",
                (
                    data["hashes"][TRIP_NAME],
                    data["hashes"][LOOKUP_NAME],
                    "2024-01",
                    identity,
                    now,
                    data["rows"],
                ),
            )
            run_id = cursor.lastrowid
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,0)",
                (run_id, now, "queued", "immutable source registered"),
            )
        self.last_error = None
        return {"run_id": run_id, "created": True, "reason": "new immutable source"}

    def create_experiment(self, fault_mode: str) -> dict:
        if fault_mode not in {"once_after_checkpoint", "always_after_checkpoint"}:
            raise ValueError("invalid fault mode")
        data = self._validated_sources()
        now = self.clock()
        identity = f"experiment:{fault_mode}:{now}:{os.getpid()}"
        with self.db.transaction(immediate=True) as connection:
            cursor = connection.execute(
                "INSERT INTO runs(state,trigger,simulated,fault_mode,source_sha,lo"
                "okup_sha,partition,identity,created_at,total_rows) "
                "VALUES ('queued','experiment',1,?,?,?,?,?,?,?)",
                (
                    fault_mode,
                    data["hashes"][TRIP_NAME],
                    data["hashes"][LOOKUP_NAME],
                    "2024-01",
                    identity,
                    now,
                    data["rows"],
                ),
            )
            run_id = cursor.lastrowid
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,0)",
                (run_id, now, "queued", f"simulated experiment: {fault_mode}"),
            )
        return self.run(run_id)

    def _claim_run(self, now: float):
        with self.db.transaction(immediate=True) as connection:
            exhausted = connection.execute(
                "SELECT * FROM runs WHERE state IN ('queued','retry_wait') AND att"
                "empts>=max_attempts"
            ).fetchall()
            for run in exhausted:
                self._exhaust_locked(connection, run, now, "retry attempts exhausted before claim")
            row = connection.execute(
                "SELECT * FROM runs WHERE (state='queued' OR (state='retry_wait' A"
                "ND next_retry<=?)) "
                "AND attempts<max_attempts ORDER BY created_at,id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            attempt = row["attempts"] + 1
            connection.execute(
                "UPDATE runs SET state='running',started_at=COALESCE(started_at,?)"
                ",attempts=?,next_retry=NULL WHERE id=?",
                (now, attempt, row["id"]),
            )
            connection.execute(
                "INSERT INTO attempts(run_id,cycle,number,started_at,state,resume_"
                "cursor) VALUES (?,?,?,?,?,?)",
                (row["id"], row["retry_cycle"], attempt, now, "running", row["cursor"]),
            )
            return dict(row) | {"attempts": attempt}

    def process_once(self) -> bool:
        if not self._mutex.acquire(blocking=False):
            return False
        local_owner = False
        entered = False
        try:
            local_owner = self._enter_operation()
            if not local_owner and self._lock_file is None:
                return False
            entered = True
            if self._stop.is_set():
                return False
            run = self._claim_run(self.clock())
            if run is None:
                return False
            self.busy = True
            try:
                self._process(run["id"])
            except WorkerStopping:
                self._pause(run["id"])
            except PermanentSourceError as exc:
                self.last_error = str(exc)
                self._fail(run["id"], str(exc), permanent=True)
            except SimulatedOperationalError as exc:
                self._fail(run["id"], str(exc), permanent=False)
            except Exception as exc:
                self.last_error = f"unexpected processing failure: {exc}"
                self._fail(run["id"], self.last_error, permanent=False)
            finally:
                self.busy = False
            return True
        finally:
            if entered:
                self._leave_operation(local_owner)
            self._mutex.release()

    def _process(self, run_id: int) -> None:
        run = self.db.row("SELECT * FROM runs WHERE id=?", (run_id,))
        data = self._validated_sources()
        if (
            run["source_sha"] != data["hashes"][TRIP_NAME]
            or run["lookup_sha"] != data["hashes"][LOOKUP_NAME]
        ):
            raise PermanentSourceError("registered source checksum no longer matches")
        parquet = self._validate_parquet(data["paths"][TRIP_NAME])
        if run["fault_mode"] == "always_after_checkpoint" and run["cursor"] >= run["total_rows"]:
            self._fault_event(run_id)
            raise SimulatedOperationalError(
                "simulated always_after_checkpoint fault at resume boundary"
            )
        offset = 0
        batches = iter(parquet.iter_batches(batch_size=self.batch_size))
        while True:
            try:
                batch = next(batches)
            except StopIteration:
                break
            except Exception as exc:
                raise PermanentSourceError("unable to iterate parquet source") from exc
            end = offset + batch.num_rows
            if end <= run["cursor"]:
                offset = end
                continue
            try:
                rows = batch.to_pylist()
            except Exception as exc:
                raise PermanentSourceError("unable to decode parquet batch") from exc
            start = max(0, run["cursor"] - offset)
            self._commit_batch(run_id, offset + start, rows[start:], data["zones"])
            run = self.db.row("SELECT * FROM runs WHERE id=?", (run_id,))
            if run["fault_mode"] == "always_after_checkpoint":
                self._fault_event(run_id)
                raise SimulatedOperationalError(
                    "simulated always_after_checkpoint fault after checkpoint"
                )
            if run["fault_mode"] == "once_after_checkpoint" and not run["fault_fired"]:
                self._fault_event(run_id, fired=True)
                raise SimulatedOperationalError(
                    "simulated once_after_checkpoint fault after checkpoint"
                )
            if self._stop.is_set():
                raise WorkerStopping()
            offset = end
        run = self.db.row("SELECT * FROM runs WHERE id=?", (run_id,))
        verified = self._validated_sources()
        if (
            run["source_sha"] != verified["hashes"][TRIP_NAME]
            or run["lookup_sha"] != verified["hashes"][LOOKUP_NAME]
        ):
            raise PermanentSourceError("source changed before completion")
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            if not run["simulated"]:
                publication = connection.execute(
                    "SELECT singleton FROM publications WHERE singleton=1"
                ).fetchone()
                if publication:
                    connection.execute(
                        "UPDATE publications SET run_id=?,published_at=? WHERE singleton=1",
                        (run_id, now),
                    )
                else:
                    connection.execute(
                        "INSERT INTO publications(singleton,run_id,published_at) VALUES (1,?,?)",
                        (run_id, now),
                    )
            connection.execute(
                "UPDATE runs SET state='succeeded',finished_at=?,published=?,error=NULL WHERE id=?",
                (now, 0 if run["simulated"] else 1, run_id),
            )
            connection.execute(
                "UPDATE attempts SET state='succeeded',finished_at=? WHERE run_id="
                "? AND state='running'",
                (now, run_id),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) SELECT id,?,?,?"
                ",cursor FROM runs WHERE id=?",
                (
                    now,
                    "experiment_complete" if run["simulated"] else "published",
                    "isolated experiment completed"
                    if run["simulated"]
                    else "complete aggregate atomically published",
                    run_id,
                ),
            )

    def _fault_event(self, run_id: int, fired: bool = False) -> None:
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE runs SET fault_fired=1 WHERE id=?"
                if fired
                else "UPDATE runs SET fault_fired=fault_fired WHERE id=?",
                (run_id,),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) SELECT id,?,'si"
                "mulated_fault',?,cursor FROM runs WHERE id=?",
                (now, "simulated fault injected after checkpoint", run_id),
            )

    def _commit_batch(
        self, run_id: int, first_index: int, rows: list[dict], zones: dict[int, str]
    ) -> None:
        accepted = rejected = warning_rows = 0
        counts: Counter[tuple[str, str]] = Counter()
        daily: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
        examples: list[tuple] = []
        quarantine: list[tuple] = []
        for relative, row in enumerate(rows):
            reject, warnings, detail = evaluate(row, zones)
            codes = reject + warnings
            for code in reject:
                counts[(code, "error")] += 1
            for code in warnings:
                counts[(code, "warning")] += 1
            row_index = first_index + relative
            if reject:
                rejected += 1
                quarantine.append(
                    (
                        run_id,
                        row_index,
                        json.dumps(codes),
                        json.dumps(detail["observed"], allow_nan=False),
                    )
                )
            else:
                accepted += 1
                warning_rows += bool(warnings)
                pickup = row["lpep_pickup_datetime"]
                key = (pickup.date().isoformat(), zones[row["PULocationID"]])
                daily[key][0] += 1
                daily[key][1] += detail["net_cents"]
                daily[key][2] += bool(warnings)
            if codes:
                examples.append(
                    (
                        run_id,
                        row_index,
                        json.dumps(codes),
                        "error" if reject else "warning",
                        json.dumps(detail["observed"], allow_nan=False),
                    )
                )
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            for (day, borough), values in daily.items():
                connection.execute(
                    "INSERT INTO staging_daily(run_id,day,borough,trips,net_cents,warn"
                    "ing_rows) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(run_id,day,borough) DO UPDATE SET trips=trips+exclude"
                    "d.trips,net_cents=net_cents+excluded.net_cents,warning_rows=warni"
                    "ng_rows+excluded.warning_rows",
                    (run_id, day, borough, *values),
                )
            for (code, severity), count in counts.items():
                connection.execute(
                    "INSERT INTO quality(run_id,code,severity,count) VALUES (?,?,?,?) "
                    "ON CONFLICT(run_id,code) DO UPDATE SET count=count+excluded.count",
                    (run_id, code, severity, count),
                )
            current_examples = connection.execute(
                "SELECT count(*) FROM examples WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            connection.executemany(
                "INSERT INTO examples(run_id,row_index,codes,severity,observed) VALUES (?,?,?,?,?)",
                examples[: max(0, 50 - current_examples)],
            )
            connection.executemany(
                "INSERT OR IGNORE INTO quarantine(run_id,row_index,codes,observed)"
                " VALUES (?,?,?,?)",
                quarantine,
            )
            new_cursor = first_index + len(rows)
            connection.execute(
                "UPDATE runs SET cursor=?,accepted=accepted+?,rejected=rejected+?,"
                "warning_rows=warning_rows+? WHERE id=?",
                (new_cursor, accepted, rejected, warning_rows, run_id),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
                (run_id, now, "checkpoint", f"committed {len(rows)} source rows", new_cursor),
            )

    def _pause(self, run_id: int) -> None:
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            run = connection.execute("SELECT cursor FROM runs WHERE id=?", (run_id,)).fetchone()
            connection.execute(
                "UPDATE runs SET state='queued',next_retry=NULL,error=? WHERE id=?",
                ("worker stopped after committed checkpoint", run_id),
            )
            connection.execute(
                "UPDATE attempts SET state='interrupted',finished_at=?,error=? WHE"
                "RE run_id=? AND state='running'",
                (now, "worker stopped cooperatively", run_id),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
                (
                    run_id,
                    now,
                    "stopped",
                    "worker stopped after committed checkpoint",
                    run["cursor"],
                ),
            )

    def _fail(self, run_id: int, message: str, *, permanent: bool) -> None:
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            run = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            retry = not permanent and run["attempts"] < run["max_attempts"]
            state = "retry_wait" if retry else "failed"
            due = now + self.retry_seconds * (2 ** (run["attempts"] - 1)) if retry else None
            connection.execute(
                "UPDATE runs SET state=?,next_retry=?,error=?,finished_at=? WHERE id=?",
                (state, due, message, None if retry else now, run_id),
            )
            connection.execute(
                "UPDATE attempts SET state=?,finished_at=?,error=? WHERE run_id=? "
                "AND state='running'",
                (state, now, message, run_id),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
                (run_id, now, state, message, run["cursor"]),
            )
            if not retry:
                connection.execute(
                    "INSERT INTO alerts(run_id,message,created_at,simulated) VALUES (?,?,?,?)",
                    (run_id, message, now, run["simulated"]),
                )

    def configure_schedule(self, enabled: bool, interval_seconds: int) -> dict:
        if type(enabled) is not bool or type(interval_seconds) is not int:
            raise ValueError("schedule values must be strict boolean and integer")
        if not 5 <= interval_seconds <= 86400:
            raise ValueError("schedule interval must be 5..86400")
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            connection.executemany(
                "UPDATE settings SET value=? WHERE key=?",
                (
                    ("1" if enabled else "0", "schedule_enabled"),
                    (str(interval_seconds), "scan_interval"),
                    (str(now + interval_seconds) if enabled else "", "next_scan"),
                    ("1", "schedule_initialized"),
                ),
            )
        self.scan_seconds = interval_seconds
        return self.schedule()

    def schedule(self) -> dict:
        def number(key):
            value = self.db.value(key)
            return float(value) if value else None

        return {
            "enabled": self.db.value("schedule_enabled") == "1",
            "interval_seconds": int(self.db.value("scan_interval")),
            "last_scan": number("last_scan"),
            "next_scan": number("next_scan"),
            "unchanged_scans": int(self.db.value("unchanged_scans")),
            "last_scan_error": self.db.value("last_scan_error") or None,
        }

    def manual_retry(self, run_id: int) -> dict:
        data = self._validated_sources()
        now = self.clock()
        with self.db.transaction(immediate=True) as connection:
            run = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if not run:
                raise KeyError(run_id)
            if (
                run["source_sha"] != data["hashes"][TRIP_NAME]
                or run["lookup_sha"] != data["hashes"][LOOKUP_NAME]
            ):
                raise PermanentSourceError("registered source checksum no longer matches")
            if run["state"] not in {"failed", "retry_wait"}:
                raise ValueError("run is not retryable")
            connection.execute(
                "UPDATE runs SET state='queued',attempts=0,retry_cycle=retry_cycle"
                "+1,next_retry=NULL,finished_at=NULL,error=NULL WHERE id=?",
                (run_id,),
            )
            connection.execute(
                "INSERT INTO events(run_id,at,kind,message,cursor) VALUES (?,?,?,?,?)",
                (run_id, now, "manual_retry", "manual retry cycle requested", run["cursor"]),
            )
        return self.run(run_id)

    def run(self, run_id: int) -> dict:
        row = self.db.row("SELECT * FROM runs WHERE id=?", (run_id,))
        if row is None:
            raise KeyError(run_id)
        for field in ("simulated", "published"):
            row[field] = bool(row[field])
        row.pop("identity", None)
        row["retry_cycle"] = int(row["retry_cycle"])
        return row
