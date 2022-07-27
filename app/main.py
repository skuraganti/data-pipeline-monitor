"""Relay FastAPI application and stable operator API."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, StrictBool

from app.engine import Engine, PermanentSourceError
from app.quality import PIPELINE_VERSION, RULES_VERSION
from app.security import protect

ROOT = Path(__file__).resolve().parent.parent


class ExperimentRequest(BaseModel):
    fault_mode: str


class ScheduleRequest(BaseModel):
    enabled: StrictBool
    interval_seconds: int = Field(ge=5, le=86400, strict=True)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, "1" if default else "0")
    if value not in {"0", "1"}:
        raise ValueError(f"{name} must be exactly 0 or 1")
    return value == "1"


def create_app(
    db_path: str | Path | None = None,
    source_dir: str | Path | None = None,
    auto_start: bool | None = None,
    clock: Callable[[], float] | None = None,
    batch_size: int = 5000,
) -> FastAPI:
    enabled = _env_bool("RELAY_AUTO_START", True) if auto_start is None else auto_start
    scan_seconds = int(os.getenv("RELAY_SCAN_SECONDS", "300"))
    retry_seconds = int(os.getenv("RELAY_RETRY_SECONDS", "5"))
    if not 5 <= scan_seconds <= 86400:
        raise ValueError("RELAY_SCAN_SECONDS must be 5..86400")
    if not 1 <= retry_seconds <= 300:
        raise ValueError("RELAY_RETRY_SECONDS must be 1..300")
    engine = Engine(
        db_path or os.getenv("RELAY_DB", ROOT / "data/relay.sqlite"),
        source_dir or os.getenv("RELAY_SOURCE_DIR", ROOT / "data/raw"),
        clock=clock,
        batch_size=batch_size,
        scan_seconds=scan_seconds,
        retry_seconds=retry_seconds,
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if enabled:
            engine.start()
        yield
        engine.stop()

    application = FastAPI(title="Relay", version="1", lifespan=lifespan)
    application.state.engine = engine
    application.state.worker_enabled = enabled
    protect(application)

    @application.get("/health")
    def health():
        try:
            engine.db.row("SELECT 1 AS ok")
        except Exception as exc:
            raise HTTPException(503, "database unavailable") from exc
        if enabled and (engine.unexpected_death or not engine.alive):
            raise HTTPException(503, "configured worker is not alive")
        return {"status": "ok", "database": True, "worker": engine.alive if enabled else None}

    @application.get("/api/state")
    def state():
        now = engine.clock()
        runs = [
            engine.run(row["id"])
            for row in engine.db.rows("SELECT id FROM runs ORDER BY id DESC LIMIT 30")
        ]
        sources = engine.db.rows(
            "SELECT id,name,sha256,bytes,rows,url,retrieved_at FROM sources ORDER BY id"
        )
        alerts = engine.db.rows(
            "SELECT id,run_id,message,created_at,acknowledged,simulated FROM a"
            "lerts ORDER BY id DESC"
        )
        for alert in alerts:
            alert["acknowledged"] = bool(alert["acknowledged"])
            alert["simulated"] = bool(alert["simulated"])
        publication = engine.db.row(
            "SELECT run_id,published_at FROM publications WHERE singleton=1"
        )
        published = None
        if publication:
            run = engine.run(publication["run_id"])
            output_rows = engine.db.row(
                "SELECT count(*) AS n FROM staging_daily WHERE run_id=?", (run["id"],)
            )["n"]
            net = engine.db.row(
                "SELECT COALESCE(sum(net_cents),0) AS n FROM staging_daily WHERE run_id=?",
                (run["id"],),
            )["n"]
            published = {
                key: run[key]
                for key in ("source_sha", "lookup_sha", "accepted", "rejected", "warning_rows")
            }
            published.update(
                {
                    "run_id": run["id"],
                    "net_cents": net,
                    "output_rows": output_rows,
                    "published_at": publication["published_at"],
                }
            )
        return {
            "now": now,
            "worker": {
                "enabled": enabled,
                "alive": engine.alive,
                "busy": engine.busy,
                "last_error": engine.last_error,
            },
            "schedule": _schedule(engine),
            "sources": sources,
            "runs": runs,
            "published": published,
            "alerts": alerts,
            "versions": {"pipeline": PIPELINE_VERSION, "rules": RULES_VERSION},
        }

    @application.get("/api/runs/{run_id}")
    def run_detail(run_id: int):
        try:
            run = engine.run(run_id)
        except KeyError as exc:
            raise HTTPException(404, "run not found") from exc
        attempts = engine.db.rows(
            "SELECT cycle,number,started_at,finished_at,state,error,resume_cursor "
            "FROM attempts WHERE run_id=? ORDER BY cycle,number",
            (run_id,),
        )
        events = engine.db.rows(
            "SELECT at,kind,message,cursor FROM events WHERE run_id=? ORDER BY id", (run_id,)
        )
        quality = engine.db.rows(
            "SELECT code,severity,count FROM quality WHERE run_id=? ORDER BY severity,code",
            (run_id,),
        )
        examples = engine.db.rows(
            "SELECT row_index,codes,'error' severity,observed FROM quarantine WHERE run_id=? "
            "ORDER BY row_index LIMIT 50",
            (run_id,),
        )
        examples += engine.db.rows(
            "SELECT row_index,codes,severity,observed FROM examples WHERE run_id=? "
            "AND severity='warning' ORDER BY row_index LIMIT ?",
            (run_id, 50 - len(examples)),
        )
        for example in examples:
            example["codes"] = json.loads(example["codes"])
            example["observed"] = json.loads(example["observed"])
        quarantine_count = engine.db.row(
            "SELECT count(*) AS n FROM quarantine WHERE run_id=?", (run_id,)
        )["n"]
        return {
            "run": run,
            "attempts": attempts,
            "events": events,
            "quality": quality,
            "examples": examples,
            "quarantined": quarantine_count,
        }

    @application.get("/api/runs/{run_id}/lineage")
    def lineage(run_id: int):
        try:
            run = engine.run(run_id)
        except KeyError as exc:
            raise HTTPException(404, "run not found") from exc
        aggregate_state = "published" if run["published"] else "pending"
        quarantine = engine.db.row(
            "SELECT count(*) AS n FROM quarantine WHERE run_id=?", (run_id,)
        )["n"]
        return {
            "run_id": run_id,
            "nodes": [
                {
                    "id": "trips",
                    "label": "Green trips",
                    "kind": "source",
                    "sha256": run["source_sha"],
                    "rows": run["total_rows"],
                },
                {
                    "id": "zones",
                    "label": "Taxi zones",
                    "kind": "source",
                    "sha256": run["lookup_sha"],
                },
                {
                    "id": "validated",
                    "label": "Validated rows",
                    "kind": "quality",
                    "rows": run["accepted"],
                    "state": run["state"],
                },
                {
                    "id": "quarantine",
                    "label": "Quarantined rows",
                    "kind": "quality",
                    "rows": quarantine,
                    "state": run["state"],
                },
                {
                    "id": "daily",
                    "label": "Daily borough metrics",
                    "kind": "aggregate",
                    "state": "isolated"
                    if run["simulated"] and run["state"] == "succeeded"
                    else aggregate_state,
                },
            ],
            "edges": [
                {
                    "from": "trips",
                    "to": "quarantine",
                    "operation": "reject",
                    "fields": ["quality rules"],
                },
                {
                    "from": "trips",
                    "to": "validated",
                    "operation": "validate",
                    "fields": ["physical row index"],
                },
                {
                    "from": "zones",
                    "to": "validated",
                    "operation": "join",
                    "fields": ["PULocationID", "DOLocationID"],
                },
                {
                    "from": "validated",
                    "to": "daily",
                    "operation": "aggregate",
                    "fields": ["pickup day", "pickup borough"],
                },
            ],
        }

    @application.get("/api/daily")
    def daily(run_id: int | None = Query(default=None)):
        if run_id is None:
            publication = engine.db.row("SELECT run_id FROM publications WHERE singleton=1")
            if not publication:
                return []
            run_id = publication["run_id"]
        run = engine.db.row("SELECT state,published FROM runs WHERE id=?", (run_id,))
        if not run:
            raise HTTPException(404, "run not found")
        if run["state"] != "succeeded":
            raise HTTPException(409, "run has no complete metrics")
        return engine.db.rows(
            "SELECT day,borough,trips,net_cents,warning_rows FROM staging_dail"
            "y WHERE run_id=? ORDER BY day,borough",
            (run_id,),
        )

    @application.post("/api/scan")
    def scan():
        try:
            return engine.scan()
        except PermanentSourceError as exc:
            raise HTTPException(422, str(exc)) from exc

    @application.post("/api/experiments")
    def experiment(request: ExperimentRequest):
        try:
            return engine.create_experiment(request.fault_mode)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except PermanentSourceError as exc:
            raise HTTPException(422, str(exc)) from exc

    @application.post("/api/runs/{run_id}/retry")
    def retry(run_id: int):
        try:
            return engine.manual_retry(run_id)
        except KeyError as exc:
            raise HTTPException(404, "run not found") from exc
        except PermanentSourceError as exc:
            raise HTTPException(422, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @application.post("/api/schedule")
    def schedule(request: ScheduleRequest):
        try:
            return engine.configure_schedule(request.enabled, request.interval_seconds)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @application.post("/api/alerts/{alert_id}/ack")
    def acknowledge(alert_id: int):
        with engine.db.transaction(immediate=True) as connection:
            cursor = connection.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))
            if cursor.rowcount == 0:
                raise HTTPException(404, "alert not found")
        return {"id": alert_id, "acknowledged": True}

    static = ROOT / "app/static"
    if static.is_dir():
        application.mount("/static", StaticFiles(directory=static), name="static")
        index = static / "index.html"
        if index.is_file():

            @application.get("/")
            def root():
                return FileResponse(index)

    return application


def _schedule(engine: Engine) -> dict:
    return engine.schedule()


async def app(scope, receive, send):
    # Uvicorn detects ASGI3; imports/tests do not initialize state or a poller.
    if not hasattr(app, "instance"):
        app.instance = create_app()
    return await app.instance(scope, receive, send)
