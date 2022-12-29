# Recovery checks

Each batch commits its daily aggregates, quality counts, quarantine evidence and cursor in one SQLite transaction. The recovery paths and their evidence are:

| Kind | What happens | Where |
|---|---|---|
| Cooperative restart | `Engine.stop()`: the current batch commits, the attempt becomes `interrupted` (`worker stopped cooperatively`) and the run is queued again | `app/engine.py`; stop/lock lifecycle in `tests/test_reliability.py`; no existing test drives the mid-run cooperative pause |
| Injected exception | **Fail once, then recover** / **Exhaust automatic retries** (`POST /api/experiments`, `once_after_checkpoint` / `always_after_checkpoint`) raise a labelled simulated error after a checkpoint, in an isolated `simulated` run that never publishes | UI, `tests/test_core.py` |
| Seeded crash state | A test writes a `running` run and attempt directly, then reopens the engine | `tests/test_core.py::test_restart_recovers_running_cursor_without_duplicate_output` |
| **Controlled kill** | `scripts/verify_recovery.py` SIGKILLs its own child worker after that child's first committed nonzero checkpoint, then resumes on the same database | below |

The controlled kill is an **intentional fault** injected by the verifier, never a spontaneous incident. The child subclasses `Engine` only to add a marker and a bounded wait after a commit; the production engine has no hook for it.

On restart, the next worker to take the file lock runs `_recover`. It requeues the `running` run, marks its attempt `interrupted` (`worker process interrupted`) and logs a `recovered` event at the committed cursor. The next attempt skips committed rows. Only a complete normal run moves the publication pointer, so an incomplete run cannot publish. The controlled-kill comparison starts with an empty publication table; preservation of an existing report is checked separately by the isolated injected-exception workflow.

## Running it over the real TLC input

```sh
uv sync --frozen
uv run python scripts/bootstrap.py        # acquires the TLC files into data/raw
uv run python scripts/verify_recovery.py  # report: data/verification/recovery.json
```

- **Before running:** it checks `data/raw` against the pinned TLC SHA-256 values and refuses to run (quoting no counts) on a mismatch.
- **Storage:** it reads the source read-only and uses its own temporary databases. It never touches the app database, containers or other processes.
- **Interrupted run:** it waits on a deadline, kills only its `Popen` child, and makes at most two attempts.
- **Comparison:** against an uninterrupted baseline, it compares identity, publication, counters, every daily group, quality counts, quarantine rows and examples. It also checks row-count conservation, exactly one publication event, the attempt history (`interrupted`, then `succeeded` from the killed cursor) and the frozen TLC counts.

`uv run pytest tests/test_recovery_process.py` runs the same kill protocol on a small synthetic fixture, without the real file.
