# Memorable demonstration

For a fresh checkout, first run `uv sync --frozen` and `uv run python scripts/bootstrap.py` as described in [README.md](README.md). Then follow the walkthrough below.

1. Start `sh stack.sh up` and open http://127.0.0.1:8282. The first scheduled scan ingests the real source and publishes56,549trips; two outside-month pickups remain quarantined and6,640accepted rows carry policy warnings. Compare the independent raw-file baseline and recorded source hashes.
2. Scan again. The source version is unchanged, no extra run/output is created, and the no-op counter increases. Set a5-second schedule briefly to observe an actual periodic scan; then restore300seconds.
3. Inspect normal run1. Follow physical rows88/90 through the outside-month rule, review real distance/nullable/signed-total warnings, and follow source/lookup hashes into validation and daily borough metrics. Original meter times are not relabeledUTC.
4. Choose **Fail once, then recover**. The simulation badge distinguishes an injected operational fault from observed source-quality issues. At cursor5000, the attempt fails after commit. The next attempt resumes there, completes with exact baseline results and keeps normal publication isolated.
5. Choose **Exhaust automatic retries**. Three attempts exhaust; inspect retained history and acknowledge the local alert. An explicit manual retry cycle retains previous cycles rather than pretending earlier failures vanished.
6. Run `uv run pytest tests/test_core.py::test_restart_recovers_running_cursor_without_duplicate_output` for the fixture-based restart/cursor regression. It seeds interrupted state and reopens the engine; it does not kill a live worker. Separately, record the current run, schedule, evidence and publication, run `sh stack.sh stop` followed by `sh stack.sh up`, and confirm that state persists. Stop this stack afterward to conserve resources.

No source values are invented or modified in the main demo. Any test fixtures, malformed-source probes and failure/slowdown injections are labeled separately. No outbound notifications or repairs to unrelated applications occur.

For the real-data controlled process-kill check, see [Recovery checks](docs/RECOVERY.md). For row-versus-flag counts and warning interpretation, see [Quality notes](docs/QUALITY.md).
