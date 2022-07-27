# Memorable demonstration

1. Start `sh stack.sh up` and open http://127.0.0.1:8282. The first scheduled scan ingests the real source and publishes56,549trips; two outside-month pickups remain quarantined and6,640accepted rows carry policy warnings. Compare the independent raw-file baseline and recorded source hashes.
2. Scan again. The source version is unchanged, no extra run/output is created, and the no-op counter increases. Set a5-second schedule briefly to observe an actual periodic scan; then restore300seconds.
3. Inspect normal run1. Follow physical rows88/90 through the outside-month rule, review real distance/nullable/signed-total warnings, and follow source/lookup hashes into validation and daily borough metrics. Original meter times are not relabeledUTC.
4. Choose **Fail once, then recover**. The simulation badge distinguishes an injected operational fault from observed source-quality issues. At cursor5000, the attempt fails after commit. The next attempt resumes there, completes with exact baseline results and keeps normal publication isolated.
5. Choose **Exhaust automatic retries**. Three attempts exhaust; inspect retained history and acknowledge the local alert. An explicit manual retry cycle retains previous cycles rather than pretending earlier failures vanished.
6. Run `uv run python verify_crash.py` to actually kill a separately owned local worker after a real checkpoint and recover the same182groups. This is controlled fault injection over observed data. Restart this Docker project to confirm durable queue/config/evidence/publication; stop it afterward to conserve resources.

No source values are invented or modified in the main demo. Any test fixtures, malformed-source probes and failure/slowdown injections are labeled separately. No outbound notifications or repairs to unrelated applications occur.
