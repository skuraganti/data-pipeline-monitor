# Release verification — 2026-10-16

Actual development and release preparation 2026-10-16. This report records checks on the sanitized public source copy before its initial commit. Deployment is checked separately after pushing, without backdated commits. Passing these bounded checks does not guarantee every future feed, model response or workload.

| Check | Observed result |
|---|---|
| Clean source environment | `uv sync --frozen`, project-specific `scripts/bootstrap.py`: passed |
| Native suite | **22 passed** with Python 3.14.8; FastAPI/Starlette emitted a TestClient deprecation warning |
| Lint | `uv run ruff check .`: passed; pinned upstream corpus files excluded from project style rules |
| Docker | Digest-pinned Python 3.13 image built; startup, HTTP health and application page passed |
| Workflow | Pinned 56,551 real TLC rows processed and aggregate published |
| Persistence | Restarted app container; workflow state retained |
| Browser | Isolated Chromium via agent-browser 0.38.1; 6 domain assertions passed; native input/click controls passed |
| Error state | Fixture temporarily unavailable: visible recoverable error; restored fixture loads after reload |
| Responsive layout | 1440×1000 and 390×844 inspected; 390px document width with no page overflow |
| Accessibility | axe-core 4.12.1, WCAG 2 A/AA and 2.1 AA tags: **0 detected violations** |
| Privacy | Candidate-file Gitleaks scan passed; no private home paths, unrelated service IDs, credentials or mutable data included |
| Release inference | **0 model calls**; recorded original model studies remain distinct from release checks |

Browser scenarios: 182 observed aggregate rows; net recorded total formats correctly; borough filter; bounded retries preserve published output; transient fault recovers; reset cancels stale simulation.

Some axe color checks on clipped scroll contents require manual review. Visible desktop/mobile views and the authored foreground/background palette were inspected; the minimum checked ink/muted/link ratio against the declared page/panel backgrounds was 4.57:1. This is a bounded audit, not a universal accessibility certification. Scroll regions are keyboard focusable and tables accompany charts/graphs.

## Corrections and retesting

The net recorded total now stays a formatted USD string instead of converting currency text to NaN. Retry/reset simulation preserves the pinned aggregate. The fixture carries the independently checked trip and lookup hashes.

## Recorded findings and limitations

The Pages queue/retry animation is explicitly simulated over actual captured aggregates. The local worker processes the original 56,551-row TLC file. Two invalid rows and 6,640 warning rows are retained rather than concealed.

Native tests use explicit fixtures, original-source boundary checks and simulated model responses; they do not perform live inference. See [PUBLIC_DEMO_DATA.md](PUBLIC_DEMO_DATA.md) for the curated Pages fixture and checksum, [DATA_SOURCES.md](DATA_SOURCES.md) for publisher/version/rights and [UPSTREAM.md](UPSTREAM.md) for attribution. Original model studies have small samples and cannot establish general performance or costs.

## Reproduce

```sh
uv sync --frozen
uv run python scripts/bootstrap.py
uv run pytest
uv run ruff check .
sh stack.sh up
```

Local URL: http://127.0.0.1:8282. The container limit is 512 MiB for the app, plus Docker and host process overhead. Tested one portfolio stack at a time on an ARM Mac with 8 GiB host RAM and a 2 GiB Docker VM. No GPU. Defaults use independent named volumes and localhost bindings. Stack stop retains state; no unrelated services are managed.

For Pages: `python3 -m http.server 8008 --bind 127.0.0.1 --directory docs`. With optional agent-browser 0.38.1 installed, run `sh scripts/check-pages.sh`; assertions are in `tests/pages-smoke.js`. No browser automation package is shipped to Pages.

## Recovery maintenance verification

- **25 tests passed** on Python 3.14.8; Ruff passed. The existing FastAPI/Starlette TestClient deprecation warning remains. Synthetic process-kill coverage is separate from the real-source check.
- The controlled-kill verifier read the SHA-256-pinned TLC files and killed only its own child at committed cursor **5,000**. A new attempt resumed from 5,000 and matched an uninterrupted baseline: 56,551 source rows, 56,549 accepted, 2 quarantined, 6,640 warning rows and all 182 daily groups, quality totals, quarantine values and examples. All eight checks passed; no model calls.
- The independent Docker build and health checks passed. A normal real-data run published; a separate explicitly simulated `once_after_checkpoint` run recovered on its second attempt from 5,000 without replacing that publication. Re-scanning unchanged input created no new normal run.
- The browser displayed the observed counters, source hashes, experiment attempt history and row examples. Restarting only this isolated app retained the publication, all daily groups, disabled schedule and attempt history exactly. The controlled-kill verifier used fresh temporary databases; it did not kill the app container.

These bounded checks do not establish recovery at every possible instruction or on every filesystem. The verifier uses a child-only pause immediately after a successful batch commit. The cooperative mid-run pause is described from source and is not claimed as a separately exercised regression. Source downloads were unavailable in the cloud worker; the real-source check ran on the Mac's verified local cache.
