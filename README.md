# Relay — data pipeline monitor

**[Open the GitHub Pages browser demo](https://skuraganti.github.io/data-pipeline-monitor/)** · [Architecture](ARCHITECTURE.md) · [Data sources](DATA_SOURCES.md) · [Verification](VERIFICATION.md)

Scheduled NYC TLC ingestion, schema/domain checks, quarantine, lineage, bounded retries, checkpoints and recoverable publication.

## Two ways to explore

The **Pages site** is a self-contained browser demonstration with relative assets and no backend, credentials or model calls. Its data modes are visibly labeled: original public-publisher snapshots/recorded observations where allowed, explicit simulation where appropriate, and an entirely synthetic Ledger fixture. The repository also contains the **full Python/Docker application**; Pages cannot execute its backend or background workers.

## Run the full application locally

Requirements: Python3.13+, [uv](https://docs.astral.sh/uv/), Docker and Docker Compose. Contour also needs Node24+ for the trusted official TypeScript parser when running native tests; its Docker image includes Node. No GPU is required. Run one portfolio stack at a time on a small machine.

```sh
uv sync --frozen
uv run python scripts/bootstrap.py
sh stack.sh up
```

Open **http://127.0.0.1:8282**. Stop with `sh stack.sh stop`; this retains this repository's independent named volume. Helpers honor your current Docker context or explicitly supplied `DOCKER_HOST`; they do not select a personal machine profile. Images/networks/volumes are uniquely named `portfolio-release-*`, keeping the original development stacks separate. Host ports bind to127.0.0.1.

## Verify

```sh
uv run pytest
uv run ruff check .
python3 -m http.server 8008 --bind 127.0.0.1 --directory docs
```

The final command previews Pages at http://127.0.0.1:8008. Bootstrap prepares this checkout's own data before data-boundary integration tests. Unit fixtures, actual-source checks, captured evaluations and browser simulations are distinguished in [VERIFICATION.md](VERIFICATION.md). No model call is required for the native test suite.

## Scope and limitations

The Pages queue/retry animation is explicitly simulated over actual captured aggregates. The local worker processes the original 56,551-row TLC file. Two invalid rows and 6,640 warning rows are retained rather than concealed.

[DEMO.md](DEMO.md) describes a local walkthrough; [docs/DEMO.md](docs/DEMO.md) describes the Pages interactions. [DECISIONS.md](DECISIONS.md), [PLAN.md](PLAN.md) and [SOURCE_MANIFEST.json](SOURCE_MANIFEST.json) record engineering choices and source versions. All original implementation was developed with Claude Code through the approved connection and ChatGPT architecture, integration, review, correction and independent verification. Upstream components and original data publishers are credited in [UPSTREAM.md](UPSTREAM.md); third-party licenses remain in force. No additional license grant for the original implementation has been selected.

## Explore projects

[Ledger](https://skuraganti.github.io/personal-spending-tracker/) · [Fieldglass](https://skuraganti.github.io/live-earth-dashboard/) · [Relay](https://skuraganti.github.io/data-pipeline-monitor/) · [Folio](https://skuraganti.github.io/private-ai-knowledge-engine/) · [Contour](https://skuraganti.github.io/ai-codebase-explorer/) · [TraceLab](https://skuraganti.github.io/ai-agent-evaluation-lab/) · [Faultline](https://skuraganti.github.io/ai-incident-investigator/)

Prerequisites: a POSIX host (macOS or Linux), Docker with Compose, and `uv`. Native scripts use POSIX process/file-lock facilities. Dependency installation needs internet; Relay, Folio and TraceLab also download original-source data during bootstrap. Defaults work without copying `.env.example`. Optional model integration uses a locally configured host bridge; inference may be remote. The supplied bridge requires `zsh`, an existing `claude-omni` command and the approved `cookllm` route; no provider is included or enabled by Pages.

## Implementation history

- Scheduled NYC TLC ingestion with schema/domain checks and quarantined invalid rows.
- Lineage, bounded retries, checkpoints and recoverable result publication.
- An operational dashboard and a browser replay over captured aggregates, with simulated scheduling and failures.
