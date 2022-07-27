# Public demonstration data

Prepared 2026-10-16; theme year 2022 is not a completion date.

182 daily borough aggregate groups recomputed from all 56,551 January 2024 TLC green-trip rows. 56,549 accepted, two quarantined, 6,640 warning rows; net recorded total 126,688,501 USD cents. Original trip-level files are downloaded only into the excluded local cache. Browser retries are a simulation.

Fixture: `docs/demo-data.json` · SHA-256 `836cb4764908d99a4d183b0e279f4c4ac9ecb22c7c41e1d72ed40420c2b8f81d` · 16,905 bytes.

Declared mode: Actual original TLC aggregates; job/fault animation is explicitly simulated in-browser

Top-level schema fields: `title`, `data_kind`, `summary`, `daily`, `source_rows`, `source_url`, `stages`, `limit`, `theme_year`, `developed_at`, `repository`, `source_sha256`, `lookup_sha256`

Original publisher URLs, revisions, retrieval methods, rights and caveats are in [DATA_SOURCES.md](DATA_SOURCES.md). Browser behavior is described in [docs/DEMO.md](docs/DEMO.md). Data and application dependencies are credited separately in [UPSTREAM.md](UPSTREAM.md).
