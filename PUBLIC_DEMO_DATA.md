# Public demonstration data

Curated input for the browser demonstration.

182 daily borough aggregate groups recomputed from all 56,551 January 2024 TLC green-trip rows. 56,549 accepted, two quarantined, 6,640 warning rows; net recorded total 126,688,501 USD cents. Original trip-level files are downloaded only into the excluded local cache. Browser retries are a simulation.

Fixture: `docs/demo-data.json` · SHA-256 `4419817ee10e08d8700a9bf62db6cb95b200d76ed6c0bdeb0a1ebafec4c6c4c7` · 16,854 bytes.

Declared mode: Actual original TLC aggregates; job/fault animation is explicitly simulated in-browser

Top-level schema fields: `title`, `data_kind`, `summary`, `daily`, `source_rows`, `source_url`, `stages`, `limit`, `repository`, `source_sha256`, `lookup_sha256`

Original publisher URLs, revisions, retrieval methods, rights and caveats are in [DATA_SOURCES.md](DATA_SOURCES.md). Browser behavior is described in [docs/DEMO.md](docs/DEMO.md). Data and application dependencies are credited separately in [UPSTREAM.md](UPSTREAM.md).
