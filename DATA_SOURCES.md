# Data sources — acquired and reviewed2026-10-16

| Candidate | Provenance/access/terms | Decision |
|---|---|---|
| NYC TLC January2024 green trips + taxi-zone lookup | Original agency-hosted public downloads, technology-provider submissions. [Publisher](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), [dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf), [Open Data FAQ](https://www.nyc.gov/opendata/get-started/FAQs), [terms](https://www.nyc.gov/main/terms-of-use) | Selected: trip-level records, real missingness/outliers, complementary lookup and modest Parquet volume support useful quality/recovery/lineage workflows |
| UCI Bike Sharing | Fanaee-T2013; original prepared Capital Bikeshare2011–2012 hourly/daily counts. [Dataset/DOI/license](https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset), DOI10.24432/C5W894, explicit CC BY4.0 | Credible small/reproducible alternative with clearer standard license; prepared aggregates and reported no missing values provide fewer authentic raw operational checks. Not acquired |

TLC/NYC public-data policies support public analysis; the Open Data FAQ states no use restrictions, while generic website terms reserve website-content rights. No standalone TLC CC BY/public-domain grant was identified. This project records the actual policies and attribution and keeps original trip-level downloads excluded. The Pages demo publishes 182 derived daily borough aggregate groups with TLC attribution; no broader license or ownership claim is made. No account/key/paid service required. TLC disclaims accuracy; publication is monthly and typically delayed. This is scheduled **historical processing**, not a live taxi stream. Original provider collection may contain errors and coverage biases; records represent green-taxi service, not all travel or individual identities.

## Actual immutable originals

`data/raw/manifest.json` records exact URLs, retrievalUTC, bytes, SHA256, ETag/Last-Modified if supplied. No source revision ID beyond the original2024-01 object and captured digest is promised.

- [green Jan2024](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet):1,362,284bytes, SHA256`1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad`,56,551rows×20fields, ~8.56MB decoded with PyArrow26.0.0.
- [zone lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv):12,331bytes, SHA256`1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`,265 unique locationIDs. Snapshot joins100% syntactically;264/265 are semantic Unknown/OutsideNYC.

No sampling was necessary after bounded size inspection; all rows are processed, physical row occurrence identity retained. No model splits apply. File paths/data cache are excluded from future tracking; acquisition script does guarded bounded public GET only. No supplied code executes. The full service processes derived output locally; the Pages fixture redistributes only daily borough aggregates, not trip-level rows. The dictionary inspected is datedMarch18,2025 and newer than this2024 schema;2025-specific CBD fee is absent here. Meter timestamps have no timezone and are not silently labeledUTC. Amounts are USD; recorded tips/totals omit cash tips. Distances are miles.

## Independent observed quality profile

Two pickups lie inDecember2023;17dropoffs outsideJanuary include15February (legitimate cross-month end times are possible). There are76zero durations,0negative durations,2,870zero distances,46distances>1000mi (maximum201,421.68mi),182negative fares,185negative totals,3,415missing passenger counts,56,551null ehail_fee and3,418null trip_type. There are0exact duplicate rows. Pickup unknown/outside zones:112/29; dropoff418/180. These are measured signals, not verified causal explanations or fraud labels.

Project quarantine policy rejects pickups outside the configuredmonth or invalid required values/dates/duration/joins. Signed amounts, missing passengers, zero distance/duration, semantic unknown zones and distance>1000mi are retained with warnings. February dropoffs remain allowed after a validJanuary pickup. Rules and thresholds are versioned; warning hits overlap and are separate from distinct affected-row counts. Synthetic pipeline failures/fixtures are clearly marked and never relabeled observed source errors. Processing dates are actual2026 dates. Source checksums and row indices link every quality example back to immutable inputs.
