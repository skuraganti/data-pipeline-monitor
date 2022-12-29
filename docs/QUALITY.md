# Quality: quarantine, warning rows and flags

`app/quality.py` (rules `2024-01-v1`) evaluates every source row. Each row lands in exactly one of two places:

- **Quarantined** (severity `error`): kept in `quarantine` with its physical row index and observed values, and excluded from aggregates. Codes: `pickup_outside_partition`, `invalid_pickup`, `invalid_dropoff`, `negative_duration`, `invalid_distance`, `invalid_total_amount`, `missing_pickup_zone`, `missing_dropoff_zone`.
- **Accepted**: counted in its pickup day and borough. If it also has `warning` codes, it is a **warning row**. Codes: `zero_distance`, `distance_over_1000`, `negative_total_amount`, `missing_passengers`, `zero_duration`, `pickup_unknown_zone`, `dropoff_unknown_zone`.

## Counting

- `rejected` and `warning_rows` count rows. The per-code `quality` totals count flags, and one row can carry several flags, so summing code totals does not give a distinct-row count. Warning flags can also be recorded on a quarantined row; `warning_rows` counts only accepted rows with warnings.
- A daily group is a (pickup day, borough) aggregate of accepted rows, with its trips, net cents and warning-row count. Groups summarise rows; they are not rows.
- For the recorded run over the pinned TLC file, 56,551 rows split into 56,549 accepted and 2 quarantined (`pickup_outside_partition`). 6,640 accepted rows carry warnings, across 182 groups.

## What a warning means

A warning is an inspectable flag, not a finding:
- **Negative totals** can be adjustments.
- **Unknown or outside-NYC zones** come from the publisher's own lookup.
- **Distances over 1,000 miles** are outliers to review.

None of these proves fraud, error or a cause. See the [data source notes](../DATA_SOURCES.md).

`GET /api/runs/{run_id}` returns counters, per-code totals and up to 50 examples, quarantined rows first. `GET /api/runs/{run_id}/lineage` ties them to the source hashes. The full quarantine is persisted beyond the 50 shown.
