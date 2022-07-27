"""Versioned Relay row-quality policy."""

from __future__ import annotations

import math
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

RULES_VERSION = "2024-01-v1"
PIPELINE_VERSION = "1"
MAX_CENTS = 1_000_000_000  # USD10million/row; <=100k rows stays exact in SQLite/JSON.
REQUIRED_COLUMNS = {
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "passenger_count",
}


def cents(value):
    try:
        value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid monetary value") from exc
    if not value.is_finite():
        raise ValueError("nonfinite monetary value")
    if abs(value) > Decimal(MAX_CENTS) / 100:
        raise ValueError("monetary value exceeds supported bound")
    try:
        result = int((value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, OverflowError) as exc:
        raise ValueError("invalid monetary precision") from exc
    if abs(result) > MAX_CENTS:
        raise ValueError("monetary value exceeds supported bound")
    return result


def finite(value):
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def safe(value):
    if isinstance(value, float) and not finite(value):
        return str(value)
    if isinstance(value, (str, int, float, type(None))):
        return value
    return str(value)


def evaluate(row, zones):
    reject, warnings = [], []
    pickup, dropoff = row.get("lpep_pickup_datetime"), row.get("lpep_dropoff_datetime")
    if not isinstance(pickup, datetime):
        reject.append("invalid_pickup")
    elif (pickup.year, pickup.month) != (2024, 1):
        reject.append("pickup_outside_partition")
    if not isinstance(dropoff, datetime):
        reject.append("invalid_dropoff")
    elif isinstance(pickup, datetime) and dropoff < pickup:
        reject.append("negative_duration")
    distance = row.get("trip_distance")
    if not finite(distance) or float(distance) < 0:
        reject.append("invalid_distance")
    elif float(distance) == 0:
        warnings.append("zero_distance")
    elif float(distance) > 1000:
        warnings.append("distance_over_1000")
    try:
        net = cents(row.get("total_amount"))
    except ValueError:
        reject.append("invalid_total_amount")
        net = 0
    if net < 0:
        warnings.append("negative_total_amount")
    pu, do = row.get("PULocationID"), row.get("DOLocationID")
    if pu not in zones:
        reject.append("missing_pickup_zone")
    if do not in zones:
        reject.append("missing_dropoff_zone")
    if row.get("passenger_count") is None:
        warnings.append("missing_passengers")
    if isinstance(pickup, datetime) and isinstance(dropoff, datetime) and pickup == dropoff:
        warnings.append("zero_duration")
    for location, prefix in ((pu, "pickup"), (do, "dropoff")):
        if zones.get(location) in {"Unknown", "N/A", "Outside of NYC"}:
            warnings.append(f"{prefix}_unknown_zone")
    observed = {
        "pickup": pickup.isoformat(sep=" ") if isinstance(pickup, datetime) else None,
        "dropoff": dropoff.isoformat(sep=" ") if isinstance(dropoff, datetime) else None,
        "pu_zone": safe(pu),
        "do_zone": safe(do),
        "distance": safe(distance),
        "total_amount": safe(row.get("total_amount")),
        "passengers": safe(row.get("passenger_count")),
    }
    return reject, warnings, {"net_cents": net, "observed": observed}
