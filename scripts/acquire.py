"""Acquire original fixed TLC files with no code execution or source sampling."""

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import tempfile
from pathlib import Path

import httpx

SOURCES = {
    "green_2024-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet",
    "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
}
MAX_BYTES = 10 * 1024 * 1024


async def acquire(output: Path, replace: bool):
    if not replace and any((output / name).exists() for name in [*SOURCES, "manifest.json"]):
        raise FileExistsError(
            "Frozen files exist; choose a new --output-dir or explicitly --replace"
        )
    captured = []
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as client:
        for name, url in SOURCES.items():
            async with asyncio.timeout(20):
                async with client.stream(
                    "GET",
                    url,
                    headers={
                        "User-Agent": "RelayLocalPortfolio/1.0",
                        "Accept-Encoding": "identity",
                    },
                ) as response:
                    response.raise_for_status()
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise ValueError("Compressed responses not accepted")
                    pieces, length = [], 0
                    async for chunk in response.aiter_raw():
                        length += len(chunk)
                        if length > MAX_BYTES:
                            raise ValueError("Source exceeds 10MiB acquisition bound")
                        pieces.append(chunk)
                    body = b"".join(pieces)
                    captured.append(
                        (
                            name,
                            body,
                            {
                                "name": name,
                                "url": url,
                                "publisher": "NYC TLC; technology-provider trip submissions",
                                "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                                "sha256": hashlib.sha256(body).hexdigest(),
                                "bytes": len(body),
                                "http_metadata": {
                                    key: response.headers.get(key)
                                    for key in ["ETag", "Last-Modified"]
                                },
                            },
                        )
                    )
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".acquire-", dir=output.parent) as temporary:
        stage = Path(temporary)
        for name, body, _ in captured:
            (stage / name).write_bytes(body)
        (stage / "manifest.json").write_text(
            json.dumps([m for _, _, m in captured], indent=2) + "\n"
        )
        # Publish the manifest last; a disk failure cannot produce a falsely verified digest.
        for name in [*SOURCES, "manifest.json"]:
            if not replace and (output / name).exists():
                raise FileExistsError("Destination appeared during acquisition; refusing overwrite")
            os.replace(stage / name, output / name)
    print(json.dumps([m for _, _, m in captured], indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--replace", action="store_true")
    arguments = parser.parse_args()
    try:
        asyncio.run(acquire(arguments.output_dir, arguments.replace))
    except (FileExistsError, ValueError, httpx.HTTPError, TimeoutError) as error:
        raise SystemExit(str(error)) from error
