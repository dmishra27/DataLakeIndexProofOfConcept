"""Download a small, reproducible NYC Taxi example dataset into `datasource/nyc-taxi`."""

from __future__ import annotations

import argparse
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_SERVICES = ("yellow", "green", "fhv")
DEFAULT_MONTHS = tuple(f"2025-{month:02d}" for month in range(1, 13))


def _download_file(url: str, destination: Path) -> None:
    """Download one remote file unless it already exists locally."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return
    urllib.request.urlretrieve(url, destination)


def _tripdata_filename(service_name: str, month: str) -> str:
    """Return the TLC trip-data filename for one service and month."""

    return f"{service_name}_tripdata_{month}.parquet"


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the example-data downloader."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--service",
        action="append",
        choices=DEFAULT_SERVICES,
        default=[],
        help="Trip-data service to download. Repeat for multiple services.",
    )
    parser.add_argument(
        "--month",
        action="append",
        default=[],
        metavar="YYYY-MM",
        help="Trip-data month to download. Repeat for multiple months.",
    )
    parser.add_argument(
        "--output-dir",
        default=Path(__file__).resolve().parent / "nyc-taxi",
        type=Path,
        help="Destination directory for downloaded example files.",
    )
    return parser


def download_examples(
    output_dir: Path,
    service_names: list[str] | None = None,
    months: list[str] | None = None,
) -> list[Path]:
    """Download the requested example files and return the local paths created."""

    selected_services = service_names or list(DEFAULT_SERVICES)
    selected_months = months or list(DEFAULT_MONTHS)
    downloaded: list[Path] = []

    zone_lookup_path = output_dir / "taxi_zone_lookup.csv"
    _download_file(ZONE_LOOKUP_URL, zone_lookup_path)
    downloaded.append(zone_lookup_path)

    for service_name in selected_services:
        for month in selected_months:
            filename = _tripdata_filename(service_name, month)
            destination = output_dir / filename
            _download_file(f"{BASE_URL}/{filename}", destination)
            downloaded.append(destination)

    return downloaded


def main() -> None:
    """Execute the example-data download CLI."""

    args = build_parser().parse_args()
    paths = download_examples(args.output_dir, args.service, args.month)
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
