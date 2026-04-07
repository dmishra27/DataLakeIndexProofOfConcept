from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from .settings import DATA_SOURCE_DIR


def norm(path: Path) -> str:
    """Return a normalized POSIX-style path string for DuckDB and Delta IO."""
    return path.as_posix()


def source_files(prefix: str) -> list[Path]:
    """Return the sorted monthly source files for a taxi service prefix."""
    files = sorted(DATA_SOURCE_DIR.glob(f"{prefix}_tripdata_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No source files found for prefix '{prefix}' in {DATA_SOURCE_DIR}")
    return files


def load_zone_lookup() -> pd.DataFrame:
    """Load and normalize the TLC taxi zone lookup dimension."""
    zones = pd.read_csv(DATA_SOURCE_DIR / "taxi_zone_lookup.csv")
    return zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone_name",
            "service_zone": "service_zone",
        }
    ).sort_values("location_id", ignore_index=True)


def load_static_dimensions() -> dict[str, pd.DataFrame]:
    """Build the small static dimensions used by the warehouse model."""
    vendor = pd.DataFrame(
        [
            {"vendor_id": 1, "vendor_name": "Creative Mobile Technologies"},
            {"vendor_id": 2, "vendor_name": "VeriFone"},
        ]
    )
    payment = pd.DataFrame(
        [
            {"payment_type_id": 0, "payment_type_name": "Flex Fare / Unknown"},
            {"payment_type_id": 1, "payment_type_name": "Credit card"},
            {"payment_type_id": 2, "payment_type_name": "Cash"},
            {"payment_type_id": 3, "payment_type_name": "No charge"},
            {"payment_type_id": 4, "payment_type_name": "Dispute"},
            {"payment_type_id": 5, "payment_type_name": "Unknown"},
            {"payment_type_id": 6, "payment_type_name": "Voided trip"},
        ]
    )
    rate_code = pd.DataFrame(
        [
            {"rate_code_id": 1, "rate_code_name": "Standard rate"},
            {"rate_code_id": 2, "rate_code_name": "JFK"},
            {"rate_code_id": 3, "rate_code_name": "Newark"},
            {"rate_code_id": 4, "rate_code_name": "Nassau/Westchester"},
            {"rate_code_id": 5, "rate_code_name": "Negotiated fare"},
            {"rate_code_id": 6, "rate_code_name": "Group ride"},
            {"rate_code_id": 99, "rate_code_name": "Unknown"},
        ]
    )
    trip_type = pd.DataFrame(
        [
            {"trip_type_id": 1, "trip_type_name": "Street-hail"},
            {"trip_type_id": 2, "trip_type_name": "Dispatch"},
        ]
    )
    return {
        "dim_location": load_zone_lookup(),
        "dim_vendor": vendor,
        "dim_payment_type": payment,
        "dim_rate_code": rate_code,
        "dim_trip_type": trip_type,
    }


def build_base_dimension() -> pd.DataFrame:
    """Build the base-number dimension from the current FHV source files."""
    con = duckdb.connect()
    fhv_file_list = ", ".join(repr(norm(path)) for path in source_files("fhv"))
    query = f"""
        WITH bases AS (
            SELECT dispatching_base_num AS base_number, TRUE AS seen_as_dispatching, FALSE AS seen_as_affiliated
            FROM read_parquet([{fhv_file_list}])
            WHERE dispatching_base_num IS NOT NULL
            UNION ALL
            SELECT Affiliated_base_number AS base_number, FALSE AS seen_as_dispatching, TRUE AS seen_as_affiliated
            FROM read_parquet([{fhv_file_list}])
            WHERE Affiliated_base_number IS NOT NULL
        )
        SELECT
            base_number,
            MAX(CASE WHEN seen_as_dispatching THEN 1 ELSE 0 END)::BOOLEAN AS seen_as_dispatching,
            MAX(CASE WHEN seen_as_affiliated THEN 1 ELSE 0 END)::BOOLEAN AS seen_as_affiliated
        FROM bases
        GROUP BY 1
        ORDER BY 1
    """
    return con.sql(query).df()
