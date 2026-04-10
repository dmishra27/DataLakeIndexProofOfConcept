from __future__ import annotations

import polars as pl
from dagster import MaterializeResult, MetadataValue, asset

from ..env import MASKED_POSTGRES_URL
from ..postgres_io import load_dataframe_to_postgres, load_delta_table_to_postgres, wait_for_postgres
from ..settings import (
    DELTA_METADATA_DIR,
    DIM_BASE,
    DIM_LOCATION,
    DIM_PAYMENT_TYPE,
    DIM_RATE_CODE,
    DIM_TRIP_TYPE,
    DIM_VENDOR,
    FACT_FHV,
    FACT_GREEN,
    FACT_TRIP,
    FACT_YELLOW,
)
from .current_snapshot_files import current_snapshot_files
from .current_snapshot_file_stats import current_snapshot_file_stats
from .metadata_exports import metadata_exports


@asset(deps=[metadata_exports, current_snapshot_file_stats, current_snapshot_files])
def postgres_mirror() -> MaterializeResult:
    """Load warehouse tables and metadata parquet outputs into Postgres."""
    wait_for_postgres()
    loaded_rows: dict[str, int] = {}
    for table_name in [
        DIM_LOCATION,
        DIM_VENDOR,
        DIM_PAYMENT_TYPE,
        DIM_RATE_CODE,
        DIM_TRIP_TYPE,
        DIM_BASE,
        FACT_YELLOW,
        FACT_GREEN,
        FACT_FHV,
        FACT_TRIP,
    ]:
        loaded_rows[f"mart.{table_name}"] = load_delta_table_to_postgres("mart", table_name)

    metadata_tables = {
        "column_catalog": pl.read_parquet(DELTA_METADATA_DIR / "column_catalog.parquet"),
        "column_statistics": pl.read_parquet(DELTA_METADATA_DIR / "column_statistics.parquet"),
        "delta_log_entries": pl.read_parquet(DELTA_METADATA_DIR / "delta_log_entries.parquet"),
        "predicate_row_group_index": pl.read_parquet(
            DELTA_METADATA_DIR / "predicate_row_group_index.parquet"
        ),
        "current_snapshot_files": pl.read_parquet(DELTA_METADATA_DIR / "current_snapshot_files.parquet"),
        "current_snapshot_file_stats": pl.read_parquet(
            DELTA_METADATA_DIR / "current_snapshot_file_stats.parquet"
        ),
    }
    for table_name, dataframe in metadata_tables.items():
        loaded_rows[f"metadata.{table_name}"] = load_dataframe_to_postgres(
            "metadata", table_name, dataframe
        )

    return MaterializeResult(
        metadata={
            "postgres_url": MASKED_POSTGRES_URL,
            "loaded_rows": MetadataValue.json(loaded_rows),
        }
    )
