from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from ..metadata_io import (
    collect_column_catalog,
    collect_column_statistics,
    collect_delta_log_entries,
    collect_predicate_row_group_index,
    export_parquet,
)
from ..settings import DELTA_METADATA_DIR, DELTA_TABLES
from .delta_facts import delta_facts


@asset(deps=[delta_facts])
def metadata_exports() -> MaterializeResult:
    """Export warehouse schema metadata, Delta logs, and row-group predicate stats to parquet."""
    DELTA_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    column_catalog = collect_column_catalog(DELTA_TABLES)
    column_statistics = collect_column_statistics(DELTA_TABLES)
    delta_log_entries = collect_delta_log_entries(DELTA_TABLES)
    predicate_row_group_index = collect_predicate_row_group_index(DELTA_TABLES)

    export_parquet(column_catalog, DELTA_METADATA_DIR / "column_catalog.parquet")
    export_parquet(column_statistics, DELTA_METADATA_DIR / "column_statistics.parquet")
    export_parquet(delta_log_entries, DELTA_METADATA_DIR / "delta_log_entries.parquet")
    export_parquet(
        predicate_row_group_index,
        DELTA_METADATA_DIR / "predicate_row_group_index.parquet",
    )

    return MaterializeResult(
        metadata={
            "metadata_dir": MetadataValue.path(str(DELTA_METADATA_DIR)),
            "column_catalog_rows": len(column_catalog),
            "column_statistics_rows": len(column_statistics),
            "delta_log_rows": len(delta_log_entries),
            "predicate_row_group_rows": len(predicate_row_group_index),
        }
    )
