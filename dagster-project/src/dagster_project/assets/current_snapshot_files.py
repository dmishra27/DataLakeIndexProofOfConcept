from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from ..metadata_io import collect_current_snapshot_files, export_parquet
from ..settings import DELTA_METADATA_DIR, DELTA_TABLES
from .delta_facts import delta_facts


@asset(deps=[delta_facts])
def current_snapshot_files() -> MaterializeResult:
    """Export one row per active Delta file with nested current-snapshot stats."""
    DELTA_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    dataframe = collect_current_snapshot_files(DELTA_TABLES)
    export_parquet(dataframe, DELTA_METADATA_DIR / "current_snapshot_files.parquet")
    return MaterializeResult(
        metadata={
            "metadata_dir": MetadataValue.path(str(DELTA_METADATA_DIR)),
            "row_count": len(dataframe),
            "table_count": len(DELTA_TABLES),
        }
    )
