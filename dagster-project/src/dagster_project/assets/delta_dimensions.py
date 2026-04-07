from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from ..delta_io import write_delta_from_dataframe
from ..settings import DELTA_WAREHOUSE_DIR, DIM_BASE
from ..source_data import build_base_dimension, load_static_dimensions


@asset
def delta_dimensions() -> MaterializeResult:
    """Materialize the warehouse dimension tables into the local Delta Lake."""
    DELTA_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    dimension_frames = load_static_dimensions()
    dimension_frames[DIM_BASE] = build_base_dimension()
    for table_name, dataframe in dimension_frames.items():
        write_delta_from_dataframe(table_name, dataframe)
    return MaterializeResult(
        metadata={
            "delta_root": MetadataValue.path(str(DELTA_WAREHOUSE_DIR)),
            "tables": MetadataValue.json(sorted(dimension_frames)),
        }
    )
