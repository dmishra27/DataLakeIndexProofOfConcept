"""Public API for the non-stupid index package."""

from .config import DEFAULT_POSTGRES_URL, DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT
from .indexer import (
    Predicate,
    build_duckdb_parquet_scan,
    build_footer_row_group_stats_frame,
    build_predicate_index_frame,
    create_footer_row_group_stats_index,
    create_predicate_index,
    find_candidate_files,
    iter_warehouse_parquet_files,
)

__all__ = [
    "DEFAULT_POSTGRES_URL",
    "DELTA_WAREHOUSE_DIR",
    "Predicate",
    "WORKSPACE_ROOT",
    "build_duckdb_parquet_scan",
    "build_footer_row_group_stats_frame",
    "build_predicate_index_frame",
    "create_footer_row_group_stats_index",
    "create_predicate_index",
    "find_candidate_files",
    "iter_warehouse_parquet_files",
]
