from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from deltalake import DeltaTable, write_deltalake

from .settings import DELTA_WAREHOUSE_DIR
from .source_data import norm


def table_path(table_name: str) -> Path:
    """Return the filesystem path for a Delta table in the local warehouse."""
    return DELTA_WAREHOUSE_DIR / table_name


def write_delta_from_dataframe(table_name: str, dataframe: pd.DataFrame) -> None:
    """Write a pandas DataFrame to a Delta table using overwrite semantics."""
    path = table_path(table_name)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(norm(path), dataframe, mode="overwrite", schema_mode="overwrite")


def write_delta_from_query(
    table_name: str,
    query: str,
    partition_by: list[str] | None = None,
) -> None:
    """Execute a DuckDB query and write the resulting batches to a Delta table."""
    path = table_path(table_name)
    path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    reader = con.execute(query).fetch_record_batch(rows_per_batch=250_000)
    write_deltalake(
        norm(path),
        reader,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=partition_by,
    )


def parquet_files_for_delta(table_name: str) -> list[str]:
    """List parquet data files for a local Delta table."""
    return sorted(norm(path) for path in table_path(table_name).glob("**/*.parquet"))


def delta_table(table_name: str) -> DeltaTable:
    """Open a local Delta table by warehouse table name."""
    return DeltaTable(norm(table_path(table_name)))
