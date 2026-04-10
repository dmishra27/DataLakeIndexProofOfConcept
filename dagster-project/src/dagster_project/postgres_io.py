from __future__ import annotations

import io
import time

import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Date, Numeric
from sqlalchemy.dialects.postgresql import JSONB

from .env import POSTGRES_URL
from .delta_io import delta_table

DELTA_COPY_BATCH_SIZE = 100_000
DELTA_COPY_COMMIT_INTERVAL = 10
FrameT = pd.DataFrame | pl.DataFrame


def postgres_engine():
    """Create a SQLAlchemy engine for the local Postgres database."""
    return create_engine(POSTGRES_URL)


def wait_for_postgres() -> None:
    """Poll the Postgres connection until the database becomes reachable."""
    engine = postgres_engine()
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError("Postgres did not become ready in time.")


def quoted_identifier(identifier: str) -> str:
    """Quote a Postgres identifier for generated COPY statements."""
    return '"' + identifier.replace('"', '""') + '"'


def frame_from_batch(batch) -> pl.DataFrame:
    """Convert an Arrow batch into a Polars DataFrame with Arrow-backed dtypes preserved."""
    return pl.from_arrow(batch)


def copy_dataframe(cursor, schema: str, table_name: str, dataframe: FrameT) -> int:
    """Bulk load a pandas or Polars DataFrame into Postgres using COPY FROM STDIN."""
    if isinstance(dataframe, pl.DataFrame):
        if dataframe.is_empty():
            return 0
    elif dataframe.empty:
        return 0
    buffer = io.StringIO()
    if isinstance(dataframe, pl.DataFrame):
        dataframe.write_csv(buffer, include_header=False, null_value="\\N")
        row_count = dataframe.height
    else:
        dataframe.to_csv(buffer, index=False, header=False, na_rep="\\N")
        row_count = len(dataframe)
    buffer.seek(0)
    columns = ", ".join(quoted_identifier(column) for column in dataframe.columns)
    cursor.copy_expert(
        f"COPY {schema}.{quoted_identifier(table_name)} ({columns}) FROM STDIN WITH CSV NULL '\\N'",
        buffer,
    )
    return row_count


def dtype_overrides(dataframe: FrameT) -> dict[str, object]:
    """Return SQLAlchemy dtype overrides for JSON payload columns."""
    overrides: dict[str, object] = {}
    typed_columns = {
        "min_numeric": Numeric(),
        "max_numeric": Numeric(),
        "min_date": Date(),
        "max_date": Date(),
        "min_boolean": Boolean(),
        "max_boolean": Boolean(),
    }
    for column_name in dataframe.columns:
        if column_name.endswith("_json"):
            overrides[column_name] = JSONB()
        elif column_name in typed_columns:
            overrides[column_name] = typed_columns[column_name]
    return overrides


def _empty_pandas_frame(dataframe: FrameT) -> pd.DataFrame:
    """Return an empty pandas frame matching the incoming pandas or Polars schema."""
    if isinstance(dataframe, pl.DataFrame):
        return dataframe.clear().to_pandas(use_pyarrow_extension_array=True)
    return dataframe.iloc[0:0]


def load_dataframe_to_postgres(schema: str, table_name: str, dataframe: FrameT) -> int:
    """Replace a Postgres table with DataFrame contents and return loaded row count."""
    engine = postgres_engine()
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    _empty_pandas_frame(dataframe).to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        dtype=dtype_overrides(dataframe),
    )
    raw_connection = engine.raw_connection()
    try:
        cursor = raw_connection.cursor()
        rows_loaded = copy_dataframe(cursor, schema, table_name, dataframe)
        raw_connection.commit()
        return rows_loaded
    finally:
        raw_connection.close()


def _progress_message(table_name: str, rows_loaded: int, batches_loaded: int, started_at: float) -> str:
    """Render a concise progress message for long-running Delta-to-Postgres loads."""
    elapsed_seconds = max(time.time() - started_at, 0.001)
    rows_per_second = rows_loaded / elapsed_seconds
    return (
        f"[postgres_mirror] {table_name}: loaded {rows_loaded:,} rows "
        f"across {batches_loaded:,} batches in {elapsed_seconds:.1f}s "
        f"({rows_per_second:,.0f} rows/s)"
    )


def load_delta_table_to_postgres(schema: str, table_name: str) -> int:
    """Mirror an active Delta table snapshot into Postgres and return loaded row count."""
    dataset = delta_table(table_name).to_pyarrow_dataset()
    batches = iter(dataset.to_batches(batch_size=DELTA_COPY_BATCH_SIZE))
    first_batch = next(batches, None)
    if first_batch is None:
        return 0
    engine = postgres_engine()
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    first_frame = frame_from_batch(first_batch)
    _empty_pandas_frame(first_frame).to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    raw_connection = engine.raw_connection()
    rows_loaded = 0
    batches_loaded = 0
    started_at = time.time()
    try:
        cursor = raw_connection.cursor()
        rows_loaded += copy_dataframe(cursor, schema, table_name, first_frame)
        batches_loaded += 1
        print(_progress_message(table_name, rows_loaded, batches_loaded, started_at), flush=True)
        for batch in batches:
            rows_loaded += copy_dataframe(cursor, schema, table_name, frame_from_batch(batch))
            batches_loaded += 1
            if batches_loaded % DELTA_COPY_COMMIT_INTERVAL == 0:
                raw_connection.commit()
                print(_progress_message(table_name, rows_loaded, batches_loaded, started_at), flush=True)
        raw_connection.commit()
        if batches_loaded % DELTA_COPY_COMMIT_INTERVAL != 0:
            print(_progress_message(table_name, rows_loaded, batches_loaded, started_at), flush=True)
    finally:
        raw_connection.close()
    return rows_loaded
