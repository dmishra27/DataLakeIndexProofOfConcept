"""Index-building, footer-ingest, and predicate-pruning helpers backed by Postgres metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import Boolean, Date, DateTime, Integer, Numeric, Text, create_engine, text

from .config import DEFAULT_POSTGRES_URL, DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT

INDEX_COLUMNS = [
    "table_name",
    "file_path",
    "absolute_file_path",
    "column_name",
    "data_type",
    "value_kind",
    "num_records",
    "null_count",
    "non_null_count",
    "has_nulls",
    "all_nulls",
    "min_value_text",
    "max_value_text",
    "min_numeric",
    "max_numeric",
    "min_timestamp",
    "max_timestamp",
    "min_date",
    "max_date",
    "min_boolean",
    "max_boolean",
]

FOOTER_INDEX_COLUMNS = [
    "table_name",
    "file_path",
    "absolute_file_path",
    "row_group_index",
    "column_name",
    "data_type",
    "value_kind",
    "num_rows",
    "total_byte_size",
    "null_count",
    "non_null_count",
    "min_value_text",
    "max_value_text",
    "min_numeric",
    "max_numeric",
    "min_timestamp",
    "max_timestamp",
    "min_date",
    "max_date",
    "min_boolean",
    "max_boolean",
]

SUPPORTED_OPERATORS = {"=", "!=", "<", "<=", ">", ">=", "between", "is_null", "is_not_null"}


@dataclass(frozen=True)
class Predicate:
    """Represent a single column predicate used for file pruning."""

    column_name: str
    operator: str
    value: object | None = None
    second_value: object | None = None

    def __post_init__(self) -> None:
        """Validate that the predicate shape matches the chosen operator."""
        if self.operator not in SUPPORTED_OPERATORS:
            raise ValueError(f"Unsupported operator: {self.operator}")
        if self.operator == "between" and (self.value is None or self.second_value is None):
            raise ValueError("between predicates require both a lower and upper value")
        if self.operator in {"is_null", "is_not_null"} and (
            self.value is not None or self.second_value is not None
        ):
            raise ValueError(f"{self.operator} predicates do not accept values")
        if self.operator not in {"between", "is_null", "is_not_null"} and self.value is None:
            raise ValueError(f"{self.operator} predicates require a value")


def postgres_engine(postgres_url: str = DEFAULT_POSTGRES_URL):
    """Create a SQLAlchemy engine for the Postgres metadata database."""

    return create_engine(postgres_url)


def normalize_value_kind(data_type: str | None) -> str:
    """Map a catalog data type string to the index's coarse comparison family."""

    if not data_type:
        return "text"
    normalized = data_type.lower()
    if any(token in normalized for token in ["int", "float", "double", "decimal"]):
        return "numeric"
    if normalized.startswith("timestamp"):
        return "timestamp"
    if normalized.startswith("date"):
        return "date"
    if normalized in {"bool", "boolean"}:
        return "boolean"
    return "text"


def _parse_decimal(value_text: object) -> Decimal | None:
    """Parse a numeric bound from metadata text, returning `None` on failure."""

    if value_text in (None, ""):
        return None
    try:
        return Decimal(str(value_text))
    except (InvalidOperation, ValueError):
        return None


def _parse_timestamp(value_text: object) -> datetime | None:
    """Parse an ISO-like timestamp bound from metadata text."""

    if value_text in (None, ""):
        return None
    normalized = str(value_text).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_date(value_text: object) -> date | None:
    """Parse a date bound from metadata text."""

    if value_text in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value_text))
    except ValueError:
        return None


def _parse_bool(value_text: object) -> bool | None:
    """Parse a boolean bound from metadata text using common truthy and falsy forms."""

    if value_text in (None, ""):
        return None
    normalized = str(value_text).strip().lower()
    if normalized in {"true", "t", "1"}:
        return True
    if normalized in {"false", "f", "0"}:
        return False
    return None


def _workspace_relative_path(path: Path, workspace_root: Path = WORKSPACE_ROOT) -> str:
    """Return a normalized workspace-relative path when possible."""

    try:
        normalized = path.relative_to(workspace_root)
    except ValueError:
        normalized = path
    return normalized.as_posix()


def _indexed_file_path(table_name: str, file_path: str, warehouse_dir: Path) -> str:
    """Return the workspace-relative parquet path recorded in NSI metadata."""

    return _workspace_relative_path(warehouse_dir / table_name / file_path)


def _optional_text(value: object) -> str | None:
    """Convert a possibly-null metadata value into normalized text."""

    if value is None or pd.isna(value):
        return None
    return str(value)


def _optional_int(value: object) -> int:
    """Convert a possibly-null numeric metadata value into an integer with zero fallback."""

    if value is None or pd.isna(value):
        return 0
    return int(value)


def _stringify_stat_value(value: object) -> str | None:
    """Convert a parquet statistics value into normalized text for storage."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _footer_value_columns(value_kind: str, min_value: object, max_value: object) -> dict[str, object]:
    """Build the typed min/max payload for one footer statistics record."""

    min_value_text = _stringify_stat_value(min_value)
    max_value_text = _stringify_stat_value(max_value)
    payload: dict[str, object] = {
        "min_value_text": min_value_text,
        "max_value_text": max_value_text,
        "min_numeric": None,
        "max_numeric": None,
        "min_timestamp": None,
        "max_timestamp": None,
        "min_date": None,
        "max_date": None,
        "min_boolean": None,
        "max_boolean": None,
    }
    if value_kind == "numeric":
        payload["min_numeric"] = _parse_decimal(min_value_text)
        payload["max_numeric"] = _parse_decimal(max_value_text)
    elif value_kind == "timestamp":
        payload["min_timestamp"] = _parse_timestamp(min_value_text)
        payload["max_timestamp"] = _parse_timestamp(max_value_text)
    elif value_kind == "date":
        payload["min_date"] = _parse_date(min_value_text)
        payload["max_date"] = _parse_date(max_value_text)
    elif value_kind == "boolean":
        payload["min_boolean"] = _parse_bool(min_value_text)
        payload["max_boolean"] = _parse_bool(max_value_text)
    return payload


def build_predicate_index_frame(
    stats_frame: pd.DataFrame,
    catalog_frame: pd.DataFrame,
    warehouse_dir: Path = DELTA_WAREHOUSE_DIR,
) -> pd.DataFrame:
    """Build the typed per-file predicate index frame from metadata export tables."""

    catalog = (
        catalog_frame[["table_name", "column_name", "data_type"]]
        .drop_duplicates()
        .set_index(["table_name", "column_name"])["data_type"]
        .to_dict()
    )
    rows: list[dict[str, object]] = []
    for record in stats_frame.to_dict(orient="records"):
        table_name = str(record["table_name"])
        column_name = str(record["column_name"])
        file_path = str(record["file_path"])
        num_records = _optional_int(record.get("num_records"))
        null_count = _optional_int(record.get("null_count"))
        data_type = str(catalog.get((table_name, column_name), "string"))
        value_kind = normalize_value_kind(data_type)
        min_value_text = _optional_text(record.get("min_value_text"))
        max_value_text = _optional_text(record.get("max_value_text"))
        row = {
            "table_name": table_name,
            "file_path": file_path,
            "absolute_file_path": _indexed_file_path(table_name, file_path, warehouse_dir),
            "column_name": column_name,
            "data_type": data_type,
            "value_kind": value_kind,
            "num_records": num_records,
            "null_count": null_count,
            "non_null_count": max(num_records - null_count, 0),
            "has_nulls": null_count > 0,
            "all_nulls": num_records > 0 and null_count >= num_records,
            "min_value_text": min_value_text,
            "max_value_text": max_value_text,
            "min_numeric": None,
            "max_numeric": None,
            "min_timestamp": None,
            "max_timestamp": None,
            "min_date": None,
            "max_date": None,
            "min_boolean": None,
            "max_boolean": None,
        }
        if value_kind == "numeric":
            row["min_numeric"] = _parse_decimal(min_value_text)
            row["max_numeric"] = _parse_decimal(max_value_text)
        elif value_kind == "timestamp":
            row["min_timestamp"] = _parse_timestamp(min_value_text)
            row["max_timestamp"] = _parse_timestamp(max_value_text)
        elif value_kind == "date":
            row["min_date"] = _parse_date(min_value_text)
            row["max_date"] = _parse_date(max_value_text)
        elif value_kind == "boolean":
            row["min_boolean"] = _parse_bool(min_value_text)
            row["max_boolean"] = _parse_bool(max_value_text)
        rows.append(row)
    return pd.DataFrame(rows, columns=INDEX_COLUMNS)


def iter_warehouse_parquet_files(warehouse_dir: Path = DELTA_WAREHOUSE_DIR) -> list[Path]:
    """Return every parquet data file stored under the Delta warehouse root."""

    return sorted(path for path in warehouse_dir.rglob("*.parquet") if path.is_file())


def _footer_row_group_records(parquet_path: Path, warehouse_dir: Path) -> list[dict[str, object]]:
    """Extract row-group footer statistics rows for one parquet file."""

    relative_path = parquet_path.relative_to(warehouse_dir)
    if len(relative_path.parts) < 2:
        return []
    table_name = relative_path.parts[0]
    file_path = "/".join(relative_path.parts[1:])
    absolute_file_path = _workspace_relative_path(parquet_path)

    parquet_file = pq.ParquetFile(parquet_path)
    metadata = parquet_file.metadata
    arrow_schema = parquet_file.schema_arrow
    rows: list[dict[str, object]] = []

    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            column_chunk = row_group.column(column_index)
            column_name = column_chunk.path_in_schema
            try:
                field = arrow_schema.field(column_name)
                data_type = str(field.type)
            except KeyError:
                data_type = str(column_chunk.physical_type).lower()
            value_kind = normalize_value_kind(data_type)
            stats = column_chunk.statistics
            null_count = int(stats.null_count) if stats and stats.null_count is not None else None
            non_null_count = None if null_count is None else max(int(row_group.num_rows) - null_count, 0)
            min_value = stats.min if stats and stats.has_min_max else None
            max_value = stats.max if stats and stats.has_min_max else None
            row = {
                "table_name": table_name,
                "file_path": file_path,
                "absolute_file_path": absolute_file_path,
                "row_group_index": row_group_index,
                "column_name": column_name,
                "data_type": data_type,
                "value_kind": value_kind,
                "num_rows": int(row_group.num_rows),
                "total_byte_size": int(row_group.total_byte_size),
                "null_count": null_count,
                "non_null_count": non_null_count,
            }
            row.update(_footer_value_columns(value_kind, min_value, max_value))
            rows.append(row)
    return rows


def build_footer_row_group_stats_frame(
    parquet_paths: list[Path],
    warehouse_dir: Path = DELTA_WAREHOUSE_DIR,
) -> pd.DataFrame:
    """Build a dataframe of parquet footer row-group statistics for the supplied files."""

    rows: list[dict[str, object]] = []
    for parquet_path in parquet_paths:
        rows.extend(_footer_row_group_records(parquet_path, warehouse_dir))
    return pd.DataFrame(rows, columns=FOOTER_INDEX_COLUMNS)


def footer_row_group_stats_dtypes() -> dict[str, object]:
    """Return explicit SQLAlchemy dtypes for the footer row-group stats table."""

    return {
        "table_name": Text(),
        "file_path": Text(),
        "absolute_file_path": Text(),
        "row_group_index": Integer(),
        "column_name": Text(),
        "data_type": Text(),
        "value_kind": Text(),
        "num_rows": Integer(),
        "total_byte_size": Integer(),
        "null_count": Integer(),
        "non_null_count": Integer(),
        "min_value_text": Text(),
        "max_value_text": Text(),
        "min_numeric": Numeric(),
        "max_numeric": Numeric(),
        "min_timestamp": DateTime(),
        "max_timestamp": DateTime(),
        "min_date": Date(),
        "max_date": Date(),
        "min_boolean": Boolean(),
        "max_boolean": Boolean(),
    }


def _read_metadata_frames(engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the metadata tables required to build the pruning index."""

    stats_frame = pd.read_sql("SELECT * FROM metadata.current_snapshot_file_stats", engine)
    catalog_frame = pd.read_sql("SELECT * FROM metadata.column_catalog", engine)
    return stats_frame, catalog_frame


def create_predicate_index(
    postgres_url: str = DEFAULT_POSTGRES_URL,
    schema_name: str = "metadata",
    table_name: str = "predicate_file_index",
    warehouse_dir: Path = DELTA_WAREHOUSE_DIR,
) -> int:
    """Rebuild the Postgres-backed pruning index table and return the row count written."""

    engine = postgres_engine(postgres_url)
    stats_frame, catalog_frame = _read_metadata_frames(engine)
    index_frame = build_predicate_index_frame(stats_frame, catalog_frame, warehouse_dir)
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    index_frame.to_sql(table_name, engine, schema=schema_name, if_exists="replace", index=False)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS {table_name}_table_file_idx
                ON {schema_name}.{table_name} (table_name, file_path)
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS {table_name}_table_column_idx
                ON {schema_name}.{table_name} (table_name, column_name)
                """
            )
        )
    return len(index_frame)


def create_footer_row_group_stats_index(
    postgres_url: str = DEFAULT_POSTGRES_URL,
    schema_name: str = "parquet_footer",
    table_name: str = "row_group_stats",
    warehouse_dir: Path = DELTA_WAREHOUSE_DIR,
) -> int:
    """Rebuild a warehouse-wide parquet footer row-group statistics table in Postgres."""

    engine = postgres_engine(postgres_url)
    parquet_paths = iter_warehouse_parquet_files(warehouse_dir)
    footer_frame = build_footer_row_group_stats_frame(parquet_paths, warehouse_dir)
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    footer_frame.to_sql(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",
        index=False,
        dtype=footer_row_group_stats_dtypes(),
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS {table_name}_table_file_row_group_idx
                ON {schema_name}.{table_name} (table_name, file_path, row_group_index)
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS {table_name}_table_column_idx
                ON {schema_name}.{table_name} (table_name, column_name)
                """
            )
        )
    return len(footer_frame)


def _comparison_column(alias: str, value_kind: str, prefix: str) -> str:
    """Return the typed min or max column name used for predicate comparisons."""

    if value_kind == "numeric":
        return f"CAST({alias}.{prefix}_numeric AS NUMERIC)"
    if value_kind == "timestamp":
        return f"CAST({alias}.{prefix}_timestamp AS TIMESTAMP)"
    if value_kind == "date":
        return f"CAST({alias}.{prefix}_date AS DATE)"
    if value_kind == "boolean":
        return f"CAST({alias}.{prefix}_boolean AS BOOLEAN)"
    return f"{alias}.{prefix}_value_text"


def _coerce_predicate_value(value_kind: str, value: object) -> object:
    """Coerce a user-supplied predicate value into the typed form stored in the index."""

    if value_kind == "numeric":
        parsed = _parse_decimal(value)
    elif value_kind == "timestamp":
        parsed = _parse_timestamp(value)
    elif value_kind == "date":
        parsed = _parse_date(value)
    elif value_kind == "boolean":
        parsed = _parse_bool(value)
    else:
        parsed = None if value is None else str(value)
    if parsed is None and value is not None:
        raise ValueError(f"Could not coerce value {value!r} to {value_kind}")
    return parsed


def _predicate_clause(alias: str, predicate: Predicate, value_kind: str, parameter_prefix: str):
    """Build the SQL fragment and bind parameters for a single pruning predicate."""

    params: dict[str, object] = {f"{parameter_prefix}_column": predicate.column_name}
    if predicate.operator == "is_null":
        return f"{alias}.null_count > 0", params
    if predicate.operator == "is_not_null":
        return f"{alias}.non_null_count > 0", params

    min_column = _comparison_column(alias, value_kind, "min")
    max_column = _comparison_column(alias, value_kind, "max")
    value = _coerce_predicate_value(value_kind, predicate.value)
    params[f"{parameter_prefix}_value"] = value

    if predicate.operator == "=":
        clause = (
            f"{alias}.non_null_count > 0 AND "
            f"{min_column} <= :{parameter_prefix}_value AND {max_column} >= :{parameter_prefix}_value"
        )
    elif predicate.operator == "!=":
        clause = (
            f"{alias}.non_null_count > 0 AND NOT ("
            f"{min_column} = :{parameter_prefix}_value AND {max_column} = :{parameter_prefix}_value "
            f"AND {alias}.null_count = 0)"
        )
    elif predicate.operator == "<":
        clause = f"{alias}.non_null_count > 0 AND {min_column} < :{parameter_prefix}_value"
    elif predicate.operator == "<=":
        clause = f"{alias}.non_null_count > 0 AND {min_column} <= :{parameter_prefix}_value"
    elif predicate.operator == ">":
        clause = f"{alias}.non_null_count > 0 AND {max_column} > :{parameter_prefix}_value"
    elif predicate.operator == ">=":
        clause = f"{alias}.non_null_count > 0 AND {max_column} >= :{parameter_prefix}_value"
    else:
        params[f"{parameter_prefix}_second_value"] = _coerce_predicate_value(
            value_kind, predicate.second_value
        )
        clause = (
            f"{alias}.non_null_count > 0 AND "
            f"{min_column} <= :{parameter_prefix}_second_value AND "
            f"{max_column} >= :{parameter_prefix}_value"
        )
    return clause, params


def build_candidate_files_query(
    table_name: str,
    predicates: list[Predicate],
    column_types: dict[str, str],
    schema_name: str = "metadata",
    index_table_name: str = "predicate_file_index",
) -> tuple[str, dict[str, object]]:
    """Build the SQL query that returns candidate parquet files for a conjunction of predicates."""

    sql = [
        "SELECT DISTINCT base.absolute_file_path",
        f"FROM {schema_name}.{index_table_name} AS base",
        "WHERE base.table_name = :table_name",
    ]
    params: dict[str, object] = {"table_name": table_name}
    for index, predicate in enumerate(predicates):
        value_kind = normalize_value_kind(column_types.get(predicate.column_name))
        alias = f"p{index}"
        clause, clause_params = _predicate_clause(alias, predicate, value_kind, f"predicate_{index}")
        sql.append(
            f"""AND EXISTS (
    SELECT 1
    FROM {schema_name}.{index_table_name} AS {alias}
    WHERE {alias}.table_name = base.table_name
      AND {alias}.file_path = base.file_path
      AND {alias}.column_name = :predicate_{index}_column
      AND {clause}
)"""
        )
        params.update(clause_params)
    sql.append("ORDER BY base.absolute_file_path")
    return "\n".join(sql), params


def _load_column_types(engine, table_name: str) -> dict[str, str]:
    """Load the known column types for one Delta table from `metadata.column_catalog`."""

    query = text(
        """
        SELECT column_name, data_type
        FROM metadata.column_catalog
        WHERE table_name = :table_name
        """
    )
    with engine.connect() as connection:
        return {row.column_name: row.data_type for row in connection.execute(query, {"table_name": table_name})}


def find_candidate_files(
    table_name: str,
    predicates: list[Predicate],
    postgres_url: str = DEFAULT_POSTGRES_URL,
    schema_name: str = "metadata",
    index_table_name: str = "predicate_file_index",
) -> list[str]:
    """Return the parquet files whose recorded bounds may satisfy all supplied predicates."""

    engine = postgres_engine(postgres_url)
    column_types = _load_column_types(engine, table_name)
    missing = sorted({predicate.column_name for predicate in predicates} - set(column_types))
    if missing:
        raise ValueError(f"Columns not found in metadata.column_catalog for {table_name}: {missing}")
    query, params = build_candidate_files_query(
        table_name,
        predicates,
        column_types=column_types,
        schema_name=schema_name,
        index_table_name=index_table_name,
    )
    with engine.connect() as connection:
        return [row.absolute_file_path for row in connection.execute(text(query), params)]


def build_duckdb_parquet_scan(file_paths: list[str]) -> str:
    """Render a DuckDB `read_parquet([...])` expression for the selected file list."""

    if not file_paths:
        raise ValueError("No matching files were found for the requested predicates")
    normalized_paths = []
    for file_path in file_paths:
        path = Path(file_path)
        if not path.is_absolute():
            path = (WORKSPACE_ROOT / path).resolve()
        normalized_paths.append(str(path).replace("\\", "/"))
    literal_paths = ", ".join(repr(path) for path in normalized_paths)
    return f"read_parquet([{literal_paths}])"
