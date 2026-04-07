from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .delta_io import delta_table, parquet_files_for_delta, table_path

PREDICATE_ROW_GROUP_INDEX_COLUMNS = [
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


def quoted_identifier(identifier: str) -> str:
    """Quote an identifier for DuckDB SQL generated in metadata helpers."""
    return '"' + identifier.replace('"', '""') + '"'


def collect_column_catalog(table_names: list[str]) -> pd.DataFrame:
    """Collect schema-level column metadata for the supplied Delta tables."""
    rows: list[dict[str, object]] = []
    for table_name in table_names:
        schema = delta_table(table_name).schema().to_pyarrow()
        for ordinal, field in enumerate(schema, start=1):
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": field.name,
                    "ordinal_position": ordinal,
                    "data_type": str(field.type),
                    "nullable": field.nullable,
                }
            )
    return pd.DataFrame(rows)


def collect_column_statistics(table_names: list[str]) -> pd.DataFrame:
    """Collect table-level column statistics by scanning the active Delta parquet files."""
    rows: list[dict[str, object]] = []
    con = duckdb.connect()
    for table_name in table_names:
        parquet_files = parquet_files_for_delta(table_name)
        columns = [field.name for field in pq.read_schema(parquet_files[0])]
        file_list = ", ".join(repr(path) for path in parquet_files)
        total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet([{file_list}])").fetchone()[0]
        for column_name in columns:
            quoted = quoted_identifier(column_name)
            query = f"""
                SELECT
                    SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS null_count,
                    COUNT(DISTINCT {quoted}) AS distinct_count,
                    MIN(CAST({quoted} AS VARCHAR)) AS min_value_text,
                    MAX(CAST({quoted} AS VARCHAR)) AS max_value_text
                FROM read_parquet([{file_list}])
            """
            null_count, distinct_count, min_value, max_value = con.execute(query).fetchone()
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "row_count": int(total_rows),
                    "null_count": int(null_count or 0),
                    "distinct_count": int(distinct_count or 0),
                    "min_value_text": min_value,
                    "max_value_text": max_value,
                }
            )
    return pd.DataFrame(rows)


def collect_delta_log_entries(table_names: list[str]) -> pd.DataFrame:
    """Flatten all Delta log JSON entries for the supplied tables into row form."""
    rows: list[dict[str, object]] = []
    for table_name in table_names:
        log_dir = table_path(table_name) / "_delta_log"
        for log_file in sorted(log_dir.glob("*.json")):
            version = int(log_file.stem)
            with log_file.open("r", encoding="utf-8") as handle:
                for entry_index, line in enumerate(handle, start=1):
                    payload = json.loads(line)
                    entry_type = next(iter(payload))
                    rows.append(
                        {
                            "table_name": table_name,
                            "version": version,
                            "log_file": log_file.name,
                            "entry_index": entry_index,
                            "entry_type": entry_type,
                            "payload_json": json.dumps(payload, sort_keys=True),
                        }
                    )
    return pd.DataFrame(rows)


def export_parquet(dataframe: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to parquet, creating parent directories when needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)


def _stringify_stat_value(value: object) -> str | None:
    """Convert Delta stat values into stable text suitable for parquet and Postgres."""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _normalize_value_kind(data_type: str | None) -> str:
    """Map a catalog data type string to the predicate index comparison family."""
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


def _normalize_parquet_stat_value(value: object) -> object:
    """Convert Parquet metadata values into stable Python scalars before text serialization."""
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (AttributeError, TypeError, ValueError):
            pass
    return value


def _collect_row_group_statistics(table_names: list[str]) -> pd.DataFrame:
    """Collect raw row-group parquet statistics for the active files in each Delta table."""
    rows: list[dict[str, object]] = []
    for table_name in table_names:
        snapshot = delta_table(table_name)
        for file_path, absolute_file_path in zip(snapshot.files(), snapshot.file_uris(), strict=True):
            parquet_file = pq.ParquetFile(absolute_file_path)
            schema = parquet_file.schema_arrow
            field_types = {field.name: str(field.type) for field in schema}
            for row_group_index in range(parquet_file.metadata.num_row_groups):
                row_group = parquet_file.metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    column_chunk = row_group.column(column_index)
                    column_name = column_chunk.path_in_schema
                    statistics = column_chunk.statistics
                    null_count = None
                    min_value_text = None
                    max_value_text = None
                    if statistics is not None:
                        if statistics.has_null_count:
                            null_count = int(statistics.null_count)
                        if statistics.has_min_max:
                            min_value_text = _stringify_stat_value(
                                _normalize_parquet_stat_value(statistics.min)
                            )
                            max_value_text = _stringify_stat_value(
                                _normalize_parquet_stat_value(statistics.max)
                            )
                    rows.append(
                        {
                            "table_name": table_name,
                            "file_path": file_path,
                            "absolute_file_path": absolute_file_path,
                            "row_group_index": row_group_index,
                            "num_rows": int(row_group.num_rows),
                            "total_byte_size": int(row_group.total_byte_size),
                            "column_name": column_name,
                            "data_type": field_types.get(column_name, "string"),
                            "null_count": null_count,
                            "min_value_text": min_value_text,
                            "max_value_text": max_value_text,
                        }
                    )
    return pd.DataFrame(rows)


def _build_predicate_row_group_index_frame(stats_frame: pd.DataFrame) -> pd.DataFrame:
    """Build the typed row-group predicate index frame from raw parquet metadata rows."""
    rows: list[dict[str, object]] = []
    for record in stats_frame.to_dict(orient="records"):
        data_type = str(record.get("data_type") or "string")
        value_kind = _normalize_value_kind(data_type)
        num_rows = _optional_int(record.get("num_rows"))
        null_count = _optional_int(record.get("null_count"))
        min_value_text = _optional_text(record.get("min_value_text"))
        max_value_text = _optional_text(record.get("max_value_text"))
        row = {
            "table_name": str(record["table_name"]),
            "file_path": str(record["file_path"]),
            "absolute_file_path": str(record["absolute_file_path"]),
            "row_group_index": _optional_int(record.get("row_group_index")),
            "column_name": str(record["column_name"]),
            "data_type": data_type,
            "value_kind": value_kind,
            "num_rows": num_rows,
            "total_byte_size": _optional_int(record.get("total_byte_size")),
            "null_count": null_count,
            "non_null_count": max(num_rows - null_count, 0),
            "has_nulls": null_count > 0,
            "all_nulls": num_rows > 0 and null_count >= num_rows,
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
    return pd.DataFrame(rows, columns=PREDICATE_ROW_GROUP_INDEX_COLUMNS)


def collect_predicate_row_group_index(table_names: list[str]) -> pd.DataFrame:
    """Collect a typed row-group predicate index for the current Delta table snapshots."""
    return _build_predicate_row_group_index_frame(_collect_row_group_statistics(table_names))


def collect_current_snapshot_file_stats(table_names: list[str]) -> pd.DataFrame:
    """Collect current-snapshot per-file, per-column stats from Delta add actions."""
    rows: list[dict[str, object]] = []
    for table_name in table_names:
        add_actions = delta_table(table_name).get_add_actions(flatten=True)
        stat_columns = {}
        for field_name in add_actions.schema.names:
            prefix, _, column_name = field_name.partition(".")
            if prefix in {"null_count", "min", "max"} and column_name:
                stat_columns.setdefault(column_name, {})[prefix] = field_name

        for record in add_actions.to_pylist():
            for column_name, columns in stat_columns.items():
                rows.append(
                    {
                        "table_name": table_name,
                        "file_path": record["path"],
                        "size_bytes": record["size_bytes"],
                        "modification_time": record["modification_time"],
                        "data_change": record["data_change"],
                        "num_records": record["num_records"],
                        "column_name": column_name,
                        "null_count": record.get(columns.get("null_count", ""), 0),
                        "min_value_text": _stringify_stat_value(record.get(columns.get("min", ""))),
                        "max_value_text": _stringify_stat_value(record.get(columns.get("max", ""))),
                    }
                )
    return pd.DataFrame(rows)


def collect_current_snapshot_files(table_names: list[str]) -> pd.DataFrame:
    """Collect one row per active Delta file with nested column stats as JSON text."""
    rows: list[dict[str, object]] = []
    for table_name in table_names:
        add_actions = delta_table(table_name).get_add_actions(flatten=True)
        stat_columns = {}
        for field_name in add_actions.schema.names:
            prefix, _, column_name = field_name.partition(".")
            if prefix in {"null_count", "min", "max"} and column_name:
                stat_columns.setdefault(column_name, {})[prefix] = field_name

        for record in add_actions.to_pylist():
            column_stats = {}
            for column_name, columns in stat_columns.items():
                column_stats[column_name] = {
                    "null_count": record.get(columns.get("null_count", ""), 0),
                    "min": _stringify_stat_value(record.get(columns.get("min", ""))),
                    "max": _stringify_stat_value(record.get(columns.get("max", ""))),
                }

            rows.append(
                {
                    "table_name": table_name,
                    "file_path": record["path"],
                    "size_bytes": record["size_bytes"],
                    "modification_time": record["modification_time"],
                    "data_change": record["data_change"],
                    "num_records": record["num_records"],
                    "column_stats_json": json.dumps(column_stats, sort_keys=True),
                }
            )
    return pd.DataFrame(rows)
