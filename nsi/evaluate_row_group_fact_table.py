"""Evaluate exact-match pruning via row-group metadata and Delta log parsing."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import text

from .config import DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT
from .indexer import build_duckdb_parquet_scan, postgres_engine


def _read_matching_trips(file_paths: list[str], column_name: str, value: Decimal) -> pd.DataFrame:
    """Read exact-match rows from the supplied parquet files with DuckDB."""

    scan = build_duckdb_parquet_scan(file_paths)
    query = f"""
        SELECT *
        FROM {scan}
        WHERE {column_name} = {value}
        ORDER BY pickup_at, dropoff_at
    """
    return duckdb.connect().execute(query).fetchdf()


def _write_output(output_dir: Path, summary: dict[str, object], trips: pd.DataFrame) -> None:
    """Write the evaluation summary and matching trips to disk."""

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trips.to_parquet(output_dir / "matching_trips.parquet", index=False)
    trips.to_csv(output_dir / "matching_trips.csv", index=False)


def _row_group_candidate_info(table_name: str, column_name: str, value: Decimal) -> tuple[list[str], int, int]:
    """Return candidate files, candidate row groups, and index row count from Postgres."""

    engine = postgres_engine()
    with engine.connect() as connection:
        row_groups = list(
            connection.execute(
                text(
                    """
                    SELECT DISTINCT file_path, absolute_file_path, row_group_index
                    FROM metadata.predicate_row_group_index
                    WHERE table_name = :table_name
                      AND column_name = :column_name
                      AND non_null_count > 0
                      AND min_numeric <= :value
                      AND max_numeric >= :value
                    ORDER BY absolute_file_path, row_group_index
                    """
                ),
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "value": value,
                },
            )
        )
        row_count = connection.execute(
            text("SELECT COUNT(*) AS row_count FROM metadata.predicate_row_group_index")
        ).one()

    candidate_files = sorted({row.absolute_file_path for row in row_groups})
    return candidate_files, len(row_groups), int(row_count.row_count)


def _absolute_path(table_name: str, relative_path: str) -> str:
    """Return the workspace-relative path for one Delta parquet file."""

    return (DELTA_WAREHOUSE_DIR / table_name / relative_path).relative_to(WORKSPACE_ROOT).as_posix()


def _delta_log_candidate_paths(
    table_name: str,
    column_name: str,
    value: Decimal,
) -> tuple[list[str], int]:
    """Parse Delta log JSON files and return active candidate parquet paths plus log count."""

    log_dir = DELTA_WAREHOUSE_DIR / table_name / "_delta_log"
    active_adds: dict[str, dict[str, object]] = {}
    logs_read = 0
    for log_path in sorted(log_dir.glob("*.json")):
        logs_read += 1
        for line in log_path.read_text(encoding="utf-8").splitlines():
            payload = json.loads(line)
            add = payload.get("add")
            if add is not None:
                active_adds[add["path"]] = add
            remove = payload.get("remove")
            if remove is not None:
                active_adds.pop(remove["path"], None)

    candidate_paths: list[str] = []
    for relative_path, add in active_adds.items():
        stats_text = add.get("stats")
        if not stats_text:
            candidate_paths.append(_absolute_path(table_name, relative_path))
            continue
        stats = json.loads(stats_text)
        minimum = stats.get("minValues", {}).get(column_name)
        maximum = stats.get("maxValues", {}).get(column_name)
        if minimum is None or maximum is None:
            candidate_paths.append(_absolute_path(table_name, relative_path))
            continue
        if Decimal(str(minimum)) <= value and Decimal(str(maximum)) >= value:
            candidate_paths.append(_absolute_path(table_name, relative_path))
    return sorted(candidate_paths), logs_read


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for row-group exact-match evaluation."""

    parser = argparse.ArgumentParser(
        description="Evaluate row-group NSI and Delta-log pruning for an exact-match predicate."
    )
    parser.add_argument("--table", required=True)
    parser.add_argument("--column", required=True)
    parser.add_argument("--value", required=True)
    return parser


def main() -> None:
    """Run both pruning evaluations for the requested exact-match predicate."""

    args = build_parser().parse_args()
    table_name = args.table
    column_name = args.column
    value = Decimal(args.value)
    label = f"{table_name}_{column_name}_eq_{str(value).replace('.', '_')}"
    predicate_output_dir = WORKSPACE_ROOT / "predicate_output" / label
    delta_log_output_dir = WORKSPACE_ROOT / "delta_log_output" / label

    candidate_files, candidate_row_groups, row_group_index_row_count = _row_group_candidate_info(
        table_name, column_name, value
    )
    predicate_trips = _read_matching_trips(candidate_files, column_name, value)
    predicate_summary = {
        "table_name": table_name,
        "column_name": column_name,
        "value": str(value),
        "candidate_file_count": len(candidate_files),
        "candidate_row_group_count": candidate_row_groups,
        "matching_trip_count": len(predicate_trips),
        "predicate_row_group_index_row_count": row_group_index_row_count,
        "candidate_files": candidate_files,
        "source": "metadata.predicate_row_group_index",
    }
    _write_output(predicate_output_dir, predicate_summary, predicate_trips)

    delta_files, delta_logs_read = _delta_log_candidate_paths(table_name, column_name, value)
    delta_trips = _read_matching_trips(delta_files, column_name, value)
    delta_summary = {
        "table_name": table_name,
        "column_name": column_name,
        "value": str(value),
        "candidate_file_count": len(delta_files),
        "matching_trip_count": len(delta_trips),
        "delta_log_json_files_read": delta_logs_read,
        "candidate_files": delta_files,
        "source": "delta_log_json",
    }
    _write_output(delta_log_output_dir, delta_summary, delta_trips)

    print(
        json.dumps(
            {
                "predicate_output": predicate_summary,
                "delta_log_output": delta_summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
