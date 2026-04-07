"""Evaluate file pruning for a fact table via the NSI index and Delta log parsing."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import text

from .config import DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT
from .indexer import Predicate, build_duckdb_parquet_scan, find_candidate_files, postgres_engine


def _predicate(column_name: str, lower_bound: Decimal, upper_bound: Decimal) -> Predicate:
    """Return the shared inclusive-range predicate for the evaluation run."""

    return Predicate(column_name, "between", str(lower_bound), str(upper_bound))


def _read_matching_trips(
    file_paths: list[str],
    column_name: str,
    lower_bound: Decimal,
    upper_bound: Decimal,
) -> pd.DataFrame:
    """Read matching trips from the supplied parquet files with DuckDB."""

    scan = build_duckdb_parquet_scan(file_paths)
    query = f"""
        SELECT *
        FROM {scan}
        WHERE {column_name} BETWEEN {lower_bound} AND {upper_bound}
        ORDER BY pickup_at, dropoff_at
    """
    return duckdb.connect().execute(query).fetchdf()


def _write_output(output_dir: Path, summary: dict[str, object], trips: pd.DataFrame) -> None:
    """Write the evaluation summary and matching trips to the requested output directory."""

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trips.to_parquet(output_dir / "matching_trips.parquet", index=False)
    trips.to_csv(output_dir / "matching_trips.csv", index=False)


def _predicate_index_file_paths(
    table_name: str,
    column_name: str,
    lower_bound: Decimal,
    upper_bound: Decimal,
) -> list[str]:
    """Load candidate parquet files for the target predicate from Postgres metadata."""

    return find_candidate_files(table_name, [_predicate(column_name, lower_bound, upper_bound)])


def _absolute_path(table_name: str, relative_path: str) -> str:
    """Return the workspace-relative path for one Delta parquet file."""

    return (DELTA_WAREHOUSE_DIR / table_name / relative_path).relative_to(WORKSPACE_ROOT).as_posix()


def _delta_log_candidate_paths(
    table_name: str,
    column_name: str,
    lower_bound: Decimal,
    upper_bound: Decimal,
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
        if Decimal(str(minimum)) <= upper_bound and Decimal(str(maximum)) >= lower_bound:
            candidate_paths.append(_absolute_path(table_name, relative_path))
    return sorted(candidate_paths), logs_read


def _existing_predicate_index_rows() -> int:
    """Return the number of rows currently stored in `metadata.predicate_file_index`."""

    engine = postgres_engine()
    with engine.connect() as connection:
        row = connection.execute(text("SELECT COUNT(*) AS row_count FROM metadata.predicate_file_index")).one()
    return int(row.row_count)


def _run_predicate_index_evaluation(
    table_name: str,
    column_name: str,
    lower_bound: Decimal,
    upper_bound: Decimal,
    output_dir: Path,
) -> dict[str, object]:
    """Evaluate pruning via the Postgres-backed predicate index and write its output files."""

    candidate_paths = _predicate_index_file_paths(table_name, column_name, lower_bound, upper_bound)
    matching_trips = _read_matching_trips(candidate_paths, column_name, lower_bound, upper_bound)
    summary = {
        "table_name": table_name,
        "column_name": column_name,
        "lower_bound": str(lower_bound),
        "upper_bound": str(upper_bound),
        "candidate_file_count": len(candidate_paths),
        "matching_trip_count": len(matching_trips),
        "predicate_index_row_count": _existing_predicate_index_rows(),
        "candidate_files": candidate_paths,
        "source": "metadata.predicate_file_index",
    }
    _write_output(output_dir, summary, matching_trips)
    return summary


def _run_delta_log_evaluation(
    table_name: str,
    column_name: str,
    lower_bound: Decimal,
    upper_bound: Decimal,
    output_dir: Path,
) -> dict[str, object]:
    """Evaluate pruning via direct Delta log parsing and write its output files."""

    candidate_paths, logs_read = _delta_log_candidate_paths(
        table_name, column_name, lower_bound, upper_bound
    )
    matching_trips = _read_matching_trips(candidate_paths, column_name, lower_bound, upper_bound)
    summary = {
        "table_name": table_name,
        "column_name": column_name,
        "lower_bound": str(lower_bound),
        "upper_bound": str(upper_bound),
        "candidate_file_count": len(candidate_paths),
        "matching_trip_count": len(matching_trips),
        "delta_log_json_files_read": logs_read,
        "candidate_files": candidate_paths,
        "source": "delta_log_json",
    }
    _write_output(output_dir, summary, matching_trips)
    return summary


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for fact-table pruning evaluation."""

    parser = argparse.ArgumentParser(description="Evaluate NSI and Delta-log pruning for a fact table.")
    parser.add_argument("--table", required=True)
    parser.add_argument("--column", default="total_amount")
    parser.add_argument("--lower", required=True)
    parser.add_argument("--upper", required=True)
    return parser


def main() -> None:
    """Run both pruning evaluations for the requested fact table and print their summaries."""

    args = build_parser().parse_args()
    table_name = args.table
    column_name = args.column
    lower_bound = Decimal(args.lower)
    upper_bound = Decimal(args.upper)
    predicate_output_dir = WORKSPACE_ROOT / "predicate_output" / table_name
    delta_log_output_dir = WORKSPACE_ROOT / "delta_log_output" / table_name

    predicate_summary = _run_predicate_index_evaluation(
        table_name,
        column_name,
        lower_bound,
        upper_bound,
        predicate_output_dir,
    )
    delta_log_summary = _run_delta_log_evaluation(
        table_name,
        column_name,
        lower_bound,
        upper_bound,
        delta_log_output_dir,
    )
    print(
        json.dumps(
            {
                "predicate_output": predicate_summary,
                "delta_log_output": delta_log_summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
