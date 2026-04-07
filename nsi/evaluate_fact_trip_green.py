"""Evaluate `fact_trip_green` pruning via Postgres metadata and Delta log parsing."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import text

from .config import DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT
from .indexer import Predicate, build_duckdb_parquet_scan, find_candidate_files, postgres_engine

TABLE_NAME = "fact_trip_green"
COLUMN_NAME = "total_amount"
LOWER_BOUND = Decimal("400")
UPPER_BOUND = Decimal("500")
PREDICATE_OUTPUT_DIR = WORKSPACE_ROOT / "predicate_output"
DELTA_LOG_OUTPUT_DIR = WORKSPACE_ROOT / "delta_log_output"


def _predicate() -> Predicate:
    """Return the shared evaluation predicate for the green trips fact table."""

    return Predicate(COLUMN_NAME, "between", str(LOWER_BOUND), str(UPPER_BOUND))


def _read_matching_trips(file_paths: list[str]) -> pd.DataFrame:
    """Read matching trips from the supplied parquet files with DuckDB."""

    scan = build_duckdb_parquet_scan(file_paths)
    query = f"""
        SELECT *
        FROM {scan}
        WHERE total_amount BETWEEN {LOWER_BOUND} AND {UPPER_BOUND}
        ORDER BY pickup_at, dropoff_at
    """
    return duckdb.connect().execute(query).fetchdf()


def _write_output(output_dir: Path, summary: dict[str, object], trips: pd.DataFrame) -> None:
    """Write the evaluation summary and matching trips to the requested output directory."""

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trips.to_parquet(output_dir / "matching_trips.parquet", index=False)
    trips.to_csv(output_dir / "matching_trips.csv", index=False)


def _predicate_index_file_paths() -> list[str]:
    """Load candidate parquet files for the target predicate from Postgres metadata."""

    return find_candidate_files(TABLE_NAME, [_predicate()])


def _delta_log_candidate_paths() -> tuple[list[str], int]:
    """Parse Delta log JSON files and return active candidate parquet paths plus log count."""

    log_dir = DELTA_WAREHOUSE_DIR / TABLE_NAME / "_delta_log"
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
            candidate_paths.append(_absolute_path(relative_path))
            continue
        stats = json.loads(stats_text)
        minimum = stats.get("minValues", {}).get(COLUMN_NAME)
        maximum = stats.get("maxValues", {}).get(COLUMN_NAME)
        if minimum is None or maximum is None:
            candidate_paths.append(_absolute_path(relative_path))
            continue
        if Decimal(str(minimum)) <= UPPER_BOUND and Decimal(str(maximum)) >= LOWER_BOUND:
            candidate_paths.append(_absolute_path(relative_path))
    return sorted(candidate_paths), logs_read


def _absolute_path(relative_path: str) -> str:
    """Resolve a table-relative parquet path to a normalized absolute path."""

    return str((DELTA_WAREHOUSE_DIR / TABLE_NAME / relative_path).resolve()).replace("\\", "/")


def _existing_predicate_index_rows() -> int:
    """Return the number of rows currently stored in `metadata.predicate_file_index`."""

    engine = postgres_engine()
    with engine.connect() as connection:
        row = connection.execute(text("SELECT COUNT(*) AS row_count FROM metadata.predicate_file_index")).one()
    return int(row.row_count)


def _run_predicate_index_evaluation() -> dict[str, object]:
    """Evaluate pruning via the Postgres-backed predicate index and write its output files."""

    candidate_paths = _predicate_index_file_paths()
    matching_trips = _read_matching_trips(candidate_paths)
    summary = {
        "table_name": TABLE_NAME,
        "column_name": COLUMN_NAME,
        "lower_bound": str(LOWER_BOUND),
        "upper_bound": str(UPPER_BOUND),
        "candidate_file_count": len(candidate_paths),
        "matching_trip_count": len(matching_trips),
        "predicate_index_row_count": _existing_predicate_index_rows(),
        "candidate_files": candidate_paths,
        "source": "metadata.predicate_file_index",
    }
    _write_output(PREDICATE_OUTPUT_DIR, summary, matching_trips)
    return summary


def _run_delta_log_evaluation() -> dict[str, object]:
    """Evaluate pruning via direct Delta log parsing and write its output files."""

    candidate_paths, logs_read = _delta_log_candidate_paths()
    matching_trips = _read_matching_trips(candidate_paths)
    summary = {
        "table_name": TABLE_NAME,
        "column_name": COLUMN_NAME,
        "lower_bound": str(LOWER_BOUND),
        "upper_bound": str(UPPER_BOUND),
        "candidate_file_count": len(candidate_paths),
        "matching_trip_count": len(matching_trips),
        "delta_log_json_files_read": logs_read,
        "candidate_files": candidate_paths,
        "source": "delta_log_json",
    }
    _write_output(DELTA_LOG_OUTPUT_DIR, summary, matching_trips)
    return summary


def main() -> None:
    """Run both pruning evaluations and print their summaries as JSON."""

    predicate_summary = _run_predicate_index_evaluation()
    delta_log_summary = _run_delta_log_evaluation()
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
