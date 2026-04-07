"""Evaluate typed multi-predicate pruning via row-group metadata and Delta log parsing."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import text

from .config import DELTA_WAREHOUSE_DIR, WORKSPACE_ROOT
from .indexer import (
    Predicate,
    _coerce_predicate_value,
    build_duckdb_parquet_scan,
    normalize_value_kind,
    postgres_engine,
)

SUPPORTED_OPERATORS = {"=", "!=", "<", "<=", ">", ">=", "between"}
SLUG_OPERATOR_NAMES = {
    "=": "eq",
    "!=": "ne",
    "<": "lt",
    "<=": "lte",
    ">": "gt",
    ">=": "gte",
    "between": None,
}


def _slug_value(value: object) -> str:
    """Convert a predicate value into a filesystem-safe label fragment."""

    return (
        str(value)
        .replace(".", "_")
        .replace(":", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )


def _parse_predicate(raw_parts: list[str]) -> Predicate:
    """Parse one `--predicate` argument group into a generic typed `Predicate`."""

    if len(raw_parts) < 3:
        raise ValueError("Each --predicate requires at least: <column> <operator> <value>")
    column_name, operator, *values = raw_parts
    if operator not in SUPPORTED_OPERATORS:
        raise ValueError(f"Unsupported row-group evaluation operator: {operator}")
    if operator == "between":
        if len(values) != 2:
            raise ValueError("between predicates require: <column> between <lower> <upper>")
        return Predicate(column_name, operator, values[0], values[1])
    if len(values) != 1:
        raise ValueError(f"{operator} predicates require exactly one value")
    return Predicate(column_name, operator, values[0])


def _predicate_label_fragment(predicate: Predicate) -> str:
    """Return the deterministic output-label fragment for one predicate."""

    column_slug = predicate.column_name
    if predicate.operator == "between":
        lower = _slug_value(predicate.value)
        upper = _slug_value(predicate.second_value)
        return f"{column_slug}_{lower}_{upper}"
    operator_slug = SLUG_OPERATOR_NAMES[predicate.operator]
    value_slug = _slug_value(predicate.value)
    return f"{column_slug}_{operator_slug}_{value_slug}"


def _output_label(table_name: str, predicates: list[Predicate]) -> str:
    """Build the stable output directory label for an evaluation run."""

    return "_".join([table_name, *(_predicate_label_fragment(predicate) for predicate in predicates)])


def _summary_predicates(predicates: list[Predicate]) -> list[dict[str, str]]:
    """Convert predicates into a JSON-friendly summary structure."""

    summary: list[dict[str, str]] = []
    for predicate in predicates:
        if predicate.operator == "between":
            summary.append(
                {
                    "column_name": predicate.column_name,
                    "operator": predicate.operator,
                    "lower": str(predicate.value),
                    "upper": str(predicate.second_value),
                }
            )
            continue
        summary.append(
            {
                "column_name": predicate.column_name,
                "operator": predicate.operator,
                "value": str(predicate.value),
            }
        )
    return summary


def _sql_literal(value: object) -> str:
    """Render a Python value as a DuckDB/SQL literal."""

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, date):
        return "'" + value.isoformat() + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _normalize_comparable_value(value_kind: str, value: object) -> object:
    """Normalize typed predicate values so Python comparisons are stable."""

    if value_kind == "timestamp" and isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _duckdb_predicate_sql(predicate: Predicate, value_kind: str) -> str:
    """Render one typed predicate as a DuckDB `WHERE` clause fragment."""

    column_name = predicate.column_name
    value = _coerce_predicate_value(value_kind, predicate.value)
    if predicate.operator == "between":
        upper_value = _coerce_predicate_value(value_kind, predicate.second_value)
        return f"{column_name} BETWEEN {_sql_literal(value)} AND {_sql_literal(upper_value)}"
    return f"{column_name} {predicate.operator} {_sql_literal(value)}"


def _predicate_value_kinds(table_name: str, predicates: list[Predicate]) -> dict[str, str]:
    """Load coarse value kinds for the requested predicate columns from Postgres."""

    column_names = [predicate.column_name for predicate in predicates]
    engine = postgres_engine()
    with engine.connect() as connection:
        rows = list(
            connection.execute(
                text(
                    """
                    SELECT column_name, data_type
                    FROM metadata.column_catalog
                    WHERE table_name = :table_name
                      AND column_name = ANY(:column_names)
                    """
                ),
                {"table_name": table_name, "column_names": column_names},
            )
        )

    kinds = {row.column_name: normalize_value_kind(row.data_type) for row in rows}
    missing = sorted(set(column_names) - set(kinds))
    if missing:
        raise ValueError(f"Missing metadata.column_catalog rows for columns: {', '.join(missing)}")
    return kinds


def _read_matching_trips(
    file_paths: list[str],
    predicates: list[Predicate],
    value_kinds: dict[str, str],
) -> pd.DataFrame:
    """Read matching rows from the supplied parquet files with DuckDB."""

    scan = build_duckdb_parquet_scan(file_paths)
    where_clause = " AND ".join(
        _duckdb_predicate_sql(predicate, value_kinds[predicate.column_name]) for predicate in predicates
    )
    query = f"""
        SELECT *
        FROM {scan}
        WHERE {where_clause}
        ORDER BY pickup_at, dropoff_at
    """
    return duckdb.connect().execute(query).fetchdf()


def _write_output(output_dir: Path, summary: dict[str, object], trips: pd.DataFrame) -> None:
    """Write the evaluation summary and matching trips to disk."""

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trips.to_parquet(output_dir / "matching_trips.parquet", index=False)
    trips.to_csv(output_dir / "matching_trips.csv", index=False)


def _row_group_predicate_clause(
    alias: str,
    predicate: Predicate,
    value_kind: str,
    parameter_prefix: str,
) -> tuple[str, dict[str, object]]:
    """Build the SQL clause and bind parameters for one typed row-group predicate."""

    params: dict[str, object] = {}
    if value_kind == "numeric":
        min_column = f"{alias}.min_numeric"
        max_column = f"{alias}.max_numeric"
    elif value_kind == "timestamp":
        min_column = f"{alias}.min_timestamp"
        max_column = f"{alias}.max_timestamp"
    elif value_kind == "date":
        min_column = f"{alias}.min_date"
        max_column = f"{alias}.max_date"
    elif value_kind == "boolean":
        min_column = f"{alias}.min_boolean"
        max_column = f"{alias}.max_boolean"
    else:
        min_column = f"{alias}.min_value_text"
        max_column = f"{alias}.max_value_text"
    if predicate.operator == "=":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = f"{min_column} <= :{parameter_prefix}_value AND {max_column} >= :{parameter_prefix}_value"
    elif predicate.operator == "!=":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = (
            f"NOT ({min_column} = :{parameter_prefix}_value AND "
            f"{max_column} = :{parameter_prefix}_value AND {alias}.null_count = 0)"
        )
    elif predicate.operator == "<":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = f"{min_column} < :{parameter_prefix}_value"
    elif predicate.operator == "<=":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = f"{min_column} <= :{parameter_prefix}_value"
    elif predicate.operator == ">":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = f"{max_column} > :{parameter_prefix}_value"
    elif predicate.operator == ">=":
        params[f"{parameter_prefix}_value"] = _coerce_predicate_value(value_kind, predicate.value)
        clause = f"{max_column} >= :{parameter_prefix}_value"
    else:
        params[f"{parameter_prefix}_lower"] = _coerce_predicate_value(value_kind, predicate.value)
        params[f"{parameter_prefix}_upper"] = _coerce_predicate_value(value_kind, predicate.second_value)
        clause = f"{min_column} <= :{parameter_prefix}_upper AND {max_column} >= :{parameter_prefix}_lower"
    return clause, params


def _row_group_candidate_info(
    table_name: str,
    predicates: list[Predicate],
    value_kinds: dict[str, str],
) -> tuple[list[str], int, int]:
    """Return candidate files, candidate row groups, and index row count from Postgres."""

    sql = [
        "SELECT DISTINCT p0.absolute_file_path, p0.row_group_index",
        "FROM metadata.predicate_row_group_index AS p0",
        "WHERE p0.table_name = :table_name",
    ]
    params: dict[str, object] = {"table_name": table_name}
    for index, predicate in enumerate(predicates):
        alias = f"p{index}"
        clause, clause_params = _row_group_predicate_clause(
            alias,
            predicate,
            value_kinds[predicate.column_name],
            f"predicate_{index}",
        )
        if index > 0:
            sql.append(
                f"""
                AND EXISTS (
                    SELECT 1
                    FROM metadata.predicate_row_group_index AS {alias}
                    WHERE {alias}.table_name = p0.table_name
                      AND {alias}.file_path = p0.file_path
                      AND {alias}.row_group_index = p0.row_group_index
                      AND {alias}.column_name = :predicate_{index}_column
                      AND {alias}.non_null_count > 0
                      AND {clause}
                )
                """.strip()
            )
        else:
            sql.append(
                f"""
                AND p0.column_name = :predicate_0_column
                AND p0.non_null_count > 0
                AND {clause}
                """.strip()
            )
            params.update(clause_params)
            params["predicate_0_column"] = predicate.column_name
            continue

        params.update(clause_params)
        params[f"predicate_{index}_column"] = predicate.column_name

    sql.append("ORDER BY p0.absolute_file_path, p0.row_group_index")

    engine = postgres_engine()
    with engine.connect() as connection:
        row_groups = list(connection.execute(text("\n".join(sql)), params))
        row_count = connection.execute(
            text("SELECT COUNT(*) AS row_count FROM metadata.predicate_row_group_index")
        ).one()

    candidate_files = sorted({row.absolute_file_path for row in row_groups})
    return candidate_files, len(row_groups), int(row_count.row_count)


def _absolute_path(table_name: str, relative_path: str) -> str:
    """Return the workspace-relative path for one Delta parquet file."""

    return (DELTA_WAREHOUSE_DIR / table_name / relative_path).relative_to(WORKSPACE_ROOT).as_posix()


def _delta_log_predicate_matches(stats: dict[str, object], predicate: Predicate, value_kind: str) -> bool:
    """Return whether one file-level Delta-log stats object may satisfy the predicate."""

    minimums = stats.get("minValues", {})
    maximums = stats.get("maxValues", {})
    minimum = minimums.get(predicate.column_name)
    maximum = maximums.get(predicate.column_name)
    if minimum is None or maximum is None:
        return True

    min_value = _normalize_comparable_value(value_kind, _coerce_predicate_value(value_kind, minimum))
    max_value = _normalize_comparable_value(value_kind, _coerce_predicate_value(value_kind, maximum))
    value = _normalize_comparable_value(value_kind, _coerce_predicate_value(value_kind, predicate.value))
    if predicate.operator == "=":
        return min_value <= value and max_value >= value
    if predicate.operator == "!=":
        return not (min_value == value and max_value == value)
    if predicate.operator == "<":
        return min_value < value
    if predicate.operator == "<=":
        return min_value <= value
    if predicate.operator == ">":
        return max_value > value
    if predicate.operator == ">=":
        return max_value >= value
    upper_value = _normalize_comparable_value(
        value_kind, _coerce_predicate_value(value_kind, predicate.second_value)
    )
    return min_value <= upper_value and max_value >= value


def _delta_log_candidate_paths(
    table_name: str,
    predicates: list[Predicate],
    value_kinds: dict[str, str],
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
        if all(
            _delta_log_predicate_matches(stats, predicate, value_kinds[predicate.column_name])
            for predicate in predicates
        ):
            candidate_paths.append(_absolute_path(table_name, relative_path))
    return sorted(candidate_paths), logs_read


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for row-group multi-predicate evaluation."""

    parser = argparse.ArgumentParser(
        description="Evaluate row-group NSI and Delta-log pruning for typed conjunctive predicates."
    )
    parser.add_argument("--table", required=True)
    parser.add_argument(
        "--predicate",
        action="append",
        nargs="+",
        required=True,
        metavar="PART",
        help="Repeatable predicate. Use: column operator value, or column between lower upper.",
    )
    return parser


def main() -> None:
    """Run both pruning evaluations for the requested typed conjunctive predicates."""

    args = build_parser().parse_args()
    predicates = [_parse_predicate(parts) for parts in args.predicate]
    value_kinds = _predicate_value_kinds(args.table, predicates)
    label = _output_label(args.table, predicates)
    predicate_output_dir = WORKSPACE_ROOT / "predicate_output" / label
    delta_log_output_dir = WORKSPACE_ROOT / "delta_log_output" / label

    candidate_files, candidate_row_groups, row_group_index_row_count = _row_group_candidate_info(
        args.table, predicates, value_kinds
    )
    predicate_trips = _read_matching_trips(candidate_files, predicates, value_kinds)
    predicate_summary = {
        "table_name": args.table,
        "predicates": _summary_predicates(predicates),
        "candidate_file_count": len(candidate_files),
        "candidate_row_group_count": candidate_row_groups,
        "matching_trip_count": len(predicate_trips),
        "predicate_row_group_index_row_count": row_group_index_row_count,
        "candidate_files": candidate_files,
        "source": "metadata.predicate_row_group_index",
    }
    _write_output(predicate_output_dir, predicate_summary, predicate_trips)

    delta_files, delta_logs_read = _delta_log_candidate_paths(args.table, predicates, value_kinds)
    delta_trips = _read_matching_trips(delta_files, predicates, value_kinds)
    delta_summary = {
        "table_name": args.table,
        "predicates": _summary_predicates(predicates),
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
