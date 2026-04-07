"""Command-line entrypoints for building and querying the non-stupid index."""

from __future__ import annotations

import argparse

from .indexer import (
    Predicate,
    build_duckdb_parquet_scan,
    create_footer_row_group_stats_index,
    create_predicate_index,
    find_candidate_files,
)


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for index builds and file-pruning queries."""

    parser = argparse.ArgumentParser(description="Build and query a predicate pruning index.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build-index", help="Rebuild metadata.predicate_file_index")
    build_parser.add_argument("--postgres-url", default=None)

    footer_parser = subparsers.add_parser(
        "build-footer-index",
        help="Rebuild parquet_footer.row_group_stats from parquet footer metadata",
    )
    footer_parser.add_argument("--postgres-url", default=None)

    files_parser = subparsers.add_parser("list-files", help="List candidate parquet files for predicates")
    files_parser.add_argument("--table", required=True)
    files_parser.add_argument("--postgres-url", default=None)
    files_parser.add_argument("--duckdb-scan", action="store_true")
    files_parser.add_argument(
        "--predicate",
        nargs=3,
        metavar=("COLUMN", "OPERATOR", "VALUE"),
        action="append",
        default=[],
        help="Add a comparison predicate. Repeat for conjunctions.",
    )
    files_parser.add_argument(
        "--between",
        nargs=3,
        metavar=("COLUMN", "LOWER", "UPPER"),
        action="append",
        default=[],
        help="Add an inclusive range predicate. Repeat for conjunctions.",
    )
    files_parser.add_argument(
        "--is-null",
        metavar="COLUMN",
        action="append",
        default=[],
        help="Add a predicate for files that may contain null values.",
    )
    files_parser.add_argument(
        "--is-not-null",
        metavar="COLUMN",
        action="append",
        default=[],
        help="Add a predicate for files that may contain non-null values.",
    )

    return parser


def main() -> None:
    """Execute the NSI command-line interface."""

    parser = build_parser()
    args = parser.parse_args()
    postgres_url = args.postgres_url if args.postgres_url else None
    if args.command == "build-index":
        row_count = create_predicate_index(postgres_url=postgres_url) if postgres_url else create_predicate_index()
        print(row_count)
        return
    if args.command == "build-footer-index":
        row_count = (
            create_footer_row_group_stats_index(postgres_url=postgres_url)
            if postgres_url
            else create_footer_row_group_stats_index()
        )
        print(row_count)
        return

    predicates = [
        Predicate(column_name=column_name, operator=operator, value=value)
        for column_name, operator, value in args.predicate
    ]
    predicates.extend(
        Predicate(column_name=column_name, operator="between", value=lower, second_value=upper)
        for column_name, lower, upper in args.between
    )
    predicates.extend(Predicate(column_name=column_name, operator="is_null") for column_name in args.is_null)
    predicates.extend(
        Predicate(column_name=column_name, operator="is_not_null") for column_name in args.is_not_null
    )
    file_paths = (
        find_candidate_files(args.table, predicates, postgres_url=postgres_url)
        if postgres_url
        else find_candidate_files(args.table, predicates)
    )
    if args.duckdb_scan:
        print(build_duckdb_parquet_scan(file_paths))
        return
    for file_path in file_paths:
        print(file_path)


if __name__ == "__main__":
    main()
