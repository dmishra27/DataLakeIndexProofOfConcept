# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

**MonkeyDData** is a local lakehouse proof-of-concept using NYC Taxi data. It combines Delta Lake (warehouse) with Postgres (metadata and analytics mirror) and benchmarks a custom **NSI** ("non-stupid index") — a Postgres-backed predicate pruning system — against Delta Lake's built-in log statistics.

## Project Layout

```
dagster-project/    # Dagster orchestration: lakehouse build + Postgres mirror
datasource/         # NYC Taxi source data downloader
docker-images/      # Postgres 17 Docker build context
nsi/                # Non-stupid index: predicate evaluation CLI + scripts
```

Generated directories (git-ignored): `delta-lake/`, `datasource/nyc-taxi/`, `predicate_output/`, `delta_log_output/`.

## Setup

```bash
# 1. Copy env template and fill in credentials (defaults work with the Docker image)
cp .env.example .env

# 2. Build and start Postgres
docker build -t datalake-postgres:17 ./docker-images/postgres
docker run --rm -p 5432:5432 --env-file .env datalake-postgres:17

# 3. Download source data (defaults to 2025-01 through 2025-12)
python ./datasource/download_examples.py

# 4. Install dagster-project dependencies (uses uv)
cd dagster-project && uv sync
```

## Common Commands

All Dagster and NSI commands run from `dagster-project/`:

```bash
# Launch Dagster dev UI (http://localhost:3000)
uv run dagster dev -m dagster_project.definitions

# Execute the full lakehouse rebuild job directly
uv run dagster job execute -m dagster_project.definitions -j rebuild_lakehouse

# Start background daemon + queue job
./scripts/start-dagster-daemon.ps1
./scripts/launch-rebuild-lakehouse.ps1

# Run tests
uv run pytest tests/

# Run a single test file
uv run pytest tests/test_predicate_index.py
```

NSI commands (from `dagster-project/`, with `nsi/` on PYTHONPATH):

```bash
export PYTHONPATH=$(pwd)/..

# Build file-level predicate index in Postgres
uv run python -m nsi.cli build-index

# Build row-group footer stats index
uv run python -m nsi.cli build-footer-index

# Query candidate files for predicates
uv run python -m nsi.cli list-files --table fact_trip_yellow \
  --predicate pickup_at '>=' 2025-02-01T00:00:00 \
  --predicate pickup_at '<' 2025-03-01T00:00:00 \
  --duckdb-scan

# File-level range evaluation (NSI vs. Delta log)
uv run python -m nsi.evaluate_fact_table \
  --table fact_trip_yellow --column total_amount --lower 400 --upper 500

# Row-group multi-predicate evaluation
uv run python -m nsi.evaluate_row_group_multi_predicate \
  --table fact_trip_yellow \
  --predicate fare_amount between 500 1000 \
  --predicate passenger_count '>' 1
```

## Data Flow

The full pipeline runs left-to-right; each arrow represents a Dagster asset materialization or a manual NSI step:

```
datasource/nyc-taxi/ (raw parquet + CSV)
    │
    ▼  [delta_dimensions asset]
delta-lake/warehouse/dim_*/           ← static lookup tables
    │
    ▼  [delta_facts asset — DuckDB SQL normalization via fact_queries.py]
delta-lake/warehouse/fact_trip_{yellow,green,fhv,trip}/
    │  partitioned by pickup_year / pickup_month
    │
    ├──▶ [metadata_exports asset — metadata_io.py]
    │        delta-lake/metadata_exports/column_catalog.parquet
    │        delta-lake/metadata_exports/column_statistics.parquet
    │        delta-lake/metadata_exports/delta_log_entries.parquet
    │        delta-lake/metadata_exports/predicate_row_group_index.parquet
    │
    ├──▶ [current_snapshot_files asset]
    │        delta-lake/metadata_exports/current_snapshot_files.parquet
    │
    └──▶ [current_snapshot_file_stats asset]
             delta-lake/metadata_exports/current_snapshot_file_stats.parquet
                 │
                 ▼  [postgres_mirror asset — postgres_io.py bulk COPY]
             Postgres: mart.*          ← Delta warehouse tables
             Postgres: metadata.*      ← catalog, stats, log, row-group index
                 │
                 ▼  [nsi.cli build-index — manual step]
             Postgres: metadata.predicate_file_index
                 │
                 ▼  [nsi.cli build-footer-index — manual step]
             Postgres: parquet_footer.row_group_stats
                 │
                 ▼  [nsi.evaluate_* scripts]
             predicate_output/<label>/summary.json
             predicate_output/<label>/matching_trips.{parquet,csv}
             delta_log_output/<label>/summary.json   ← comparison baseline
             delta_log_output/<label>/matching_trips.{parquet,csv}
```

## Dagster Jobs & Assets

### Job: `rebuild_lakehouse`

Defined in `definitions.py`. Materializes all assets in dependency order. Equivalent to running `dagster job execute -m dagster_project.definitions -j rebuild_lakehouse`.

### Asset dependency graph

```
delta_dimensions
    └── delta_facts
            ├── metadata_exports ─────────────┐
            ├── current_snapshot_files ────────┼── postgres_mirror
            └── current_snapshot_file_stats ───┘
```

### Asset reference

| Asset | File | What it does |
|---|---|---|
| `delta_dimensions` | `assets/delta_dimensions.py` | Loads static lookup CSVs + FHV base data via `source_data.py`; writes dim_location, dim_vendor, dim_payment_type, dim_rate_code, dim_trip_type, dim_base to Delta |
| `delta_facts` | `assets/delta_facts.py` | Runs DuckDB normalization queries (yellow/green/FHV/union); writes four fact tables to Delta, partitioned by pickup_year/pickup_month. Depends on `delta_dimensions`. |
| `metadata_exports` | `assets/metadata_exports.py` | Collects column catalog, per-file column stats, flattened Delta log entries, and row-group predicate bounds from `metadata_io.py`; exports four parquet files to `delta-lake/metadata_exports/`. Depends on `delta_facts`. |
| `current_snapshot_files` | `assets/current_snapshot_files.py` | Exports one row per active Delta parquet file (from Delta snapshot). Depends on `delta_facts`. |
| `current_snapshot_file_stats` | `assets/current_snapshot_file_stats.py` | Exports per-file, per-column min/max stats from the Delta snapshot. Depends on `delta_facts`. |
| `postgres_mirror` | `assets/postgres_mirror.py` | Bulk-loads all mart tables (10 Delta tables via `load_delta_table_to_postgres`) and all metadata parquet outputs (6 tables via `load_dataframe_to_postgres`) into Postgres. Depends on `metadata_exports`, `current_snapshot_files`, `current_snapshot_file_stats`. |

## Postgres Schema

### `mart` schema — mirror of Delta warehouse

| Table | Description |
|---|---|
| `dim_location` | Taxi zone lookup: location_id, borough, zone_name, service_zone |
| `dim_vendor` | vendor_id, vendor_name |
| `dim_payment_type` | payment_type_id, payment_type_name |
| `dim_rate_code` | rate_code_id, rate_code_name |
| `dim_trip_type` | trip_type_id, trip_type_name (yellow/green distinction) |
| `dim_base` | FHV base registry: base_number, seen_as_dispatching, seen_as_affiliated |
| `fact_trip_yellow` | Normalized yellow taxi trips (~40 cols), partitioned by pickup_year/pickup_month |
| `fact_trip_green` | Normalized green taxi trips (~42 cols, adds trip_type_id, ehail_fee) |
| `fact_trip_fhv` | FHV trips (~15 cols: dispatching_base_number, affiliated_base_number, etc.) |
| `fact_trip` | Union of yellow + green + FHV with nullable columns for non-matching service types |

### `metadata` schema — catalog and pruning indexes

| Table | Description |
|---|---|
| `column_catalog` | table_name, column_name, ordinal_position, data_type, nullable — one row per table column |
| `column_statistics` | Per-file, per-column stats exported from Delta snapshot (raw, before typed parsing) |
| `current_snapshot_files` | One row per active Delta parquet file with nested snapshot stats |
| `current_snapshot_file_stats` | Per-file, per-column min/max from Delta current snapshot — source for `predicate_file_index` |
| `predicate_file_index` | **NSI file-level index** (written by `nsi.cli build-index`): typed min/max per file per column across 6 value families |
| `predicate_row_group_index` | Row-group-level predicate bounds exported by `metadata_exports` asset; used by row-group evaluation scripts |
| `delta_log_entries` | Flattened Delta log JSON add/remove entries — used by `evaluate_*` scripts as the comparison baseline |

### `parquet_footer` schema — physical file metadata

| Table | Description |
|---|---|
| `row_group_stats` | Written by `nsi.cli build-footer-index`. One row per (file, row_group, column); typed min/max columns identical to `predicate_file_index` plus `num_rows`, `total_byte_size` |

## NSI Module Reference

### `nsi/config.py`

| Symbol | Kind | Purpose |
|---|---|---|
| `WORKSPACE_ROOT` | `Path` | Repo root (`nsi/../../`); anchor for all relative paths |
| `DELTA_WAREHOUSE_DIR` | `Path` | `<WORKSPACE_ROOT>/delta-lake/warehouse` |
| `DEFAULT_POSTGRES_URL` | `str` | SQLAlchemy URL built from `.env` credentials |
| `_load_dotenv(dotenv_path)` | func | Reads `KEY=VALUE` lines from a `.env` file into `os.environ` using `setdefault` (does not overwrite existing env vars) |

---

### `nsi/indexer.py`

#### Data structures

**`Predicate`** (frozen dataclass)
- Fields: `column_name: str`, `operator: str`, `value: object | None`, `second_value: object | None`
- Valid operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `between`, `is_null`, `is_not_null`
- `between` requires both `value` and `second_value`; `is_null`/`is_not_null` take no values

**`INDEX_COLUMNS`** — column list for `predicate_file_index` (22 fields including typed min/max pairs)

**`FOOTER_INDEX_COLUMNS`** — column list for `row_group_stats` (21 fields, adds `row_group_index`, `num_rows`, `total_byte_size`)

#### Public functions

| Function | Inputs | Returns | Purpose |
|---|---|---|---|
| `postgres_engine(postgres_url)` | optional URL string | `Engine` | Creates SQLAlchemy engine; defaults to `DEFAULT_POSTGRES_URL` |
| `normalize_value_kind(data_type)` | catalog data type string | `str` one of `numeric\|timestamp\|date\|boolean\|text` | Maps Arrow/SQL type names to the coarse comparison family used for typed index columns |
| `build_predicate_index_frame(stats_frame, catalog_frame, warehouse_dir)` | stats DataFrame, catalog DataFrame, warehouse path | `pd.DataFrame` with `INDEX_COLUMNS` | Joins file-level stats with column catalog; parses typed min/max values into the correct typed columns; computes null/non-null counts |
| `iter_warehouse_parquet_files(warehouse_dir)` | warehouse path | `list[Path]` | Recursively finds every `.parquet` data file under the warehouse root, sorted |
| `build_footer_row_group_stats_frame(parquet_paths, warehouse_dir)` | list of parquet paths, warehouse path | `pd.DataFrame` with `FOOTER_INDEX_COLUMNS` | Opens each parquet file with PyArrow, extracts per-row-group column statistics from the file footer |
| `footer_row_group_stats_dtypes()` | — | `dict[str, SQLAlchemy type]` | Explicit column type map for `to_sql`; prevents SQLAlchemy from inferring wrong types for typed min/max columns |
| `create_predicate_index(postgres_url, schema_name, table_name, warehouse_dir)` | optional Postgres URL, schema/table names | `int` rows written | Reads `metadata.current_snapshot_file_stats` and `metadata.column_catalog` from Postgres, calls `build_predicate_index_frame`, replaces the index table, creates two indexes on `(table_name, file_path)` and `(table_name, column_name)` |
| `create_footer_row_group_stats_index(postgres_url, schema_name, table_name, warehouse_dir)` | optional Postgres URL, schema/table names | `int` rows written | Walks all warehouse parquet files, calls `build_footer_row_group_stats_frame`, replaces the footer table, creates two indexes |
| `build_candidate_files_query(table_name, predicates, column_types, schema_name, index_table_name)` | table name, predicate list, column type map, optional schema/table names | `tuple[str SQL, dict params]` | Renders a `SELECT DISTINCT absolute_file_path` query with one correlated `EXISTS` subquery per predicate; each subquery tests the appropriate typed min/max columns |
| `find_candidate_files(table_name, predicates, postgres_url, schema_name, index_table_name)` | table name, predicate list, optional Postgres URL, optional schema/table names | `list[str]` absolute file paths | Loads column types from `metadata.column_catalog`, builds and executes the candidate-files query, raises `ValueError` for unknown columns |
| `build_duckdb_parquet_scan(file_paths)` | list of absolute/relative path strings | `str` DuckDB expression | Renders `read_parquet(['path1', 'path2', ...])` with normalized forward-slash paths; raises `ValueError` if the list is empty |

#### Private helpers (frequently called from public functions)

| Function | Purpose |
|---|---|
| `_parse_decimal(value_text)` | Parse a numeric bound; returns `Decimal` or `None` |
| `_parse_timestamp(value_text)` | Parse an ISO timestamp; handles `Z` suffix; returns `datetime` or `None` |
| `_parse_date(value_text)` | Parse an ISO date string; returns `date` or `None` |
| `_parse_bool(value_text)` | Parse common truthy/falsy strings (`true/t/1/false/f/0`); returns `bool` or `None` |
| `_footer_row_group_records(parquet_path, warehouse_dir)` | Open one parquet file, extract all row-group × column stats; returns list of dicts |
| `_footer_value_columns(value_kind, min_value, max_value)` | Build the typed payload dict for one footer row (all 8 typed min/max fields) |
| `_predicate_clause(alias, predicate, value_kind, parameter_prefix)` | Build the SQL `WHERE` fragment + bind params for one predicate against the index table |
| `_comparison_column(alias, value_kind, prefix)` | Return the SQL expression for the typed min or max column (`CAST(alias.min_numeric AS NUMERIC)`, etc.) |
| `_coerce_predicate_value(value_kind, value)` | Parse a user-supplied string value into the correct Python type for binding |

---

### `nsi/cli.py`

| Function | Inputs | Returns | Purpose |
|---|---|---|---|
| `build_parser()` | — | `ArgumentParser` | Defines three subcommands: `build-index` (calls `create_predicate_index`), `build-footer-index` (calls `create_footer_row_group_stats_index`), `list-files` (calls `find_candidate_files`). `list-files` accepts `--predicate COL OP VAL`, `--between COL LOWER UPPER`, `--is-null COL`, `--is-not-null COL` (all repeatable for conjunctions) and `--duckdb-scan` to print a DuckDB expression instead of raw paths |
| `main()` | — | — | Parses args and dispatches; prints row count for index builds, file paths or DuckDB scan expression for `list-files` |

---

### `nsi/evaluate_fact_table.py`

Parameterized file-level evaluation. Compares NSI (`metadata.predicate_file_index`) against Delta log parsing for a numeric range predicate on any fact table column.

| Function | Inputs | Returns | Purpose |
|---|---|---|---|
| `main()` | CLI args: `--table`, `--column`, `--lower`, `--upper` | — | Runs both evaluations; prints combined JSON summary |
| `_run_predicate_index_evaluation(table_name, column_name, lower_bound, upper_bound, output_dir)` | table/column names, `Decimal` bounds, output `Path` | `dict` summary | Uses `find_candidate_files`, reads matching rows with DuckDB, writes output |
| `_run_delta_log_evaluation(table_name, column_name, lower_bound, upper_bound, output_dir)` | same | `dict` summary | Parses `_delta_log/*.json` files directly; reconstructs active add-set, applies min/max range test |
| `_delta_log_candidate_paths(table_name, column_name, lower_bound, upper_bound)` | table/column names, `Decimal` bounds | `tuple[list[str], int]` paths + log count | Replays Delta log JSON (add/remove) to get active files; applies per-file stats range test |
| `_read_matching_trips(file_paths, column_name, lower_bound, upper_bound)` | file paths, column name, `Decimal` bounds | `pd.DataFrame` | DuckDB `BETWEEN` query over the candidate file list |
| `_write_output(output_dir, summary, trips)` | output `Path`, summary dict, trips DataFrame | — | Writes `summary.json`, `matching_trips.parquet`, `matching_trips.csv` |

---

### `nsi/evaluate_fact_trip_green.py`

Hardcoded convenience script: evaluates `fact_trip_green`, `total_amount BETWEEN 400 AND 500`. All functions mirror `evaluate_fact_table.py` but are not parameterized. Run with `python -m nsi.evaluate_fact_trip_green`.

---

### `nsi/evaluate_row_group_fact_table.py`

Row-group-level exact-match evaluation. Queries `metadata.predicate_row_group_index` rather than `predicate_file_index`.

| Function | Inputs | Returns | Purpose |
|---|---|---|---|
| `main()` | CLI args: `--table`, `--column`, `--value` | — | Runs NSI row-group eval and Delta-log file-level eval; prints combined JSON |
| `_row_group_candidate_info(table_name, column_name, value)` | table/column names, `Decimal` value | `tuple[list[str] files, int row_groups, int index_rows]` | SQL against `metadata.predicate_row_group_index`: `min_numeric <= value AND max_numeric >= value` |
| `_delta_log_candidate_paths(table_name, column_name, value)` | same | `tuple[list[str], int]` | Same Delta log replay as `evaluate_fact_table.py` but for exact-match |
| `_read_matching_trips(file_paths, column_name, value)` | file paths, column name, `Decimal` value | `pd.DataFrame` | DuckDB equality query |
| `_write_output(output_dir, summary, trips)` | output `Path`, summary dict, trips DataFrame | — | Writes summary + matching trips |

---

### `nsi/evaluate_row_group_multi_predicate.py`

Row-group multi-predicate conjunction evaluation. Supports all operators (`=`, `!=`, `<`, `<=`, `>`, `>=`, `between`) and all value kinds (numeric, timestamp, date, boolean, text).

| Function | Inputs | Returns | Purpose |
|---|---|---|---|
| `main()` | CLI args: `--table`, `--predicate PART...` (repeatable) | — | Parses predicates, resolves value kinds from catalog, runs both evaluations, prints JSON |
| `_parse_predicate(raw_parts)` | list of string tokens (`col op val` or `col between lo hi`) | `Predicate` | Validates operator and argument count |
| `_row_group_candidate_info(table_name, predicates, value_kinds)` | table name, predicate list, value-kind map | `tuple[list[str] files, int row_groups, int index_rows]` | Correlated `EXISTS` query per predicate against `metadata.predicate_row_group_index`; each predicate tests the appropriate typed min/max columns |
| `_delta_log_candidate_paths(table_name, predicates, value_kinds)` | same + value-kind map | `tuple[list[str], int]` | Delta log replay with `_delta_log_predicate_matches` applied to all predicates conjunctively |
| `_delta_log_predicate_matches(stats, predicate, value_kind)` | Delta log stats dict, predicate, value kind | `bool` | Tests one predicate against file-level min/max from the Delta log stats JSON |
| `_row_group_predicate_clause(alias, predicate, value_kind, parameter_prefix)` | SQL alias, predicate, value kind, param prefix | `tuple[str SQL clause, dict params]` | Renders one predicate as a SQL fragment for `metadata.predicate_row_group_index`, selecting the correct typed column pair |
| `_duckdb_predicate_sql(predicate, value_kind)` | predicate, value kind | `str` SQL fragment | Renders one predicate as a DuckDB `WHERE` clause fragment (used to filter matching rows after candidate file selection) |
| `_predicate_value_kinds(table_name, predicates)` | table name, predicate list | `dict[str column_name, str value_kind]` | Queries `metadata.column_catalog` for data types; raises `ValueError` for missing columns |
| `_output_label(table_name, predicates)` | table name, predicate list | `str` | Builds a deterministic filesystem-safe label for the output directory |
| `_read_matching_trips(file_paths, predicates, value_kinds)` | file paths, predicate list, value-kind map | `pd.DataFrame` | DuckDB query with `AND`-joined predicate fragments |

## Architecture

### Key implementation files

| File | Role |
|---|---|
| `dagster_project/fact_queries.py` | DuckDB SQL normalization for yellow/green/FHV taxi schemas |
| `dagster_project/delta_io.py` | Delta Lake read/write via `deltalake` + DuckDB |
| `dagster_project/postgres_io.py` | Bulk COPY into Postgres via Polars + Arrow |
| `dagster_project/metadata_io.py` | Schema catalog, file stats, Delta log parsing, row-group predicate export |
| `nsi/indexer.py` | Full NSI index build and file-pruning query logic |

### DataFrame conventions

- **Polars** is preferred for all data transformation and Postgres bulk loading (Arrow-native, no pandas overhead).
- **Pandas** is used only where SQLAlchemy requires it (schema bootstrap, `to_sql` in NSI indexer).
- DuckDB is the query engine for normalization; deltalake handles Delta protocol reads/writes.

## Dependencies (dagster-project/pyproject.toml)

Python ≥ 3.12, managed with `uv`.

Key pins: `dagster==1.12.18`, `polars>=1.28,<2.0`, `deltalake>=0.25,<0.26`, `duckdb>=1.2,<2.0`, `pyarrow>=18,<22`, `sqlalchemy>=2.0,<3.0`.

## Glossary

**NSI (non-stupid index)**
A Postgres-backed predicate pruning index that records per-file (and per-row-group) typed min/max bounds for every column in every Delta Lake parquet file. It is "non-stupid" in contrast to Delta Lake's built-in log statistics, which only track the first 32 columns and store bounds as raw JSON strings rather than typed values. The NSI stores bounds in six typed column pairs (`min/max_numeric`, `min/max_timestamp`, `min/max_date`, `min/max_boolean`, `min/max_value_text`) enabling accurate SQL comparisons.

**Predicate pruning**
The technique of eliminating data files (or row groups within files) before scanning them, by testing whether their recorded min/max bounds can possibly satisfy a query predicate. A file where `max_value < lower_bound` cannot contain any matching rows and can be skipped entirely. Pruning reduces I/O and query latency at the cost of maintaining the index.

**File-level vs. row-group-level index**
- *File-level* (`metadata.predicate_file_index`, built by `nsi.cli build-index`): one row per (file, column). Pruning operates at the granularity of whole parquet files. Coarser but cheaper to build.
- *Row-group-level* (`parquet_footer.row_group_stats`, built by `nsi.cli build-footer-index`; `metadata.predicate_row_group_index`, populated by the `metadata_exports` Dagster asset): one row per (file, row_group, column). Pruning can skip individual row groups within a file. Finer-grained but larger index and slower build.

**Delta log statistics**
Delta Lake writes a `_delta_log/` directory of JSON commit files alongside each table. Each `add` action embeds a `stats` JSON blob containing file-level `minValues`, `maxValues`, and `nullCount` for up to 32 columns. The evaluation scripts (`evaluate_fact_table.py`, `evaluate_row_group_multi_predicate.py`) parse these JSON files directly as the **comparison baseline** for NSI pruning effectiveness.

**Data skipping**
The general term for any mechanism that avoids reading irrelevant data during a query. Delta Lake implements data skipping using its log statistics. The NSI implements a more selective form of data skipping using Postgres-stored typed bounds and correlated SQL `EXISTS` queries. Evaluation scripts measure skipping effectiveness as `total_files - candidate_file_count`.

## Session Starter

Five prompts to quickly orient a new developer in this codebase:

1. **"Walk me through what happens when I run `dagster job execute -j rebuild_lakehouse`. Which assets run, in what order, and what files do they produce?"**

2. **"Show me how `nsi.cli list-files` works under the hood — from parsing the CLI arguments through to the SQL query that runs against Postgres."**

3. **"Compare `metadata.predicate_file_index` and `parquet_footer.row_group_stats`. How are they built differently, what do they have in common, and when would you query one vs. the other?"**

4. **"Trace a single NYC Taxi trip record from the raw downloaded parquet file through DuckDB normalization, into the Delta Lake fact table, mirrored into Postgres, and finally used in a pruning evaluation."**

5. **"Explain the evaluation harness: what does `evaluate_fact_table.py` actually measure, what gets written to `predicate_output/` vs. `delta_log_output/`, and how would you interpret the `candidate_file_count` difference between the two summaries?"**
