# NSI

`nsi` stands for "non-stupid index". It provides:

- A typed file-pruning index in Postgres: `metadata.predicate_file_index`
- A typed row-group-pruning index in Postgres: `metadata.predicate_row_group_index`
- A parquet-footer row-group stats index in Postgres: `parquet_footer.row_group_stats`
- A small Python API for file pruning
- CLI entrypoints for rebuilding indexes, ingesting footer stats, and querying candidate files
- Evaluation scripts that compare Postgres-backed pruning against Delta-log pruning

## What It Uses

The package assumes the Dagster pipeline has already populated Postgres metadata
tables such as:

- `metadata.column_catalog`
- `metadata.current_snapshot_file_stats`
- `metadata.predicate_file_index`
- `metadata.predicate_row_group_index`

The package also reads active Delta files under:

- `delta-lake/warehouse/<table_name>/`
- `delta-lake/warehouse/**.parquet` when ingesting footer row-group stats

## Python API

Basic file pruning:

```python
from nsi import Predicate, build_duckdb_parquet_scan, find_candidate_files

files = find_candidate_files(
    "fact_trip_yellow",
    [
        Predicate("pickup_at", ">=", "2025-02-01T00:00:00"),
        Predicate("pickup_at", "<", "2025-03-01T00:00:00"),
    ],
)

scan = build_duckdb_parquet_scan(files)
print(scan)
```

Warehouse-wide parquet footer ingest:

```python
from nsi import create_footer_row_group_stats_index

row_count = create_footer_row_group_stats_index()
print(row_count)
```

## CLI

Build the file-level predicate index:

```powershell
uv run python -m nsi.cli build-index
```

Build the parquet footer row-group stats index for the whole warehouse:

```powershell
uv run python -m nsi.cli build-footer-index
```

List candidate files from `metadata.predicate_file_index`:

```powershell
uv run python -m nsi.cli list-files --table fact_trip_yellow --predicate pickup_at '>=' 2025-02-01T00:00:00
uv run python -m nsi.cli list-files --table fact_trip_yellow --predicate pickup_at '>=' 2025-02-01T00:00:00 --predicate pickup_at '<' 2025-03-01T00:00:00 --duckdb-scan
uv run python -m nsi.cli list-files --table fact_trip_yellow --between total_amount 400 500
```

## Evaluation Scripts

These scripts write summaries plus matching records into `predicate_output/`
and `delta_log_output/`.

Generic file-level range evaluation:

```powershell
uv run python -m nsi.evaluate_fact_table --table fact_trip_yellow --column total_amount --lower 400 --upper 500
```

Green-table convenience evaluation:

```powershell
uv run python -m nsi.evaluate_fact_trip_green
```

Generic row-group exact-match evaluation:

```powershell
uv run python -m nsi.evaluate_row_group_fact_table --table fact_trip_yellow --column fare_amount --value 1213.30
```

Generic row-group multi-predicate evaluation for typed conjunctive filters:

```powershell
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip_yellow --predicate fare_amount between 500 1000 --predicate passenger_count '>' 1
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip_yellow --predicate fare_amount between 900 1000 --predicate passenger_count '>' 2
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip_yellow --predicate fare_amount between 500 1000 --predicate passenger_count '=' 8
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip --predicate service_type '=' yellow --predicate fare_amount between 900 1000 --predicate passenger_count '>' 2
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip --predicate service_type '=' fhv --predicate pickup_at '>=' 2025-02-15T08:00:00 --predicate pickup_at '<' 2025-02-15T10:00:00
uv run python -m nsi.evaluate_row_group_multi_predicate --table fact_trip --predicate pickup_location_id '=' 132 --predicate pickup_date '=' 2025-02-15
```

The multi-predicate row-group evaluator supports numeric, text, date, timestamp,
and boolean predicates with `=`, `!=`, `<`, `<=`, `>`, `>=`, and `between`.

## Output Layout

Each evaluation writes:

- `summary.json`: counts, source metadata, and candidate file list
- `matching_trips.parquet`: matching records as parquet
- `matching_trips.csv`: matching records as csv

Outputs land under table- or predicate-specific directories inside:

- `predicate_output/`
- `delta_log_output/`

## Notes

- File-level pruning can only eliminate whole files.
- Row-group pruning is more selective because it intersects row-group bounds.
- Delta-log pruning uses file-level `add.stats` from active log entries, so it
  cannot prune below the file level.
- `parquet_footer.row_group_stats` is built directly from parquet footer metadata
  across the whole warehouse and stores only filenames plus row-group statistics,
  not copied table records.
