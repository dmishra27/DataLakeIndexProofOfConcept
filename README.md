# MonkeyDData Project

This repository is a local Delta Lake and Postgres experimentation project for
NYC Taxi data. It builds a small lakehouse with Dagster, exports metadata into
Postgres, and compares different predicate-pruning approaches for DuckDB-style
parquet scans.

## What The Project Does

The project has four main goals:

- download reproducible NYC Taxi source data
- build Delta Lake dimension and fact tables
- mirror warehouse and metadata tables into Postgres
- evaluate whether NSI can prune more files than the Delta log alone

The main fact tables are:

- `fact_trip_yellow`
- `fact_trip_green`
- `fact_trip_fhv`
- `fact_trip`

## Repository Layout

### `dagster-project/`

The Dagster application that builds and mirrors the lakehouse.

Key responsibilities:

- creates Delta dimensions and fact tables under `delta-lake/warehouse/`
- exports metadata parquet files under `delta-lake/metadata_exports/`
- loads warehouse tables and metadata into Postgres schemas such as `mart` and
  `metadata`

Important files:

- `src/dagster_project/definitions.py`: Dagster definitions and jobs
- `src/dagster_project/assets/`: asset graph for Delta, metadata, and Postgres
- `tests/`: unit tests for metadata collection, source handling, and predicate
  indexing support

### `nsi/`

`nsi` means "non-stupid index". It contains the predicate-pruning code and
evaluation helpers.

Key responsibilities:

- builds `metadata.predicate_file_index`
- builds `parquet_footer.row_group_stats`
- queries Postgres-backed predicate metadata to return candidate files
- compares NSI pruning against Delta-log pruning

Important files:

- `indexer.py`: index builders and public pruning helpers
- `cli.py`: command-line entrypoints
- `evaluate_*.py`: benchmarking scripts for file-level and row-group-level
  predicate tests

### `datasource/`

Holds the source-data bootstrap tooling.

- `download_examples.py`: downloads the default yearly NYC Taxi dataset
- `nyc-taxi/`: local source data directory, ignored in git except for its
  `.gitignore`

### `docker-images/`

Container build context for local infrastructure.

- `postgres/`: Dockerfile and notes for the local Postgres instance

### Generated Directories

These are created locally and are ignored by git:

- `delta-lake/`: Delta warehouse and exported metadata parquet files
- `predicate_output/`: benchmark outputs from NSI-based scans
- `delta_log_output/`: benchmark outputs from Delta-log-based scans

## Typical Workflow

1. Copy `.env.example` to `.env`.
2. Build and run Postgres from `docker-images/postgres/`.
3. Download the yearly source dataset with `python .\datasource\download_examples.py`.
4. Run the Dagster rebuild job from `dagster-project/`.
5. Build NSI indexes from the generated Postgres metadata.
6. Run evaluation scripts to compare NSI and Delta-log pruning.

## Common Commands

Start from the repository root unless noted otherwise.

```powershell
Copy-Item .\.env.example .\.env
docker build -t datalake-postgres:17 .\docker-images\postgres
docker run --rm -p 5432:5432 --env-file .\.env datalake-postgres:17
python .\datasource\download_examples.py
```

From `dagster-project/`:

```powershell
$env:PYTHONPATH=(Resolve-Path ..)
$env:DAGSTER_HOME="$PWD\.dagster-home"
.\.venv\Scripts\python.exe -m dagster job execute -m dagster_project.definitions -j rebuild_lakehouse
.\.venv\Scripts\python.exe -m nsi.cli build-index
.\.venv\Scripts\python.exe -m nsi.cli build-footer-index
```

## Why NSI Exists

Delta-log pruning only has the file-level statistics recorded in active Delta
log entries. NSI is meant to show that a Postgres-backed predicate index can be
more selective, especially when some columns are poorly represented in the
Delta-log stats. The repository includes scripts and benchmark outputs for
comparing those two approaches directly.
