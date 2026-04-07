## Dagster project

This project is pinned to `dagster==1.12.18` and set up for local work with:

- Postgres-backed Dagster services
- Docker-based runs
- Delta Lake / Arrow / Pandas data access

`dagster==1.14.20` was requested, but that version is not published on PyPI. The
closest practical choice on April 3, 2026 is `1.12.18`, published on March 5,
2026.

## Workspace layout

- `src/dagster_project/definitions.py`: primary Dagster code location
- `src/dagster_project/assets/`: one file per Dagster asset
- `.venv/`: created by `uv sync`
- `..\delta-lake\warehouse\`: Delta dimensions and fact tables
- `..\delta-lake\metadata_exports\`: parquet exports for schema stats and Delta logs

## Commands

```powershell
$env:UV_CACHE_DIR="$PWD\.uv-cache"
uv sync
uv run dagster dev -m dagster_project.definitions
uv run dagster job execute -m dagster_project.definitions -j rebuild_lakehouse
```

## Daemon-managed Runs

To keep a run alive even if the shell session times out, use the local Dagster
daemon instance instead of `dagster job execute`.

```powershell
cd .\dagster-project
.\scripts\start-dagster-daemon.ps1
.\scripts\launch-rebuild-lakehouse.ps1
```

This uses:

- `workspace.yaml`
- `.dagster-home\dagster.yaml`

The daemon runs detached in the background and picks up queued runs from the
local Dagster instance, so the pipeline is no longer tied to the originating
shell command.

If Docker Desktop is running, the Postgres image tarball will live under
`..\docker-images\postgres\`.

## Local Postgres

- Host: `127.0.0.1`
- Port: `5432`
- Database: `datalake`
- User: `datalake`
- Password: `datalake`

Schemas created by the pipeline:

- `mart`: Delta-backed dimensions and facts copied into Postgres
- `metadata`: column catalog, column statistics, and flattened Delta log entries
