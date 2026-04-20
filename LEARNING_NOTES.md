# Learning Notes — MonkeyDData Lakehouse

---

## 2026-04-20 — Codebase Walkthrough: Flow 05 (End-to-End Synthesis)

### Scope
Cross-cutting analysis across all four flows. No new source files — this section
synthesises findings already recorded in Flows 01–04 and places the repo in the
broader context of lakehouse indexing alternatives.

---

### Files Involved
All files from Flows 01–04. No additional source files read.

---

### 1. The Single Core Thesis

The repo proves one specific claim:

> **Delta Lake's built-in log statistics are an inadequate predicate pruning mechanism
> for wide, high-cardinality fact tables. A typed, Postgres-backed secondary index (NSI)
> running alongside the lakehouse can skip more files and row groups for the same query
> — at the cost of a manual build step after every pipeline run.**

The three structural weaknesses in Delta log stats that the NSI directly attacks:

1. **Only the first 32 columns are tracked.** Stats for column 33+ are absent. Any
   predicate on a late-position column in a wide schema falls back to full-table scan.
2. **Bounds are stored as untyped JSON.** `"minValues": {"fare_amount": 0.01}` is a
   JSON number, but `"minValues": {"pickup_at": "2025-01-01T00:05:00"}` is a string.
   Comparing timestamps as strings is unreliable. The NSI stores bounds in six typed
   column pairs (`min_numeric`, `min_timestamp`, `min_date`, etc.) enabling accurate
   SQL comparisons.
3. **Granularity is file-level only.** A Delta parquet file may contain hundreds of
   row groups. If only some row groups match a predicate, Delta must scan the whole file.
   The NSI's row-group index can skip individual row groups within a file.

---

### 2. How the Four Flows Form One Argument

The four flows are not independent subsystems — each one creates the raw material the
next depends on, and the whole chain only makes sense as a single argument:

```
Flow 01 (Ingestion)
  Creates the lakehouse and its _delta_log.
  The write granularity (full overwrite, partitioned by pickup_year/pickup_month)
  determines how many files exist per table — which directly controls how much
  pruning is even possible. More, smaller files = more pruning opportunity.
      ↓
Flow 02 (Metadata Mirror)
  Extracts the index-building raw material from the Delta snapshot into Postgres.
  The critical output is metadata.current_snapshot_file_stats: one row per
  (file, column) with text min/max values from the Delta add-action stats blob.
  This is the bridge between the Delta format and the typed NSI.
      ↓
Flow 03 (NSI Build)
  Promotes text min/max → typed min/max (Decimal, datetime, date, bool).
  Two index granularities are built: file-level (predicate_file_index) and
  row-group-level (predicate_row_group_index or row_group_stats).
  The Postgres indexes on (table_name, file_path) and (table_name, column_name)
  are what make the correlated EXISTS queries fast at query time.
      ↓
Flow 04 (Benchmark)
  Runs both pruning paths against the same predicates and compares
  candidate_file_count. The NSI path queries Postgres. The Delta log path
  replays JSON files. Both DuckDB-scan the resulting file lists and produce
  deterministic, diff-able output.
```

The argument breaks if any step is stale: a rebuilt lakehouse (Flow 01) without
re-running the metadata mirror (Flow 02) and NSI build (Flow 03) leaves the benchmark
(Flow 04) comparing NSI against the old Delta snapshot.

---

### 3. Where NSI Beats Delta Log Pruning — and Where It Doesn't

**NSI wins clearly:**

- **Typed timestamp/date comparisons.** Delta log stores these as ISO strings in JSON.
  String ordering is unreliable for timestamps with mixed timezone representations or
  sub-second precision. The NSI parses them into `datetime` objects before storage.
  Any predicate on `pickup_at` or `dropoff_at` will be more accurate through NSI.

- **Columns beyond position 32.** `fact_trip` has ~30 columns (safe for Delta), but
  `fact_trip_green` has ~42. Any column past position 32 has no Delta log stats and
  cannot be pruned by the baseline. NSI covers all columns unconditionally.

- **Row-group granularity.** When data within a file is not uniformly distributed —
  e.g., a file sorted by pickup_at will have early-hour trips in the first row groups
  and late-hour trips in the last — the NSI can skip individual row groups that don't
  satisfy a `pickup_hour > 20` predicate. Delta log will scan the whole file.

- **Multi-predicate conjunctions on mixed column types.** `evaluate_row_group_multi_predicate`
  handles arbitrary conjunctions of typed predicates. The Delta log baseline in
  `evaluate_fact_table.py` handles only a single numeric BETWEEN. The generalized
  multi-type Delta log comparison exists only in `_delta_log_predicate_matches`
  (in `evaluate_row_group_multi_predicate.py`).

**Delta log is equal or better:**

- **Time-partitioned queries on pickup_year/pickup_month.** The fact tables are
  partitioned by these columns. A query like `WHERE pickup_at BETWEEN 2025-02-01 AND
  2025-02-28` will have Hive-style directory pruning applied before either index is
  consulted. Both NSI and Delta log return the same candidate files — the partition
  layout does the work. NSI adds no value here.

- **High-selectivity BETWEEN on numeric columns with well-separated per-file ranges.**
  If files happen to contain narrow, non-overlapping value ranges (common when data is
  inserted in sort order), Delta log's JSON numeric comparison is functionally accurate
  and will skip the same files as NSI. The typed advantage only materialises when the
  JSON representation is ambiguous or imprecise.

- **Zero-maintenance consistency.** Delta log stats are written automatically on every
  `write_deltalake()` call. NSI requires a manual `nsi.cli build-index` run. In the
  current PoC, where every pipeline run is a full overwrite followed by a manual NSI
  rebuild, they're kept in sync by convention. In any real incremental-write scenario
  the NSI will be stale the moment a new commit lands.

- **Correctness at scale.** The Delta log baseline is reliable for tables with ≤10
  commits (the current state: one commit per full overwrite). The NSI, by contrast,
  is always correct because it reads from the Delta snapshot via Postgres. But if you
  use the `deltalake` Python API (i.e., `get_add_actions()`) instead of raw JSON
  globbing, the Delta log approach would also be correct at any commit depth.

---

### 4. What Would Need to Change for Production

These are gaps identified directly from the code — not speculative:

**Correctness blockers:**

1. **Fix the Delta log baseline for checkpointed tables** (`evaluate_*.py`,
   all four files). Replace raw `_delta_log/*.json` globbing with
   `delta_table(table_name).get_add_actions()`. This is a one-function fix with high
   impact: the current baseline silently returns wrong answers past 10 commits.

2. **Disambiguate `absolute_file_path`** (`nsi/indexer.py:_indexed_file_path`).
   The column stores workspace-relative paths. Rename it `relative_file_path` or
   change it to store true absolute paths. Either choice; the inconsistency between
   NSI and `evaluate_fact_trip_green` (which uses `.resolve()`) must be resolved.

3. **VACUUM awareness for the footer index** (`nsi/indexer.py:iter_warehouse_parquet_files`).
   `rglob("*.parquet")` includes tombstoned files. Replace with
   `delta_table(table_name).file_uris()` per table, or add a post-VACUUM reconciliation
   step.

**Architecture gaps for incremental loads:**

4. **NSI build-index must become a Dagster asset**, not a manual CLI step. The natural
   place is as a downstream asset of `postgres_mirror`, triggered automatically after
   every successful mirror run. Until this happens, the NSI will silently go stale
   after every pipeline run that isn't followed by a manual rebuild.

5. **Incremental index updates.** Full `to_sql(if_exists="replace")` on every rebuild
   (Flow 03 finding) works for the PoC but does not scale. A production NSI needs a
   diff-based update: delete rows for removed files, insert rows for added files,
   keyed on `(table_name, file_path)`. This requires tracking Delta commit versions
   against index build versions.

**Performance gaps:**

6. **Replace `to_sql` with COPY FROM STDIN in NSI build commands.** `create_predicate_index`
   and `create_footer_row_group_stats_index` both use SQLAlchemy's row-by-row insert
   path (Flow 03). For a warehouse with 10,000 files × 40 columns = 400,000 rows per
   index, COPY would be 10–50× faster.

7. **Merge `collect_column_statistics` into a single DuckDB SUMMARIZE call** (Flow 02).
   The current N×M per-column full-table scans is the pipeline's dominant cost. DuckDB's
   `SUMMARIZE table` returns count/min/max/distinct for all columns in one pass.

8. **Deduplicate `_normalize_value_kind`** across `metadata_io.py` and `nsi/indexer.py`.
   Both files carry identical implementations (Flow 02 and Flow 03 finding). Extract to
   a shared `nsi.types` module.

**Operational gaps:**

9. **Index staleness detection.** No mechanism exists to check whether the NSI is current
   relative to the Delta table version. A production system would compare
   `delta_table(name).version()` against a recorded `built_at_delta_version` column in
   the NSI metadata.

10. **Path portability.** Workspace-relative paths break the moment the warehouse moves
    to object storage (S3, ADLS, GCS). All file path storage and resolution must be
    changed to URI-based addressing before cloud deployment.

---

### 5. Architectural Comparison: Hudi Secondary Indexes and Iceberg Native Indexing

The NSI is a hand-rolled version of a capability that Apache Hudi and Apache Iceberg
approach differently. Understanding the differences clarifies what the NSI is actually
contributing.

**Apache Hudi — Column Stats Index (CSI):**

Hudi's closest equivalent is the Column Stats Index, part of the Metadata Table
(`hoodie/.hoodie/metadata/`). When enabled, it stores min/max/null_count per file per
column in a dedicated Hudi table maintained as part of the write path.

| Dimension | Hudi CSI | NSI (this repo) |
|---|---|---|
| Maintenance | Automatic (write-time) | Manual (`nsi.cli build-index`) |
| Column coverage | All columns | All columns |
| Typed comparisons | Yes | Yes (6 families) |
| Row-group granularity | No (file-level only) | Yes |
| Storage | Hudi table (parquet) | Postgres |
| Multi-predicate | Via engine push-down | Via SQL EXISTS |
| Staleness risk | Low | High without automation |

The NSI's structural advantage over Hudi CSI is **row-group granularity** — Hudi's
metadata table does not track intra-file statistics. The NSI's structural disadvantage
is that it must be manually triggered; Hudi's is maintained automatically on every write.
Hudi also supports a Bloom filter index for high-cardinality point-lookup columns (e.g.
trip_id exact match), which the NSI has no equivalent for.

**Apache Iceberg — Manifest Statistics:**

Iceberg embeds column-level statistics (lower_bound, upper_bound, null_value_count,
nan_value_count) directly in manifest files, written automatically on every commit.
Unlike Delta log's 32-column cap, Iceberg tracks **all columns** by default.

| Dimension | Iceberg Manifests | NSI (this repo) |
|---|---|---|
| Maintenance | Automatic (write-time) | Manual |
| Column coverage | All columns | All columns |
| Typed comparisons | Yes (native Avro types) | Yes (6 families) |
| Row-group granularity | No (file-level) | Yes |
| Storage | Manifest avro files | Postgres |
| Checkpoint-safe | Always (no replay) | N/A (no checkpoints) |
| Staleness risk | None | High |

Iceberg's manifest statistics close most of the gap this PoC identifies against Delta
log. The NSI's **typed comparison advantage** largely disappears against Iceberg (which
stores bounds as native Avro typed values, not JSON strings). The NSI's remaining
differentiator would be row-group granularity — but Iceberg also does not have that
natively, so the NSI's row-group index would be genuinely additive even alongside Iceberg.

**The honest architectural summary:**

The NSI solves real weaknesses in Delta Lake specifically. Against Iceberg (which fixes
column coverage and typing natively) the value proposition reduces to row-group skipping
plus the ability to express arbitrary Postgres SQL predicates during candidate selection.
The Postgres backing is both the NSI's strength (rich query model, JOINs, EXISTS) and
its weakness (manual build, no write-path integration, workspace-path coupling).

---

### Diagram Inaccuracies / Gaps (Synthesis Level)

| Severity | Issue |
|----------|-------|
| Significant | The architecture diagram presents NSI and Delta log as equivalent-level alternatives. They are not: Delta log is always current; NSI requires manual maintenance. The diagram should show the manual trigger dependency explicitly. |
| Significant | The Delta log baseline is presented as a reliable reference throughout. As noted in Flow 04, it breaks silently past 10 commits. The diagram should note the checkpoint limitation. |
| Structural | There is no diagram node for "NSI staleness / rebuild trigger." This is the most important operational concern for any use beyond the PoC and it is completely invisible. |
| Gap | The comparison between `metadata.predicate_row_group_index` (Dagster-built) and `parquet_footer.row_group_stats` (CLI-built) is not resolved anywhere in the architecture documentation. Both exist, both cover row groups, both write to Postgres — but only one is queried by the evaluation scripts (`metadata.predicate_row_group_index`). The role of `parquet_footer.row_group_stats` in the evaluation path is never explained. |

---

### Input → Output

```
Flows 01–04 (all pipeline stages)
    → one coherent benchmark: NSI candidate_file_count vs Delta log candidate_file_count
    → for the same predicate, smaller candidate_file_count = better pruning effectiveness
    → predicate_output/{label}/summary.json   ← NSI result
    → delta_log_output/{label}/summary.json   ← Delta baseline result
    → row-level diff of matching_trips.parquet between the two dirs
         = zero rows different → pruning is conservative (no false negatives)
         = rows missing from NSI output → NSI has a false negative (a bug)
```

---

### One Insight to Remember

The NSI's advantage over Delta log statistics is real but **format-specific**: it
directly fixes known weaknesses in Delta Lake's log format (32-column cap, untyped JSON
bounds). Against Iceberg, which stores typed bounds for all columns natively in manifest
files, the only remaining NSI differentiator is row-group granularity. If this project
ever migrates to Iceberg, the file-level NSI becomes redundant — only the row-group
index (`predicate_row_group_index`) remains genuinely additive.

---

## 2026-04-20 — Codebase Walkthrough: Flow 01 (Data Ingestion)

### Scope
Architecture nodes covered: NYC Taxi source files, `download_examples.py`,
`delta_dimensions` asset, `delta_facts` asset, dim_* tables, fact_trip_* tables,
`_delta_log` auto-generation.

---

### Files Involved
- `datasource/download_examples.py`
- `dagster-project/src/dagster_project/assets/delta_dimensions.py`
- `dagster-project/src/dagster_project/assets/delta_facts.py`
- `dagster-project/src/dagster_project/source_data.py`
- `dagster-project/src/dagster_project/fact_queries.py`
- `dagster-project/src/dagster_project/delta_io.py`
- `dagster-project/src/dagster_project/settings.py`

---

### Key Findings

**`dim_base` is derived, not hardcoded.**
All five static dimensions (`dim_location`, `dim_vendor`, `dim_payment_type`,
`dim_rate_code`, `dim_trip_type`) are hardcoded inline in `load_static_dimensions()`.
`dim_base` is the exception — it is built at runtime by `build_base_dimension()`,
which runs a DuckDB deduplication query over all 12 FHV monthly parquets to extract
unique base numbers and their dispatching/affiliated flags.

**`trip_query()` re-reads all 36 source parquets independently.**
`fact_trip` is a 3-way `UNION ALL` of yellow, green, and FHV sub-selects, each
re-scanning the raw source parquets from scratch. It does NOT read from the
per-service Delta tables already written in the same job run. This means the
full rebuild reads 36 parquet files 4× total (once per service fact + once for
`fact_trip`). If rebuild time becomes a concern, this is the first place to
optimize — switch `trip_query()` to read from the already-written Delta tables.

**`write_delta_from_query` streams 250k-row Arrow batches.**
DuckDB's `fetch_record_batch(rows_per_batch=250_000)` returns a `RecordBatchReader`
that `write_deltalake()` consumes incrementally. Full result set is never held in
memory. Confirmed at `delta_io.py:34`.

**Every run is a full overwrite.**
Both `write_delta_from_dataframe` and `write_delta_from_query` use
`mode="overwrite", schema_mode="overwrite"`. There is no incremental/append path
in the current implementation.

**`pickup_year` uses ISO week year (`%G`), not calendar year (`%Y`).**
In `fact_queries.py`, all four queries derive `pickup_year` via
`strftime(..., '%G')`. This handles year-boundary edge cases (late-December trips
that belong to the following ISO week year) correctly, but means the partition
value can differ from the calendar year for a small number of trips.

**FHV rows have all financial columns as NULL in `fact_trip`.**
`trip_query()` casts `fare_amount`, `tip_amount`, `total_amount`, and all other
financial columns to `NULL` for FHV rows. Any financial aggregation over `fact_trip`
without a `WHERE service_type != 'fhv'` filter will silently undercount.

**`_delta_log` is written by the `deltalake` library, not application code.**
No application function calls `_delta_log` directly. It is a side effect of every
`write_deltalake()` call. Each commit appends a JSON file containing `add` actions
with embedded `stats` blobs (`minValues`, `maxValues`, `nullCount`). These stats
blobs are what the Delta Log Baseline evaluation path reads later for comparison
against the NSI.

---

### Diagram Inaccuracies / Gaps

| Severity | Location | Issue |
|----------|----------|-------|
| Minor | `delta_facts` node | Diagram does not indicate that `trip_query()` re-reads source parquets independently (not from already-written Delta tables). Rebuild cost is higher than the diagram implies. |
| Minor | `fact_trip` description | Diagram notes "nullable cols for non-matching services" but does not call out that FHV rows have *all* financial columns as NULL, making financial queries on this table silently wrong without a service filter. |
| Gap | Not diagrammed | `settings.py` holds `WORKSPACE_ROOT`, `DELTA_WAREHOUSE_DIR`, `DATA_SOURCE_DIR`, and all table-name constants. It is invisible in the diagram but is the path anchor for everything. |

---

### Input → Output

```
datasource/nyc-taxi/*.parquet (36 files) + taxi_zone_lookup.csv
    → delta-lake/warehouse/dim_*/          (6 dimension tables, unpartitioned)
    → delta-lake/warehouse/fact_trip_*/    (4 fact tables, partitioned by pickup_year/pickup_month)
    → delta-lake/warehouse/*/_delta_log/   (auto-generated by deltalake library)
```

---

### One Insight to Remember

`trip_query()` re-reads all 36 source parquets independently — it is not derived
from the per-service fact tables already written in the same job run. If the
lakehouse rebuild becomes slow, replacing the raw-parquet scans in `trip_query()`
with `read_parquet([...fact_trip_yellow/...])` Delta reads is the highest-leverage
optimization available.

---

## 2026-04-20 — Codebase Walkthrough: Flow 04 (Benchmark Evaluation)

### Scope
Architecture nodes covered: `evaluate_fact_table.py`, `evaluate_row_group_fact_table.py`,
`evaluate_row_group_multi_predicate.py`, `evaluate_fact_trip_green.py`, the NSI pruning
path, the Delta log baseline path, and benchmark output under `predicate_output/` and
`delta_log_output/`.

---

### Files Involved
- `nsi/evaluate_fact_table.py`
- `nsi/evaluate_row_group_fact_table.py`
- `nsi/evaluate_row_group_multi_predicate.py`
- `nsi/evaluate_fact_trip_green.py`

---

### Key Findings

**The Delta log baseline always operates at file level, even in row-group evaluation scripts.**
All four scripts share the same `_delta_log_candidate_paths()` implementation which replays
`_delta_log/*.json` files and applies per-file min/max stats. The row-group scripts
(`evaluate_row_group_fact_table.py`, `evaluate_row_group_multi_predicate.py`) compare
NSI row-group pruning against this **file-level** Delta baseline — not row-group vs.
row-group. When you read the benchmark summary and see NSI's `candidate_row_group_count`,
the Delta log side has no equivalent metric — it can only report `candidate_file_count`.
This is an apples-to-partial-oranges comparison by design.

**The Delta log replay is correct but ignores checkpoint parquet files.**
`_delta_log_candidate_paths` in all four scripts globs only `*.json`, replays `add` and
`remove` actions in sorted order, and builds an `active_adds` dict. Delta Lake
auto-checkpoints every 10 commits by writing a `.checkpoint.parquet` file and
`_last_checkpoint` JSON marker, after which older JSON log entries are redundant and may
be removed. If the warehouse has been checkpointed, this replay logic will read an
incomplete or empty set of JSON files and return wrong candidate lists. This is a real
correctness risk for any warehouse that has been written to more than 10 times.

**`_delta_log_candidate_paths` is copy-pasted across all four evaluation scripts.**
The core logic (glob, read, replay add/remove, test stats) is duplicated verbatim in
`evaluate_fact_table.py`, `evaluate_row_group_fact_table.py`, and
`evaluate_row_group_multi_predicate.py`. The fourth file (`evaluate_fact_trip_green.py`)
copies the same block but hardcodes the table/column. The only structural difference is in
the final predicate-matching step. There is no shared utility function.

**`evaluate_fact_trip_green._absolute_path` returns TRUE OS absolute paths; every other
script returns workspace-relative paths.**
`evaluate_fact_trip_green.py` uses `.resolve()`:
```python
str((DELTA_WAREHOUSE_DIR / TABLE_NAME / relative_path).resolve()).replace("\\", "/")
```
while `evaluate_fact_table.py` and `evaluate_row_group_*.py` use `.relative_to(WORKSPACE_ROOT)`.
Both work with `build_duckdb_parquet_scan` (which resolves relative paths), but `summary.json`
outputs from `evaluate_fact_trip_green` will contain full OS paths while outputs from the
other scripts contain workspace-relative paths. Cross-script comparison of `candidate_files`
lists in JSON output will show different path formats for the same files.

**`evaluate_row_group_multi_predicate._row_group_candidate_info` uses asymmetric SQL for
the first vs. subsequent predicates.**
Predicate index 0 is applied as a direct `WHERE` clause on the base table alias `p0`.
Predicates index 1+ are applied via correlated `EXISTS` subqueries that join back on
`(table_name, file_path, row_group_index, column_name)`. This is more efficient than
using `EXISTS` for all predicates (saves one self-join), but it is structurally
inconsistent with `build_candidate_files_query` in `indexer.py`, which uses `EXISTS` for
every predicate including the first.

**`_duckdb_predicate_sql` uses string interpolation, not parameterized queries.**
The DuckDB WHERE clause for reading matching trips is built with `_sql_literal()` inlining
coerced Python values directly into SQL text. For local CLI use this is acceptable.
`_sql_literal` handles string quoting with `replace("'", "''")`, so basic injection is
mitigated. But this is the only place in the entire codebase that deviates from
parameterized queries — worth knowing if you ever add a web-facing query path.

**`evaluate_fact_trip_green` writes output to `predicate_output/` root, not a subdirectory.**
```python
PREDICATE_OUTPUT_DIR = WORKSPACE_ROOT / "predicate_output"
DELTA_LOG_OUTPUT_DIR = WORKSPACE_ROOT / "delta_log_output"
```
All other scripts write to `predicate_output/{table_name}/` or `predicate_output/{label}/`.
Running `evaluate_fact_trip_green` will write `summary.json` directly into the root output
dirs, and could overwrite output from a previous evaluation run that also happened to land
at the same path.

**Matching trips are always `ORDER BY pickup_at, dropoff_at` — output is deterministic.**
Both the NSI path and the Delta log path use the same sort order. This means you can
diff `matching_trips.parquet` between `predicate_output/` and `delta_log_output/` to
verify that both paths return identical rows, not just identical counts. If the counts
match but a row-level diff finds differences, that indicates a pruning correctness bug
(the NSI skipped a file that contained matching rows, i.e., a false negative).

**`evaluate_fact_table.py` supports only numeric BETWEEN predicates on the Delta log side.**
The Delta log baseline in this script hardcodes `Decimal(str(minimum)) <= upper_bound`
comparisons. For non-numeric columns (timestamps, dates, strings), this would fail or
produce wrong results. The generalized multi-type Delta log comparison lives only in
`evaluate_row_group_multi_predicate._delta_log_predicate_matches`.

---

### Diagram Inaccuracies / Gaps

| Severity | Location | Issue |
|----------|----------|-------|
| Significant | "Delta Log Baseline" node | Diagram does not make clear that the Delta log path is always **file-level** even when compared against NSI row-group pruning. Readers may assume row-group vs. row-group comparison. |
| Significant | Delta log correctness | Diagram implies the Delta log baseline is a reliable reference. In practice it will silently return wrong results on any table that has been checkpointed (>10 commits). Not documented anywhere. |
| Gap | `evaluate_fact_trip_green.py` output paths | Diagram lists it as writing to `predicate_output/label/` by implication, but the script actually writes to the root `predicate_output/` directory — no label subdirectory. |
| Gap | `_absolute_path` inconsistency | Diagram shows all candidate file paths flowing into `build_duckdb_parquet_scan` uniformly. The reality is that `evaluate_fact_trip_green` emits true absolute paths while all other scripts emit workspace-relative paths — same end result but different `summary.json` content. |
| Gap | Copy-paste duplication | The diagram shows `_delta_log_candidate_paths` as a shared step. In code it is copy-pasted four times with no shared implementation. |

---

### Input → Output

```
Postgres metadata.predicate_file_index          ← NSI file-level path
    → find_candidate_files() → file list
    → DuckDB read_parquet([...]) BETWEEN/= filter
    → predicate_output/{label}/summary.json
    → predicate_output/{label}/matching_trips.{parquet,csv}

Postgres metadata.predicate_row_group_index     ← NSI row-group path
    → correlated EXISTS query → (file, row_group) list
    → deduplicated file list → DuckDB conjunctive filter
    → predicate_output/{label}/summary.json
    → predicate_output/{label}/matching_trips.{parquet,csv}

delta-lake/warehouse/{table}/_delta_log/*.json  ← Delta baseline (all scripts)
    → replay add/remove → active_adds dict
    → per-file stats range/type test → file list
    → DuckDB read_parquet([...]) filter
    → delta_log_output/{label}/summary.json
    → delta_log_output/{label}/matching_trips.{parquet,csv}
```

---

### One Insight to Remember

The Delta log replay in all four evaluation scripts globs only `*.json` and will silently
produce incorrect candidate lists if the table has been checkpointed (Delta writes
checkpoint parquet files after every 10 commits and may delete the older JSON entries).
In the current setup with a single full overwrite (`mode="overwrite"`), each table has
exactly one commit and one JSON log file, so this doesn't bite you today. But any future
incremental write strategy — appends, schema evolution, OPTIMIZE runs — will push tables
past 10 commits and the baseline evaluation will start returning wrong answers without any
error or warning.

---

## 2026-04-20 — Codebase Walkthrough: Flow 03 (NSI Index Build)

### Scope
Architecture nodes covered: `nsi/config.py`, `nsi/cli.py`, `nsi/indexer.py`,
`nsi.cli build-index` → `metadata.predicate_file_index`,
`nsi.cli build-footer-index` → `parquet_footer.row_group_stats`,
and the predicate-pruning query logic used downstream.

---

### Files Involved
- `nsi/config.py`
- `nsi/cli.py`
- `nsi/indexer.py`

---

### Key Findings

**`absolute_file_path` is misnamed — the column stores workspace-relative paths.**
`_indexed_file_path()` calls `_workspace_relative_path()` which returns
`path.relative_to(WORKSPACE_ROOT).as_posix()`. So a stored value looks like
`delta-lake/warehouse/fact_trip_yellow/pickup_year=2025/pickup_month=1/0-xxx.parquet` —
not an OS-level absolute path. `build_duckdb_parquet_scan()` compensates by detecting
non-absolute paths and prepending `WORKSPACE_ROOT` before passing them to DuckDB.
But anyone querying `metadata.predicate_file_index` directly and trying to use
`absolute_file_path` as a filesystem path will get failures.

**`build-index` reads from Postgres; `build-footer-index` reads from disk — fundamentally different data sources.**
`create_predicate_index` reads `metadata.current_snapshot_file_stats` and
`metadata.column_catalog` from Postgres (Dagster-populated, Delta-snapshot-derived).
`create_footer_row_group_stats_index` walks the filesystem with
`iter_warehouse_parquet_files()` → `rglob("*.parquet")`, opening every parquet file
directly via PyArrow regardless of the Delta snapshot. This means `row_group_stats`
can include files that have been logically deleted in Delta (tombstoned but not yet
vacuumed), while `predicate_file_index` only covers the active snapshot.

**`create_predicate_index` and `create_footer_row_group_stats_index` both use
`to_sql(if_exists="replace")` — not COPY FROM STDIN.**
Unlike `postgres_mirror`, neither NSI build command uses psycopg2's bulk COPY path.
For a large warehouse with thousands of files and 30+ columns each, `to_sql` does
row-by-row inserts via SQLAlchemy, which will be noticeably slower than the COPY
approach used elsewhere in the pipeline.

**The candidate-files query self-joins `predicate_file_index` once per predicate.**
`build_candidate_files_query()` generates one correlated `EXISTS` subquery per predicate,
each re-joining `predicate_file_index` on `(table_name, file_path, column_name)`. This
is necessary because the index has one row per (file, column): to test two predicates
on different columns for the same file requires two separate row lookups. The structure is:
```sql
SELECT DISTINCT base.absolute_file_path
FROM metadata.predicate_file_index AS base
WHERE base.table_name = :table_name
AND EXISTS (SELECT 1 FROM ... AS p0 WHERE p0.file_path = base.file_path AND p0.column_name = :col AND <clause>)
AND EXISTS (SELECT 1 FROM ... AS p1 WHERE p1.file_path = base.file_path AND p1.column_name = :col AND <clause>)
```

**Predicate clause semantics are conservative (no false negatives, but not tight).**
Each operator maps to an overlap test against stored min/max:
- `=` : `min <= value AND max >= value` — file might contain a match
- `!=` : eliminates only files where ALL values equal the filter value (min=max=value, null_count=0)
- `<` / `<=` / `>` / `>=` : standard one-sided bound tests
- `between` : `min <= upper AND max >= lower` — standard interval overlap
The `!=` case is particularly conservative: it will only skip a file if every single
stored value is exactly the excluded value, which almost never happens in practice.

**`nsi/config.py` is entirely independent from `dagster_project/settings.py`.**
The NSI package has its own `WORKSPACE_ROOT = Path(__file__).parents[1]`, its own
`_load_dotenv()` (custom, no third-party dependency), and its own Postgres URL
assembly. `_load_dotenv` uses `os.environ.setdefault` — shell env vars always win
over `.env` file values, which is the correct precedence for production use.

**`iter_warehouse_parquet_files` picks up all parquet files, including those in
dimension tables and tombstoned-but-not-vacuumed files.**
`rglob("*.parquet")` does not consult the Delta log. Dimension tables like `dim_vendor`
get row-group stats entries in `parquet_footer.row_group_stats`, which is fine but means
the footer index covers more tables than just the fact tables. More critically, if Delta
has removed (tombstoned) a file without running VACUUM, the footer index will contain
stale row groups that the file-level index does not.

**`_load_dotenv` is a third duplicate of the same utility.**
`metadata_io.py` and `postgres_io.py` in the Dagster project use a different
mechanism (`dagster_project/env.py`) to load Postgres credentials. The NSI package
rolls its own `_load_dotenv` in `config.py`. All three paths ultimately read the same
`.env` file but through different code. This is not a bug, but it is fragile.

---

### Diagram Inaccuracies / Gaps

| Severity | Location | Issue |
|----------|----------|-------|
| Significant | `absolute_file_path` column | CLAUDE.md and diagram describe this as "absolute file paths" throughout. In practice the index stores workspace-relative paths; `build_duckdb_parquet_scan` resolves them. Calling these "absolute" is misleading. |
| Gap | `build-footer-index` data source | Diagram does not flag that `iter_warehouse_parquet_files` uses `rglob`, not the Delta snapshot. The footer index can therefore include tombstoned files that the file-level index does not. |
| Gap | `to_sql` vs COPY | Diagram shows both NSI build paths writing to Postgres but does not distinguish the slow `to_sql` insert path from the fast COPY path used in `postgres_mirror`. At scale this is a meaningful performance difference. |
| Minor | `_predicate_clause` for `!=` | CLAUDE.md documents `!=` as pruning files where the min=max=filter_value. This is correct but the practical skip rate for `!=` is near zero — worth documenting as a known weak operator. |
| Minor | `nsi/config.py` not shown | `config.py` auto-loads `.env` at import time (module-level `_load_dotenv(...)` call). This is a side effect on import that is invisible in the diagram. |

---

### Input → Output

```
Postgres metadata.current_snapshot_file_stats   ← file-level stats from Delta snapshot
Postgres metadata.column_catalog                ← column type catalog
    → [build_predicate_index_frame()]
    → Postgres metadata.predicate_file_index    ← NSI file-level index (to_sql replace)
    → CREATE INDEX on (table_name, file_path) and (table_name, column_name)

delta-lake/warehouse/**/*.parquet               ← physical files on disk (rglob, not snapshot)
    → [_footer_row_group_records() per file via PyArrow]
    → Postgres parquet_footer.row_group_stats   ← NSI row-group index (to_sql replace)
    → CREATE INDEX on (table_name, file_path, row_group_index) and (table_name, column_name)
```

---

### One Insight to Remember

`absolute_file_path` in `metadata.predicate_file_index` is not an OS-level absolute path
— it is workspace-relative (e.g. `delta-lake/warehouse/fact_trip_yellow/.../0-xxx.parquet`).
`build_duckdb_parquet_scan` silently converts these to real absolute paths before use.
This matters if you ever query the index table directly in Postgres and try to `COPY` or
open those paths: they will fail unless you prepend the workspace root manually.

---

## 2026-04-20 — Codebase Walkthrough: Flow 02 (Metadata Mirror)

### Scope
Architecture nodes covered: `metadata_exports` asset, `current_snapshot_files` asset,
`current_snapshot_file_stats` asset, `postgres_mirror` asset, all six metadata parquet
exports, and the Postgres `mart` + `metadata` schemas.

---

### Files Involved
- `dagster-project/src/dagster_project/assets/metadata_exports.py`
- `dagster-project/src/dagster_project/assets/current_snapshot_files.py`
- `dagster-project/src/dagster_project/assets/current_snapshot_file_stats.py`
- `dagster-project/src/dagster_project/assets/postgres_mirror.py`
- `dagster-project/src/dagster_project/metadata_io.py`
- `dagster-project/src/dagster_project/postgres_io.py`

---

### Key Findings

**`collect_column_statistics` is the most expensive function in the pipeline.**
It runs one DuckDB full-table scan *per column* across all active parquet files for
each table. For `fact_trip` (~30 columns), that is 30+ separate DuckDB aggregation
queries, each scanning the full table. No batching or columnar projection is used to
combine them. This is the dominant cost in the `metadata_exports` asset.

**`current_snapshot_files` and `current_snapshot_file_stats` both independently call
`delta_table().get_add_actions(flatten=True)`.**
They are separate assets and share no in-memory state. Each re-opens every Delta
table's snapshot independently. The two outputs differ only in shape: `current_snapshot_files`
produces one row per file with a `column_stats_json` blob; `current_snapshot_file_stats`
pivots to one row per (file, column). Since they are the same underlying data, they could
be merged into one asset if build time becomes a concern.

**`postgres_mirror` reads metadata from disk parquets, not from in-memory DataFrames.**
`metadata_exports` writes parquets to `delta-lake/metadata_exports/`. Then `postgres_mirror`
independently re-reads them with `pl.read_parquet(...)`. There is no in-memory handoff
between these assets — they communicate through the filesystem. This is by Dagster design
(assets are independently materializable) but means the full metadata is serialized and
deserialized between the two steps.

**`load_delta_table_to_postgres` uses a two-phase schema+copy pattern.**
Phase 1: derive table DDL by calling `to_sql` with an *empty* pandas DataFrame — this
creates the Postgres table with correct column types but inserts nothing. Phase 2: stream
100k-row Arrow batches via psycopg2's `COPY FROM STDIN` with CSV format, committing every
10 batches (every 1M rows). This avoids SQLAlchemy's slow row-by-row insert path entirely.
Confirmed: `DELTA_COPY_BATCH_SIZE = 100_000`, `DELTA_COPY_COMMIT_INTERVAL = 10`.

**`load_dataframe_to_postgres` uses the same two-phase pattern for metadata tables.**
It calls `to_sql` with an empty frame for DDL, then `COPY FROM STDIN`. One difference:
`dtype_overrides()` is called to force JSONB on any column ending in `_json` and explicit
`Numeric`/`Date`/`Boolean` types on typed min/max columns — preventing SQLAlchemy from
inferring `TEXT` for those columns.

**`_normalize_value_kind` is duplicated between `metadata_io.py` and `nsi/indexer.py`.**
Both modules contain a private function mapping Arrow/SQL type names to the five value
families (`numeric`, `timestamp`, `date`, `boolean`, `text`). The implementations are
functionally identical but maintained separately. If the type-mapping logic ever needs to
change, it must be updated in both places.

**`predicate_row_group_index` is built inside the Dagster pipeline from PyArrow footer stats.**
`collect_predicate_row_group_index` in `metadata_io.py` opens every active Delta parquet
file via `pq.ParquetFile()` and reads row-group column statistics from the physical file
footer. It then types and indexes those stats via `_build_predicate_row_group_index_frame`.
This produces `metadata.predicate_row_group_index` in Postgres. This is a **separate path**
from `nsi.cli build-footer-index`, which writes to `parquet_footer.row_group_stats`. Both
tables contain row-group-level typed min/max, but they are built by different tools and
written to different Postgres schemas.

**`wait_for_postgres` polls with a 120-second deadline.**
`postgres_mirror` calls this before any writes. It retries every 2 seconds. If Postgres is
not reachable after 2 minutes, it raises `TimeoutError`. There is no exponential backoff.

---

### Diagram Inaccuracies / Gaps

| Severity | Location | Issue |
|----------|----------|-------|
| Misleading | `ME_CAT → PG_CAT` arrows | The diagram draws direct arrows from metadata parquet files into Postgres, implying a streaming handoff. In reality `postgres_mirror` re-reads parquets from disk — the "connection" goes through the filesystem. |
| Gap | Not diagrammed | `_normalize_value_kind` exists in *both* `metadata_io.py` and `nsi/indexer.py`. The diagram treats type normalization as a single step but it is duplicated code. |
| Gap | `collect_column_statistics` node | The diagram does not hint at the per-column full-scan cost. It is the most expensive single step in the metadata pipeline and is not visually distinguished from the cheaper catalog/log collection steps. |
| Gap | Two row-group index paths | The diagram shows `predicate_row_group_index` flowing from `metadata_exports`, and separately shows `nsi.cli build-footer-index` writing `row_group_stats`. What is not clear is that both read the same physical parquet footers and produce almost identical typed min/max schemas — they are parallel implementations, not complementary ones. |
| Minor | `current_snapshot_files` / `current_snapshot_file_stats` | The diagram does not show that both assets call `get_add_actions(flatten=True)` independently, doubling the Delta snapshot reads for every table. |

---

### Input → Output

```
delta-lake/warehouse/*/           (Delta tables + _delta_log/)
    → delta-lake/metadata_exports/column_catalog.parquet
    → delta-lake/metadata_exports/column_statistics.parquet         ← expensive: N×cols DuckDB scans
    → delta-lake/metadata_exports/delta_log_entries.parquet
    → delta-lake/metadata_exports/predicate_row_group_index.parquet ← PyArrow footer reads
    → delta-lake/metadata_exports/current_snapshot_files.parquet
    → delta-lake/metadata_exports/current_snapshot_file_stats.parquet

delta-lake/metadata_exports/*.parquet + delta-lake/warehouse/*/
    → Postgres mart.*             (10 tables via COPY FROM STDIN, 100k-row batches)
    → Postgres metadata.*         (6 tables via COPY FROM STDIN)
```

---

### One Insight to Remember

`postgres_mirror` reads metadata from parquet files on disk, not from in-memory DataFrames
passed down from `metadata_exports`. If you ever see a stale Postgres `metadata` schema
after a partial pipeline run, check whether `metadata_exports` actually completed and wrote
fresh parquets before `postgres_mirror` ran. The Dagster dependency edge prevents this
during a full job run, but manually re-materializing `postgres_mirror` alone will silently
load whatever parquets are currently on disk — which may be from a previous run.

---
