# SESSIONS.md — MonkeyDData DataLakeIndexProofOfConcept

---

## Session 01 — 2026-04-20 (Completed)

### What Was Accomplished

Full cold-start onboarding of the `DataLakeIndexProofOfConcept` repository
from zero knowledge to complete architecture understanding in a single session.

**Discovery**
- Found repo via LinkedIn post by Dipankar Mazumdar (Director, Cloudera /
  Apache Iceberg + Hudi contributor) discussing lakehouse indexing trends
- Post context: Iceberg introducing native indexes; Hudi secondary indexes
  already live since 2016; NSI is an independent PoC proving the same concept
  on Delta Lake

**Setup Completed**
- Cloned repo to GitHub: `https://github.com/dmishra27/DataLakeIndexProofOfConcept`
- Opened in VS Code + Claude Code via `cc` launcher
- Ran `/init` to generate initial `CLAUDE.md`
- Generated `ARCHITECTURE.md` (full Mermaid flowchart, all 5 flows)
- Generated `ARCHITECTURE_viewer.html` (5 focused sub-diagrams, dark theme,
  browser-renderable — open by double-clicking in File Explorer)

**Codebase Walkthrough — All 5 Flows**
- Flow 01: Data Ingestion (NYC Taxi → Dagster → Delta Lake)
- Flow 02: Metadata Mirror (Delta snapshot → Parquet exports → Postgres)
- Flow 03: NSI Index Build (nsi/indexer.py + nsi/cli.py → Postgres indexes)
- Flow 04: Benchmark Evaluation (NSI vs Delta log candidate file comparison)
- Flow 05: End-to-End Synthesis (thesis, production gaps, Hudi/Iceberg comparison)

**Output Files Created**
- `CLAUDE.md` — project context for Claude Code sessions
- `ARCHITECTURE.md` — full Mermaid architecture diagram
- `ARCHITECTURE_viewer.html` — browser-viewable 5-flow diagram (copy to project root)
- `LEARNING_NOTES.md` — complete 5-flow walkthrough with findings and gaps

**Session stopped after:** git commit and push of all documentation files.

---

### Critical Bugs / Gaps Discovered (from LEARNING_NOTES.md)

These were found by reading actual source code — not assumptions:

| Priority | File | Issue |
|----------|------|-------|
| 🔴 High | `evaluate_*.py` (all 4) | Delta log replay globs only `*.json` — silently wrong past 10 commits when Delta checkpoints exist |
| 🔴 High | `nsi/indexer.py` | `absolute_file_path` column stores workspace-relative paths, not OS absolute paths — misleading name, breaks direct Postgres queries |
| 🟡 Medium | `metadata_io.py` + `nsi/indexer.py` | `_normalize_value_kind` duplicated in both files — must update in two places if type mapping changes |
| 🟡 Medium | `nsi/indexer.py` | Both NSI build commands use slow `to_sql` row-by-row inserts — not COPY FROM STDIN like `postgres_mirror` |
| 🟡 Medium | `metadata_io.py` | `collect_column_statistics` runs one full DuckDB scan per column — 30+ scans for `fact_trip` — most expensive step |
| 🟡 Medium | `dagster_project/assets/delta_facts.py` | `trip_query()` re-reads all 36 source parquets independently — not from already-written Delta tables |
| 🟠 Low | `evaluate_fact_trip_green.py` | Writes to `predicate_output/` root, not a label subdirectory — can overwrite previous run outputs |
| 🟠 Low | `evaluate_fact_trip_green.py` | Uses `.resolve()` for paths; all other scripts use workspace-relative — inconsistent `summary.json` output |
| 🟠 Low | `nsi/indexer.py` | `iter_warehouse_parquet_files` uses `rglob` not Delta snapshot — includes tombstoned files |

---

## Session 02 — 2026-04-21 (TODO — Next Session)

### Start Here — Session Opener Prompt

Paste this into Claude Code chat at the start of the session:

```
Read CLAUDE.md, SESSIONS.md, and LEARNING_NOTES.md.

Summarise where we left off and confirm:
1. Are all 5 flows documented in LEARNING_NOTES.md?
2. Are CLAUDE.md, ARCHITECTURE.md, ARCHITECTURE_viewer.html,
   LEARNING_NOTES.md, and SESSIONS.md committed to GitHub?
3. What are the top 3 production bugs found yesterday?

Then wait for my instruction before proceeding.
```

---

### Next Steps — In Priority Order

---

#### STEP A — Verify GitHub Commit (5 mins)

Check that yesterday's commit landed correctly:

```powershell
git log --oneline -5
git status
```

Expected output:
```
abc1234 docs: complete 5-flow architecture walkthrough with production gap analysis
nothing to commit, working tree clean
```

If anything is missing, run:
```powershell
git add .
git commit -m "docs: complete 5-flow architecture walkthrough with production gap analysis"
git push origin main
```

---

#### STEP B — SIGIR Connection Analysis (Most Important — 20 mins)

This turns the technical work into a career asset.
Paste into Claude Code:

```
Read LEARNING_NOTES.md in full — especially Flow 05.

I have a co-authored ACM SIGIR 2024 publication on
corpus index pruning — selectively removing posting
list entries to reduce retrieval cost while preserving
recall quality.

Do three things:

1. Draw explicit technical parallels between:
   - Corpus index pruning (my SIGIR paper)
   - NSI predicate file skipping (this repo)
   - Hudi Column Stats Index
   - Iceberg manifest statistics
   - Upcoming Iceberg native index proposals

2. Identify where my SIGIR background gives genuine
   insight others would miss — specifically around:
   - Conservative predicate semantics (no false negatives)
   - The recall/precision trade-off in file skipping
   - The index selectivity problem

3. Write me a 2-minute verbal answer to:
   "Tell me about a recent technical project that
   demonstrates your understanding of data retrieval
   optimisation at scale."

   Rules for the answer:
   - Open with the core problem, not the technology
   - Reference my SIGIR publication naturally
   - Explain NSI vs Delta log in plain English
   - Connect to the industry trend (Hudi/Iceberg)
   - End with what I would do differently in production
   - Sound like a senior practitioner, not a student
```

---

#### STEP C — LinkedIn Post (15 mins)

```
Based on LEARNING_NOTES.md Flow 05 synthesis,
write a LinkedIn post that:

1. Opens with the Delta log 32-column limitation
   as a hook — most engineers don't know this
2. Explains what NSI proves in 2-3 sentences
3. Connects to the Hudi/Iceberg industry trend
4. References Dipankar Mazumdar's post naturally
5. Mentions my ACM SIGIR 2024 background in
   one sentence — contextual, not boastful
6. Ends with a genuine question for engagement

Rules:
- 250-300 words maximum
- No bullet points in the post body
- Tone: senior practitioner sharing a real insight
- Not promotional, not academic
- Add 5 relevant hashtags at the end
```

---

#### STEP D — Fix the Critical Bugs (30 mins)

Start with the highest priority bug — Delta log checkpoint issue:

```
Read LEARNING_NOTES.md Flow 04 — specifically the
checkpoint bug finding.

The Delta log replay in all four evaluate_*.py files
globs only *.json and silently breaks past 10 commits.

For each of the four files:
- evaluate_fact_table.py
- evaluate_row_group_fact_table.py
- evaluate_row_group_multi_predicate.py
- evaluate_fact_trip_green.py

Replace the raw JSON globbing in _delta_log_candidate_paths
with delta_table(table_name).get_add_actions() from the
deltalake Python library.

Also extract the shared logic into a single utility
function in nsi/delta_log_utils.py to eliminate the
copy-paste duplication.

Show me the changes before applying them.
```

---

#### STEP E — Set Up the Environment (45 mins)

Only attempt this after Steps A-D are complete.

Pre-requisites to check first:
```powershell
docker --version        # needs Docker Desktop running
python --version        # needs Python 3.11+
```

Then run:
```powershell
# Step 1 - Environment file
Copy-Item .\.env.example .\.env
# Edit .env and set POSTGRES_PASSWORD and other values

# Step 2 - Build and run Postgres
docker build -t datalake-postgres:17 .\docker-images\postgres
docker run --rm -p 5432:5432 --env-file .\.env datalake-postgres:17
```

In a second terminal:
```powershell
# Step 3 - Download NYC Taxi data (~2GB, takes time)
python .\datasource\download_examples.py
```

Ask Claude Code before running anything:
```
Before I run the Dagster pipeline, read all files in
dagster-project/ and tell me:
1. Any missing dependencies I need to install first
2. Correct Python version required
3. Any known issues running on Windows
4. What to verify in .env before running
```

---

#### STEP F — Run the Full Pipeline (60 mins)

Only after environment is confirmed working:

```powershell
cd dagster-project
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .

$env:PYTHONPATH=(Resolve-Path ..)
$env:DAGSTER_HOME="$PWD\.dagster-home"

# Run full lakehouse rebuild
.\.venv\Scripts\python.exe -m dagster job execute `
  -m dagster_project.definitions `
  -j rebuild_lakehouse

# Build NSI indexes
.\.venv\Scripts\python.exe -m nsi.cli build-index
.\.venv\Scripts\python.exe -m nsi.cli build-footer-index
```

---

#### STEP G — Run Benchmarks and Interpret Results (30 mins)

```powershell
.\.venv\Scripts\python.exe -m nsi.evaluate_fact_table
.\.venv\Scripts\python.exe -m nsi.evaluate_row_group_fact_table
.\.venv\Scripts\python.exe -m nsi.evaluate_row_group_multi_predicate
```

Then in Claude Code:
```
Read all files in predicate_output/ and delta_log_output/.

Compare the results and tell me:
1. By how much does NSI outperform Delta log pruning?
2. Which predicates show the biggest improvement?
3. Which NYC Taxi columns benefit most from NSI?
4. Do the results confirm the 3 structural weaknesses
   of Delta log stats identified in LEARNING_NOTES.md?
5. How do these results compare to Dipankar Mazumdar's
   Hudi benchmark (90% scan reduction, 58% improvement)?
```

---

### Session 02 Checklist

```
[ ] STEP A — Verify GitHub commit is clean
[ ] STEP B — SIGIR connection analysis completed
[ ] STEP C — LinkedIn post drafted
[ ] STEP D — Checkpoint bug fixed in all 4 evaluate_*.py files
[ ] STEP E — Environment set up (Docker + Python venv)
[ ] STEP F — Full pipeline executed successfully
[ ] STEP G — Benchmarks run and results interpreted
[ ] Commit all changes: git add . && git push origin main
```

---

## Key Context to Remember

**What this repo is:**
A benchmarking PoC proving that a Postgres-backed predicate index (NSI =
Non-Stupid Index) can skip more Parquet files than Delta Lake's built-in
log statistics for the same query — using NYC Taxi data as the test dataset.

**The core thesis:**
Delta log stats have three weaknesses: 32-column cap, untyped JSON bounds,
file-level granularity only. NSI fixes all three. Against Iceberg (which
fixes column coverage and typing natively), only the row-group index remains
genuinely additive.

**Why it matters for your profile:**
The NSI's predicate pruning is structurally identical to corpus index pruning
from your ACM SIGIR 2024 paper — same problem (reduce scan cost while
preserving recall), different domain (Parquet files vs document posting lists).
This is a rare and genuine connection that positions you at the intersection
of IR research and modern lakehouse engineering.

**Repo owner:**
MonkeyDData (tagline: "Adventures on the high seas of data engineering")
Discovered via Dipankar Mazumdar (Director Data/AI @Cloudera, Apache
Iceberg + Hudi contributor, author of "Engineering Lakehouses").

---

## Output Files Reference

| File | Purpose | Location |
|------|---------|----------|
| `CLAUDE.md` | Claude Code project context | repo root |
| `ARCHITECTURE.md` | Full Mermaid diagram | repo root |
| `ARCHITECTURE_viewer.html` | Browser-viewable 5-flow diagrams | repo root |
| `LEARNING_NOTES.md` | Complete 5-flow walkthrough + bugs | repo root |
| `SESSIONS.md` | This file — session log + next steps | repo root |

---
