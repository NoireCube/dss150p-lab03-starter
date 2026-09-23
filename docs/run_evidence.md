# Run Evidence

Environment: Windows machine, Docker Desktop / Docker Engine version
29.7.2 (build a7dcaa6). All commands and DAG runs below were executed
against the real Docker Compose stack described in the README — a
containerized PostgreSQL 16 (`dss150p-postgres`, port 5432) and a
containerized Apache Airflow 2.10.5 instance (`dss150p-airflow-init`,
`dss150p-airflow-webserver`, `dss150p-airflow-scheduler`) — not a
substitute or native install. DAG runs were triggered through the
Airflow web UI at `http://localhost:8080`, and evidence below is drawn
from that UI (Grid view, Event Log, Run Details) plus real CLI output.
Full chronological screenshots are in `docs/run_evidence_screenshots.pdf`.

## Week 4 (Goal 1)

- Python version: `Python 3.12.10` — installed separately since the
  system default (3.14) has no prebuilt `pandas`/`pyarrow` wheels yet.
  venv at `.venv`, activated via `.venv\Scripts\Activate.ps1`.
- Import check: `python -c "import pandas, pyarrow, psycopg; print('all good')"`
  → `all good`
- `.env` is not tracked: confirmed via `.gitignore`; real
  `POSTGRES_PASSWORD` set, not the `change_me` placeholder.
- Docker evidence: `docker --version` → `Docker version 29.7.2, build a7dcaa6`.
  `docker compose up -d postgres` brought up container `dss150p-postgres`
  on port 5432 (after removing a stale leftover container
  `dss150p_lab_postgres` that was holding the port). Schema applied via
  `Get-Content sql\init\01_warehouse_schema.sql | docker exec -i dss150p-postgres psql -U dss150p -d dss150p`
  — confirmed clean with `SELECT COUNT(*) FROM curated.sales_order_lines;` → `0`.

## Week 5 (Goal 2)

`python -m src.cli extract` (run_id=`run_20260923T072401Z_9ce78de4`) then
`python -m src.cli transform` (run_id=`run_20260923T072402Z_ca734532`)
against the real source data:

- Staging row counts: customers=3000, products=599, orders=49998
- staging_quarantine rows=3
- curated rows=49897
- curated_quarantine rows=101

`python -m src.cli load` run twice:
- First run (run_id=`run_20260923T072500Z_3cf245d8`):
  `upsert_affected_rows=49897`
- Second run (run_id=`run_20260923T072516Z_09d8cb26`):
  `upsert_affected_rows=0` — confirmed idempotent.
  Verified directly in PostgreSQL:
```sql
  SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders
  FROM curated.sales_order_lines;
  -- total=49897, distinct_orders=49897 (no duplicates after two loads)
```
- `python -m pytest tests/ -v` → 6 passed in 0.54s (dedupe, quarantine,
  curated join, record_hash stability, validate rules)

## Week 6 (Goal 3)

`python -m src.cli benchmark --repeats 5` (5 repeats, median timing,
filtered read = status DELIVERED):

  | storage_type | file_size_bytes | write_s | full_read_s | filtered_read_s | row_count |
  |---|---|---|---|---|---|
  | csv | 14,947,456 | 0.6108 | 0.1603 | 0.2020 | 49,897 |
  | json_lines | 30,639,946 | 0.4897 | 0.4834 | 0.2005 | 49,897 |
  | parquet | 5,456,450 | 0.0988 | 0.0511 | 0.0263 | 49,897 |
  | postgresql | 16,564,224 (pg_total_relation_size) | n/a (already loaded) | 0.2875 | 0.0560 | 49,897 |

  Copied to `docs/benchmark_results.csv`. See
  `docs/benchmark_interpretation.md` for discussion.
- `python -m src.cli load-partition --year 2026 --month 1` run twice:
  both times `rows_read=2506 rows_affected=0` — idempotent (rows already
  present from the full load).

## Week 7 (Goal 4)

- DAG ID: `dss150p_sales_pipeline`, Airflow v2.10.5
  (`release:b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf`)
- Parameters: `run_mode` (`full` | `partition`), `year`, `month`
- Full-mode run: triggered via the UI with `run_mode=full, year=2026,
  month=1`. Triggered twice — Grid view confirms 2/2 total success
  (first run start 2026-09-23 07:43:34 UTC, last run start 07:44:48 UTC);
  `load_partition` correctly skipped both times.
- Partition-mode run: triggered with `run_mode=partition, year=2026,
  month=2` — Run ID `manual__2026-09-23T07:46:57+00:00`, status
  success, duration 00:00:54; `load_full` correctly skipped.
- (One trigger attempt hit a "CSRF token missing" Bad Request — session
  had sat open too long; re-triggering fresh resolved it.)
- Deliberate failure run: `orders.csv` temporarily renamed out of
  `data/source/`, DAG triggered via the UI with `run_mode=full, year=2026,
  month=1` (Run `2026-09-23, 07:52:07 UTC`, externally triggered=True).
  Event Log confirms the `extract` task's retry sequence:
  - try 1: running 07:52:13 UTC → failed 07:52:16 UTC
  - try 2: running 07:53:16 UTC → failed 07:53:19 UTC
  - try 3: running 07:54:20 UTC → failed 07:54:22 UTC

  (three tries total, matching `retries: 2`, ~1 minute apart per
  `retry_delay`). Run ended 07:54:27 UTC, total duration 00:02:13.
  Downstream tasks (`transform`, `load_full`, `load_partition`,
  `load_done`, `validate`) show `upstream_failed` in the Grid view and
  never ran.
- Recovery run: `orders.csv` restored, DAG re-triggered — Run ID
  `manual__2026-09-23T08:02:34+00:00`, status success, duration 00:00:28
  (started 08:02:39 UTC, ended 08:03:07 UTC). PostgreSQL still shows
  `total=49897, distinct_orders=49897` afterward — recovery required no
  manual database cleanup and created no duplicate rows.
- Because `extract` is a plain file copy with no side effects to undo,
  and every downstream step is UPSERT-based on `order_id`/`record_hash`,
  simply re-triggering the whole DAG run after fixing the source problem
  is safe and idempotent by construction.