# Run Evidence

Environment note: Docker Desktop/Docker Engine was not available in the
sandbox this was built in (no access to Docker Hub). All commands below
were instead run against an equivalent local stack -- Python 3.12 venv,
a native PostgreSQL 16 server (same `sql/init/*.sql` schema), and a real
local Apache Airflow 2.10.5 instance (installed temporarily into the same
venv, `airflow dags test`, purely to validate the DAG) -- so every result
below is a real, executed run, not a prediction. Please re-verify the
`docker compose` commands on your machine per the README; the application
code itself (`src/`) does not know or care whether Postgres is reached via
`localhost` or the `postgres` Compose service name.

## Week 4 (Goal 1)

- Python version: `Python 3.12.3` (`python -m venv .venv` + `pip install -r requirements.txt`)
- Key package versions: pandas==2.2.3, pyarrow==17.0.0, psycopg==3.2.3, python-dotenv==1.0.1, PyYAML==6.0.2
- Git log evidence (see `git log --oneline --decorate`):
  ```
  d9932a3 feat(goal4): complete Airflow DAG with run_mode branching, retries, timeout, and failure callback
  3bd5464 feat(goal3): CSV/JSON Lines/Parquet/PostgreSQL benchmark comparison and Parquet partitioning
  697b6d1 test(goal2): add unit tests for dedupe, quarantine, curated join, and validation rules
  4ce6dc3 feat(goal2): rerun-safe PostgreSQL UPSERT keyed on order_id via record_hash
  1fb8a3c feat(goal2): implement staging/curated transformations and data validation
  6cccfaf feat(goal2): wire extract stage and thin CLI orchestration
  4af75bf merge: goal 1 reproducible environment
  d25bf92 feat: add reproducible environment scaffolding (.env.example, .gitignore)
  ```
- `.env` is not tracked: `git check-ignore .env` returns `.env` (confirmed ignored).
- Docker image/container evidence: not directly executable here -- see the
  environment note above. `Dockerfile` / `docker-compose.yml` were reviewed
  and are unmodified from the starter's working shape (they already build
  a `python:3.11-slim` image and install `requirements.txt`); the pipeline
  code has no Docker-specific dependency, so the venv run below is a valid
  proxy. Please run `docker compose build pipeline && docker compose up -d postgres && docker compose run --rm pipeline python -m src.cli validate-env` yourself and attach that output.
- External configuration evidence: `python -m src.cli validate-env` output:
  ```
  PROJECT_ROOT= /home/claude/dss150p-lab03-starter
  DB host/database= localhost dss150p
  Configured source= data/source
  ```
  No password appears in any committed `.py`/`.yml`/`.sql` file -- only in
  the git-ignored `.env` (`config/settings.yml` holds no secrets; `src/config.py`
  reads credentials from environment variables via `python-dotenv`).

## Week 5 (Goal 2)

One `python -m src.cli load` run against the real source data
(3,003 customer rows / 601 product rows / 50,005 order rows):

- Raw row counts (copied verbatim from `data/source/`): customers=3003, products=601, orders=50005
- Staging row counts: customers=3000, products=599, orders=49998
- Curated row count: 49897
- Quarantine row counts: staging_quarantine=3 (1 invalid product price, 1 invalid
  quantity, 1 disallowed status), curated_quarantine=101 (1 order referencing an
  unknown customer_id, 1 order referencing an unknown product_id, and 99 orders
  referencing product `P0078` -- the one product quarantined upstream for a
  negative unit_price, so its order lines have no valid price to curate)
- First load affected rows: `upsert_affected_rows=49897` (every row new)
- Second rerun affected rows: `upsert_affected_rows=0` -- confirmed idempotent.
  Verified directly in PostgreSQL:
  ```sql
  SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders
  FROM curated.sales_order_lines;
  -- total=49897, distinct_orders=49897 (no duplicates after two loads)
  ```

## Week 6 (Goal 3)

- Benchmark table attached: yes -- `data/benchmarks/benchmark_results.csv`
  (5 repeats, median timing, status filter = DELIVERED):

  | storage_type | file_size_bytes | write_s | full_read_s | filtered_read_s | row_count |
  |---|---|---|---|---|---|
  | csv | 15,097,146 | 0.820 | 0.211 | 0.238 | 49,897 |
  | json_lines | 30,839,534 | 0.647 | 0.561 | 0.272 | 49,897 |
  | parquet | 5,456,487 | 0.099 | 0.053 | 0.029 | 49,897 |
  | postgresql | 16,564,224 (pg_total_relation_size) | n/a (already loaded) | 0.334 | 0.056 | 49,897 |

  See `docs/benchmark_interpretation.md` for discussion.
- Partition selected: order_year=2026 / order_month=1 (and separately tested 2026/2)
- Partition row count: 2,506 rows for 2026-01
- PostgreSQL verification query and result:
  ```
  partition year=2026 month=1: rows_read=2506 rows_affected=0
  ```
  (0 affected because these rows were already present from the full `load`
  run moments earlier with an unchanged `record_hash` -- rerunning the same
  partition load a second time also returned `rows_affected=0`, confirmed
  via `SELECT * FROM audit.partition_loads WHERE partition_key='year=2026/month=1'`.)

## Week 7 (Goal 4)

- DAG ID: `dss150p_sales_pipeline`
- Schedule: `0 2 * * *` (daily 02:00 UTC), `catchup=False`
- Parameters used: `run_mode` (`full` | `partition`), `year`, `month`
- Successful full-mode run: `airflow dags test dss150p_sales_pipeline 2026-01-01 -c '{"run_mode":"full","year":2026,"month":1}'`
  -> `DagRun Finished ... state=success`; tasks executed:
  `extract -> transform -> choose_load_branch -> load_full -> load_done -> validate`
  (branch correctly skipped `load_partition`: log shows
  `Skipping tasks [('load_partition', -1)]`).
- Successful partition-mode run: `airflow dags test dss150p_sales_pipeline 2026-01-02 -c '{"run_mode":"partition","year":2026,"month":2}'`
  -> `state=success`; log shows `Skipping tasks [('load_full', -1)]`, confirming
  the opposite branch is honored.
- Deliberate failure run: `orders.csv` temporarily renamed out of `data/source/`,
  then `airflow dags test dss150p_sales_pipeline 2026-01-03 -c '{"run_mode":"full","year":2026,"month":1}'`.
  The `extract` task raised `FileNotFoundError` (from `extract_sources`), was
  retried twice per `retries: 2` (`Marking task as UP_FOR_RETRY` x2, ~1 minute
  apart per `retry_delay`), then `Marking task as FAILED` on the third attempt.
  `on_failure_callback` printed:
  `TASK FAILED: dag_id=dss150p_sales_pipeline task_id=extract run_id=manual__2026-01-03T00:00:00+00:00 try_number=3 ...`.
  Downstream tasks (`transform`, both load branches, `validate`) never ran.
- Recovery run: `orders.csv` restored, DAG re-run for the same date
  (`manual__2026-01-03T00:00:00+00:00`) -> `state=success`. PostgreSQL still
  shows `total=49897, distinct_orders=49897` afterward -- recovery required
  no manual database cleanup and created no duplicate business rows.
- It is safe to simply re-trigger the whole DAG run (rather than clearing
  only the failed task) after fixing the source problem: `extract` is a
  plain file copy with no side effects to undo, and every downstream step
  is UPSERT-based on `order_id`/`record_hash`, so re-running the full chain
  from `extract` is idempotent by construction.
