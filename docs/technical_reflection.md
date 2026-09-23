# Technical Reflection

## Partition run, deliberate failure/retries, recovery run

See `docs/run_evidence.md` (Week 7 section) for the full evidence trail:
a successful `run_mode=full` DAG run, a successful `run_mode=partition` run
that correctly skipped the unused branch, a deliberate failure (source file
temporarily removed) that retried twice per `retries: 2` and then failed
with a clear `on_failure_callback` message, and a clean recovery run on the
same DAG run_id after the source was restored, with PostgreSQL confirmed at
`total=49897, distinct_orders=49897` both before and after -- no manual
cleanup, no duplicate business rows.

## Modularity

Business logic lives only in `src/extract`, `src/transform`, `src/load`,
`src/validate`, and `src/benchmark`; `src/cli.py` and `dags/dss150p_pipeline.py`
only call those functions in sequence. This means the same transformation
code is exercised identically whether it's invoked from a terminal, from
Airflow's `BashOperator`, or directly from `pytest` (see
`tests/test_transform_rules.py`, which imports `src.transform.*` functions
directly with no CLI or Airflow involved). It also means a rule change
(e.g. a new quarantine condition) touches exactly one file and is covered
by a unit test, rather than requiring a change to orchestration code.

## Idempotency

Every write path in this pipeline is safe to re-run: `extract_sources`
overwrites the same run-specific directory with `shutil.copy2` rather than
appending; `build_staging`/`build_curated` are pure functions of their
input plus `run_id`; and `upsert_curated`/`load_partition` use
`INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE record_hash IS
DISTINCT FROM EXCLUDED.record_hash`, so re-running the full pipeline (or
just a partition load) against unchanged source data affects zero rows,
confirmed directly (Week 5 and Week 6 evidence). `record_hash` deliberately
excludes `pipeline_run_id`/`processed_at_utc` so the hash reflects only
business content, not when or how many times the pipeline happened to run.

## Storage trade-offs

See `docs/benchmark_interpretation.md` for the full discussion: Parquet won
on every dimension measured (size, write, full read, filtered read) for
this batch-analytical workload because it is columnar, typed, and
compressed with predicate pushdown; CSV is the simplest and most portable
but the least efficient; JSON Lines is the most verbose on disk but the
easiest format to append to or stream one record at a time; PostgreSQL adds
query flexibility (arbitrary `WHERE`/`JOIN`/aggregation, concurrent access,
constraints) at the cost of needing a running server and, without a
supporting index, a full sequential scan for a filtered read.

## Orchestration vs. business logic

The DAG (`dags/dss150p_pipeline.py`) only expresses *when* and *in what
order* things run -- `extract >> transform >> branch >> [load_full |
load_partition] >> load_done >> validate`, a daily schedule, `retries=2`,
a 15-minute `execution_timeout`, and a `run_mode` branch -- never *how* a
row is cleaned, joined, or upserted. That separation is what let the same
transformation code be validated by `pytest` in milliseconds and by real
`airflow dags test` runs against real PostgreSQL, without either test path
needing to know about the other.

## Backfill reasoning (10.6 optional challenge)

This DAG intentionally runs with `catchup=False`. The pipeline is not
consuming a partitioned, append-only event stream where each scheduled run
owns a distinct slice of history (a specific `data_interval_start/end`) --
it recomputes staging/curated from whatever is currently in
`data/source/`, a full snapshot, on every run. If catchup were enabled and
the scheduler had missed, say, five daily runs, it would trigger five
DAG runs back-to-back, but all five would extract and process the exact
same current source snapshot -- there is no way for an older scheduled run
to see the source data "as it was" on that day. That would be pure wasted
compute (five redundant full pipeline runs), not real backfill, and would
briefly increase concurrent load on the shared PostgreSQL instance for no
analytical benefit. Idempotency (`record_hash`-gated UPSERT) means it would
still be *safe* to backfill -- no duplicate rows would result -- but safe
is not the same as useful. If this pipeline's source ever became a true
incremental/watermarked feed (see `docs/technical_questions.md` Q8), each
`data_interval` would then correspond to a genuine slice of new data, and
enabling catchup with an explicit `airflow dags backfill` for the missed
date range would become the correct way to reprocess a specific historical
window without double-counting, since the watermark-scoped extraction would
pull only that window's records regardless of when the run actually
executes.
