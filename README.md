## Status: all four goals implemented and tested

Every TODO in `src/`, `src/cli.py`, and `dags/dss150p_pipeline.py` is
implemented. This was built and verified end-to-end on a real local
environment (Windows, Docker Desktop, Python 3.12 venv) against the
**actual source data** in `data/source/` (3,000 customer rows, 599
product rows, 49,998 order rows) — not a synthetic substitute — and run
against a real PostgreSQL 16 instance and a real Apache Airflow instance
via Docker Compose. See `docs/run_evidence.md` for full run evidence
(timestamps, run IDs). Highlights:

- `python -m src.cli load` loads 49,897 curated rows on first run; a
  second run affects **0 rows** (confirmed idempotent via `record_hash`).
- `python -m src.cli validate` passes with no data-quality issues.
- `python -m src.cli benchmark --repeats 5` produces
  `docs/benchmark_results.csv` and writes the year/month-partitioned
  Parquet dataset.
- `python -m src.cli load-partition --year 2026 --month 1` loads only
  that partition and is itself rerun-safe.
- The Airflow DAG `dss150p_sales_pipeline` was triggered via the UI for
  both `run_mode=full` and `run_mode=partition`, plus a
  deliberate-failure/retry/recovery scenario (source file removed, three
  failed tries matching `retries: 2`, all downstream tasks correctly
  `upstream_failed`, then restore and a fully green recovery run with no
  manual DB cleanup needed).

See `docs/run_evidence.md`, `docs/benchmark_interpretation.md`,
`docs/technical_questions.md`, `docs/technical_reflection.md`, and
`docs/data_dictionary.csv` for full write-ups and evidence.