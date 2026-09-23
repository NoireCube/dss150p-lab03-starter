# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

## Status: all four goals implemented and tested

Every TODO in `src/`, `src/cli.py`, and `dags/dss150p_pipeline.py` is
implemented. This was built and verified against the **actual source data
in `data/source/`** (3,003 customer rows, 601 product rows, 50,005 order
rows) -- not a synthetic substitute -- and run end-to-end against a real
PostgreSQL 16 instance and a real Apache Airflow 2.10.5 instance (Docker
was unavailable in the build sandbox; see `docs/run_evidence.md` for that
caveat and full evidence). Highlights:

- `python -m src.cli load` loads 49,897 curated rows on first run; a second
  run affects **0 rows** (confirmed idempotent via `record_hash`).
- `python -m src.cli validate` passes with no data-quality issues.
- `python -m src.cli benchmark --repeats 5` produces
  `data/benchmarks/benchmark_results.csv` (copied to `docs/benchmark_results.csv`
  as committed evidence) and writes the year/month-partitioned Parquet dataset.
- `python -m src.cli load-partition --year 2026 --month 1` loads only that
  partition (2,506 rows) and is itself rerun-safe.
- The Airflow DAG was run via `airflow dags test` for both `run_mode=full`
  and `run_mode=partition`, plus a deliberate-failure/retry/recovery
  scenario (source file removed, two retries, failure, restore, clean
  recovery with no duplicate rows).

See `docs/run_evidence.md`, `docs/benchmark_interpretation.md`,
`docs/technical_questions.md`, `docs/technical_reflection.md`, and
`docs/data_dictionary.csv` for full write-ups and evidence.

### Verifying the Docker/Compose path yourself

The application code has no Docker-specific dependency (it only reads
`POSTGRES_*` from the environment via `src/config.py`), so the venv-based
evidence above is a faithful proxy for `docker compose run --rm pipeline ...`.
Please still run the Docker path yourself before submitting, since it
wasn't executable in the environment this was built in:

```bash
docker compose build pipeline
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
docker compose run --rm pipeline python -m src.cli load
docker compose run --rm pipeline python -m src.cli validate
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

