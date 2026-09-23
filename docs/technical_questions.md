# Technical Questions

1. **Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?**
   `record_hash` gives the UPSERT a cheap, deterministic way to tell "did the
   business content of this row actually change?" without comparing every
   column individually in SQL. The load does
   `... ON CONFLICT (order_id) DO UPDATE ... WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash`,
   so a rerun over unchanged source data writes zero rows instead of
   rewriting the whole table every time. It must exclude any column that
   changes purely because the pipeline ran again rather than because the
   business facts changed -- `pipeline_run_id` and `processed_at_utc` here.
   If those were included, every rerun would produce a different hash for
   identical data and the WHERE clause would never skip anything, defeating
   the whole point.

2. **Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?**
   Staging and curated apply lossy, opinionated transformations (dedup rules,
   type coercion, quarantine, joins) based on the *current* understanding of
   the business rules. If those rules turn out to be wrong, or need to change
   (e.g. discovering the dedup should prefer `created_at` in some edge case,
   or that a quarantine threshold was too strict), the only way to reprocess
   correctly is to still have the untouched original bytes. Raw also serves
   as the audit trail proving what the source system actually sent on a
   given run, independent of any bug introduced later in staging/curated
   code.

3. **What is the difference between a data-quality rejection and a system exception?**
   A data-quality rejection is an *expected*, individually-scoped condition
   in the data itself -- a quantity out of range, a disallowed status, an
   orphan foreign key -- and the correct response is to route that one row
   to quarantine with a reason and continue processing the rest (this repo's
   `_quarantine_rows` / the orphan-handling in `build_curated`). A system
   exception is an *unexpected* failure of the pipeline or its environment --
   a missing source file, a dropped database connection, a malformed file
   that cannot even be parsed -- and the correct response is to fail the run
   loudly (raise, let Airflow retry/alert) rather than silently continuing,
   because there is no safe per-row action to take and continuing could mask
   a much bigger problem.

4. **Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?**
   Parquet is columnar and typed, so a query that only needs a few columns
   (or wants to filter on one) can skip reading and parsing the others
   entirely, and its per-row-group statistics let a reader skip whole
   chunks of the file that cannot match a filter (predicate pushdown, see
   `docs/benchmark_interpretation.md` Q3). CSV is row-oriented, untyped text:
   every read must scan and re-parse every byte of every row regardless of
   which columns or filters are actually needed. That difference showed up
   directly in this benchmark: Parquet's filtered read (0.029s) was roughly
   8x faster than CSV's (0.238s) on identical data.

5. **Why is a DAG that contains all transformation logic directly considered harder to maintain?**
   It couples orchestration concerns (scheduling, retries, dependency
   ordering, parameters) to business logic (how to clean a field, how to
   join tables) inside one framework-specific file, so the transformation
   logic can only be tested by running Airflow, cannot be reused outside a
   DAG run (e.g. from a notebook or a unit test), and every small business
   rule change requires touching -- and risking -- the same file that
   controls production scheduling. This repo keeps the DAG as thin
   `BashOperator` calls into `src.cli`, which itself only wires together
   pure functions in `src/transform`, `src/load`, etc.; those functions are
   unit-tested directly in `tests/test_transform_rules.py` with no Airflow
   involved at all.

6. **How do retries interact with idempotency? Give an example where retries without idempotency cause damage.**
   Airflow's retry mechanism re-executes a *task*, not a fine-grained
   operation, so if that task is not idempotent, a retry after a partial
   success re-does the already-completed part too. Example: if `load` used
   plain `INSERT` instead of `INSERT ... ON CONFLICT`, and the task
   succeeded in writing all 49,897 rows to PostgreSQL but then failed
   afterward (e.g. the process was killed before Airflow recorded success,
   or a downstream step in the same task failed), a retry would attempt to
   `INSERT` the same 49,897 `order_id` values again -- either erroring on
   the primary key, or, if the key weren't enforced, silently duplicating
   every row. Because this pipeline's load is `ON CONFLICT (order_id) DO
   UPDATE ... WHERE record_hash IS DISTINCT`, a retry after partial or full
   success is safe: unchanged rows are simply skipped again.

7. **What trade-off is introduced by partitioning too aggressively?**
   As covered in `docs/benchmark_interpretation.md` Q5: too many, too-small
   partitions trade query-time pruning benefit for metadata and small-file
   overhead (more file-open/footer-read costs, more directory listing work)
   that can outweigh the savings, especially if the partition key does not
   match actual query filters. There is also a write-side cost: a highly
   granular partition key means each incoming batch scatters across many
   partition directories instead of a few, so a single load touches many
   small files instead of appending efficiently to a few larger ones.

8. **How would you adapt the pipeline if the source became an API or database instead of local files?**
   Only `src/extract/` would need to change -- `extract_sources(run_id)`
   would call the API/DB instead of `shutil.copy2`, but it would still need
   to return/materialize a run-specific, immutable raw snapshot (e.g. write
   the API response or a DB query result to `data/raw/run_id=<run_id>/` in
   the same shape) so every downstream stage (`staging`, `curated`, `load`,
   `validate`, `benchmark`) keeps working unmodified. The main new concerns
   would be: (a) incremental extraction -- tracking a watermark (last
   `updated_at` seen, or an API cursor/page token) so each run only pulls
   new/changed records instead of the full dataset every time; (b) handling
   pagination and rate limits/retries at the API boundary specifically,
   separate from the pipeline-level Airflow retries; and (c) credentials
   for the API/DB would join `POSTGRES_*` in `.env`/`src/config.py` rather
   than a bare file path. Staging's dedup-by-`updated_at` logic already
   generalizes naturally to an incremental source, since it is designed to
   collapse multiple versions of the same business key regardless of how
   many extraction runs contributed them.
