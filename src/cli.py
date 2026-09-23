"""DSS150P modular pipeline CLI.

Thin orchestration only -- every command wires together functions from
src.extract / src.transform / src.load / src.validate / src.benchmark. No
business logic lives here.
"""
import argparse
import sys

import pandas as pd

from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id, utc_now_iso
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.validate.quality import validate_curated
from src.load.postgres import upsert_curated, load_partition, record_pipeline_run
from src.benchmark.storage import run_benchmark, write_partitioned_parquet


def _run_pipeline_to_curated(run_id: str):
    """extract -> staging -> curated. Returns (curated_df, staging, s_q, c_q)."""
    raw_dir = extract_sources(run_id)
    staging, staging_quarantine = build_staging(raw_dir, run_id)
    curated, curated_quarantine = build_curated(staging, run_id)
    return curated, staging, staging_quarantine, curated_quarantine


def cmd_extract(run_id: str):
    raw_dir = extract_sources(run_id)
    print(f"run_id={run_id}")
    print(f"raw_dir={raw_dir}")
    for f in sorted(raw_dir.iterdir()):
        print(f"  copied {f.name}")


def cmd_transform(run_id: str):
    started = utc_now_iso()
    curated, staging, s_q, c_q = _run_pipeline_to_curated(run_id)
    rows_staging = sum(len(v) for v in staging.values())
    rows_quarantined = len(s_q) + len(c_q)
    print(f"run_id={run_id}")
    for name, frame in staging.items():
        print(f"  staging.{name} rows={len(frame)}")
    print(f"  staging_quarantine rows={len(s_q)}")
    print(f"  curated rows={len(curated)}")
    print(f"  curated_quarantine rows={len(c_q)}")
    return curated, rows_staging, rows_quarantined, started


def cmd_load(run_id: str):
    started = utc_now_iso()
    curated, rows_staging, rows_quarantined, started = cmd_transform(run_id)
    affected = upsert_curated(curated, run_id)
    record_pipeline_run(run_id, started, "load_ok",
                         rows_staging=rows_staging, rows_curated=len(curated),
                         rows_quarantined=rows_quarantined,
                         message=f"upsert affected {affected} row(s)")
    print(f"  upsert_affected_rows={affected}")
    return curated


def cmd_validate(run_id: str):
    curated = cmd_load(run_id)
    errors = validate_curated(curated)
    if errors:
        print(f"VALIDATION FAILED ({len(errors)} issue(s)):")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    print("VALIDATION PASSED: no data quality issues found.")


def cmd_benchmark(run_id: str, repeats: int):
    curated, *_ = _run_pipeline_to_curated(run_id)
    curated_dir = path_for("curated_dir") / f"run_id={run_id}"
    curated_path = curated_dir / "sales_order_lines.parquet"
    benchmark_dir = path_for("benchmark_dir")

    results = run_benchmark(curated_path, benchmark_dir, repeats=repeats)
    print(results.to_string(index=False))

    partition_dir = path_for("partition_dir")
    write_partitioned_parquet(curated, partition_dir)
    print(f"partitioned dataset written to {partition_dir}")


def cmd_load_partition(year: int, month: int, run_id: str):
    partition_dir = path_for("partition_dir")
    if not partition_dir.exists():
        raise SystemExit(
            "No partitioned dataset found. Run `python -m src.cli benchmark` first "
            "to materialize data/partitioned/."
        )
    df = pd.read_parquet(
        partition_dir,
        filters=[("order_year", "==", year), ("order_month", "==", month)],
    )
    if df.empty:
        print(f"No rows found for order_year={year}, order_month={month}.")
        return
    if "order_timestamp" not in df.columns:
        raise RuntimeError("order_timestamp missing from partitioned dataset")

    bad = df[(df["order_timestamp"].dt.year != year) | (df["order_timestamp"].dt.month != month)]
    if len(bad):
        raise RuntimeError(f"Partition pruning returned {len(bad)} row(s) outside year={year}/month={month}")

    affected = load_partition(df, year, month, run_id)
    print(f"partition year={year} month={month}: rows_read={len(df)} rows_affected={affected}")


def cmd_run_all(run_id: str):
    cmd_validate(run_id)


def main():
    parser = argparse.ArgumentParser(description="DSS150P modular pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate-env")
    sub.add_parser("extract")
    sub.add_parser("transform")
    sub.add_parser("load")
    sub.add_parser("validate")
    b = sub.add_parser("benchmark")
    b.add_argument("--repeats", type=int, default=SETTINGS["storage_benchmark"]["repeats"])
    p = sub.add_parser("load-partition")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    sub.add_parser("run-all")
    args = parser.parse_args()

    if args.command == "validate-env":
        print("PROJECT_ROOT=", PROJECT_ROOT)
        print("DB host/database=", DB["host"], DB["dbname"])
        print("Configured source=", SETTINGS["pipeline"]["source_dir"])
        return

    run_id = new_run_id()

    if args.command == "extract":
        cmd_extract(run_id)
    elif args.command == "transform":
        cmd_transform(run_id)
    elif args.command == "load":
        cmd_load(run_id)
    elif args.command == "validate":
        cmd_validate(run_id)
    elif args.command == "benchmark":
        cmd_benchmark(run_id, args.repeats)
    elif args.command == "load-partition":
        cmd_load_partition(args.year, args.month, run_id)
    elif args.command == "run-all":
        cmd_run_all(run_id)


if __name__ == "__main__":
    main()
