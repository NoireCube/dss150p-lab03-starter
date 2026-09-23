"""Goal 3 -- storage format benchmarking and partitioning.

Compares the same logical curated dataset stored as CSV, JSON Lines, Parquet,
and PostgreSQL. All file-format reads/writes are repeated `repeats` times and
reported as a median so single-run noise does not skew the comparison.
PostgreSQL storage footprint is reported from pg_total_relation_size rather
than any single-file measurement, since it is a server-managed store.
"""
import json
import statistics
import time
from pathlib import Path

import pandas as pd

from src.config import DB, SETTINGS

FILTER_STATUS = SETTINGS["storage_benchmark"]["filter_status"]


def _median_timed(fn, repeats: int) -> float:
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def _bench_csv(df: pd.DataFrame, output_dir: Path, repeats: int) -> dict:
    path = output_dir / "sales_order_lines.csv"
    write_time = _median_timed(lambda: df.to_csv(path, index=False), repeats)
    full_read_time = _median_timed(lambda: pd.read_csv(path), repeats)

    def filtered_read():
        chunk_iter = pd.read_csv(path, chunksize=5000)
        return pd.concat([c[c["status"] == FILTER_STATUS] for c in chunk_iter], ignore_index=True)

    filtered_time = _median_timed(filtered_read, repeats)
    row_count = len(pd.read_csv(path))
    return {
        "storage_type": "csv", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_time, "full_read_seconds": full_read_time,
        "filtered_read_seconds": filtered_time, "row_count": row_count,
        "notes": "typed on read; no native schema/compression",
    }


def _bench_json_lines(df: pd.DataFrame, output_dir: Path, repeats: int) -> dict:
    path = output_dir / "sales_order_lines.jsonl"
    write_time = _median_timed(lambda: df.to_json(path, orient="records", lines=True, date_format="iso"), repeats)
    full_read_time = _median_timed(lambda: pd.read_json(path, lines=True), repeats)

    def filtered_read():
        rows = []
        with path.open(encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                if rec.get("status") == FILTER_STATUS:
                    rows.append(rec)
        return pd.DataFrame(rows)

    filtered_time = _median_timed(filtered_read, repeats)
    row_count = len(pd.read_json(path, lines=True))
    return {
        "storage_type": "json_lines", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_time, "full_read_seconds": full_read_time,
        "filtered_read_seconds": filtered_time, "row_count": row_count,
        "notes": "verbose (repeated keys); append/stream friendly",
    }


def _bench_parquet(df: pd.DataFrame, output_dir: Path, repeats: int) -> dict:
    path = output_dir / "sales_order_lines.parquet"
    write_time = _median_timed(lambda: df.to_parquet(path, index=False), repeats)
    full_read_time = _median_timed(lambda: pd.read_parquet(path), repeats)

    def filtered_read():
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
        table = pq.read_table(path, filters=[("status", "==", FILTER_STATUS)])
        return table.to_pandas()

    filtered_time = _median_timed(filtered_read, repeats)
    row_count = len(pd.read_parquet(path, columns=["order_id"]))
    return {
        "storage_type": "parquet", "file_size_bytes": path.stat().st_size,
        "write_seconds": write_time, "full_read_seconds": full_read_time,
        "filtered_read_seconds": filtered_time, "row_count": row_count,
        "notes": "columnar + compressed; predicate/column pushdown on read",
    }


def _bench_postgres(repeats: int) -> dict:
    import psycopg
    conn = psycopg.connect(
        host=DB["host"], port=DB["port"], dbname=DB["dbname"],
        user=DB["user"], password=DB["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines');")
            size_bytes = cur.fetchone()[0]

            def full_read():
                with conn.cursor() as c:
                    c.execute("SELECT * FROM curated.sales_order_lines;")
                    return c.fetchall()

            def filtered_read():
                with conn.cursor() as c:
                    c.execute("SELECT * FROM curated.sales_order_lines WHERE status = %s;", (FILTER_STATUS,))
                    return c.fetchall()

            full_time = _median_timed(full_read, repeats)
            filtered_time = _median_timed(filtered_read, repeats)
            cur.execute("SELECT COUNT(*) FROM curated.sales_order_lines;")
            row_count = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "storage_type": "postgresql", "file_size_bytes": size_bytes,
        "write_seconds": None, "full_read_seconds": full_time,
        "filtered_read_seconds": filtered_time, "row_count": row_count,
        "notes": "already loaded; write measured separately by load timing; "
                 "size is pg_total_relation_size (table+indexes+toast), not a single file",
    }


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare CSV, JSON Lines, Parquet, and PostgreSQL for the curated dataset.

    Returns a DataFrame matching templates/benchmark_results_template.csv and
    writes it to <output_dir>/benchmark_results.csv.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(curated_path)

    rows = [
        _bench_csv(df, output_dir, repeats),
        _bench_json_lines(df, output_dir, repeats),
        _bench_parquet(df, output_dir, repeats),
        _bench_postgres(repeats),
    ]
    results = pd.DataFrame(rows, columns=[
        "storage_type", "file_size_bytes", "write_seconds", "full_read_seconds",
        "filtered_read_seconds", "row_count", "notes",
    ])
    results.to_csv(output_dir / "benchmark_results.csv", index=False)
    return results


def write_partitioned_parquet(df: pd.DataFrame, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    output_dir = Path(output_dir)
    df = df.copy()
    df["order_year"] = df["order_timestamp"].dt.year
    df["order_month"] = df["order_timestamp"].dt.month
    df.to_parquet(output_dir, partition_cols=["order_year", "order_month"], index=False)
    return output_dir
