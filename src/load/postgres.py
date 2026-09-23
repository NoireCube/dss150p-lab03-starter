"""Goal 2/3 -- rerun-safe PostgreSQL loading.

Both functions load into curated.sales_order_lines using order_id as the
conflict key. A COPY into a temporary staging table followed by a single
INSERT .. ON CONFLICT keeps the load fast for tens of thousands of rows and
keeps the UPSERT logic in one place. record_hash is used to skip rewriting
rows whose business content has not changed, so a rerun with unchanged
records does not touch storage or create duplicate business keys.
"""
import io

import psycopg

from src.config import DB
from src.common.audit import utc_now_iso

CURATED_COLUMNS = [
    "order_id", "customer_id", "product_id", "order_timestamp",
    "customer_city", "customer_tier", "product_name", "category", "brand",
    "quantity", "unit_price", "discount_pct",
    "gross_amount", "discount_amount", "net_amount", "status",
    "source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash",
]

UPSERT_SQL = f"""
INSERT INTO curated.sales_order_lines ({", ".join(CURATED_COLUMNS)})
SELECT {", ".join(CURATED_COLUMNS)} FROM tmp_curated_load
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    order_timestamp = EXCLUDED.order_timestamp,
    customer_city = EXCLUDED.customer_city,
    customer_tier = EXCLUDED.customer_tier,
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    brand = EXCLUDED.brand,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount_pct = EXCLUDED.discount_pct,
    gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_amount = EXCLUDED.net_amount,
    status = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    pipeline_run_id = EXCLUDED.pipeline_run_id,
    processed_at_utc = EXCLUDED.processed_at_utc,
    record_hash = EXCLUDED.record_hash
WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
"""


def _connect():
    return psycopg.connect(
        host=DB["host"], port=DB["port"], dbname=DB["dbname"],
        user=DB["user"], password=DB["password"],
    )


def _copy_and_upsert(cur, df) -> int:
    cur.execute(f"""
        CREATE TEMP TABLE tmp_curated_load (LIKE curated.sales_order_lines INCLUDING DEFAULTS)
        ON COMMIT DROP;
    """)
    buf = io.StringIO()
    df[CURATED_COLUMNS].to_csv(buf, index=False, header=False)
    buf.seek(0)
    with cur.copy(
        f"COPY tmp_curated_load ({', '.join(CURATED_COLUMNS)}) FROM STDIN WITH (FORMAT csv)"
    ) as copy:
        copy.write(buf.read())
    cur.execute(UPSERT_SQL)
    return cur.rowcount


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    order_id is the conflict key. Rows whose record_hash is unchanged are
    skipped by the WHERE clause in UPSERT_SQL, so a rerun with identical
    business content affects zero rows instead of rewriting everything.
    """
    if df.empty:
        return 0
    with _connect() as conn:
        with conn.cursor() as cur:
            affected = _copy_and_upsert(cur, df)
        conn.commit()
    return affected


def record_pipeline_run(run_id: str, started_at_utc: str, status: str,
                         rows_staging=None, rows_curated=None, rows_quarantined=None,
                         message: str = "") -> None:
    """Upsert one row of audit.pipeline_runs (idempotent per run_id)."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs
                    (pipeline_run_id, started_at_utc, completed_at_utc, status,
                     rows_staging, rows_curated, rows_quarantined, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id) DO UPDATE SET
                    completed_at_utc = EXCLUDED.completed_at_utc,
                    status = EXCLUDED.status,
                    rows_staging = COALESCE(EXCLUDED.rows_staging, audit.pipeline_runs.rows_staging),
                    rows_curated = COALESCE(EXCLUDED.rows_curated, audit.pipeline_runs.rows_curated),
                    rows_quarantined = COALESCE(EXCLUDED.rows_quarantined, audit.pipeline_runs.rows_quarantined),
                    message = EXCLUDED.message;
                """,
                (run_id, started_at_utc, utc_now_iso(), status,
                 rows_staging, rows_curated, rows_quarantined, message),
            )
        conn.commit()


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    ts = df["order_timestamp"]
    mask = (ts.dt.year == year) & (ts.dt.month == month)
    partition_df = df.loc[mask]
    partition_key = f"year={year}/month={month}"

    if partition_df.empty:
        return 0

    with _connect() as conn:
        with conn.cursor() as cur:
            affected = _copy_and_upsert(cur, partition_df)
            cur.execute(
                """
                INSERT INTO audit.partition_loads
                    (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (partition_key) DO UPDATE SET
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id;
                """,
                (partition_key, utc_now_iso(), len(partition_df), run_id),
            )
        conn.commit()
    return affected
