"""Goal 2 -- raw -> staging transformations.

Staging is responsible for source-level technical cleanup and typing only.
No cross-source business logic (joins, monetary calculations) belongs here --
that is curated's job.
"""
from pathlib import Path
import json

import pandas as pd

from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso

ALLOWED_STATUSES = set(SETTINGS["quality"]["allowed_order_statuses"])
MIN_QTY = SETTINGS["quality"]["min_quantity"]
MAX_QTY = SETTINGS["quality"]["max_quantity"]


def _dedupe_latest(df: pd.DataFrame, key: str, updated_col: str) -> pd.DataFrame:
    """Keep the most recent version of each duplicate business key."""
    df = df.sort_values(updated_col)
    return df.drop_duplicates(subset=[key], keep="last").reset_index(drop=True)


def _quarantine_rows(df: pd.DataFrame, mask: pd.Series, reason: str, source_table: str,
                      run_id: str, staged_at: str) -> pd.DataFrame:
    """Build quarantine records for the rows selected by mask."""
    if not mask.any():
        return pd.DataFrame()
    bad = df.loc[mask].copy()
    bad["source_table"] = source_table
    bad["reason"] = reason
    bad["pipeline_run_id"] = run_id
    bad["staged_at_utc"] = staged_at
    # Keep the raw values readable by collapsing the row to a JSON string plus
    # the audit/reason columns, so heterogeneous schemas across source_table
    # can still live in one quarantine table.
    record_cols = [c for c in df.columns]
    bad["record"] = bad[record_cols].apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
    return bad[["source_table", "reason", "pipeline_run_id", "staged_at_utc", "record"]]


def _build_customers_staging(raw_dir: Path, run_id: str, staged_at: str):
    df = pd.read_csv(raw_dir / "customers.csv")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

    df = _dedupe_latest(df, "customer_id", "updated_at")

    # Normalize email: trim + lowercase. Missing email is retained (not an
    # error condition) but stays a visible NaN rather than a placeholder.
    df["email"] = df["email"].astype("string").str.strip().str.lower()
    df["email"] = df["email"].where(df["email"].notna() & (df["email"] != ""), pd.NA)

    # Normalize city: trim + title-case.
    df["city"] = df["city"].astype("string").str.strip().str.title()

    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = staged_at

    quarantine = pd.DataFrame()  # no hard rejection rules defined for customers
    return df, quarantine


def _build_products_staging(raw_dir: Path, run_id: str, staged_at: str):
    with (raw_dir / "products.json").open(encoding="utf-8") as f:
        records = json.load(f)
    df = pd.json_normalize(records)
    df = df.rename(columns={"category.name": "category", "category.department": "department"})
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df = _dedupe_latest(df, "product_id", "updated_at")

    invalid_mask = df["unit_price"].isna() | (df["unit_price"] < 0)
    quarantine = _quarantine_rows(df, invalid_mask, "invalid unit_price (negative or non-numeric)",
                                   "products", run_id, staged_at)
    clean = df.loc[~invalid_mask].copy()

    clean["pipeline_run_id"] = run_id
    clean["staged_at_utc"] = staged_at
    return clean, quarantine


def _build_orders_staging(raw_dir: Path, run_id: str, staged_at: str):
    df = pd.read_csv(raw_dir / "orders.csv")
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    df = _dedupe_latest(df, "order_id", "updated_at")

    reasons = pd.Series([""] * len(df), index=df.index, dtype="object")

    qty_bad = df["quantity"].isna() | (df["quantity"] < MIN_QTY) | (df["quantity"] > MAX_QTY)
    reasons.loc[qty_bad] += f"quantity outside [{MIN_QTY},{MAX_QTY}]; "

    status_bad = ~df["status"].isin(ALLOWED_STATUSES)
    reasons.loc[status_bad] += "status not in allowed list; "

    price_bad = df["unit_price"].isna() | (df["unit_price"] < 0)
    reasons.loc[price_bad] += "invalid unit_price; "

    invalid_mask = qty_bad | status_bad | price_bad
    df["_reason"] = reasons

    quarantine = pd.DataFrame()
    if invalid_mask.any():
        bad = df.loc[invalid_mask].copy()
        bad["source_table"] = "orders"
        bad["reason"] = bad["_reason"].str.rstrip("; ")
        bad["pipeline_run_id"] = run_id
        bad["staged_at_utc"] = staged_at
        record_cols = [c for c in df.columns if c not in ("_reason",)]
        bad["record"] = bad[record_cols].apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
        quarantine = bad[["source_table", "reason", "pipeline_run_id", "staged_at_utc", "record"]]

    clean = df.loc[~invalid_mask].drop(columns=["_reason"]).copy()
    clean["pipeline_run_id"] = run_id
    clean["staged_at_utc"] = staged_at
    return clean, quarantine


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Returns (staging: dict[str, DataFrame], quarantine: DataFrame).
    Also persists each staging dataset as Parquet under data/staging/ and the
    combined quarantine table under data/quarantine/, both partitioned by
    run_id, so downstream goals can reuse them without recomputation.
    """
    raw_dir = Path(raw_dir)
    staged_at = utc_now_iso()

    customers, q_customers = _build_customers_staging(raw_dir, run_id, staged_at)
    products, q_products = _build_products_staging(raw_dir, run_id, staged_at)
    orders, q_orders = _build_orders_staging(raw_dir, run_id, staged_at)

    quarantine = pd.concat([q_customers, q_products, q_orders], ignore_index=True)

    staging = {"customers": customers, "products": products, "orders": orders}

    staging_dir = path_for("staging_dir") / f"run_id={run_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in staging.items():
        frame.to_parquet(staging_dir / f"{name}.parquet", index=False)

    quarantine_dir = path_for("quarantine_dir") / f"run_id={run_id}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine.to_parquet(quarantine_dir / "staging_quarantine.parquet", index=False)

    return staging, quarantine
