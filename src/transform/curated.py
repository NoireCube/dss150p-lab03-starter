"""Goal 2 -- curated transformation.

Curated applies cross-source business logic: joins staging orders to
customers and products, computes monetary measures, and produces the
consumer-oriented sales_order_lines rows. Orphan references are quarantined
with a reason, never silently dropped.
"""
import json

import pandas as pd

from src.config import path_for
from src.common.audit import utc_now_iso, record_hash

CURATED_HASH_KEYS = [
    "order_id", "customer_id", "product_id", "order_timestamp",
    "quantity", "unit_price", "discount_pct", "status",
    "customer_city", "customer_tier", "product_name", "category", "brand",
    "gross_amount", "discount_amount", "net_amount", "source_updated_at",
]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products into analysis-ready sales rows.

    Returns (curated: DataFrame, quarantine: DataFrame).
    """
    orders = staging["orders"]
    customers = staging["customers"]
    products = staging["products"]
    processed_at = utc_now_iso()

    known_customers = set(customers["customer_id"])
    known_products = set(products["product_id"])

    orphan_customer = ~orders["customer_id"].isin(known_customers)
    orphan_product = ~orders["product_id"].isin(known_products)
    orphan_mask = orphan_customer | orphan_product

    quarantine_rows = []
    if orphan_mask.any():
        bad = orders.loc[orphan_mask].copy()
        reasons = []
        for _, row in bad.iterrows():
            parts = []
            if row["customer_id"] not in known_customers:
                parts.append(f"customer_id {row['customer_id']} not in staging.customers "
                              f"(missing or removed upstream)")
            if row["product_id"] not in known_products:
                parts.append(f"product_id {row['product_id']} not in staging.products "
                              f"(missing, or quarantined upstream for an invalid price)")
            reasons.append("; ".join(parts))
        bad["reason"] = reasons
        bad["source_table"] = "curated_join"
        bad["pipeline_run_id"] = run_id
        bad["staged_at_utc"] = processed_at
        record_cols = [c for c in orders.columns]
        bad["record"] = bad[record_cols].apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
        quarantine_rows.append(bad[["source_table", "reason", "pipeline_run_id", "staged_at_utc", "record"]])

    valid_orders = orders.loc[~orphan_mask].copy()

    merged = valid_orders.merge(
        customers[["customer_id", "city", "customer_tier"]].rename(columns={"city": "customer_city"}),
        on="customer_id", how="left",
    ).merge(
        products[["product_id", "name", "category", "brand"]].rename(columns={"name": "product_name"}),
        on="product_id", how="left",
    )

    merged["source_updated_at"] = merged["updated_at"]
    merged["gross_amount"] = (merged["quantity"] * merged["unit_price"]).round(2)
    merged["discount_amount"] = (merged["gross_amount"] * merged["discount_pct"]).round(2)
    merged["net_amount"] = (merged["gross_amount"] - merged["discount_amount"]).round(2)

    merged["pipeline_run_id"] = run_id
    merged["processed_at_utc"] = processed_at
    merged["record_hash"] = merged.apply(
        lambda r: record_hash(r.to_dict(), CURATED_HASH_KEYS), axis=1
    )

    curated_cols = [
        "order_id", "customer_id", "product_id", "order_timestamp",
        "customer_city", "customer_tier", "product_name", "category", "brand",
        "quantity", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "net_amount", "status",
        "source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash",
    ]
    curated = merged[curated_cols].reset_index(drop=True)

    quarantine = pd.concat(quarantine_rows, ignore_index=True) if quarantine_rows else pd.DataFrame(
        columns=["source_table", "reason", "pipeline_run_id", "staged_at_utc", "record"]
    )

    curated_dir = path_for("curated_dir") / f"run_id={run_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / "sales_order_lines.parquet", index=False)

    if len(quarantine):
        quarantine_dir = path_for("quarantine_dir") / f"run_id={run_id}"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        existing_path = quarantine_dir / "curated_quarantine.parquet"
        quarantine.to_parquet(existing_path, index=False)

    return curated, quarantine
