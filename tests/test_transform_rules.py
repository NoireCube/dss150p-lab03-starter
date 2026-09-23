"""Unit tests for the staging/curated/validate transformation rules.

These tests build small in-memory fixtures instead of depending on the
50k-row source dataset, so they run fast and pin down exact expected
behavior for each rule.
"""
import json
from pathlib import Path

import pandas as pd
import pytest

from src.transform.staging import _dedupe_latest, _build_orders_staging, _build_customers_staging
from src.transform.curated import build_curated
from src.validate.quality import validate_curated
from src.common.audit import record_hash


def _write_source_fixture(tmp_path: Path):
    """Write a tiny customers/products/orders fixture that exercises every
    documented staging rule: a duplicate business key, a dirty email/city,
    an invalid quantity, a disallowed status, and an orphan reference."""
    customers_csv = (
        "customer_id,first_name,last_name,email,city,customer_tier,created_at,updated_at\n"
        "C001,Ana,Cruz,  ANA.CRUZ@EXAMPLE.COM  ,  manila  ,Gold,2023-01-01T00:00:00+00:00,2024-01-01T00:00:00+00:00\n"
        "C001,Ana,Cruz,ana.cruz@example.com,Manila,Gold,2023-01-01T00:00:00+00:00,2025-06-01T00:00:00+00:00\n"
        "C002,Bea,Reyes,,Pasig,Silver,2023-01-01T00:00:00+00:00,2024-01-01T00:00:00+00:00\n"
    )
    (tmp_path / "customers.csv").write_text(customers_csv, encoding="utf-8")

    orders_csv = (
        "order_id,customer_id,product_id,order_timestamp,quantity,unit_price,discount_pct,status,updated_at\n"
        "O001,C001,P001,2026-01-05T00:00:00+00:00,2,100.0,0.1,DELIVERED,2026-01-05T01:00:00+00:00\n"
        "O002,C001,P001,2026-01-06T00:00:00+00:00,0,100.0,0,PENDING,2026-01-06T01:00:00+00:00\n"
        "O003,C001,P001,2026-01-07T00:00:00+00:00,3,100.0,0,GHOSTED,2026-01-07T01:00:00+00:00\n"
        "O004,C999,P001,2026-01-08T00:00:00+00:00,1,100.0,0,PAID,2026-01-08T01:00:00+00:00\n"
    )
    (tmp_path / "orders.csv").write_text(orders_csv, encoding="utf-8")

    products = [
        {"product_id": "P001", "name": "Widget", "category": {"name": "Tools", "department": "Hardware"},
         "brand": "Acme", "unit_price": 100.0, "active": True, "updated_at": "2026-01-01T00:00:00+00:00"},
    ]
    (tmp_path / "products.json").write_text(json.dumps(products), encoding="utf-8")
    return tmp_path


def test_dedupe_latest_keeps_greatest_updated_at():
    df = pd.DataFrame({
        "key": ["A", "A", "B"],
        "updated_at": pd.to_datetime(["2024-01-01", "2025-01-01", "2024-06-01"], utc=True),
        "value": [1, 2, 3],
    })
    out = _dedupe_latest(df, "key", "updated_at")
    assert len(out) == 2
    assert out.loc[out["key"] == "A", "value"].iloc[0] == 2


def test_customer_staging_normalizes_email_and_city_and_dedupes(tmp_path):
    _write_source_fixture(tmp_path)
    clean, quarantine = _build_customers_staging(tmp_path, "run_test", "2026-01-01T00:00:00+00:00")
    assert len(clean) == 2  # C001 duplicate collapsed to 1, plus C002
    row = clean.loc[clean["customer_id"] == "C001"].iloc[0]
    assert row["email"] == "ana.cruz@example.com"
    assert row["city"] == "Manila"
    # missing email is retained as null, not dropped or quarantined
    assert clean.loc[clean["customer_id"] == "C002", "email"].isna().all()


def test_order_staging_quarantines_bad_quantity_and_status(tmp_path):
    _write_source_fixture(tmp_path)
    clean, quarantine = _build_orders_staging(tmp_path, "run_test", "2026-01-01T00:00:00+00:00")
    assert set(clean["order_id"]) == {"O001", "O004"}
    reasons = dict(zip(quarantine["reason"], quarantine["reason"]))
    assert any("quantity" in r for r in quarantine["reason"])
    assert any("status" in r for r in quarantine["reason"])


def test_curated_quarantines_orphan_customer_without_dropping_silently(tmp_path):
    _write_source_fixture(tmp_path)
    customers, _ = _build_customers_staging(tmp_path, "run_test", "2026-01-01T00:00:00+00:00")
    orders, _ = _build_orders_staging(tmp_path, "run_test", "2026-01-01T00:00:00+00:00")
    products = pd.DataFrame([{
        "product_id": "P001", "name": "Widget", "category": "Tools", "department": "Hardware",
        "brand": "Acme", "unit_price": 100.0, "active": True,
        "updated_at": pd.Timestamp("2026-01-01", tz="UTC"),
        "pipeline_run_id": "run_test", "staged_at_utc": "2026-01-01T00:00:00+00:00",
    }])
    staging = {"customers": customers, "products": products, "orders": orders}
    curated, quarantine = build_curated(staging, "run_test")

    # O004 references C999 which does not exist -> must be quarantined, not dropped
    assert "O004" not in set(curated["order_id"])
    assert len(quarantine) == 1
    assert "C999" in quarantine.iloc[0]["reason"]
    # O001 is the only fully valid order left
    assert list(curated["order_id"]) == ["O001"]
    row = curated.iloc[0]
    assert row["gross_amount"] == 200.0
    assert row["discount_amount"] == 20.0
    assert row["net_amount"] == 180.0


def test_record_hash_is_stable_across_pipeline_runs():
    business_fields = ["order_id", "quantity"]
    row_a = {"order_id": "O001", "quantity": 2, "pipeline_run_id": "run_1"}
    row_b = {"order_id": "O001", "quantity": 2, "pipeline_run_id": "run_2"}
    assert record_hash(row_a, business_fields) == record_hash(row_b, business_fields)


def test_validate_curated_flags_duplicate_and_out_of_range_rows():
    df = pd.DataFrame({
        "order_id": ["O1", "O1", "O2"],
        "quantity": [2, 2, 999],
        "gross_amount": [10.0, 10.0, 10.0],
        "discount_amount": [0.0, 0.0, -5.0],
        "net_amount": [10.0, 10.0, 15.0],
        "status": ["DELIVERED", "DELIVERED", "GHOSTED"],
        "pipeline_run_id": ["r1", "r1", "r1"],
        "processed_at_utc": ["t", "t", "t"],
        "record_hash": ["h1", "h1", "h2"],
    })
    errors = validate_curated(df)
    assert any("duplicate order_id" in e for e in errors)
    assert any("quantity" in e for e in errors)
    assert any("negative discount_amount" in e for e in errors)
    assert any("status" in e for e in errors)
