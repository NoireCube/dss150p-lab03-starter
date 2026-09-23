"""Data-quality validation for the curated sales_order_lines dataset."""
from src.config import SETTINGS

ALLOWED_STATUSES = set(SETTINGS["quality"]["allowed_order_statuses"])
MIN_QTY = SETTINGS["quality"]["min_quantity"]
MAX_QTY = SETTINGS["quality"]["max_quantity"]

REQUIRED_AUDIT_FIELDS = ["pipeline_run_id", "processed_at_utc", "record_hash"]


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors (empty = passing)."""
    errors: list[str] = []

    if df["order_id"].isna().any():
        errors.append(f"{df['order_id'].isna().sum()} row(s) have a null order_id")

    dup_count = df["order_id"].duplicated().sum()
    if dup_count:
        errors.append(f"{dup_count} duplicate order_id value(s) found in curated output")

    qty_bad = df["quantity"].isna() | (df["quantity"] < MIN_QTY) | (df["quantity"] > MAX_QTY)
    if qty_bad.any():
        errors.append(f"{qty_bad.sum()} row(s) have quantity outside [{MIN_QTY},{MAX_QTY}]")

    for col in ("gross_amount", "discount_amount", "net_amount"):
        neg = df[col] < 0
        if neg.any():
            errors.append(f"{neg.sum()} row(s) have negative {col}")

    status_bad = ~df["status"].isin(ALLOWED_STATUSES)
    if status_bad.any():
        errors.append(f"{status_bad.sum()} row(s) have a status outside {sorted(ALLOWED_STATUSES)}")

    for field in REQUIRED_AUDIT_FIELDS:
        if field not in df.columns:
            errors.append(f"missing required audit column: {field}")
        elif df[field].isna().any():
            errors.append(f"{df[field].isna().sum()} row(s) missing required audit field {field}")

    return errors
