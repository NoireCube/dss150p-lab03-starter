from pathlib import Path
import shutil
from src.config import path_for

SOURCE_FILES = ("customers.csv", "products.json", "orders.csv")


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
    source_dir = path_for("source_dir")
    raw_dir = path_for("raw_dir") / f"run_id={run_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for filename in SOURCE_FILES:
        src_path = source_dir / filename
        if not src_path.exists():
            raise FileNotFoundError(f"Missing required source file: {src_path}")
        # copy2 preserves metadata (mtime) but writes a *new* file at the
        # destination; the original under data/source/ is never opened for
        # writing, so the source snapshot stays immutable and reproducible.
        shutil.copy2(src_path, raw_dir / filename)

    return raw_dir
