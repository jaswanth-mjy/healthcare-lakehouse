"""
Bronze Layer Ingestion Script
==============================
Reads raw CMS Hospital data (CSV) and lands it into the bronze layer
as partitioned JSON files with ingestion metadata attached.

Run: python scripts/ingest_bronze.py
"""

import json
import os
import logging
from datetime import date, datetime
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
RAW_INPUT   = "data/raw/hospitals.csv"
BRONZE_DIR  = "bronze/hospitals"
SOURCE_NAME = "cms_hospital_general_info"
SOURCE_URL  = "https://data.cms.gov/provider-data/"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: lowercase, spaces → underscores, strip slashes."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\/\s]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    return df


def add_metadata(records: list[dict], ingestion_ts: str) -> list[dict]:
    """
    Attach Bronze-layer metadata to every record.
    This is the key pattern: raw data is NEVER modified — we only ADD fields.
    """
    for rec in records:
        rec["_source"]        = SOURCE_NAME
        rec["_source_url"]    = SOURCE_URL
        rec["_ingested_at"]   = ingestion_ts
        rec["_ingestion_date"]= str(date.today())
    return records


def validate_raw(df: pd.DataFrame) -> None:
    """
    Lightweight validation at Bronze — just checks the file loaded correctly.
    Deep quality checks happen in the Silver layer (Great Expectations).
    """
    assert len(df) > 0,               "ERROR: CSV loaded 0 rows — check file path"
    assert "facility_id" in df.columns, "ERROR: 'facility_id' column not found after rename"
    log.info(f"  Raw validation passed — {len(df)} rows, {len(df.columns)} columns")


def ingest_hospitals(input_path: str = RAW_INPUT) -> str:
    """
    Main ingestion function.
    Returns: path to the output JSON file written in bronze/
    """
    ingestion_ts = datetime.utcnow().isoformat() + "Z"
    today        = date.today().isoformat()          # e.g. "2024-06-14"

    log.info(f"Starting Bronze ingestion: {input_path}")

    # 1. Read raw CSV
    df = pd.read_csv(input_path, dtype=str)          # dtype=str preserves leading zeros (ZIP, Facility ID)
    log.info(f"  Loaded {len(df)} rows from {input_path}")

    # 2. Normalize column names
    df = clean_column_names(df)

    # 3. Validate
    validate_raw(df)

    # 4. Convert to list of dicts (raw records — no transformation yet)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    # 5. Attach ingestion metadata
    records = add_metadata(records, ingestion_ts)

    # 6. Write to bronze/ partitioned by date
    out_dir  = os.path.join(BRONZE_DIR, f"ingestion_date={today}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "hospitals.json")

    with open(out_path, "w") as f:
        json.dump(records, f, indent=2, default=str)

    log.info(f"  Written {len(records)} records → {out_path}")

    # 7. Write ingestion manifest (audit trail)
    manifest = {
        "source":          SOURCE_NAME,
        "source_url":      SOURCE_URL,
        "input_file":      input_path,
        "output_file":     out_path,
        "row_count":       len(records),
        "column_count":    len(df.columns),
        "ingested_at":     ingestion_ts,
        "ingestion_date":  today,
        "status":          "success"
    }
    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    log.info(f"  Manifest written → {manifest_path}")
    log.info("Bronze ingestion complete ✓")
    return out_path


if __name__ == "__main__":
    result = ingest_hospitals()
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        try:
            with open(github_output, "a") as f:
                f.write(f"bronze_output={result}\n")
        except OSError as e:
            raise RuntimeError(f"Failed to write bronze_output to GITHUB_OUTPUT. Error: {e}") from e
    print(f"\nOutput: {result}")
