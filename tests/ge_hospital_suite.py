"""
ge_hospital_suite.py
=====================
Great Expectations data quality checks for the Silver layer.
Run after bronze ingestion, before dbt models execute.

Run: python tests/ge_hospital_suite.py
"""

import json
import pandas as pd
from datetime import date

# ── Load bronze output for validation ─────────────────────────────────────────
def load_bronze(bronze_path: str = None) -> pd.DataFrame:
    if bronze_path is None:
        today = date.today().isoformat()
        bronze_path = f"bronze/hospitals/ingestion_date={today}/hospitals.json"
    with open(bronze_path) as f:
        records = json.load(f)
    return pd.DataFrame(records)


# ── Expectation runner ────────────────────────────────────────────────────────
def run_expectations(df: pd.DataFrame) -> dict:
    results = []
    passed  = 0
    failed  = 0

    def check(name: str, result: bool, detail: str = ""):
        nonlocal passed, failed
        status = "PASS" if result else "FAIL"
        if result:
            passed += 1
        else:
            failed += 1
        results.append({"check": name, "status": status, "detail": detail})
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))

    print("\nRunning data quality suite on Bronze layer...\n")

    # ── Completeness checks ───────────────────────────────────────────────────
    check(
        "facility_id: no nulls",
        df["facility_id"].notna().all(),
        f"{df['facility_id'].isna().sum()} nulls found"
    )
    check(
        "facility_name: no nulls",
        df["facility_name"].notna().all(),
        f"{df['facility_name'].isna().sum()} nulls found"
    )
    check(
        "state: no nulls",
        df["state"].notna().all(),
    )

    # ── Uniqueness ────────────────────────────────────────────────────────────
    check(
        "facility_id: unique",
        df["facility_id"].nunique() == len(df),
        f"{len(df) - df['facility_id'].nunique()} duplicates"
    )

    # ── Value range checks ────────────────────────────────────────────────────
    valid_ratings = {"1", "2", "3", "4", "5", None, "Not Available"}
    actual_ratings = set(df["hospital_overall_rating"].astype(str).unique())
    invalid = actual_ratings - valid_ratings - {"nan"}
    check(
        "overall_rating: values in 1–5 or null",
        len(invalid) == 0,
        f"Invalid values: {invalid}" if invalid else ""
    )

    # ── Metadata presence ──────────────────────────────────────────────────────
    check("_source metadata: present",      "_source"      in df.columns)
    check("_ingested_at metadata: present", "_ingested_at" in df.columns)

    # ── Row count freshness ───────────────────────────────────────────────────
    check(
        "row count: at least 1 row",
        len(df) >= 1,
        f"Found {len(df)} rows"
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\nResults: {passed}/{total} checks passed", end="")
    if failed > 0:
        print(f"  ⚠  {failed} FAILED — fix before running dbt")
        raise AssertionError(f"{failed} data quality checks failed.")
    else:
        print("  ✓  All checks passed")

    return {"passed": passed, "failed": failed, "checks": results}


if __name__ == "__main__":
    df = load_bronze()
    print(f"Loaded {len(df)} rows from Bronze layer")
    run_expectations(df)
