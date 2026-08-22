from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "README.md",
    "databricks.yml",
    "resources/day1_job.yml",
    "notebooks/day1/00_platform_bootstrap.py",
    "notebooks/day1/01_source_simulator.py",
    "notebooks/day1/02_bronze_ingestion.py",
    "notebooks/day1/03_bronze_validation.py",
    "notebooks/day1/04_future_scenarios.py",
    "docs/day1-conversational-guide.md",
    "docs/production-mapping.md",
    "docs/workflow-setup.md",
]

CSV_EXPECTATIONS = {
    "data/baseline/customers_batch_001.csv": {
        "rows": 3,
        "key": "customer_id",
        "ids": {"C001", "C002", "C003"},
    },
    "data/baseline/orders_batch_001.csv": {
        "rows": 3,
        "key": "order_id",
        "ids": {"O1001", "O1002", "O1003"},
    },
    "data/baseline/customer_cdc_batch_001.csv": {
        "rows": 1,
        "key": "event_id",
        "ids": {"E0001"},
    },
    "data/future_scenarios/amit_move_to_mumbai.csv": {
        "rows": 1,
        "key": "event_id",
        "ids": {"E0002"},
    },
    "data/future_scenarios/meera_delete.csv": {
        "rows": 1,
        "key": "event_id",
        "ids": {"E0003"},
    },
    "data/future_scenarios/sara_updates_out_of_order.csv": {
        "rows": 2,
        "key": "event_id",
        "ids": {"E0004", "E0005"},
    },
    "data/future_scenarios/customer_with_email.csv": {
        "rows": 1,
        "key": "customer_id",
        "ids": {"C005"},
    },
}


def validate_required_files() -> None:
    missing = [
        relative
        for relative in REQUIRED_FILES
        if not (ROOT / relative).is_file()
    ]
    if missing:
        raise AssertionError(f"Missing required files: {missing}")


def validate_notebook_syntax() -> None:
    for path in sorted((ROOT / "notebooks").rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if not source.startswith("# Databricks notebook source"):
            raise AssertionError(
                f"{path.relative_to(ROOT)} is not a Databricks source notebook"
            )
        compile(source, str(path), "exec")


def validate_csv_files() -> None:
    for relative, expected in CSV_EXPECTATIONS.items():
        path = ROOT / relative
        with path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            rows = list(csv.DictReader(handle))

        if len(rows) != expected["rows"]:
            raise AssertionError(
                f"{relative}: expected {expected['rows']} rows, "
                f"found {len(rows)}"
            )

        actual_ids = {
            row[expected["key"]]
            for row in rows
        }
        if actual_ids != expected["ids"]:
            raise AssertionError(
                f"{relative}: expected IDs {expected['ids']}, "
                f"found {actual_ids}"
            )


def main() -> None:
    validate_required_files()
    validate_notebook_syntax()
    validate_csv_files()
    print("PASS: Day 1 repository validation completed.")


if __name__ == "__main__":
    main()

