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
    "notebooks/day2_and_day3/01_bronze_ingestion.py",
    "notebooks/day2_and_day3/02_bronze_validation.py",
    "notebooks/day4/01_scd_type1_problem_and_solution.py",
    "notebooks/day5/01_scd_type2_problem_and_solution.py",
    "data/day4/customer_cdc_batch_002.csv",
    "docs/day1-conversational-guide.md",
    "docs/day2-and-day3-part1-ingestion-guide.md",
    "docs/day2-and-day3-part2-validation-guide.md",
    "docs/day4-scd-type1-guide.md",
    "docs/day5-scd-type2-guide.md",
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
    "data/day4/customer_cdc_batch_002.csv": {
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


def validate_day2_through_day5_contracts() -> None:
    day2_ingestion = (
        ROOT / "notebooks/day2_and_day3/01_bronze_ingestion.py"
    ).read_text(encoding="utf-8")
    day4_scd1 = (
        ROOT / "notebooks/day4/01_scd_type1_problem_and_solution.py"
    ).read_text(encoding="utf-8")
    day5_scd2 = (
        ROOT / "notebooks/day5/01_scd_type2_problem_and_solution.py"
    ).read_text(encoding="utf-8")

    required_ingestion_patterns = [
        '.format("cloudFiles")',
        '.format("delta")',
        'checkpointLocation',
        'availableNow=True',
        '.toTable(',
    ]
    missing_ingestion = [
        pattern
        for pattern in required_ingestion_patterns
        if pattern not in day2_ingestion
    ]
    if missing_ingestion:
        raise AssertionError(
            "Day 2 ingestion is missing required patterns: "
            f"{missing_ingestion}"
        )

    scd1_patterns = [
        "MERGE INTO",
        "WHEN MATCHED",
        "THEN UPDATE SET",
        "WHEN NOT MATCHED THEN INSERT",
        "sequence_number",
        "row_number()",
        "unionByName",
        "createOrReplaceTempView",
    ]
    missing_scd1 = [
        pattern
        for pattern in scd1_patterns
        if pattern not in day4_scd1
    ]
    if missing_scd1:
        raise AssertionError(
            f"Day 4 SCD Type 1 is missing patterns: {missing_scd1}"
        )

    removed_prerequisite_checks = [
        "tableExists(",
        "re.fullmatch",
        "unsupported_operations",
        "invalid_rows",
        "raise RuntimeError",
        "raise ValueError",
        "raise AssertionError",
    ]
    unexpected_checks = [
        pattern
        for pattern in removed_prerequisite_checks
        if pattern in day4_scd1
    ]
    if unexpected_checks:
        raise AssertionError(
            "Day 4 should focus on the SCD Type 1 problem and solution. "
            f"Found removed prerequisite checks: {unexpected_checks}"
        )

    scd2_patterns = [
        "customer_history",
        "customer_sk",
        "effective_from",
        "effective_to",
        "is_current",
        "left_anti",
        "row_number()",
        "merge_customer_id",
        "unionByName",
        "MERGE INTO",
        "WHEN MATCHED",
        "target.is_current = FALSE",
        "WHEN NOT MATCHED THEN INSERT",
    ]
    missing_scd2 = [
        pattern
        for pattern in scd2_patterns
        if pattern not in day5_scd2
    ]
    if missing_scd2:
        raise AssertionError(
            f"Day 5 SCD Type 2 is missing patterns: {missing_scd2}"
        )


def main() -> None:
    validate_required_files()
    validate_notebook_syntax()
    validate_csv_files()
    validate_day2_through_day5_contracts()
    print(
        "PASS: Day 1 through Day 5 repository "
        "validation completed."
    )


if __name__ == "__main__":
    main()
