# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 Part 4 - Validate SCD Type 1
# MAGIC
# MAGIC A successful MERGE is not enough. This notebook proves that Amit now has one current row, Mumbai replaced Delhi, the baseline customers remain correct, and rerunning the MERGE does not create duplicates.

# COMMAND ----------

import re

from pyspark.sql import functions as F

dbutils.widgets.text(
    "catalog",
    "master_databricks_new",
    "Existing Unity Catalog catalog",
)

CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(
        "Use a catalog name containing only letters, numbers, "
        "and underscores for this beginner lab."
    )

CUSTOMER_CURRENT = (
    f"{CATALOG}.abb_retail_silver.customer_current"
)

if not spark.catalog.tableExists(CUSTOMER_CURRENT):
    raise RuntimeError(
        f"Silver table does not exist: {CUSTOMER_CURRENT}. "
        "Run 03_apply_scd_type1 first."
    )

silver_df = spark.table(CUSTOMER_CURRENT)

# COMMAND ----------

expected_customers = {
    "C001": {
        "customer_name": "Amit Sharma",
        "city": "Mumbai",
        "last_sequence_number": 2,
        "source_event_id": "E0002",
    },
    "C002": {
        "customer_name": "Meera Iyer",
        "city": "Pune",
        "last_sequence_number": 0,
    },
    "C003": {
        "customer_name": "Sara Khan",
        "city": "Jaipur",
        "last_sequence_number": 0,
    },
    "C004": {
        "customer_name": "Ravi Kumar",
        "city": "Chennai",
        "last_sequence_number": 1,
        "source_event_id": "E0001",
    },
}

actual_count = silver_df.count()
if actual_count != len(expected_customers):
    raise AssertionError(
        f"Expected {len(expected_customers)} current customers; "
        f"found {actual_count}"
    )

distinct_key_count = silver_df.select("customer_id").distinct().count()
if distinct_key_count != actual_count:
    raise AssertionError(
        "SCD Type 1 table contains more than one row for a customer"
    )

for customer_id, expectations in expected_customers.items():
    rows = (
        silver_df
        .filter(F.col("customer_id") == customer_id)
        .collect()
    )
    if len(rows) != 1:
        raise AssertionError(
            f"Expected one row for {customer_id}; found {len(rows)}"
        )

    actual = rows[0].asDict()
    for column_name, expected_value in expectations.items():
        if actual[column_name] != expected_value:
            raise AssertionError(
                f"{customer_id}.{column_name}: expected "
                f"{expected_value!r}, found {actual[column_name]!r}"
            )

    print(f"PASS: {customer_id} matches expected current state")

# COMMAND ----------

old_amit_rows = (
    silver_df
    .filter(
        (F.col("customer_id") == "C001")
        & (F.col("city") == "Delhi")
    )
    .count()
)

if old_amit_rows != 0:
    raise AssertionError(
        "SCD Type 1 failed: Amit's old Delhi business row remains"
    )

print("PASS: Mumbai replaced Delhi; no second history row exists")

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {CUSTOMER_CURRENT}")
merge_count = history_df.filter(F.col("operation") == "MERGE").count()

if merge_count < 1:
    raise AssertionError("Delta history does not contain a MERGE operation")

display(silver_df.orderBy("customer_id"))
display(
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics",
    )
)

print(
    "PASS: Day 3 SCD Type 1 is correct and rerunnable. "
    "The business table keeps current state; Delta transaction "
    "history is operational history, not an SCD Type 2 model."
)

