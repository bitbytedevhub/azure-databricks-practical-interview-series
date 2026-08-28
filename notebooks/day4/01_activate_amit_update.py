# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 Part 1 - Activate Amit's CDC update
# MAGIC
# MAGIC Day 1 prepared future scenarios outside the active landing directory. This notebook safely copies only Amit's Delhi-to-Mumbai update into the customer CDC inbox. Rerunning it does not create another delivery.

# COMMAND ----------

import re

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

LANDING_ROOT = (
    f"/Volumes/{CATALOG}/abb_retail_bronze/incoming_files"
)
SCENARIO_FILE = (
    f"{LANDING_ROOT}/practice_scenarios/amit_move/"
    "amit_move_to_mumbai.csv"
)
ACTIVE_CDC_FILE = (
    f"{LANDING_ROOT}/customer_cdc/"
    "customer_cdc_batch_002.csv"
)

print(f"Scenario file: {SCENARIO_FILE}")
print(f"Active delivery: {ACTIVE_CDC_FILE}")

# COMMAND ----------


def file_exists(path: str) -> bool:
    """Return True only when the exact volume file exists."""
    parent_path, file_name = path.rsplit("/", 1)
    try:
        return file_name in {
            item.name.rstrip("/")
            for item in dbutils.fs.ls(parent_path)
        }
    except Exception:
        return False


if not file_exists(SCENARIO_FILE):
    raise FileNotFoundError(
        "Amit's prepared scenario file was not found. Run "
        "notebooks/day1/04_future_scenarios.py first."
    )

if file_exists(ACTIVE_CDC_FILE):
    source_text = dbutils.fs.head(SCENARIO_FILE, 100_000)
    active_text = dbutils.fs.head(ACTIVE_CDC_FILE, 100_000)
    if source_text != active_text:
        raise RuntimeError(
            "The active batch-002 filename already exists with different "
            "content. Do not overwrite an immutable source delivery."
        )
    print("SKIP: Amit's update is already active with identical content")
else:
    copied = dbutils.fs.cp(SCENARIO_FILE, ACTIVE_CDC_FILE)
    if not copied:
        raise RuntimeError("Databricks did not confirm the file copy")
    print(f"CREATED: {ACTIVE_CDC_FILE}")

# COMMAND ----------

activated_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ACTIVE_CDC_FILE)
)

if activated_df.count() != 1:
    raise AssertionError("Amit's controlled scenario must contain one row")

event = activated_df.first().asDict()
expected = {
    "event_id": "E0002",
    "customer_id": "C001",
    "operation": "UPDATE",
    "city": "Mumbai",
    "sequence_number": "2",
}

for column_name, expected_value in expected.items():
    actual_value = event[column_name]
    if actual_value != expected_value:
        raise AssertionError(
            f"{column_name}: expected {expected_value!r}, "
            f"found {actual_value!r}"
        )

display(activated_df)
print("PASS: Day 4 CDC scenario is active and safe to rerun")
