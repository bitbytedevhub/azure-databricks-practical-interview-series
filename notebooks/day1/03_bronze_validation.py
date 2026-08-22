# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 - Bronze validation
# MAGIC
# MAGIC Validation is executable proof. A green notebook cell is not sufficient evidence that data was written to the correct objects.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text(
    "catalog",
    "master_databricks_new",
    "Existing Unity Catalog catalog",
)

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = "abb_retail_bronze"

CUSTOMERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
)
ORDERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw"
)
CDC_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
)

REQUIRED_TABLES = [
    CUSTOMERS_TABLE,
    ORDERS_TABLE,
    CDC_TABLE,
]

for table_name in REQUIRED_TABLES:
    if not spark.catalog.tableExists(table_name):
        raise AssertionError(
            f"Required table does not exist: {table_name}"
        )
    print(f"PASS: {table_name} exists")

# COMMAND ----------

EXPECTED_BASELINE_COUNTS = {
    CUSTOMERS_TABLE: 3,
    ORDERS_TABLE: 3,
    CDC_TABLE: 1,
}

for table_name, expected_count in (
    EXPECTED_BASELINE_COUNTS.items()
):
    actual_count = spark.table(table_name).count()
    assert actual_count == expected_count, (
        f"{table_name}: expected {expected_count}, "
        f"found {actual_count}. Did you activate a future scenario?"
    )
    print(
        f"PASS: {table_name} contains "
        f"{actual_count} baseline rows"
    )

# COMMAND ----------

REQUIRED_METADATA_COLUMNS = {
    "_source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_modification_time",
    "_ingested_at",
    "_ingestion_run_id",
}

for table_name in REQUIRED_TABLES:
    actual_columns = set(
        spark.table(table_name).columns
    )
    missing_columns = (
        REQUIRED_METADATA_COLUMNS - actual_columns
    )
    assert not missing_columns, (
        f"{table_name} is missing "
        f"{sorted(missing_columns)}"
    )
    print(f"PASS: {table_name} has ingestion metadata")

# COMMAND ----------

for table_name in REQUIRED_TABLES:
    columns = spark.table(table_name).columns
    if "_rescued_data" not in columns:
        print(f"{table_name}: no rescued-data column")
        continue

    rescued_count = (
        spark.table(table_name)
        .filter(F.col("_rescued_data").isNotNull())
        .count()
    )
    print(
        f"{table_name}: rescued rows = {rescued_count}"
    )

# COMMAND ----------

display(spark.table(CUSTOMERS_TABLE))
display(spark.table(ORDERS_TABLE))
display(spark.table(CDC_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rerun proof
# MAGIC
# MAGIC 1. Run the next cell to capture counts.
# MAGIC 2. Return to 02_bronze_ingestion and run all cells without adding files.
# MAGIC 3. Return here and run the comparison cell.
# MAGIC
# MAGIC A checkpoint prevents the same discovered files from being reprocessed. It does not remove a business duplicate delivered in a different new file.

# COMMAND ----------

before_counts = {
    table_name: spark.table(table_name).count()
    for table_name in REQUIRED_TABLES
}
print(before_counts)

# COMMAND ----------

after_counts = {
    table_name: spark.table(table_name).count()
    for table_name in REQUIRED_TABLES
}

for table_name in REQUIRED_TABLES:
    before = before_counts[table_name]
    after = after_counts[table_name]
    print(
        f"{table_name}: before={before}, after={after}"
    )
    assert before == after, (
        f"Rerun changed the row count for {table_name}"
    )

print(
    "PASS: Rerunning ingestion did not duplicate "
    "the existing source files."
)

