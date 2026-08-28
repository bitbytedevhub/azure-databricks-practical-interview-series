# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 - Bronze validation and rerun evidence
# MAGIC
# MAGIC A successful ingestion cell is not enough. This notebook proves table existence, baseline counts, source contracts, metadata, file coverage, rescued data, business-key quality, and safe file-level reruns.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "master_databricks_new"
BRONZE_SCHEMA = "abb_retail_bronze"
OPS_SCHEMA = "abb_retail_ops"

if "." in CATALOG:
    raise ValueError(
        "CATALOG must contain only the catalog name."
    )

available_catalogs = {
    row[0]
    for row in spark.sql("SHOW CATALOGS").collect()
}

if CATALOG not in available_catalogs:
    raise ValueError(
        f"Catalog {CATALOG!r} was not found. "
        f"Available catalogs: {sorted(available_catalogs)}"
    )

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

CUSTOMERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
)
ORDERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw"
)
CUSTOMER_CDC_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
)

REQUIRED_TABLES = [
    CUSTOMERS_TABLE,
    ORDERS_TABLE,
    CUSTOMER_CDC_TABLE,
]

for table_name in REQUIRED_TABLES:
    if not spark.catalog.tableExists(table_name):
        raise AssertionError(
            f"Required Bronze table does not exist: "
            f"{table_name}"
        )
    print(f"PASS: {table_name} exists")

# COMMAND ----------

EXPECTED_BASELINE_COUNTS = {
    CUSTOMERS_TABLE: 3,
    ORDERS_TABLE: 3,
    CUSTOMER_CDC_TABLE: 1,
}

for table_name, expected_count in (
    EXPECTED_BASELINE_COUNTS.items()
):
    actual_count = spark.table(table_name).count()
    print(
        f"{table_name}: expected={expected_count}, "
        f"actual={actual_count}"
    )
    if actual_count != expected_count:
        raise AssertionError(
            f"Row-count validation failed for "
            f"{table_name}. Expected {expected_count}, "
            f"found {actual_count}."
        )

# COMMAND ----------

EXPECTED_SOURCE_COLUMNS = {
    CUSTOMERS_TABLE: {
        "customer_id",
        "customer_name",
        "city",
        "loyalty_tier",
        "is_active",
        "updated_at",
    },
    ORDERS_TABLE: {
        "order_id",
        "customer_id",
        "order_amount",
        "currency",
        "order_status",
        "order_timestamp",
    },
    CUSTOMER_CDC_TABLE: {
        "event_id",
        "customer_id",
        "operation",
        "customer_name",
        "city",
        "loyalty_tier",
        "is_active",
        "event_timestamp",
        "sequence_number",
    },
}

for table_name, expected_columns in (
    EXPECTED_SOURCE_COLUMNS.items()
):
    actual_columns = set(
        spark.table(table_name).columns
    )
    missing_columns = (
        expected_columns - actual_columns
    )
    if missing_columns:
        raise AssertionError(
            f"{table_name} is missing source columns: "
            f"{sorted(missing_columns)}"
        )
    print(
        f"PASS: {table_name} has all source columns"
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
    missing_metadata = (
        REQUIRED_METADATA_COLUMNS - actual_columns
    )
    if missing_metadata:
        raise AssertionError(
            f"{table_name} is missing metadata: "
            f"{sorted(missing_metadata)}"
        )

    metadata_null_count = (
        spark.table(table_name)
        .filter(
            F.col("_source_file").isNull()
            | F.col("_source_file_name").isNull()
            | F.col("_ingested_at").isNull()
            | F.col("_ingestion_run_id").isNull()
        )
        .count()
    )
    if metadata_null_count:
        raise AssertionError(
            f"{table_name} contains "
            f"{metadata_null_count} rows with "
            "missing metadata"
        )
    print(
        f"PASS: {table_name} metadata is complete"
    )

# COMMAND ----------

EXPECTED_SOURCE_FILES = {
    CUSTOMERS_TABLE: {
        "customers_batch_001.csv"
    },
    ORDERS_TABLE: {
        "orders_batch_001.csv"
    },
    CUSTOMER_CDC_TABLE: {
        "customer_cdc_batch_001.csv"
    },
}

for table_name, expected_files in (
    EXPECTED_SOURCE_FILES.items()
):
    actual_files = {
        row["_source_file_name"]
        for row in (
            spark.table(table_name)
            .select("_source_file_name")
            .distinct()
            .collect()
        )
    }
    missing_files = expected_files - actual_files
    if missing_files:
        raise AssertionError(
            f"{table_name} is missing expected files: "
            f"{sorted(missing_files)}"
        )
    print(
        f"PASS: {table_name} contains "
        f"{sorted(expected_files)}"
    )

    display(
        spark.table(table_name)
        .groupBy("_source_file_name")
        .agg(
            F.count("*").alias("bronze_row_count")
        )
        .orderBy("_source_file_name")
    )

# COMMAND ----------

for table_name in REQUIRED_TABLES:
    if "_rescued_data" not in (
        spark.table(table_name).columns
    ):
        print(
            f"{table_name}: rescued-data column absent"
        )
        continue

    rescued_df = (
        spark.table(table_name)
        .filter(
            F.col("_rescued_data").isNotNull()
        )
    )
    rescued_count = rescued_df.count()
    print(
        f"{table_name}: rescued rows = "
        f"{rescued_count}"
    )
    if rescued_count:
        display(rescued_df)
        raise AssertionError(
            f"Baseline table {table_name} contains "
            f"{rescued_count} rescued rows"
        )

# COMMAND ----------

BUSINESS_KEYS = {
    CUSTOMERS_TABLE: "customer_id",
    ORDERS_TABLE: "order_id",
    CUSTOMER_CDC_TABLE: "event_id",
}

for table_name, key_column in BUSINESS_KEYS.items():
    missing_key_count = (
        spark.table(table_name)
        .filter(
            F.col(key_column).isNull()
            | (
                F.trim(F.col(key_column)) == ""
            )
        )
        .count()
    )
    if missing_key_count:
        raise AssertionError(
            f"{table_name} contains "
            f"{missing_key_count} missing "
            f"{key_column} values"
        )

    duplicate_keys = (
        spark.table(table_name)
        .groupBy(key_column)
        .agg(
            F.count("*").alias("record_count")
        )
        .filter(F.col("record_count") > 1)
        .orderBy(F.desc("record_count"))
    )
    duplicate_count = duplicate_keys.count()
    print(
        f"{table_name}: duplicate key groups = "
        f"{duplicate_count}"
    )
    if duplicate_count:
        display(duplicate_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual rerun proof
# MAGIC
# MAGIC 1. Run the next cell to capture counts.
# MAGIC 2. Return to 01_bronze_ingestion and run all cells without adding or changing source files.
# MAGIC 3. Return here and run the after-count and comparison cells.
# MAGIC 4. Do not rerun the before-count cell after returning.

# COMMAND ----------

before_counts = {
    table_name: spark.table(table_name).count()
    for table_name in REQUIRED_TABLES
}

print("Counts before rerun:")
for table_name, row_count in before_counts.items():
    print(f"  {table_name}: {row_count}")

# COMMAND ----------

after_counts = {
    table_name: spark.table(table_name).count()
    for table_name in REQUIRED_TABLES
}

print("Counts after rerun:")
for table_name, row_count in after_counts.items():
    print(f"  {table_name}: {row_count}")

# COMMAND ----------

for table_name in REQUIRED_TABLES:
    before = before_counts[table_name]
    after = after_counts[table_name]
    print(
        f"{table_name}: before={before}, "
        f"after={after}"
    )
    if before != after:
        raise AssertionError(
            f"Rerunning ingestion changed the count "
            f"for {table_name}. Before={before}, "
            f"after={after}"
        )

print(
    "PASS: Rerunning ingestion did not duplicate "
    "previously processed files"
)

# COMMAND ----------

validation_summary = []

for table_name in REQUIRED_TABLES:
    table_df = spark.table(table_name)
    rescued_count = 0
    if "_rescued_data" in table_df.columns:
        rescued_count = (
            table_df
            .filter(
                F.col("_rescued_data").isNotNull()
            )
            .count()
        )

    validation_summary.append(
        {
            "table_name": table_name,
            "row_count": table_df.count(),
            "source_file_count": (
                table_df
                .select("_source_file_name")
                .distinct()
                .count()
            ),
            "rescued_row_count": rescued_count,
            "rerun_count_unchanged": (
                before_counts[table_name]
                == after_counts[table_name]
            ),
        }
    )

display(
    spark.createDataFrame(validation_summary)
)
