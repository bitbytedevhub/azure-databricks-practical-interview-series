# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 Part 2 - Incrementally ingest Amit's update
# MAGIC
# MAGIC This notebook reuses the exact customer CDC source, schema location, checkpoint, and Bronze target established on Day 2. Auto Loader therefore processes only the new batch-002 file.

# COMMAND ----------

import re
import uuid

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

BRONZE_SCHEMA = "abb_retail_bronze"
OPS_SCHEMA = "abb_retail_ops"
LANDING_ROOT = (
    f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/incoming_files"
)
STATE_ROOT = (
    f"/Volumes/{CATALOG}/{OPS_SCHEMA}/pipeline_state"
)
SOURCE_PATH = f"{LANDING_ROOT}/customer_cdc"
SCHEMA_LOCATION = f"{STATE_ROOT}/schemas/customer_cdc"
CHECKPOINT_LOCATION = f"{STATE_ROOT}/checkpoints/customer_cdc"
TARGET_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
)
RUN_ID = str(uuid.uuid4())

if not spark.catalog.tableExists(TARGET_TABLE):
    raise RuntimeError(
        f"Day 2 target {TARGET_TABLE} does not exist. "
        "Complete Day 2 Bronze ingestion first."
    )

before_count = spark.table(TARGET_TABLE).count()

print(f"Source:     {SOURCE_PATH}")
print(f"Schema:     {SCHEMA_LOCATION}")
print(f"Checkpoint: {CHECKPOINT_LOCATION}")
print(f"Target:     {TARGET_TABLE}")
print(f"Run ID:     {RUN_ID}")

# COMMAND ----------

source_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
)

bronze_df = (
    source_df
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_file_name", F.col("_metadata.file_name"))
    .withColumn("_source_file_size", F.col("_metadata.file_size"))
    .withColumn(
        "_source_modification_time",
        F.col("_metadata.file_modification_time"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingestion_run_id", F.lit(RUN_ID))
)

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

# COMMAND ----------

after_count = spark.table(TARGET_TABLE).count()
amit_event_count = (
    spark.table(TARGET_TABLE)
    .filter(F.col("event_id") == "E0002")
    .count()
)

print(f"Rows before: {before_count}")
print(f"Rows after:  {after_count}")
print(f"New rows:    {after_count - before_count}")

if amit_event_count != 1:
    raise AssertionError(
        "Expected exactly one Bronze event E0002 after ingestion; "
        f"found {amit_event_count}"
    )

display(
    spark.table(TARGET_TABLE)
    .filter(F.col("event_id") == "E0002")
)

print(
    "PASS: Amit's update is in Bronze. A no-file-change rerun "
    "will add zero rows because the Day 2 checkpoint was reused."
)

