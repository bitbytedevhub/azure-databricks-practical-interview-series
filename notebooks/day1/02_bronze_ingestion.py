# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 - Bronze Auto Loader ingestion
# MAGIC
# MAGIC This repeatable runtime notebook incrementally ingests three independent CSV streams. It does not create catalogs, schemas, or volumes.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

dbutils.widgets.text(
    "catalog",
    "master_databricks_new",
    "Existing Unity Catalog catalog",
)

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = "abb_retail_bronze"
OPS_SCHEMA = "abb_retail_ops"

LANDING_ROOT = (
    f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/incoming_files"
)
STATE_ROOT = (
    f"/Volumes/{CATALOG}/{OPS_SCHEMA}/pipeline_state"
)
RUN_ID = str(uuid.uuid4())

available_catalogs = {
    row[0] for row in spark.sql("SHOW CATALOGS").collect()
}
if CATALOG not in available_catalogs:
    raise ValueError(
        f"Catalog {CATALOG!r} is not visible. "
        f"Available: {sorted(available_catalogs)}"
    )

for required_path in [LANDING_ROOT, STATE_ROOT]:
    try:
        dbutils.fs.ls(required_path)
    except Exception as error:
        raise RuntimeError(
            f"Required volume path is inaccessible: {required_path}"
        ) from error

print(f"Run ID:       {RUN_ID}")
print(f"Landing root: {LANDING_ROOT}")
print(f"State root:   {STATE_ROOT}")

# COMMAND ----------

def ingest_csv_to_bronze(
    source_name: str,
    source_path: str,
    target_table: str,
) -> None:
    """Incrementally preserve one CSV stream in a Bronze Delta table."""
    schema_location = (
        f"{STATE_ROOT}/schemas/{source_name}"
    )
    checkpoint_location = (
        f"{STATE_ROOT}/checkpoints/{source_name}"
    )

    print(f"Source:     {source_path}")
    print(f"Target:     {target_table}")
    print(f"Schema:     {schema_location}")
    print(f"Checkpoint: {checkpoint_location}")

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option(
            "cloudFiles.schemaLocation",
            schema_location,
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            "rescue",
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "false",
        )
        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )
        .load(source_path)
    )

    bronze_df = (
        source_df
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "_source_file_name",
            F.col("_metadata.file_name"),
        )
        .withColumn(
            "_source_file_size",
            F.col("_metadata.file_size"),
        )
        .withColumn(
            "_source_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_ingestion_run_id",
            F.lit(RUN_ID),
        )
    )

    query = (
        bronze_df.writeStream
        .option(
            "checkpointLocation",
            checkpoint_location,
        )
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    print(f"COMPLETED: {target_table}")

# COMMAND ----------

ingest_csv_to_bronze(
    source_name="customers",
    source_path=f"{LANDING_ROOT}/customers",
    target_table=(
        f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
    ),
)

ingest_csv_to_bronze(
    source_name="orders",
    source_path=f"{LANDING_ROOT}/orders",
    target_table=(
        f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw"
    ),
)

ingest_csv_to_bronze(
    source_name="customer_cdc",
    source_path=f"{LANDING_ROOT}/customer_cdc",
    target_table=(
        f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
    ),
)

print("PASS: All available Bronze files were processed.")

