# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 Part 1 - CSV to Bronze with Auto Loader
# MAGIC
# MAGIC Day 1 created three CSV datasets. This notebook incrementally ingests them into separate Bronze Delta tables. It does not create catalogs, schemas, or volumes, and it does not apply SCD logic.

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

CATALOG = "master_databricks_new"
BRONZE_SCHEMA = "abb_retail_bronze"
OPS_SCHEMA = "abb_retail_ops"

if "." in CATALOG:
    raise ValueError(
        "CATALOG must contain only the catalog name, "
        "for example master_databricks_new."
    )

print(f"Catalog:       {CATALOG}")
print(f"Bronze schema: {BRONZE_SCHEMA}")
print(f"Ops schema:    {OPS_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail fast
# MAGIC
# MAGIC Before starting a streaming query, prove that the catalog and required schemas exist. A configuration failure should stop before Spark begins reading business files.

# COMMAND ----------

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

available_schemas = {
    row[0]
    for row in spark.sql(
        f"SHOW SCHEMAS IN {CATALOG}"
    ).collect()
}

required_schemas = {
    BRONZE_SCHEMA,
    OPS_SCHEMA,
}

missing_schemas = required_schemas - available_schemas
if missing_schemas:
    raise ValueError(
        f"Required schemas are missing: "
        f"{sorted(missing_schemas)}"
    )

print("PASS: Catalog and schemas are available")

# COMMAND ----------

LANDING_ROOT = (
    f"/Volumes/{CATALOG}/"
    f"{BRONZE_SCHEMA}/incoming_files"
)

STATE_ROOT = (
    f"/Volumes/{CATALOG}/"
    f"{OPS_SCHEMA}/pipeline_state"
)

CUSTOMERS_PATH = f"{LANDING_ROOT}/customers"
ORDERS_PATH = f"{LANDING_ROOT}/orders"
CUSTOMER_CDC_PATH = f"{LANDING_ROOT}/customer_cdc"

SOURCE_PATHS = {
    "customers": CUSTOMERS_PATH,
    "orders": ORDERS_PATH,
    "customer_cdc": CUSTOMER_CDC_PATH,
}

for source_name, source_path in SOURCE_PATHS.items():
    try:
        csv_files = [
            item
            for item in dbutils.fs.ls(source_path)
            if item.path.lower().endswith(".csv")
        ]
    except Exception as error:
        raise RuntimeError(
            f"Cannot access source path: {source_path}"
        ) from error

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found for {source_name}: "
            f"{source_path}"
        )

    print(
        f"PASS: {source_name} contains "
        f"{len(csv_files)} CSV file(s)"
    )

try:
    dbutils.fs.ls(STATE_ROOT)
except Exception as error:
    raise RuntimeError(
        f"Cannot access pipeline state volume: {STATE_ROOT}"
    ) from error

# COMMAND ----------

SCHEMA_STATE_ROOT = f"{STATE_ROOT}/schemas"
CHECKPOINT_ROOT = f"{STATE_ROOT}/checkpoints"

dbutils.fs.mkdirs(SCHEMA_STATE_ROOT)
dbutils.fs.mkdirs(CHECKPOINT_ROOT)

RUN_ID = str(uuid.uuid4())

print(f"Run ID:            {RUN_ID}")
print(f"Landing root:      {LANDING_ROOT}")
print(f"Schema state root: {SCHEMA_STATE_ROOT}")
print(f"Checkpoint root:   {CHECKPOINT_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable ingestion pattern
# MAGIC
# MAGIC Each stream owns one source directory, schema location, checkpoint, and target table. A checkpoint tracks file-processing progress; it does not remove business duplicates delivered through different files.

# COMMAND ----------

def ingest_csv_to_bronze(
    source_name: str,
    source_path: str,
    target_table: str,
) -> None:
    """Incrementally preserve one CSV stream in a Bronze Delta table."""

    schema_location = (
        f"{SCHEMA_STATE_ROOT}/{source_name}"
    )
    checkpoint_location = (
        f"{CHECKPOINT_ROOT}/{source_name}"
    )

    print("=" * 70)
    print(f"Source name: {source_name}")
    print(f"Source path: {source_path}")
    print(f"Target:      {target_table}")
    print(f"Schema:      {schema_location}")
    print(f"Checkpoint:  {checkpoint_location}")
    print("=" * 70)

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
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
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
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
        .format("delta")
        .outputMode("append")
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

CUSTOMERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
)
ORDERS_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw"
)
CUSTOMER_CDC_TABLE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
)

ingest_csv_to_bronze(
    source_name="customers",
    source_path=CUSTOMERS_PATH,
    target_table=CUSTOMERS_TABLE,
)

ingest_csv_to_bronze(
    source_name="orders",
    source_path=ORDERS_PATH,
    target_table=ORDERS_TABLE,
)

ingest_csv_to_bronze(
    source_name="customer_cdc",
    source_path=CUSTOMER_CDC_PATH,
    target_table=CUSTOMER_CDC_TABLE,
)

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}"
    )
)

display(spark.table(CUSTOMERS_TABLE).limit(10))
display(spark.table(ORDERS_TABLE).limit(10))
display(spark.table(CUSTOMER_CDC_TABLE).limit(10))

print("PASS: Day 2 Bronze ingestion completed")


