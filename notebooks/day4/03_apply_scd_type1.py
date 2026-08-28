# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 Part 3 - Apply SCD Type 1 in Silver
# MAGIC
# MAGIC SCD Type 1 keeps one current row per customer. When Amit moves from Delhi to Mumbai, the Silver business row is overwritten. The table does not retain Delhi as a second business-history row.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
SILVER_SCHEMA = "abb_retail_silver"
CUSTOMERS_BRONZE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
)
CDC_BRONZE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.customer_cdc_raw"
)
CUSTOMER_CURRENT = (
    f"{CATALOG}.{SILVER_SCHEMA}.customer_current"
)

for required_table in [CUSTOMERS_BRONZE, CDC_BRONZE]:
    if not spark.catalog.tableExists(required_table):
        raise RuntimeError(
            f"Required Bronze table does not exist: {required_table}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a deterministic current-state source
# MAGIC
# MAGIC The baseline snapshot receives sequence zero. CDC INSERT and UPDATE events use their supplied sequence numbers. We sort inside each customer and retain only the latest valid source row so MERGE never receives two rows for the same target key.

# COMMAND ----------

snapshot_df = (
    spark.table(CUSTOMERS_BRONZE)
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.trim("city").alias("city"),
        F.trim("loyalty_tier").alias("loyalty_tier"),
        (
            F.when(F.lower(F.trim("is_active")) == "true", True)
            .when(F.lower(F.trim("is_active")) == "false", False)
        ).alias("is_active"),
        F.lit("SNAPSHOT").alias("operation"),
        F.to_timestamp("updated_at").alias("event_timestamp"),
        F.lit(0).cast("long").alias("sequence_number"),
        F.concat(
            F.lit("SNAPSHOT:"),
            F.col("_source_file_name"),
        ).alias("source_event_id"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

cdc_source = spark.table(CDC_BRONZE)

normalized_operation = F.upper(F.trim("operation"))

unsupported_operations = (
    cdc_source
    .filter(
        normalized_operation.isNull()
        | ~normalized_operation.isin("INSERT", "UPDATE")
    )
    .select("operation")
    .distinct()
    .collect()
)

if unsupported_operations:
    values = sorted(
        "<NULL>" if row["operation"] is None else row["operation"]
        for row in unsupported_operations
    )
    raise ValueError(
        "Day 4 handles INSERT and UPDATE only. Found unsupported "
        f"operations {values}. Apply the later delete-policy lesson "
        "before processing these events."
    )

cdc_df = (
    cdc_source
    .filter(F.upper(F.trim("operation")).isin("INSERT", "UPDATE"))
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.trim("city").alias("city"),
        F.trim("loyalty_tier").alias("loyalty_tier"),
        (
            F.when(F.lower(F.trim("is_active")) == "true", True)
            .when(F.lower(F.trim("is_active")) == "false", False)
        ).alias("is_active"),
        F.upper(F.trim("operation")).alias("operation"),
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        F.col("sequence_number").cast("long").alias("sequence_number"),
        F.trim("event_id").alias("source_event_id"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

candidate_df = snapshot_df.unionByName(cdc_df)

invalid_rows = candidate_df.filter(
    F.col("customer_id").isNull()
    | (F.col("customer_id") == "")
    | F.col("customer_name").isNull()
    | (F.col("customer_name") == "")
    | F.col("city").isNull()
    | (F.col("city") == "")
    | F.col("is_active").isNull()
    | F.col("event_timestamp").isNull()
    | F.col("sequence_number").isNull()
    | F.col("source_event_id").isNull()
    | (F.col("source_event_id") == "")
)

invalid_count = invalid_rows.count()
if invalid_count:
    display(invalid_rows)
    raise ValueError(
        f"Cannot apply SCD Type 1: {invalid_count} candidate rows "
        "failed the Silver data contract"
    )

latest_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        F.desc("sequence_number"),
        F.desc("event_timestamp"),
        F.desc("bronze_ingested_at"),
        F.desc("source_event_id"),
    )
)

latest_customer_df = (
    candidate_df
    .withColumn("_row_number", F.row_number().over(latest_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

duplicate_source_keys = (
    latest_customer_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
if duplicate_source_keys:
    raise AssertionError("MERGE source contains duplicate customer keys")

latest_customer_df.createOrReplaceTempView("scd_type1_customer_source")
display(latest_customer_df.orderBy("customer_id"))

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CUSTOMER_CURRENT} (
        customer_id STRING,
        customer_name STRING,
        city STRING,
        loyalty_tier STRING,
        is_active BOOLEAN,
        last_operation STRING,
        last_event_timestamp TIMESTAMP,
        last_sequence_number BIGINT,
        source_event_id STRING,
        source_file STRING,
        silver_updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Current customer state maintained with SCD Type 1'
    """
)

spark.sql(
    f"""
    MERGE INTO {CUSTOMER_CURRENT} AS target
    USING scd_type1_customer_source AS source
      ON target.customer_id = source.customer_id

    WHEN MATCHED AND (
         source.sequence_number > target.last_sequence_number
      OR (
           source.sequence_number = target.last_sequence_number
       AND source.event_timestamp > target.last_event_timestamp
      )
      OR (
           source.sequence_number = target.last_sequence_number
       AND source.event_timestamp = target.last_event_timestamp
       AND source.source_event_id > target.source_event_id
      )
    ) THEN UPDATE SET
      target.customer_name = source.customer_name,
      target.city = source.city,
      target.loyalty_tier = source.loyalty_tier,
      target.is_active = source.is_active,
      target.last_operation = source.operation,
      target.last_event_timestamp = source.event_timestamp,
      target.last_sequence_number = source.sequence_number,
      target.source_event_id = source.source_event_id,
      target.source_file = source.source_file,
      target.silver_updated_at = current_timestamp()

    WHEN NOT MATCHED THEN INSERT (
      customer_id,
      customer_name,
      city,
      loyalty_tier,
      is_active,
      last_operation,
      last_event_timestamp,
      last_sequence_number,
      source_event_id,
      source_file,
      silver_updated_at
    ) VALUES (
      source.customer_id,
      source.customer_name,
      source.city,
      source.loyalty_tier,
      source.is_active,
      source.operation,
      source.event_timestamp,
      source.sequence_number,
      source.source_event_id,
      source.source_file,
      current_timestamp()
    )
    """
)

# COMMAND ----------

display(
    spark.table(CUSTOMER_CURRENT)
    .orderBy("customer_id")
)

print(
    "PASS: SCD Type 1 MERGE completed. The current-state table "
    "contains one latest row per customer."
)
