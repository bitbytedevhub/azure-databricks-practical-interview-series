# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 - Interview Problem: Implement SCD Type 1
# MAGIC
# MAGIC **Business problem:** Amit currently lives in Delhi. A new CDC event says that he moved to Mumbai.
# MAGIC
# MAGIC **Expected solution:** The Silver current-state table must contain one row for Amit, and the city must be Mumbai. SCD Type 1 overwrites the old business value instead of creating a second historical row.
# MAGIC
# MAGIC This notebook assumes the Day 1-Day 3 lab is already complete and `customer_cdc_batch_002.csv` has been ingested into the existing Bronze CDC table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Point to the tables created in our lab
# MAGIC
# MAGIC We are not creating or checking the catalog and Bronze tables again. They already exist from the earlier classes.
# MAGIC
# MAGIC Change only the catalog value if your workspace catalog has a different name.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "master_databricks_new"

CUSTOMERS_BRONZE = (
    f"{CATALOG}.abb_retail_bronze.customers_raw"
)
CDC_BRONZE = (
    f"{CATALOG}.abb_retail_bronze.customer_cdc_raw"
)
CUSTOMER_CURRENT = (
    f"{CATALOG}.abb_retail_silver.customer_current"
)

print(f"Customer snapshot: {CUSTOMERS_BRONZE}")
print(f"Customer changes:  {CDC_BRONZE}")
print(f"Silver target:     {CUSTOMER_CURRENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - See the SCD Type 1 problem
# MAGIC
# MAGIC The original customer snapshot says Amit lives in Delhi. The new CDC event says he lives in Mumbai.
# MAGIC
# MAGIC We display both records before writing any SCD code. This makes the business problem visible.

# COMMAND ----------

amit_snapshot_df = (
    spark.table(CUSTOMERS_BRONZE)
    .filter(F.col("customer_id") == "C001")
    .select(
        "customer_id",
        "customer_name",
        "city",
        "loyalty_tier",
        "is_active",
        "updated_at",
    )
)

amit_update_df = (
    spark.table(CDC_BRONZE)
    .filter(F.col("event_id") == "E0002")
    .select(
        "event_id",
        "customer_id",
        "operation",
        "customer_name",
        "city",
        "loyalty_tier",
        "is_active",
        "event_timestamp",
        "sequence_number",
    )
)

print("Original snapshot: Amit is in Delhi")
display(amit_snapshot_df)

print("New CDC event: Amit moved to Mumbai")
display(amit_update_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Give the snapshot and CDC events the same structure
# MAGIC
# MAGIC Spark can combine two DataFrames only when their column structures match.
# MAGIC
# MAGIC The snapshot does not contain an operation, event ID, or sequence number, so we add them. We give every snapshot row sequence `0` because it represents our starting state.

# COMMAND ----------

snapshot_df = (
    spark.table(CUSTOMERS_BRONZE)
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.trim("city").alias("city"),
        F.trim("loyalty_tier").alias("loyalty_tier"),
        F.when(
            F.lower(
                F.trim(F.col("is_active").cast("string"))
            ).isin("true", "1", "yes", "y"),
            F.lit(True),
        ).otherwise(F.lit(False)).alias("is_active"),
        F.lit("SNAPSHOT").alias("operation"),
        F.to_timestamp("updated_at").alias("event_timestamp"),
        F.lit(0).cast("long").alias("sequence_number"),
        F.concat(
            F.lit("SNAPSHOT:"),
            F.col("customer_id"),
        ).alias("source_event_id"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

cdc_df = (
    spark.table(CDC_BRONZE)
    .filter(
        F.upper(F.trim("operation")).isin("INSERT", "UPDATE")
    )
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.trim("city").alias("city"),
        F.trim("loyalty_tier").alias("loyalty_tier"),
        F.when(
            F.lower(
                F.trim(F.col("is_active").cast("string"))
            ).isin("true", "1", "yes", "y"),
            F.lit(True),
        ).otherwise(F.lit(False)).alias("is_active"),
        F.upper(F.trim("operation")).alias("operation"),
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        F.col("sequence_number").cast("long").alias("sequence_number"),
        F.trim("event_id").alias("source_event_id"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Combine all possible customer states
# MAGIC
# MAGIC After `unionByName`, Amit has two candidate records:
# MAGIC
# MAGIC - Delhi from the snapshot with sequence `0`.
# MAGIC - Mumbai from event `E0002` with sequence `2`.
# MAGIC
# MAGIC We have not selected the winner yet.

# COMMAND ----------

candidate_df = snapshot_df.unionByName(cdc_df)

display(
    candidate_df
    .filter(F.col("customer_id") == "C001")
    .orderBy("sequence_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Select the latest record for every customer
# MAGIC
# MAGIC We divide the data by `customer_id` and sort each customer's records from newest to oldest.
# MAGIC
# MAGIC The sequence number is the first ordering rule. If two events have the same sequence number, we use the event timestamp, Bronze ingestion time, and event ID as tie-breakers.

# COMMAND ----------

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
    .withColumn(
        "row_number",
        F.row_number().over(latest_window),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print("The selected SCD Type 1 source record for Amit is Mumbai")
display(
    latest_customer_df
    .filter(F.col("customer_id") == "C001")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Make the selected records available to SQL
# MAGIC
# MAGIC A temporary view gives the DataFrame a SQL name. The view exists only for this Spark session and becomes the source of our Delta `MERGE`.

# COMMAND ----------

latest_customer_df.createOrReplaceTempView(
    "scd_type1_customer_source"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create the Silver current-state table
# MAGIC
# MAGIC This table has one row per customer. It stores the latest values plus the event information used to decide which update won.
# MAGIC
# MAGIC `IF NOT EXISTS` makes the creation safe when the notebook is rerun. This is part of the SCD solution, not a prerequisite check.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Apply SCD Type 1 using Delta MERGE
# MAGIC
# MAGIC The `MERGE` matches source and target using `customer_id`.
# MAGIC
# MAGIC - If the customer exists and the incoming event is newer, update the existing row.
# MAGIC - If the customer does not exist, insert a new row.
# MAGIC - If the same or an older event is processed again, make no business change.

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Step 9 - Prove the SCD Type 1 result
# MAGIC
# MAGIC We now inspect the business outcome rather than checking platform prerequisites.
# MAGIC
# MAGIC Amit must appear once, his city must be Mumbai, and no second Delhi business row should remain in Silver.

# COMMAND ----------

print("Final Silver record for Amit")
display(
    spark.table(CUSTOMER_CURRENT)
    .filter(F.col("customer_id") == "C001")
    .select(
        "customer_id",
        "customer_name",
        "city",
        "last_operation",
        "last_sequence_number",
        "source_event_id",
        "silver_updated_at",
    )
)

print("One current row per customer")
display(
    spark.table(CUSTOMER_CURRENT)
    .groupBy("customer_id")
    .count()
    .orderBy("customer_id")
)

print("Amit's Delhi row count in Silver should be zero")
display(
    spark.table(CUSTOMER_CURRENT)
    .filter(
        (F.col("customer_id") == "C001")
        & (F.col("city") == "Delhi")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Rerun the MERGE and observe idempotency
# MAGIC
# MAGIC Run this complete notebook again without adding another event.
# MAGIC
# MAGIC The selected source event is still `E0002`, sequence `2`. The target already contains the same event, so the `WHEN MATCHED AND` condition is false. The notebook does not insert a second Amit row and does not replace Mumbai with an older value.
# MAGIC
# MAGIC **Final interview statement:** SCD Type 1 keeps one current business row. Our conditional `MERGE` applies only a strictly newer event, so Amit's Delhi value is replaced by Mumbai and rerunning the same source state creates no duplicate customer row.
