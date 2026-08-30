# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 - Interview Problem: Implement SCD Type 2
# MAGIC
# MAGIC **Business problem:** Amit originally lived in Delhi. CDC event `E0002` says that he moved to Mumbai. The business wants the old Delhi version and the new Mumbai version.
# MAGIC
# MAGIC **Expected solution:** Close the Delhi record and insert Mumbai as the current record. Silver must preserve two versions of Amit without creating duplicates when the notebook is rerun.
# MAGIC
# MAGIC This notebook assumes the Day 1-Day 4 lab is complete and `E0002` is already available in the Bronze CDC table. It creates a separate SCD Type 2 target and does not modify the Day 4 `customer_current` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Point to the existing Bronze tables and the new history target
# MAGIC
# MAGIC The platform setup is already complete. We only identify the tables needed for this interview problem.
# MAGIC
# MAGIC SCD Type 1 uses `customer_current` because it keeps one current row. SCD Type 2 uses `customer_history` because one customer can have several versions.

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
CUSTOMER_HISTORY = (
    f"{CATALOG}.abb_retail_silver.customer_history"
)

print(f"Customer snapshot: {CUSTOMERS_BRONZE}")
print(f"Customer changes:  {CDC_BRONZE}")
print(f"History target:     {CUSTOMER_HISTORY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Display the history problem
# MAGIC
# MAGIC The original snapshot says Amit lives in Delhi. Event `E0002` says he moved to Mumbai.
# MAGIC
# MAGIC SCD Type 1 would replace Delhi. SCD Type 2 must retain Delhi as an expired version and add Mumbai as the new current version.

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

print("Original version: Amit lives in Delhi")
display(amit_snapshot_df)

print("New event: Amit moved to Mumbai")
display(amit_update_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Create the SCD Type 2 Silver table
# MAGIC
# MAGIC The business key `customer_id` repeats because one customer can have several versions. `customer_sk` uniquely identifies each version.
# MAGIC
# MAGIC - `effective_from` tells us when a version became valid.
# MAGIC - `effective_to` tells us when it stopped being valid.
# MAGIC - `is_current` identifies the active version.
# MAGIC - A current row has `effective_to = NULL`.

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CUSTOMER_HISTORY} (
        customer_sk STRING,
        customer_id STRING,
        customer_name STRING,
        city STRING,
        loyalty_tier STRING,
        is_active BOOLEAN,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN,
        operation STRING,
        sequence_number BIGINT,
        source_event_id STRING,
        source_file STRING,
        bronze_ingested_at TIMESTAMP,
        silver_created_at TIMESTAMP,
        silver_updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Customer history maintained using SCD Type 2'
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Convert the snapshot into initial history versions
# MAGIC
# MAGIC The snapshot is the starting state. We give it sequence `0`, mark it current, and leave its end time empty.
# MAGIC
# MAGIC The surrogate key is a deterministic SHA-256 hash of the customer ID and the word `SNAPSHOT`. A rerun calculates the same key, so it cannot create another copy of the same starting version.

# COMMAND ----------

initial_snapshot_df = (
    spark.table(CUSTOMERS_BRONZE)
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.trim(F.col("customer_id")),
                F.lit("SNAPSHOT"),
            ),
            256,
        ).alias("customer_sk"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("customer_name")).alias("customer_name"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("loyalty_tier")).alias("loyalty_tier"),
        F.when(
            F.lower(
                F.trim(F.col("is_active").cast("string"))
            ).isin("true", "1", "yes", "y"),
            F.lit(True),
        ).otherwise(F.lit(False)).alias("is_active"),
        F.to_timestamp("updated_at").alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.lit("SNAPSHOT").alias("operation"),
        F.lit(0).cast("long").alias("sequence_number"),
        F.concat(
            F.lit("SNAPSHOT:"),
            F.trim(F.col("customer_id")),
        ).alias("source_event_id"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

display(
    initial_snapshot_df
    .filter(F.col("customer_id") == "C001")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Load the initial versions safely
# MAGIC
# MAGIC This merge inserts a snapshot version only when its deterministic surrogate key is missing. If the notebook runs again, the same snapshot key matches and no duplicate Delhi row is inserted.
# MAGIC
# MAGIC Notice that we do not update a matching snapshot row. If Delhi was already closed by a later event, rerunning the initialization must not make Delhi current again.

# COMMAND ----------

initial_snapshot_df.createOrReplaceTempView(
    "initial_customer_history_source"
)

spark.sql(
    f"""
    MERGE INTO {CUSTOMER_HISTORY} AS target
    USING initial_customer_history_source AS source
      ON target.customer_sk = source.customer_sk

    WHEN NOT MATCHED THEN INSERT (
      customer_sk,
      customer_id,
      customer_name,
      city,
      loyalty_tier,
      is_active,
      effective_from,
      effective_to,
      is_current,
      operation,
      sequence_number,
      source_event_id,
      source_file,
      bronze_ingested_at,
      silver_created_at,
      silver_updated_at
    ) VALUES (
      source.customer_sk,
      source.customer_id,
      source.customer_name,
      source.city,
      source.loyalty_tier,
      source.is_active,
      source.effective_from,
      source.effective_to,
      source.is_current,
      source.operation,
      source.sequence_number,
      source.source_event_id,
      source.source_file,
      source.bronze_ingested_at,
      current_timestamp(),
      current_timestamp()
    )
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Normalize and deduplicate the CDC event
# MAGIC
# MAGIC This lesson isolates event `E0002` so we can see one complete SCD Type 2 change clearly.
# MAGIC
# MAGIC We normalize strings and data types, then keep one row per event ID. Auto Loader checkpointing prevents the same discovered file from being read again, while event-ID deduplication protects us when identical business content arrives under a different filename.

# COMMAND ----------

cdc_source_df = (
    spark.table(CDC_BRONZE)
    .filter(F.col("event_id") == "E0002")
    .filter(
        F.upper(F.trim(F.col("operation")))
        .isin("INSERT", "UPDATE")
    )
    .select(
        F.trim(F.col("event_id")).alias("source_event_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("customer_name")).alias("customer_name"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("loyalty_tier")).alias("loyalty_tier"),
        F.when(
            F.lower(
                F.trim(F.col("is_active").cast("string"))
            ).isin("true", "1", "yes", "y"),
            F.lit(True),
        ).otherwise(F.lit(False)).alias("is_active"),
        F.upper(F.trim(F.col("operation"))).alias("operation"),
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        F.col("sequence_number").cast("long").alias("sequence_number"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )
)

event_deduplication_window = (
    Window
    .partitionBy("source_event_id")
    .orderBy(
        F.desc("bronze_ingested_at"),
        F.desc("source_file"),
    )
)

deduplicated_cdc_df = (
    cdc_source_df
    .withColumn(
        "event_row_number",
        F.row_number().over(event_deduplication_window),
    )
    .filter(F.col("event_row_number") == 1)
    .drop("event_row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Remove events that already created a history version
# MAGIC
# MAGIC Every accepted event is stored as `source_event_id` in Silver. A left-anti join keeps only events that do not already exist in the history table.
# MAGIC
# MAGIC On the first run, `E0002` remains. On the second run, it is removed before the final merge.

# COMMAND ----------

processed_events_df = (
    spark.table(CUSTOMER_HISTORY)
    .select("source_event_id")
    .distinct()
)

unprocessed_cdc_df = (
    deduplicated_cdc_df
    .join(
        processed_events_df,
        on="source_event_id",
        how="left_anti",
    )
)

display(unprocessed_cdc_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Accept only a strictly newer event
# MAGIC
# MAGIC We compare each unprocessed event with the customer's current Silver version.
# MAGIC
# MAGIC The ordering rules are applied in priority order: a higher sequence wins; when sequences tie, a later event timestamp wins; when both tie, the greater event ID is the deterministic final tie-breaker.
# MAGIC
# MAGIC This prevents a delayed older event from closing the latest customer version.

# COMMAND ----------

current_customer_df = (
    spark.table(CUSTOMER_HISTORY)
    .filter(F.col("is_current") == True)
    .select(
        F.col("customer_id").alias("target_customer_id"),
        F.col("sequence_number").alias("target_sequence_number"),
        F.col("effective_from").alias("target_event_timestamp"),
        F.col("source_event_id").alias("target_event_id"),
    )
)

cdc_with_current_df = (
    unprocessed_cdc_df
    .join(
        current_customer_df,
        unprocessed_cdc_df.customer_id
        == current_customer_df.target_customer_id,
        "left",
    )
)

eligible_cdc_df = (
    cdc_with_current_df
    .filter(
        F.col("target_customer_id").isNull()
        | (
            F.col("sequence_number")
            > F.col("target_sequence_number")
        )
        | (
            (F.col("sequence_number") == F.col("target_sequence_number"))
            & (F.col("event_timestamp") > F.col("target_event_timestamp"))
        )
        | (
            (F.col("sequence_number") == F.col("target_sequence_number"))
            & (F.col("event_timestamp") == F.col("target_event_timestamp"))
            & (F.col("source_event_id") > F.col("target_event_id"))
        )
    )
    .withColumn(
        "customer_sk",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("customer_id"),
                F.col("source_event_id"),
            ),
            256,
        ),
    )
)

display(eligible_cdc_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Stage two actions for an existing customer
# MAGIC
# MAGIC SCD Type 2 must perform two actions for Amit:
# MAGIC
# MAGIC 1. Match and close the current Delhi row.
# MAGIC 2. Deliberately avoid a match and insert the new Mumbai row.
# MAGIC
# MAGIC We therefore stage two copies of an existing-customer event. The first uses the real customer ID as `merge_customer_id`; the second uses `NULL`. A brand-new customer needs only the insert copy.

# COMMAND ----------

existing_customer_events_df = (
    eligible_cdc_df
    .filter(F.col("target_customer_id").isNotNull())
)

source_columns = [
    "customer_sk",
    "customer_id",
    "customer_name",
    "city",
    "loyalty_tier",
    "is_active",
    "operation",
    "event_timestamp",
    "sequence_number",
    "source_event_id",
    "source_file",
    "bronze_ingested_at",
]

close_current_rows_df = (
    existing_customer_events_df
    .select(
        F.col("customer_id").alias("merge_customer_id"),
        *source_columns,
    )
)

insert_new_version_rows_df = (
    eligible_cdc_df
    .select(
        F.lit(None).cast("string").alias("merge_customer_id"),
        *source_columns,
    )
)

staged_scd2_df = (
    close_current_rows_df
    .unionByName(insert_new_version_rows_df)
)

display(
    staged_scd2_df
    .select(
        "merge_customer_id",
        "customer_id",
        "city",
        "sequence_number",
        "source_event_id",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Apply SCD Type 2 with Delta MERGE
# MAGIC
# MAGIC The copy with `merge_customer_id = C001` matches the current Delhi row. The update closes Delhi by setting its end time and changing `is_current` to false.
# MAGIC
# MAGIC The copy with `merge_customer_id = NULL` cannot match. It reaches `WHEN NOT MATCHED` and inserts Mumbai with `is_current = true` and no end time.

# COMMAND ----------

staged_scd2_df.createOrReplaceTempView(
    "scd_type2_staged_source"
)

spark.sql(
    f"""
    MERGE INTO {CUSTOMER_HISTORY} AS target
    USING scd_type2_staged_source AS source
      ON target.customer_id = source.merge_customer_id
     AND target.is_current = TRUE

    WHEN MATCHED AND (
         source.sequence_number > target.sequence_number
      OR (
           source.sequence_number = target.sequence_number
       AND source.event_timestamp > target.effective_from
      )
      OR (
           source.sequence_number = target.sequence_number
       AND source.event_timestamp = target.effective_from
       AND source.source_event_id > target.source_event_id
      )
    ) THEN UPDATE SET
      target.effective_to = source.event_timestamp,
      target.is_current = FALSE,
      target.silver_updated_at = current_timestamp()

    WHEN NOT MATCHED THEN INSERT (
      customer_sk,
      customer_id,
      customer_name,
      city,
      loyalty_tier,
      is_active,
      effective_from,
      effective_to,
      is_current,
      operation,
      sequence_number,
      source_event_id,
      source_file,
      bronze_ingested_at,
      silver_created_at,
      silver_updated_at
    ) VALUES (
      source.customer_sk,
      source.customer_id,
      source.customer_name,
      source.city,
      source.loyalty_tier,
      source.is_active,
      source.event_timestamp,
      NULL,
      TRUE,
      source.operation,
      source.sequence_number,
      source.source_event_id,
      source.source_file,
      source.bronze_ingested_at,
      current_timestamp(),
      current_timestamp()
    )
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Prove that history was preserved
# MAGIC
# MAGIC Amit must now have two versions. Delhi is historical and has an end time. Mumbai is current and has a null end time.
# MAGIC
# MAGIC We use an exclusive end boundary: the Delhi version is valid before the Mumbai event time, while the Mumbai version becomes valid at that exact time. This prevents both versions from being active at the boundary.

# COMMAND ----------

display(
    spark.table(CUSTOMER_HISTORY)
    .filter(F.col("customer_id") == "C001")
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "city",
        "effective_from",
        "effective_to",
        "is_current",
        "sequence_number",
        "source_event_id",
    )
    .orderBy("effective_from")
)

print("Current customer version: Amit must be in Mumbai")
display(
    spark.table(CUSTOMER_HISTORY)
    .filter(
        (F.col("customer_id") == "C001")
        & (F.col("is_current") == True)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Rerun and explain idempotency
# MAGIC
# MAGIC Run the complete notebook again. The snapshot merge does not reopen Delhi, and the left-anti join removes `E0002` because that event already created a history version.
# MAGIC
# MAGIC **Final interview statement:** SCD Type 2 preserves history by expiring the current version and inserting a new version. Auto Loader checkpointing protects file discovery, while event-ID deduplication and deterministic version keys protect the Silver history from repeated business events.
# MAGIC
# MAGIC **Production boundary:** This lesson deliberately processes one event for Amit. When several changes for the same customer arrive in one microbatch, process them in sequence order or rebuild the affected timeline so that intermediate versions are not lost.
