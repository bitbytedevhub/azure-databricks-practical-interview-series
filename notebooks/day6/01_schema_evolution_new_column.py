# Databricks notebook source
# MAGIC %md
# MAGIC # Day 6 - Schema evolution modes
# MAGIC
# MAGIC **Interview question:** How is schema evolution handled when a new column is added mid-pipeline, and what can break in production?
# MAGIC
# MAGIC Session flow:
# MAGIC 1. Compare the five Auto Loader schema-evolution modes.
# MAGIC 2. Run the same `email` change through each new-column policy.
# MAGIC 3. Demonstrate type widening separately.
# MAGIC 4. Show that Bronze evolution does not automatically update Silver.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "master_databricks_new"
BRONZE_SCHEMA = "abb_retail_bronze"
SILVER_SCHEMA = "abb_retail_silver"
OPS_SCHEMA = "abb_retail_ops"

DEMO_SOURCE_ROOT = (
    f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/incoming_files/"
    "day6_schema_evolution"
)

DEMO_STATE_ROOT = (
    f"/Volumes/{CATALOG}/{OPS_SCHEMA}/pipeline_state/"
    "day6_schema_evolution"
)

MODE_CONFIGS = {
    "rescue": {
        "mode": "rescue",
        "table": f"{CATALOG}.{BRONZE_SCHEMA}.day6_rescue_demo",
        "use_rescued_data": True,
    },
    "none": {
        "mode": "none",
        "table": f"{CATALOG}.{BRONZE_SCHEMA}.day6_none_demo",
        "use_rescued_data": False,
    },
    "fail": {
        "mode": "failOnNewColumns",
        "table": f"{CATALOG}.{BRONZE_SCHEMA}.day6_fail_demo",
        "use_rescued_data": True,
    },
    "add": {
        "mode": "addNewColumns",
        "table": f"{CATALOG}.{BRONZE_SCHEMA}.day6_add_columns_demo",
        "use_rescued_data": True,
    },
}

for key, config in MODE_CONFIGS.items():
    config["source"] = f"{DEMO_SOURCE_ROOT}/{key}"
    config["schema"] = f"{DEMO_STATE_ROOT}/{key}/schema"
    config["checkpoint"] = f"{DEMO_STATE_ROOT}/{key}/checkpoint"

TYPE_SOURCE_PATH = f"{DEMO_SOURCE_ROOT}/type_widening"
TYPE_SCHEMA_PATH = f"{DEMO_STATE_ROOT}/type_widening/schema"
TYPE_CHECKPOINT_PATH = f"{DEMO_STATE_ROOT}/type_widening/checkpoint"
TYPE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.day6_type_widening_demo"

SILVER_DEMO_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.day6_customer_contract_demo"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema-evolution modes
# MAGIC
# MAGIC | Mode | New-column behavior |
# MAGIC |---|---|
# MAGIC | `rescue` | Continue and store the field in `_rescued_data`. |
# MAGIC | `none` | Continue and ignore the field unless rescued-data handling is configured. |
# MAGIC | `failOnNewColumns` | Fail without automatically updating the stored schema. |
# MAGIC | `addNewColumns` | Update the stored schema, stop once, and use the new schema after restart. |
# MAGIC | `addNewColumnsWithTypeWidening` | Perform `addNewColumns` behavior and widen supported types. |
# MAGIC
# MAGIC When Auto Loader infers the schema, `addNewColumns` is the default. When you provide a fixed schema, `none` is the default. Set the policy explicitly in production.
# MAGIC
# MAGIC Current guidance: use a fixed schema for a known contract, `addNewColumns` for unknown additive fields, `addNewColumnsWithTypeWidening` for compatible type changes, `failOnNewColumns` for a strict contract, and `VARIANT` for highly unpredictable semi-structured data such as JSON.
# MAGIC
# MAGIC **Scope:** This notebook demonstrates direct Auto Loader Structured Streaming into Delta. Lakeflow Spark Declarative Pipelines streaming tables support merge evolution by default and can restart automatically, so their managed behavior is not identical to this direct-stream demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reset only the isolated Day 6 demo
# MAGIC
# MAGIC This reset removes only Day 6 demo state. It does not touch the original ingestion, SCD Type 1, or SCD Type 2 tables.
# MAGIC
# MAGIC Do not delete production checkpoints to replay data.

# COMMAND ----------

for config in MODE_CONFIGS.values():
    spark.sql(f"DROP TABLE IF EXISTS {config['table']}")
    dbutils.fs.rm(config["source"], True)
    dbutils.fs.rm(config["schema"], True)
    dbutils.fs.rm(config["checkpoint"], True)

spark.sql(f"DROP TABLE IF EXISTS {TYPE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_DEMO_TABLE}")

dbutils.fs.rm(TYPE_SOURCE_PATH, True)
dbutils.fs.rm(TYPE_SCHEMA_PATH, True)
dbutils.fs.rm(TYPE_CHECKPOINT_PATH, True)

print("Day 6 isolated demo state is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the source deliveries
# MAGIC
# MAGIC The first CSV has the original schema. The second adds `email`.
# MAGIC
# MAGIC `dbutils.fs.put` is used only to keep the lab self-contained. In production, these files usually arrive from an upstream application or ADLS landing process.

# COMMAND ----------

BASE_CUSTOMER_CSV = """customer_id,customer_name,city,is_active,updated_at
C001,Amit Sharma,Delhi,true,2026-01-01T09:00:00
"""

NEW_COLUMN_CUSTOMER_CSV = """customer_id,customer_name,city,is_active,email,updated_at
C002,Nisha Verma,Kolkata,true,nisha.verma@example.com,2026-01-02T09:00:00
"""

CUSTOMER_FIXED_SCHEMA = """
customer_id STRING,
customer_name STRING,
city STRING,
is_active STRING,
updated_at STRING
"""


def deliver_base_file(config):
    dbutils.fs.mkdirs(config["source"])
    dbutils.fs.put(
        f"{config['source']}/customers_batch_001.csv",
        BASE_CUSTOMER_CSV,
        True,
    )


def deliver_new_column_file(config):
    dbutils.fs.mkdirs(config["source"])
    dbutils.fs.put(
        f"{config['source']}/customers_batch_002.csv",
        NEW_COLUMN_CUSTOMER_CSV,
        True,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reusable CSV Auto Loader function
# MAGIC
# MAGIC `schemaEvolutionMode` controls the reader response. `mergeSchema` separately allows the Delta target table to add accepted columns.
# MAGIC
# MAGIC This is Auto Loader reading CSV files through `cloudFiles`. The standalone Spark `from_csv()` parser does not provide automatic schema evolution.

# COMMAND ----------

def run_csv_mode(config):
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", config["schema"])
        .option(
            "cloudFiles.schemaEvolutionMode",
            config["mode"],
        )
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
    )

    # Make `none` an explicit fixed-contract demonstration.
    if config["mode"] == "none":
        reader = reader.schema(CUSTOMER_FIXED_SCHEMA)

    if config["use_rescued_data"]:
        reader = reader.option(
            "rescuedDataColumn",
            "_rescued_data",
        )

    source_df = reader.load(config["source"])

    bronze_df = source_df.withColumn(
        "_source_file_name",
        F.col("_metadata.file_name"),
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config["table"])
    )

    # Appropriate for this interactive lab. In Lakeflow Jobs, the Jobs
    # service tracks active streaming queries without awaitTermination().
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mode: `rescue`
# MAGIC
# MAGIC Expected result: the stream continues and `email` is stored inside `_rescued_data` instead of becoming a normal column.

# COMMAND ----------

rescue_config = MODE_CONFIGS["rescue"]

deliver_base_file(rescue_config)
run_csv_mode(rescue_config)

deliver_new_column_file(rescue_config)
run_csv_mode(rescue_config)

display(
    spark.table(rescue_config["table"])
    .select(
        "customer_id",
        "customer_name",
        "_rescued_data",
        "_source_file_name",
    )
    .orderBy("customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Mode: `none`
# MAGIC
# MAGIC Expected result: the stream continues and `email` is ignored. This demo supplies the original fixed schema and intentionally omits `rescuedDataColumn` so the contract is unambiguous.

# COMMAND ----------

none_config = MODE_CONFIGS["none"]

deliver_base_file(none_config)
run_csv_mode(none_config)

deliver_new_column_file(none_config)
run_csv_mode(none_config)

display(
    spark.table(none_config["table"])
    .orderBy("customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Mode: `failOnNewColumns`
# MAGIC
# MAGIC Expected result: the changed file is rejected and the stored schema is not automatically updated.

# COMMAND ----------

fail_config = MODE_CONFIGS["fail"]

deliver_base_file(fail_config)
run_csv_mode(fail_config)

deliver_new_column_file(fail_config)

try:
    run_csv_mode(fail_config)
except Exception as error:
    print("EXPECTED: failOnNewColumns rejected the new column")
    print(str(error)[:1_000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Mode: `addNewColumns`
# MAGIC
# MAGIC Expected result:
# MAGIC 1. The first attempt after `email` arrives updates the saved schema and stops.
# MAGIC 2. The restarted query processes the file.
# MAGIC 3. `mergeSchema` adds `email` to the Delta table.
# MAGIC
# MAGIC In production, Databricks recommends configuring the Lakeflow Job to restart automatically after this expected schema-change failure.

# COMMAND ----------

add_config = MODE_CONFIGS["add"]

deliver_base_file(add_config)
run_csv_mode(add_config)

deliver_new_column_file(add_config)

try:
    run_csv_mode(add_config)
except Exception as error:
    print("EXPECTED: Auto Loader learned email and stopped once")
    print(str(error)[:1_000])

# COMMAND ----------

run_csv_mode(add_config)

spark.table(add_config["table"]).printSchema()

display(
    spark.table(add_config["table"])
    .select(
        "customer_id",
        "customer_name",
        "email",
        "_source_file_name",
    )
    .orderBy("customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Mode: `addNewColumnsWithTypeWidening`
# MAGIC
# MAGIC This Public Preview feature requires Databricks Runtime 16.4 or above. The customer CSV demo reads source fields as strings, so this separate Parquet example changes `amount` from `INT` to `LONG`.
# MAGIC
# MAGIC Unsupported changes such as `INT` to `STRING` are not widened. With rescued-data handling configured, those values are routed to `_rescued_data`.

# COMMAND ----------

def run_type_widening_demo():
    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", TYPE_SCHEMA_PATH)
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumnsWithTypeWidening",
        )
        .load(TYPE_SOURCE_PATH)
    )

    query = (
        source_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", TYPE_CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TYPE_TABLE)
    )

    query.awaitTermination()

# COMMAND ----------

(
    spark.createDataFrame(
        [(1, 100)],
        "id INT, amount INT",
    )
    .write
    .mode("append")
    .parquet(TYPE_SOURCE_PATH)
)

run_type_widening_demo()

spark.sql(
    f"""
    ALTER TABLE {TYPE_TABLE}
    SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
    """
)

# COMMAND ----------

(
    spark.createDataFrame(
        [(2, 3_000_000_000)],
        "id INT, amount BIGINT",
    )
    .write
    .mode("append")
    .parquet(TYPE_SOURCE_PATH)
)

try:
    run_type_widening_demo()
except Exception as error:
    print("EXPECTED: the evolved schema requires a restart")
    print(str(error)[:1_000])

run_type_widening_demo()

spark.table(TYPE_TABLE).printSchema()
display(spark.table(TYPE_TABLE).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Bronze evolution does not automatically update Silver
# MAGIC
# MAGIC The first write represents the old Silver contract and intentionally omits `email`.

# COMMAND ----------

bronze_add_df = spark.table(add_config["table"])

(
    bronze_add_df
    .select(
        "customer_id",
        "customer_name",
        "city",
        "is_active",
        "updated_at",
    )
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_DEMO_TABLE)
)

spark.table(SILVER_DEMO_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC After review and approval, update the Silver mapping deliberately. `overwriteSchema` is used only because this isolated teaching table is rebuilt from the complete demo dataset.
# MAGIC
# MAGIC Current per-operation alternatives include `MERGE WITH SCHEMA EVOLUTION` or `.withSchemaEvolution()` in Databricks Runtime 15.4 LTS and above, and `INSERT WITH SCHEMA EVOLUTION` in Databricks Runtime 18.1 and above. For streaming writes, per-write `mergeSchema` remains correct. Avoid the legacy session-wide auto-merge setting in production.

# COMMAND ----------

(
    bronze_add_df
    .select(
        "customer_id",
        "customer_name",
        "city",
        "is_active",
        "email",
        "updated_at",
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_DEMO_TABLE)
)

spark.table(SILVER_DEMO_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final interview answer
# MAGIC
# MAGIC When a new column arrives mid-pipeline, first check the configured Auto Loader `schemaEvolutionMode`:
# MAGIC
# MAGIC - With inferred schema, `addNewColumns` is the default. With a provided schema, `none` is the default. Set the policy explicitly.
# MAGIC
# MAGIC - `rescue`: continue and store the field in `_rescued_data`.
# MAGIC - `none`: continue and ignore the field unless rescued-data handling is configured.
# MAGIC - `failOnNewColumns`: fail and require a deliberate contract update.
# MAGIC - `addNewColumns`: update the schema location, stop once, and use the evolved schema after restart. Configure Lakeflow Jobs to restart automatically in production.
# MAGIC - `addNewColumnsWithTypeWidening`: also widen supported data types.
# MAGIC
# MAGIC The Delta destination must separately permit the change, usually through per-write `mergeSchema` or another approved schema update. Prefer per-operation evolution over the legacy session-wide configuration.
# MAGIC
# MAGIC Bronze evolution does not automatically evolve Silver or downstream consumers. Explicit selections can omit the field; MERGE can ignore or reject it depending on assignments and evolution settings; streams reading an evolved Delta table must restart; schema updates can conflict with concurrent writes; and views, materialized views, BI models, APIs, and governance contracts may require changes.
