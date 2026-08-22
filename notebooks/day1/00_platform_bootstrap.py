# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 - Platform bootstrap
# MAGIC
# MAGIC This one-time notebook uses the Unity Catalog catalog already created for the workspace. It creates the ABB Retail schemas and governed volumes required by the remaining lessons.

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
        "For this beginner lab, use a catalog name containing "
        "letters, numbers, and underscores only."
    )

available_catalogs = {
    row[0] for row in spark.sql("SHOW CATALOGS").collect()
}

if CATALOG not in available_catalogs:
    raise ValueError(
        f"Catalog {CATALOG!r} is not visible. "
        f"Available catalogs: {sorted(available_catalogs)}"
    )

spark.sql(f"USE CATALOG {CATALOG}")
print(f"PASS: Using existing catalog {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why schemas?
# MAGIC
# MAGIC A catalog is the governed building. Schemas are separate departments. Bronze preserves raw deliveries, Silver holds cleaned business data, Gold serves reporting, and Ops holds pipeline state.

# COMMAND ----------

schema_definitions = {
    "abb_retail_bronze": "Raw source data for the ABB Retail interview series",
    "abb_retail_silver": "Cleaned and validated ABB Retail data",
    "abb_retail_gold": "Business-ready ABB Retail reporting data",
    "abb_retail_ops": "ABB Retail checkpoints and operational state",
}

for schema_name, comment in schema_definitions.items():
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
        {CATALOG}.{schema_name}
        COMMENT '{comment}'
        """
    )
    print(f"READY: {CATALOG}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why volumes?
# MAGIC
# MAGIC incoming_files is the governed receiving dock for CSV deliveries. pipeline_state stores Auto Loader schema history and checkpoints. Business files and operational state have different responsibilities and should not share a directory.

# COMMAND ----------

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    {CATALOG}.abb_retail_bronze.incoming_files
    COMMENT 'Landing area for incoming ABB Retail CSV files'
    """
)

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    {CATALOG}.abb_retail_ops.pipeline_state
    COMMENT 'Auto Loader schemas and checkpoints for ABB Retail'
    """
)

LANDING_ROOT = (
    f"/Volumes/{CATALOG}/abb_retail_bronze/incoming_files"
)
STATE_ROOT = (
    f"/Volumes/{CATALOG}/abb_retail_ops/pipeline_state"
)

print(f"Landing root: {LANDING_ROOT}")
print(f"State root:   {STATE_ROOT}")

# COMMAND ----------

required_schemas = set(schema_definitions)
actual_schemas = {
    row[0]
    for row in spark.sql(
        f"SHOW SCHEMAS IN {CATALOG}"
    ).collect()
}

missing_schemas = required_schemas - actual_schemas
if missing_schemas:
    raise AssertionError(
        f"Missing schemas: {sorted(missing_schemas)}"
    )

dbutils.fs.ls(LANDING_ROOT)
dbutils.fs.ls(STATE_ROOT)

print("PASS: Day 1 governed environment is ready.")

