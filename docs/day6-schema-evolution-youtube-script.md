# Day 6: Schema Evolution Modes in Azure Databricks — Inquiry-led explainer, ~22 minutes

## Hook options

### Hook 1 — Recommended

Yesterday, the customer file did not contain `email`. Today, the source adds it without warning. Should the pipeline stop, ignore the field, rescue it, or add it to the table? Databricks can do all four. The result depends on one setting. Let us run every mode and see exactly what happens.

### Hook 2 — Interview challenge

An interviewer asks: “A new column arrives while your pipeline is running. What happens?” Saying “Databricks handles schema evolution” is incomplete. Which mode is configured? Does the stream fail? Does the Delta table evolve? Does Silver receive the column? We will answer each question with code.

### Hook 3 — Misconception flip

Schema evolution does not always mean automatically adding a column. One mode adds it, one hides it in rescued data, one ignores it, and one deliberately fails. Today we will use the same `email` column to prove the difference.

## Complete script

### 【0:00】Hook and session goal

**PRESENTER**

“Welcome back to the Azure Databricks Practical Interview Series.

Yesterday, our customer file did not contain `email`. Today, the source sends the same customer structure with one additional column.

What should the pipeline do?

Should it stop? Should it ignore `email`? Should it preserve the field somewhere else? Or should it add `email` to the table?

Azure Databricks can perform all of those behaviors. The result depends on the schema-evolution mode we select.

The goal of this session is very specific.

First, we will understand the available Auto Loader schema-evolution modes.

Second, we will run the same new-column scenario through each mode.

Finally, we will answer the interview question: **How is schema evolution handled when a new column is added mid-pipeline, and what can break in production?**”

[ON SCREEN]

```text
SESSION GOAL
1. Understand the modes
2. Demonstrate each behavior
3. Answer the production interview question
```

### 【1:15】The five Auto Loader modes

**PRESENTER**

“Before writing code, let us see the choices.

For a new column, Auto Loader provides five relevant modes.”

[ON SCREEN]

| Mode | What happens when a new column arrives? |
|---|---|
| `rescue` | The stream continues and stores the field in `_rescued_data`. |
| `none` | The stream continues and ignores the field unless rescued-data handling is configured. |
| `failOnNewColumns` | The stream fails and does not update its stored schema. |
| `addNewColumns` | Auto Loader updates its schema, stops once, and uses the new schema after restart. |
| `addNewColumnsWithTypeWidening` | Performs `addNewColumns` behavior and also widens supported data types. |

**PRESENTER**

“These modes are different production policies.

`rescue` preserves unexpected data. `none` continues with the existing columns. `failOnNewColumns` enforces a strict contract. `addNewColumns` accepts additive fields. `addNewColumnsWithTypeWidening` also handles supported type promotions.

There is no universally correct mode. The correct choice depends on the data contract.

Two defaults are important in an interview. When we allow Auto Loader to infer the schema, the default mode is `addNewColumns`. When we provide a fixed schema, the default mode is `none`. Also, `addNewColumns` cannot be combined with a fixed schema, although schema hints can still be used.

The current Databricks recommendation is straightforward: use a fixed schema when the contract is known, `addNewColumns` for unknown additive fields, `addNewColumnsWithTypeWidening` when compatible type changes are expected, and `failOnNewColumns` for a strict contract. If semi-structured data such as JSON is highly unpredictable, consider ingesting it as a `VARIANT` column instead of continuously widening a relational table.

One scope note: today we are demonstrating direct Auto Loader Structured Streaming into a Delta table. A Lakeflow Spark Declarative Pipelines streaming table manages evolution differently: merge schema evolution is supported by default and the pipeline can restart automatically. Do not apply every manual-restart statement from this direct-stream demo word for word to a managed streaming table.”

### 【3:00】Create an isolated demonstration lab

**PRESENTER**

“To keep the lesson focused, we will not depend on the customer tables used by the SCD lessons.

Each mode receives its own source folder, schema location, checkpoint, and Bronze table. If every mode shared the same state, the demonstrations would interfere with one another.”

[ON SCREEN]

```python
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
```

### 【4:30】Reset only the isolated Day 6 demo

**PRESENTER**

“Auto Loader remembers earlier executions, so the demonstration needs a clean state.

This reset removes only Day 6 demo folders and tables. It does not touch our SCD tables or original ingestion pipeline.

This is a lab reset. In production, we do not delete checkpoints to replay pipelines.”

[ON SCREEN]

```python
for config in MODE_CONFIGS.values():
    spark.sql(f"DROP TABLE IF EXISTS {config['table']}")
    dbutils.fs.rm(config["source"], True)
    dbutils.fs.rm(config["schema"], True)
    dbutils.fs.rm(config["checkpoint"], True)

TYPE_SOURCE_PATH = f"{DEMO_SOURCE_ROOT}/type_widening"
TYPE_SCHEMA_PATH = f"{DEMO_STATE_ROOT}/type_widening/schema"
TYPE_CHECKPOINT_PATH = f"{DEMO_STATE_ROOT}/type_widening/checkpoint"
TYPE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.day6_type_widening_demo"
SILVER_DEMO_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.day6_customer_contract_demo"
)

spark.sql(f"DROP TABLE IF EXISTS {TYPE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_DEMO_TABLE}")
dbutils.fs.rm(TYPE_SOURCE_PATH, True)
dbutils.fs.rm(TYPE_SCHEMA_PATH, True)
dbutils.fs.rm(TYPE_CHECKPOINT_PATH, True)

print("Day 6 isolated demo state is ready")
```

### 【5:30】Create the before and after CSV files

**PRESENTER**

“The first file represents yesterday and does not contain `email`. The second represents today and adds `email`.

The notebook creates these files only to keep the lab self-contained. In production, they normally arrive from ADLS or another upstream system.”

[ON SCREEN]

```python
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
```

### 【6:30】Create one reusable Auto Loader function

**PRESENTER**

“The ingestion code stays the same for every demonstration. Only the mode and isolated paths change.

The reader uses `schemaEvolutionMode`. The writer uses `mergeSchema` because Auto Loader’s schema and the Delta target schema are separate contracts.”

[ON SCREEN]

```python
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

    # Make the `none` demonstration an explicit fixed-contract example.
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

    # Appropriate for this interactive lab. Lakeflow Jobs tracks the
    # streaming query itself, so production Jobs do not need this call.
    query.awaitTermination()
```

**PRESENTER**

“The schema learned by Auto Loader is stored under `_schemas` inside `schemaLocation`. That reader schema is separate from the schema of the Delta destination.

Also, `cloudFiles.format = csv` means Auto Loader is managing the file schema. The standalone Spark `from_csv()` parser does not provide automatic schema evolution. Those two cases should not be confused in an interview.

`awaitTermination()` is useful here because we are running an interactive teaching notebook and want the failure to appear in the cell. In a production Lakeflow Job, Databricks advises allowing the Jobs service to track the streaming query instead of blocking the notebook with `awaitTermination()`.”

### 【8:00】Demo 1 — `rescue`

**PRESENTER**

“First, deliver the original file to establish the schema. Then deliver the file containing `email`.”

[ON SCREEN]

```python
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
```

**PRESENTER**

“The stream continues. `email` does not become a normal column. Its value is preserved inside `_rescued_data`.

Use `rescue` when uninterrupted landing and data preservation are more important than automatically exposing every unexpected field.”

### 【10:00】Demo 2 — `none`

**PRESENTER**

“With `none`, we provide the original schema as a fixed contract and then deliver the changed file.”

[ON SCREEN]

```python
none_config = MODE_CONFIGS["none"]

deliver_base_file(none_config)
run_csv_mode(none_config)

deliver_new_column_file(none_config)
run_csv_mode(none_config)

display(
    spark.table(none_config["table"])
    .orderBy("customer_id")
)
```

**PRESENTER**

“The stream continues, but `email` is ignored because the fixed schema does not contain it and we deliberately did not configure a rescued-data column.

The job can remain green while the new field is absent. If `rescuedDataColumn` were configured with `none`, the field could be rescued instead.”

### 【11:30】Demo 3 — `failOnNewColumns`

**PRESENTER**

“With `failOnNewColumns`, the source cannot introduce a field without approval.”

[ON SCREEN]

```python
fail_config = MODE_CONFIGS["fail"]

deliver_base_file(fail_config)
run_csv_mode(fail_config)

deliver_new_column_file(fail_config)

try:
    run_csv_mode(fail_config)
except Exception as error:
    print("EXPECTED: failOnNewColumns rejected the new column")
    print(str(error)[:1_000])
```

**PRESENTER**

“The stream fails, and Auto Loader does not automatically add `email` to its stored schema.

The team must update the approved schema or remove the offending file before processing can continue. Use this mode for a strict governed contract.”

### 【13:00】Demo 4 — `addNewColumns`

**PRESENTER**

“This is the mode most interviewers expect for an approved additive column.

First, establish the old schema. Then deliver the file containing `email`.”

[ON SCREEN]

```python
add_config = MODE_CONFIGS["add"]

deliver_base_file(add_config)
run_csv_mode(add_config)

deliver_new_column_file(add_config)

try:
    run_csv_mode(add_config)
except Exception as error:
    print("EXPECTED: Auto Loader learned email and stopped once")
    print(str(error)[:1_000])
```

**PRESENTER**

“The first execution fails. That is expected.

Auto Loader discovers `email`, updates the schema stored in `schemaLocation`, and stops because the active query was planned with the previous schema.

In production, Databricks recommends running Auto Loader as a Lakeflow Job configured to restart automatically after this expected schema-change failure.

Now restart the same ingestion.”

[ON SCREEN]

```python
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
```

**PRESENTER**

“After restart, `email` is a normal column. The old row contains null, and the new row contains Nisha’s email.

`addNewColumns` evolves Auto Loader’s schema. `mergeSchema` allows the Delta destination to add the field.”

### 【16:00】Demo 5 — `addNewColumnsWithTypeWidening`

**PRESENTER**

“This mode performs the same new-column behavior and also widens supported data types.

Our CSV fields are strings, so we will use Parquet. The feature is in Public Preview and requires Databricks Runtime 16.4 or above.”

[ON SCREEN]

```python
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
```

[ON SCREEN]

```python
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
```

**PRESENTER**

“`amount` widens from integer to long because Auto Loader uses the widening mode and the Delta table also permits type widening.

Unsupported changes, such as integer to string, are not safe widening. When rescued-data handling is configured, Auto Loader places unsupported values in `_rescued_data`; otherwise the change requires deliberate handling.”

### 【18:30】Bronze evolution does not automatically update Silver

**PRESENTER**

“Bronze now contains `email`. Does Silver receive it automatically?

No. This Silver selection represents the old business contract.”

[ON SCREEN]

```python
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
```

**PRESENTER**

“Nothing fails, but `email` is absent because Silver follows its explicit contract. After approval, update that mapping deliberately.”

[ON SCREEN]

```python
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
```

**PRESENTER**

“Overwrite is used only because this is an isolated teaching table rebuilt from the complete demo data. In production, update the transformation, alter the approved schema, or use per-operation MERGE schema evolution.”

**PRESENTER — CURRENT SYNTAX NOTE**

“Current runtimes also provide explicit per-operation syntax. Databricks Runtime 15.4 LTS and above supports `MERGE WITH SCHEMA EVOLUTION`, or `.withSchemaEvolution()` in the Delta API. Databricks Runtime 18.1 and above supports `INSERT WITH SCHEMA EVOLUTION`.

For this streaming write, `.option("mergeSchema", "true")` is still the correct API. Databricks recommends per-operation evolution instead of enabling the legacy session-wide `spark.databricks.delta.schema.autoMerge.enabled` setting.”

### 【20:00】What can break in production?

**PRESENTER**

“A new column can break the pipeline in seven places.”

[ON SCREEN]

```text
1. Auto Loader can stop
2. Delta can reject the new schema
3. Silver can silently omit the column
4. MERGE can fail or silently omit an unapproved source column
5. Streams reading an evolved Delta table terminate
6. Concurrent writes can conflict, and materialized views can recompute
7. Views, BI models, APIs and governance contracts can require updates
```

**PRESENTER**

“Direct Auto Loader Structured Streaming stops when `addNewColumns` discovers the field, so the job should restart.

Auto Loader and Delta keep separate schemas. The reader can learn `email` while the table write still fails.

An explicit Silver selection can silently omit the field. MERGE behavior depends on its assignments. Without schema evolution, an extra source column can be ignored by `UPDATE SET *` or `INSERT *` when the existing target columns are satisfied, while an explicit assignment to a missing target column can fail. If the new field is approved, enable schema evolution for that individual MERGE.

Updating a Delta table schema terminates streams reading that table, so those streams must restart. Schema updates can also conflict with concurrent writes. A materialized-view schema or definition change triggers a full recomputation, while views, dashboards, and APIs have their own contracts.

Finally, `email` is personal data. Automatically passing it through every `select *` can become a governance failure.”

### 【21:15】Final interview answer

**PRESENTER**

“When a new column arrives mid-pipeline, I first check the configured Auto Loader `schemaEvolutionMode`.

If Auto Loader infers the schema, `addNewColumns` is the default. If a fixed schema is supplied, `none` is the default, so in production I set the policy explicitly.

With `rescue`, the stream continues and stores the field in `_rescued_data`.

With `none`, it continues and ignores the field unless rescued-data handling is configured.

With `failOnNewColumns`, the stream fails and requires a deliberate contract update.

With `addNewColumns`, Auto Loader records the field in its schema location and stops once. After restart, the query uses the evolved schema. In production, I configure the Lakeflow Job to restart automatically.

With `addNewColumnsWithTypeWidening`, it can also widen supported data types.

The Delta destination must separately allow the change, usually with per-write `mergeSchema` or another approved schema update. I prefer per-operation evolution over the legacy session-wide configuration.

Bronze evolution does not automatically evolve Silver or downstream consumers. Selections can omit the field, MERGE behavior depends on whether evolution is enabled, streams reading the changed Delta table must restart, concurrent writes can conflict, and consumer and governance contracts may require updates.

Therefore, I treat schema evolution as an end-to-end data-contract change—not as one Auto Loader setting.”

### 【22:15】Conclusion

**PRESENTER**

“Today, the same `email` column was rescued, ignored, rejected, and added as a normal column.

We also saw that type widening is a separate capability and that Bronze success does not automatically update Silver.

Remember this:

**The configured mode decides how Auto Loader reacts, but every downstream contract must still be handled deliberately.**”

## Retention map

- **0:00 — Open loop:** One field can be stopped, ignored, rescued, or added.
- **1:15 — Contract:** Five modes appear before the code.
- **8:00 — Demonstration ladder:** The same source change moves through four new-column policies.
- **16:00 — Special case:** Type widening is separated from the CSV example.
- **18:30 — Turn:** Bronze evolves while Silver remains unchanged.
- **21:15 — Payoff:** The complete interview answer closes the question.

## Official references

- Configure schema inference and evolution in Auto Loader: https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema
- Schema evolution in Azure Databricks: https://learn.microsoft.com/en-us/azure/databricks/data-engineering/schema-evolution
- Auto Loader best practices: https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/best-practices
- Update table schemas with schema evolution: https://learn.microsoft.com/en-us/azure/databricks/tables/update-schema
