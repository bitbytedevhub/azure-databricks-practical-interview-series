# Day 1 conversational recording guide

## Opening hook

Presenter:

Welcome to Day 1 of the Azure Databricks Practical Interview Series.

You may already know definitions such as SCD Type 1, SCD Type 2, CDC, schema evolution, and Delta Lake. But an interviewer may ask a more important question: can you build it, rerun it, break it, and explain why it behaves that way?

Our fictional company is ABB Retail. Customer, order, and change-event files arrive every day. The business does not care that a notebook contains advanced Spark code. It cares that tomorrow morning's dashboard is correct.

By the end of the series, we will build a production-mapped lakehouse that handles updates, deletes, duplicates, late events, schema changes, retries, validation, permissions, and recovery.

Today we build the governed, repeatable foundation every later episode will reuse.

On screen:

    CSV source files
           |
        Bronze
           |
        Silver
           |
         Gold

## Step 1 - Create the notebook folder

Presenter:

Create a folder named azure-databricks-practical-interview-series and add the five Day 1 notebooks. Keeping related notebooks together makes deployment, review, and troubleshooting easier.

Create:

    00_platform_bootstrap
    01_source_simulator
    02_bronze_ingestion
    03_bronze_validation
    04_future_scenarios

## Step 2 - Use the existing workspace catalog

Presenter:

Think of the catalog as a governed building. Schemas are departments, and tables or volumes are the assets inside each department.

For a first-time learner, the catalog matching the workspace name already has managed storage. We use that catalog instead of asking the learner to configure Azure storage credentials and external locations.

Run SHOW CATALOGS and select the workspace catalog. In this lab the example is master_databricks_new.

Interview explanation:

In production, a platform team might provision abb_retail_dev, abb_retail_test, and abb_retail_prod catalogs. In this personal workspace, project-prefixed schemas provide logical isolation while retaining the same engineering pattern.

## Step 3 - Create Bronze, Silver, Gold, and Ops schemas

Presenter:

Bronze preserves deliveries. Silver applies data quality and business rules. Gold serves reporting. Ops stores checkpoint and schema state.

Run 00_platform_bootstrap and verify:

    master_databricks_new.abb_retail_bronze
    master_databricks_new.abb_retail_silver
    master_databricks_new.abb_retail_gold
    master_databricks_new.abb_retail_ops

Why IF NOT EXISTS?

It prevents an already-exists error during a safe retry. It does not prove that an existing object's owner, permissions, comment, and storage are correct, so validation remains necessary.

## Step 4 - Create governed volumes

Presenter:

The incoming-files volume is the delivery dock for CSV files. The pipeline-state volume is Auto Loader's memory.

The paths are:

    /Volumes/<catalog>/abb_retail_bronze/incoming_files
    /Volumes/<catalog>/abb_retail_ops/pipeline_state

Never place all sources and all checkpoints in one shared directory. Each source has a different schema, progress history, and recovery boundary.

## Step 5 - Generate source CSV files

Presenter:

Open 01_source_simulator. In production, these files would arrive from operational systems. The lab generates deterministic deliveries so every learner sees the same result.

The safe writer:

- uses Python's CSV library so commas and quotes are encoded correctly;
- writes a header row;
- rejects unexpected dictionary fields;
- skips an existing filename instead of silently replacing an immutable delivery.

Baseline state:

- Amit lives in Delhi.
- Meera is active in Pune.
- Sara is active in Jaipur.
- Three orders exist.
- One insert CDC event exists for Ravi.

Expected files:

    customers/customers_batch_001.csv
    orders/orders_batch_001.csv
    customer_cdc/customer_cdc_batch_001.csv

## Step 6 - Preview without transforming

Presenter:

Read the customer CSV with header enabled and schema inference disabled. This is only a preview. Bronze should preserve source values before Silver decides which strings are valid dates, decimals, Booleans, or business states.

What if order_amount contains UNKNOWN?

If Bronze casts aggressively, the pipeline may fail or lose the original value. Preserving text allows Silver to quarantine it deliberately.

## Step 7 - Configure Auto Loader

Presenter:

Open 02_bronze_ingestion. The catalog widget receives configuration; it does not create a catalog.

Each stream gets four locations:

    source path
    schema location
    checkpoint location
    target Delta table

The schema location remembers discovered columns. The checkpoint remembers processed files and streaming progress. The target stores preserved Bronze rows.

## Step 8 - Add Bronze metadata

Presenter:

Every row receives:

    _source_file
    _source_file_name
    _source_file_size
    _source_modification_time
    _ingested_at
    _ingestion_run_id

This supports operational questions such as which source file produced a row, when it arrived, and which notebook execution handled it.

## Step 9 - Explain rescue mode

Presenter:

The lab uses rescue mode. Unexpected fields are preserved in _rescued_data rather than silently discarded.

A rescued record is not automatically bad. It means the source did not match the schema Auto Loader knew. The team must decide whether the cause is a valid new column, malformed CSV, data-type problem, or broken source contract.

## Step 10 - Run the three ingestions

Expected tables:

    customers_raw: 3 rows
    orders_raw: 3 rows
    customer_cdc_raw: 1 row

Each stream has a separate schema location and checkpoint. Reusing a checkpoint across unrelated queries is unsafe because a checkpoint belongs to one exact source-to-target streaming query.

## Step 11 - Validate executable evidence

Presenter:

Open 03_bronze_validation. A green cell proves only that code completed. Validation proves that the exact tables exist in the correct namespace, contain expected baseline counts, include metadata, and expose rescued records.

Production mapping:

Fixed counts are appropriate for this reproducible lab. Production may use source control totals, minimum thresholds, reconciliation, and anomaly detection instead.

## Step 12 - Prove rerun safety

Presenter:

Capture counts, rerun 02_bronze_ingestion without adding files, and compare counts. They must remain unchanged because the checkpoints remember the existing files.

Interview question:

Does a checkpoint remove business duplicates?

Answer:

No. If order O1001 arrives again in a different new file, Auto Loader processes the new file. Silver must deduplicate using order_id and an agreed event-ordering rule.

## Step 13 - Prepare future scenarios

Run 04_future_scenarios. The files remain outside active source folders:

- Amit moves from Delhi to Mumbai.
- Meera receives a delete event.
- Sara receives sequence 3 before sequence 2.
- A new customer file introduces email.

These scenarios represent later SCD Type 1, SCD Type 2, CDC ordering, delete handling, and schema-evolution episodes.

## Step 14 - Create the Workflow

Create the abb-retail-day1 job with:

    Task: bronze_ingestion
    Notebook: 02_bronze_ingestion
    Parameter: catalog = master_databricks_new

    Task: validate_bronze
    Notebook: 03_bronze_validation
    Depends on: bronze_ingestion
    Parameter: catalog = master_databricks_new

The dependency prevents validation from running against a failed or partially completed ingestion.

## Closing script

Today we did not implement SCD Type 1 or SCD Type 2.

We built the environment those solutions require: governed storage, reproducible CSV deliveries, incremental Bronze ingestion, operational metadata, rescued-data inspection, validation, and rerun evidence.

In Day 2, we will ingest the three CSV streams into Bronze with Auto Loader. In Day 3, we will validate the Bronze tables and prove checkpoint-based rerun safety. In Day 4, Amit will move from Delhi to Mumbai. We will implement SCD Type 1, prove that Delhi was replaced, and discuss when losing history is acceptable.
