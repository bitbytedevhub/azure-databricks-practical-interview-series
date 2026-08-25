# Azure Databricks Practical Interview Series

A beginner-friendly, production-mapped Azure Databricks interview lab. Every concept is demonstrated with runnable code, expected results, failure reasoning, and an interview explanation.

## Current course boundary

Day 1 now ends after the learner creates the three governed CSV datasets. Auto Loader, Bronze ingestion, validation, and rerun evidence begin on Day 2.

## Day 1 outcome

By the end of Day 1, you will have:

- selected the Unity Catalog catalog automatically created for your workspace;
- created isolated ABB Retail Bronze, Silver, Gold, and Ops schemas;
- created governed volumes for incoming CSV files and Auto Loader state;
- generated repeatable customer, order, and CDC source files;
- previewed the customer, order, and CDC CSV files.

## Day 2 outcome so far

- ingest all three CSV streams with Auto Loader;
- give every stream a separate schema location and checkpoint;
- create customers_raw, orders_raw, and customer_cdc_raw;
- attach source-file and ingestion metadata;
- validate tables, counts, contracts, metadata, and source files;
- inspect rescued records and business-key quality;
- prove that rerunning ingestion does not process the same files again.

## Repository structure

    notebooks/day1/
      00_platform_bootstrap.py
      01_source_simulator.py
      04_future_scenarios.py
    notebooks/day2/
      01_bronze_ingestion.py
      02_bronze_validation.py
    data/
      baseline/
      future_scenarios/
    docs/
      day1-conversational-guide.md
      day2-part1-ingestion-guide.md
      day2-part2-validation-guide.md
      production-mapping.md
      workflow-setup.md
    resources/day1_job.yml
    databricks.yml
    tests/validate_repository.py

## Before running

1. Open your Azure Databricks workspace.
2. Open Catalog Explorer and find the catalog whose name matches your workspace.
3. Import the Day 1 and Day 2 source notebooks.
4. Open 00_platform_bootstrap.
5. Set the catalog widget to your existing workspace catalog.

The repository default is master_databricks_new. Change it if your catalog has another name. The widget does not create a catalog; it passes the name of an existing catalog into the notebook.

## Notebook order

    00_platform_bootstrap
            |
    01_source_simulator
            |
    day2/01_bronze_ingestion
            |
    day2/02_bronze_validation
            |
    04_future_scenarios

Run 04_future_scenarios only to prepare files for later lessons. It does not copy those files into the active ingestion directories.

## Personal lab versus production

In this lab, the existing workspace catalog acts as the development catalog:

    master_databricks_new.abb_retail_bronze
    master_databricks_new.abb_retail_silver
    master_databricks_new.abb_retail_gold
    master_databricks_new.abb_retail_ops

An enterprise platform team might instead provision:

    abb_retail_dev.bronze
    abb_retail_dev.silver
    abb_retail_dev.gold
    abb_retail_dev.ops

The engineering patterns remain the same. See docs/production-mapping.md.

## Local validation

Run:

    python tests/validate_repository.py

## Checkpoint safety

Do not delete a streaming checkpoint while retaining its target Bronze table unless you deliberately intend to reprocess source files. A checkpoint reset and a target-table reset must be handled as one controlled development recovery procedure.

