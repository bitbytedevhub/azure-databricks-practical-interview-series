# Azure Databricks Practical Interview Series

A beginner-friendly, production-mapped Azure Databricks interview lab. Every concept is demonstrated with runnable code, expected results, failure reasoning, and an interview explanation.

## Current course boundary

Day 1 creates the governed CSV lab. Day 2 performs Auto Loader Bronze ingestion. Day 3 validates Bronze ingestion and proves checkpoint-based idempotency. Day 4 focuses on the SCD Type 1 interview problem: replace Amit's Delhi value with the newer Mumbai CDC event in Silver.

## Day 1 outcome

By the end of Day 1, you will have:

- selected the Unity Catalog catalog automatically created for your workspace;
- created isolated ABB Retail Bronze, Silver, Gold, and Ops schemas;
- created governed volumes for incoming CSV files and Auto Loader state;
- generated repeatable customer, order, and CDC source files;
- previewed the customer, order, and CDC CSV files.

## Day 2 & Day 3 outcome

- ingest all three CSV streams with Auto Loader;
- give every stream a separate schema location and checkpoint;
- create customers_raw, orders_raw, and customer_cdc_raw;
- attach source-file and ingestion metadata;
- validate tables, counts, contracts, metadata, and source files;
- inspect rescued records and business-key quality;
- prove that rerunning ingestion does not process the same files again.

Day 2 focuses on ingestion. Day 3 focuses on validation, checkpoints, and rerun safety.

## Day 4 outcome

- upload the provided `customer_cdc_batch_002.csv` file and ingest it with the existing Day 2 Auto Loader notebook;
- display the Delhi snapshot and Mumbai CDC event as the business problem;
- give snapshot and CDC rows one comparable structure;
- select one deterministic latest record per customer;
- apply SCD Type 1 with a conditional Delta MERGE;
- prove that Silver contains one current Mumbai row for Amit;
- explain why Bronze preserves both source states while Silver keeps only the current business state;
- prove that rerunning the same source state creates no duplicate customer row.

## Repository structure

    notebooks/day1/
      00_platform_bootstrap.py
      01_source_simulator.py
      04_future_scenarios.py
    notebooks/day2_and_day3/
      01_bronze_ingestion.py
      02_bronze_validation.py
    notebooks/day4/
      01_scd_type1_problem_and_solution.py
    data/
      baseline/
      day4/
        customer_cdc_batch_002.csv
      future_scenarios/
    docs/
      day1-conversational-guide.md
      day2-and-day3-part1-ingestion-guide.md
      day2-and-day3-part2-validation-guide.md
      day4-scd-type1-guide.md
      production-mapping.md
      workflow-setup.md
    resources/day1_job.yml
    databricks.yml
    tests/validate_repository.py

## Before running

1. Open your Azure Databricks workspace.
2. Open Catalog Explorer and find the catalog whose name matches your workspace.
3. Import the Day 1, Day 2 & Day 3, and Day 4 source notebooks.
4. Open 00_platform_bootstrap.
5. Set the catalog widget to your existing workspace catalog.

The repository default is master_databricks_new. Change it if your catalog has another name. The widget does not create a catalog; it passes the name of an existing catalog into the notebook.

## Notebook order

    00_platform_bootstrap
            |
    01_source_simulator
            |
    day2_and_day3/01_bronze_ingestion
            |
    day2_and_day3/02_bronze_validation
            |
    upload data/day4/customer_cdc_batch_002.csv
            |
    rerun day2_and_day3/01_bronze_ingestion
            |
    day4/01_scd_type1_problem_and_solution

If Bronze already contains event `E0002`, skip the Day 4 upload and ingestion rerun. The Day 4 notebook intentionally assumes the Day 1-Day 3 lab exists and removes platform prerequisite checks so the lesson stays focused on SCD Type 1.

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
