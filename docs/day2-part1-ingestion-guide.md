# Day 2 Part 1 - Beginner recording guide

## Outcome

Start with three CSV deliveries:

    customers_batch_001.csv
    orders_batch_001.csv
    customer_cdc_batch_001.csv

Finish with three Bronze Delta tables:

    customers_raw
    orders_raw
    customer_cdc_raw

## Opening

Presenter:

Day 1 created files, not Delta tables. Before we can solve SCD, we must discover files incrementally, preserve source values, attach traceability, remember processing progress, and write governed Bronze tables.

## Beginner mental model

    Source path
    The receiving dock where files arrive

    Schema location
    Auto Loader's memory of source columns

    Checkpoint
    The register of files and progress already processed

    Target table
    The governed Bronze storage area

Every source gets a separate source directory, schema location, checkpoint, and target.

## Recording order

1. Open notebooks/day2/01_bronze_ingestion.py.
2. Explain that CATALOG contains only master_databricks_new.
3. Verify the catalog and Bronze/Ops schemas.
4. Build Unity Catalog volume paths.
5. Confirm customer, order, and CDC files exist.
6. Generate one ingestion run ID.
7. Create schema-state and checkpoint parent directories.
8. Explain the reusable Auto Loader function.
9. Ingest customers.
10. Ingest orders.
11. Ingest CDC events.
12. Display the resulting tables.

## Key explanations

### Why cloudFiles?

cloudFiles activates Auto Loader. An ordinary batch directory read does not maintain file-processing state.

### Why keep CSV values as strings?

Bronze preserves source evidence. Silver later converts order amounts, timestamps, Booleans, and sequence numbers under explicit data-quality rules.

### Why rescue mode?

Unexpected source information is preserved in _rescued_data for investigation instead of being silently discarded.

### Why availableNow?

It processes everything currently available and then stops. This gives a scheduled job incremental streaming behavior without requiring an always-running training cluster.

### Why metadata?

The source path, filename, size, modification time, ingestion timestamp, and run ID let the team trace a bad row back to its delivery and pipeline execution.

## Stop point

Stop after displaying the three Bronze tables. Do not activate Amit's move or apply SCD logic yet.


