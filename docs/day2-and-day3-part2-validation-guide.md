# Day 3 - Bronze validation and idempotency beginner recording guide

## Opening

Presenter:

A green ingestion cell proves only that code completed. It does not prove that the correct tables, rows, files, columns, and metadata were produced.

## Validation sequence

    Table existence
            |
    Baseline counts
            |
    Source columns
            |
    Technical metadata
            |
    Source-file reconciliation
            |
    Rescued records
            |
    Business-key quality
            |
    Rerun evidence

## Baseline expectations

    customers_raw      3 rows
    orders_raw         3 rows
    customer_cdc_raw   1 row

Exact counts are appropriate for this fixed lab. Production may use source control totals, minimum and maximum thresholds, historical anomalies, and file-level reconciliation.

## Source contracts

The validation notebook confirms that each table contains its expected business columns. It separately verifies that every Bronze table contains source-file and ingestion metadata.

## File reconciliation

Expected:

    customers_batch_001.csv      3 rows
    orders_batch_001.csv         3 rows
    customer_cdc_batch_001.csv   1 row

A correct total count can still hide the wrong file mix, so file-level evidence is stronger.

## Rescued data

A rescued record is not automatically bad. It means some source content did not match the schema Auto Loader understood. Possible causes include a new column, changed header, malformed CSV, quotation problem, delimiter change, or incompatible value.

## Business keys

For the baseline, customer_id, order_id, and event_id must be populated. Duplicate keys are reported but not removed from Bronze. Bronze preserves what arrived; Silver applies business deduplication.

## Rerun demonstration

1. Capture counts in notebooks/day2_and_day3/02_bronze_validation.py.
2. Rerun 01_bronze_ingestion without adding or changing files.
3. Return without rerunning the before-count cell.
4. Capture after counts.
5. Compare every table.

Expected:

    customers_raw      before 3, after 3
    orders_raw         before 3, after 3
    customer_cdc_raw   before 1, after 1

## Core interview distinction

A checkpoint tracks streaming progress and processed files. It does not enforce business-key uniqueness. If O1001 arrives through a different new file, Auto Loader processes that file and Silver must apply the agreed deduplication rule.

## Checkpoint warning

Deleting only a checkpoint while retaining its Bronze target can cause old files to be processed again. Resetting a development stream must coordinate target tables, checkpoints, schema state, and the source-file strategy.

## Stop point

Stop after the validation summary and rerun proof. Day 4 activates Amit's Delhi-to-Mumbai update and begins the SCD problem.
