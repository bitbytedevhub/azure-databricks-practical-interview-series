# Personal lab to production mapping

## Namespace

Personal lab:

    master_databricks_new.abb_retail_bronze.customers_raw

Typical enterprise development namespace:

    abb_retail_dev.bronze.customers_raw

The personal workspace catalog is already provisioned. In production, a platform team usually creates environment catalogs, managed storage, workspace bindings, groups, service principals, and grants through controlled infrastructure deployment.

## Identity

The learner runs notebooks as their own user. Production jobs should generally run as a service principal so execution is not tied to an employee account.

## Source simulator

The lab writes small CSV files directly to a managed volume. Production sources may use Azure Data Lake Storage, Event Hubs, database CDC, file transfer, or managed connectors.

## Bronze contract

The lab preserves raw values as strings, captures unexpected data, and adds source metadata. Silver owns type conversion, deduplication, CDC ordering, delete rules, and business validation.

## Checkpoints

Each source stream owns a dedicated checkpoint. A checkpoint tracks source-processing progress; it is not a business deduplication table.

## Recovery

Deleting only a checkpoint while retaining the target table can reinsert old source rows. A development reset must coordinate the source, schema state, checkpoint, and target table.

