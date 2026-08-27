# Day 3 - SCD Type 1 beginner recording guide

## Interview problem

An interviewer says:

> Amit currently lives in Delhi. A CDC update says he moved to Mumbai. Build an SCD Type 1 table, prove the result, and explain what happens when the job runs twice.

## Outcome

Start with:

    Bronze customers snapshot: C001 = Delhi
    CDC event E0002:          C001 = Mumbai, sequence 2

Finish with:

    Silver customer_current:  C001 = Mumbai
    Rows for C001:            exactly 1
    Old Delhi business row:   removed

SCD Type 1 keeps only the latest business state. It does not add effective dates or preserve a second historical row.

## Notebook order

    01_activate_amit_update
              |
    02_ingest_amit_update
              |
    03_apply_scd_type1
              |
    04_validate_scd_type1

## Step 1 - Activate one immutable delivery

Presenter:

Day 1 prepared Amit's update outside the active CDC inbox. We now copy it into the inbox as `customer_cdc_batch_002.csv`.

Why copy instead of editing batch 001?

Production source files should be treated as immutable deliveries. Overwriting an already processed filename makes recovery and audit evidence ambiguous. A new delivery receives a new filename.

What happens on a rerun?

The activation notebook compares an existing batch-002 file with the prepared scenario. Identical content is skipped. Different content under the same filename fails instead of being silently overwritten.

## Step 2 - Reuse the Day 2 checkpoint

Presenter:

The source path, schema location, checkpoint, and Bronze target must be exactly the same as Day 2:

    Source:     incoming_files/customer_cdc
    Schema:     pipeline_state/schemas/customer_cdc
    Checkpoint: pipeline_state/checkpoints/customer_cdc
    Target:     abb_retail_bronze.customer_cdc_raw

Why reuse the checkpoint?

The checkpoint already remembers batch 001. Auto Loader therefore discovers batch 002 and appends only E0002. A new checkpoint would forget the earlier progress and could process both files again.

Why does Bronze still append?

Bronze records what arrived. It should not overwrite Amit's earlier customer snapshot. The current-state business decision belongs in Silver.

## Step 3 - Establish the Silver contract

Presenter:

Bronze keeps strings because it preserves source evidence. Silver converts values deliberately:

- `is_active` becomes Boolean;
- timestamps become `TIMESTAMP`;
- `sequence_number` becomes `BIGINT`;
- blank keys and invalid types fail validation.

Why validate before MERGE?

A technically successful cast can produce null. Merging a null timestamp or sequence number would make event ordering unreliable.

## Step 4 - Select one latest source row

Presenter:

Delta MERGE requires a deterministic source. If two source rows match the same target customer, the operation can be ambiguous.

We partition by `customer_id` and order by:

1. sequence number descending;
2. event timestamp descending;
3. Bronze ingestion timestamp descending;
4. source event ID descending.

Sequence number is the primary rule because arrival order is not business order. A late event can arrive tomorrow while representing an older change.

## Step 5 - Apply the SCD Type 1 MERGE

Presenter:

The MERGE has two business paths:

    Customer exists + source is newer -> UPDATE
    Customer does not exist           -> INSERT

For Amit, C001 already exists. Sequence 2 is newer than the snapshot's sequence 0, so Delhi is overwritten by Mumbai.

Why not update every matched row?

An unconditional update would allow an older or identical event to rewrite the row during a retry. The matched condition accepts only a strictly newer ordered event.

What happens when the MERGE runs twice?

The same E0002 event has the same sequence, timestamp, and event ID. It is not newer, so the second MERGE makes no business change and creates no duplicate customer row.

## Step 6 - Validate the business outcome

The validation notebook proves:

    customer_current row count = 4
    distinct customer IDs      = 4
    C001 row count              = 1
    C001 city                   = Mumbai
    C001 sequence               = 2
    C001 source event           = E0002
    C001 Delhi rows             = 0

It also displays Delta history and confirms that a MERGE occurred.

## Important interview nuance

SCD Type 1 does not preserve historical business rows. Delta Lake may still retain older physical table versions for a configured retention period, but Delta time travel is not a substitute for an SCD Type 2 model. Type 2 explicitly exposes business history with effective dates and current-row indicators.

## Why not process DELETE yet?

Delete behavior is a separate business contract. A source delete might mean physical deletion, soft deletion, anonymization, or account closure. Day 3 deliberately fails if a DELETE event is present rather than silently choosing a policy.

## Production mapping

This lab recomputes the latest source state from tiny Bronze tables for clarity. A production pipeline may process only new CDC increments, record batch control totals, quarantine invalid events, apply expectations, monitor MERGE metrics, and serialize concurrent writes to the same target.

## Closing script

Today we converted raw snapshots and CDC events into one trusted current-state customer table. Amit moved from Delhi to Mumbai, and SCD Type 1 replaced the old value instead of creating a second row. We used sequence-aware ordering, a conditional Delta MERGE, data-contract checks, and rerun validation. That is the difference between knowing the SCD Type 1 definition and implementing it safely.

