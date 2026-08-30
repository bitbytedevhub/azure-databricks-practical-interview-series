# Day 4 — SCD Type 1 problem-and-solution guide

## Today’s interview question

**How would you implement SCD Type 1 when a customer’s latest details arrive through CDC?**

Day 4 assumes that the Day 1–3 lab is already ready. The catalog, Bronze tables, volume, Auto Loader notebook, schema location, and checkpoint location already exist. We therefore do not repeat catalog checks, table-existence checks, permission checks, or other platform setup. This class stays focused on the SCD Type 1 problem and its solution.

## What the learner needs

Use the upload-ready file:

`data/day4/customer_cdc_batch_002.csv`

Upload it to the existing customer CDC incoming folder:

`/Volumes/<your-catalog>/abb_retail_bronze/incoming_files/customer_cdc/`

Then rerun the existing Bronze ingestion notebook:

`notebooks/day2_and_day3/01_bronze_ingestion.py`

Auto Loader will discover the new file and append its event to the existing Bronze CDC table. If event `E0002` is already visible in the Bronze CDC table, skip the upload and ingestion steps and begin directly with the Day 4 notebook.

## Notebook to create or import

Use this single notebook for the entire lesson:

`notebooks/day4/01_scd_type1_problem_and_solution.py`

The catalog variable near the beginning is set to `master_databricks_new`. Change only that value if the learner used a different catalog name during the earlier lab setup.

## The production-style scenario

The original customer snapshot says that Amit (`C001`) lives in Delhi. A later CDC event, `E0002`, says that Amit now lives in Mumbai and has sequence number `2`.

The business wants a current customer table. It does not want both Delhi and Mumbai to appear as active versions. In SCD Type 1, the latest value replaces the old value in Silver, so the expected current state is one row for Amit with `city = Mumbai`.

This mirrors a common production requirement: operational systems send changes after the initial snapshot, while reporting teams need one current version of each customer.

## Step-by-step teaching flow

### 1. Show the problem before writing the solution

Display Amit’s record from the Bronze snapshot and event `E0002` from the Bronze CDC table. This lets the viewer see the conflict clearly:

- Snapshot: Amit is in Delhi.
- CDC: Amit has changed to Mumbai.

The learner should understand the business question before seeing a `MERGE` statement.

### 2. Put snapshot and CDC rows into one common shape

Select the same business columns from both sources. Give the snapshot a starting sequence number of `0`; the CDC event already has a later sequence number.

Why? Spark can compare and combine the rows reliably only when both DataFrames have a compatible structure. The sequence gives us an explicit way to decide which version is newer.

### 3. Combine the candidates

Use `unionByName` to place the snapshot and CDC candidates into one DataFrame.

Why `unionByName`? It aligns values using column names. A positional union can silently place values under the wrong column if the column order differs.

### 4. Select the latest row for every customer

Use a window partitioned by `customer_id`. Order by sequence number and event time in descending order, then assign `row_number()`.

Keep only row number `1`. This produces one deterministic latest candidate for each customer before the merge.

Why do this before `MERGE`? A source batch can contain several events for the same customer. Sending all of them directly into one merge can create ambiguity and may fail because several source rows try to modify the same target row.

### 5. Create a temporary view

Expose the latest-customer DataFrame as a temporary view so that the SQL `MERGE` can read it.

The view is only a notebook-session bridge between PySpark and SQL. It is not another permanent layer or a duplicate table.

### 6. Create the Silver current-state table

Create the Delta table if it does not yet exist. This is the SCD Type 1 target and contains one current row per customer.

This is solution logic, not a platform prevalidation. It allows the same lesson to demonstrate the initial load and later updates using one target.

### 7. Apply the SCD Type 1 merge

Match source and target using `customer_id`.

- When the customer exists and the source sequence is newer, update the current row.
- When the customer does not exist, insert a new row.
- When the source is not newer, do nothing.

The sequence condition matters. Without it, a delayed older event could overwrite a newer value and incorrectly move Amit back to Delhi.

### 8. Prove the business result

Display Amit from the Silver table. The result should contain exactly one current row and the city should be Mumbai.

Then compare the layers:

- Bronze still preserves the original Delhi snapshot and the Mumbai CDC event.
- Silver contains only Amit’s latest current state: Mumbai.

This distinction is important in an interview. SCD Type 1 overwrites history in the current-state target; it does not erase the raw history preserved in Bronze.

### 9. Rerun the notebook

Run the Day 4 notebook again. The matched condition updates only when the incoming sequence number is greater than the stored sequence number, so the same event does not create another customer row or replace the row with an older version.

Be precise: this rerun safety comes from the conditional Delta `MERGE`. Auto Loader checkpointing protected the earlier file-ingestion step from rereading the same discovered file.

## What was intentionally removed from Day 4

The earlier version included catalog-name validation, table-existence checks, unsupported-operation checks, invalid-row checks, and a separate assertion notebook. Those are useful production controls, but repeating them here distracts from the interview problem.

In a real pipeline, governance, data-quality checks, monitoring, quarantine handling, and alerting would still be added around this transformation. They are separate engineering concerns and can be taught in later episodes.

## Short presenter conclusion

“Today we solved a real SCD Type 1 problem. Bronze showed two versions of Amit: the original Delhi snapshot and a later Mumbai change. We ranked the candidates, kept the latest version, and used a conditional Delta merge to maintain one current Silver row. Amit is now in Mumbai in Silver, while Bronze still preserves the source history. That is the key SCD Type 1 distinction to explain in an interview.”
