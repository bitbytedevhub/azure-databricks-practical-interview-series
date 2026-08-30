# Day 5 - SCD Type 2 problem-and-solution guide

## Interview question

**Amit moved from Delhi to Mumbai. The business wants to preserve both versions. How would you implement SCD Type 2 with Delta Lake?**

Day 5 assumes the Day 1-Day 4 lab is ready. Event `E0002` must already be present in `customer_cdc_raw`. No additional CSV file or Bronze ingestion run is required.

Use this notebook:

`notebooks/day5/01_scd_type2_problem_and_solution.py`

The notebook creates a separate Silver table named `customer_history`; it does not modify the Day 4 SCD Type 1 table.

## Expected result

| customer_id | city | effective_to | is_current |
|---|---|---|---|
| C001 | Delhi | Mumbai change time | false |
| C001 | Mumbai | NULL | true |

SCD Type 1 overwrote Delhi in the current-state table. SCD Type 2 closes Delhi and inserts Mumbai, allowing point-in-time reporting.

## Teaching flow

### 1. Display the problem

Show Amit's Delhi snapshot and Mumbai CDC event before presenting the solution. The viewer should first understand why one row is insufficient.

### 2. Create the history target

Explain the SCD Type 2 tracking columns:

- `customer_sk` uniquely identifies a version;
- `effective_from` marks the start of the version;
- `effective_to` marks its exclusive end;
- `is_current` identifies the active version;
- `sequence_number` helps reject old events.

### 3. Load the initial snapshot

Convert every snapshot customer into a current starting version. Use a deterministic hash of customer ID plus `SNAPSHOT` as the surrogate key.

The initialization merge inserts only missing keys. It deliberately does not update matching snapshots because a rerun must never reopen a historical Delhi version.

### 4. Normalize and deduplicate E0002

Normalize strings, timestamps, sequence numbers, and the Boolean `is_active` field. Keep one row per event ID and remove any event already stored in Silver history.

This distinguishes two protections:

- Auto Loader checkpointing remembers discovered files.
- Event-ID deduplication remembers accepted business events.

### 5. Accept only a newer event

Compare the event with the current Silver version using this priority:

1. Higher sequence number.
2. Same sequence and later event timestamp.
3. Same sequence and timestamp but greater event ID.

A late older event must not close the latest version.

### 6. Stage the two SCD Type 2 actions

An existing-customer update needs two staged rows:

- `merge_customer_id = C001` matches and closes Delhi.
- `merge_customer_id = NULL` does not match and inserts Mumbai.

A new customer receives only the insert row.

### 7. Run the Delta merge

The matched action sets Delhi's `effective_to` to the Mumbai event timestamp and `is_current` to false. The not-matched action inserts Mumbai with `effective_to = NULL` and `is_current = true`.

### 8. Prove the result and rerun

Display both Amit versions, then rerun the notebook. The deterministic snapshot key prevents another snapshot version, and the processed-event anti join removes E0002 before the final merge. No duplicate Mumbai version is created.

## Production boundaries to explain

- This focused lesson isolates one update event for Amit.
- Several changes for one customer in the same microbatch must be processed in sequence order or used to rebuild that customer's timeline.
- Delete events require an agreed business rule: close the current version, add an inactive version, or follow a regulatory deletion procedure.
- A current production pipeline should add data-quality monitoring, quarantine handling, and alerting around the transformation.

## Short introduction script

“Welcome back. In Day 4, we used SCD Type 1 to replace Amit's Delhi value with Mumbai. Today the business wants to keep both versions. We will close Delhi, insert Mumbai as the new current version, and make the history safe when the same event is processed again.”

## Short conclusion script

“SCD Type 2 preserves history. We closed Amit's Delhi version with an effective end time and inserted Mumbai as the current version. In an interview, remember the two actions: expire the old row and insert the new row. Event-ID deduplication and deterministic keys make the same business event safe to rerun.”
