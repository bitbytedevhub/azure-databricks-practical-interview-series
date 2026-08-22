# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 - Prepare future interview scenarios
# MAGIC
# MAGIC These files remain outside the active source directories. Later episodes copy one scenario into an active directory with a new filename and rerun ingestion.

# COMMAND ----------

import csv
import io

dbutils.widgets.text(
    "catalog",
    "master_databricks_new",
    "Existing Unity Catalog catalog",
)

CATALOG = dbutils.widgets.get("catalog").strip()
SCENARIO_ROOT = (
    f"/Volumes/{CATALOG}/abb_retail_bronze/"
    "incoming_files/practice_scenarios"
)

for scenario_name in [
    "amit_move",
    "meera_delete",
    "sara_out_of_order",
    "schema_evolution",
]:
    dbutils.fs.mkdirs(
        f"{SCENARIO_ROOT}/{scenario_name}"
    )


def write_csv_if_missing(
    path: str,
    columns: list[str],
    rows: list[dict],
) -> None:
    parent_path, file_name = path.rsplit("/", 1)
    existing_files = {
        item.name.rstrip("/")
        for item in dbutils.fs.ls(parent_path)
    }
    if file_name in existing_files:
        print(f"SKIP: {path} already exists")
        return

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=columns,
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    writer.writerows(rows)
    dbutils.fs.put(path, buffer.getvalue(), False)
    print(f"CREATED: {path}")

# COMMAND ----------

CDC_COLUMNS = [
    "event_id",
    "customer_id",
    "operation",
    "customer_name",
    "city",
    "loyalty_tier",
    "is_active",
    "event_timestamp",
    "sequence_number",
]

write_csv_if_missing(
    f"{SCENARIO_ROOT}/amit_move/amit_move_to_mumbai.csv",
    CDC_COLUMNS,
    [
        {
            "event_id": "E0002",
            "customer_id": "C001",
            "operation": "UPDATE",
            "customer_name": "Amit Sharma",
            "city": "Mumbai",
            "loyalty_tier": "Gold",
            "is_active": "true",
            "event_timestamp": "2026-01-02T09:00:00",
            "sequence_number": "2",
        }
    ],
)

write_csv_if_missing(
    f"{SCENARIO_ROOT}/meera_delete/meera_delete.csv",
    CDC_COLUMNS,
    [
        {
            "event_id": "E0003",
            "customer_id": "C002",
            "operation": "DELETE",
            "customer_name": "",
            "city": "",
            "loyalty_tier": "",
            "is_active": "false",
            "event_timestamp": "2026-01-02T10:00:00",
            "sequence_number": "2",
        }
    ],
)

write_csv_if_missing(
    f"{SCENARIO_ROOT}/sara_out_of_order/sara_updates.csv",
    CDC_COLUMNS,
    [
        {
            "event_id": "E0005",
            "customer_id": "C003",
            "operation": "UPDATE",
            "customer_name": "Sara Khan",
            "city": "Bengaluru",
            "loyalty_tier": "Gold",
            "is_active": "true",
            "event_timestamp": "2026-01-03T11:00:00",
            "sequence_number": "3",
        },
        {
            "event_id": "E0004",
            "customer_id": "C003",
            "operation": "UPDATE",
            "customer_name": "Sara Khan",
            "city": "Hyderabad",
            "loyalty_tier": "Silver",
            "is_active": "true",
            "event_timestamp": "2026-01-03T10:00:00",
            "sequence_number": "2",
        },
    ],
)

# COMMAND ----------

CUSTOMER_COLUMNS_WITH_EMAIL = [
    "customer_id",
    "customer_name",
    "city",
    "loyalty_tier",
    "is_active",
    "email",
    "updated_at",
]

write_csv_if_missing(
    (
        f"{SCENARIO_ROOT}/schema_evolution/"
        "customer_with_email.csv"
    ),
    CUSTOMER_COLUMNS_WITH_EMAIL,
    [
        {
            "customer_id": "C005",
            "customer_name": "Nisha Verma",
            "city": "Kolkata",
            "loyalty_tier": "Bronze",
            "is_active": "true",
            "email": "nisha.verma@example.com",
            "updated_at": "2026-01-04T09:00:00",
        }
    ],
)

display(dbutils.fs.ls(SCENARIO_ROOT))
print("PASS: Future scenario files are prepared but inactive.")

