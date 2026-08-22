# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 - CSV source simulator
# MAGIC
# MAGIC Production files normally arrive from customer, order, and CDC systems. This lab creates small, reproducible CSV deliveries inside a governed Unity Catalog volume.

# COMMAND ----------

import csv
import io

dbutils.widgets.text(
    "catalog",
    "master_databricks_new",
    "Existing Unity Catalog catalog",
)

CATALOG = dbutils.widgets.get("catalog").strip()
LANDING_ROOT = (
    f"/Volumes/{CATALOG}/abb_retail_bronze/incoming_files"
)

CUSTOMERS_PATH = f"{LANDING_ROOT}/customers"
ORDERS_PATH = f"{LANDING_ROOT}/orders"
CUSTOMER_CDC_PATH = f"{LANDING_ROOT}/customer_cdc"

for directory in [
    CUSTOMERS_PATH,
    ORDERS_PATH,
    CUSTOMER_CDC_PATH,
]:
    dbutils.fs.mkdirs(directory)
    print(f"Directory ready: {directory}")

# COMMAND ----------

def write_csv_if_missing(
    path: str,
    columns: list[str],
    rows: list[dict],
) -> None:
    """Create an immutable lab delivery and skip an existing filename."""
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

    dbutils.fs.put(
        path,
        buffer.getvalue(),
        overwrite=False,
    )
    print(f"CREATED: {path} with {len(rows)} data rows")

# COMMAND ----------

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "city",
    "loyalty_tier",
    "is_active",
    "updated_at",
]

customers = [
    {
        "customer_id": "C001",
        "customer_name": "Amit Sharma",
        "city": "Delhi",
        "loyalty_tier": "Gold",
        "is_active": "true",
        "updated_at": "2026-01-01T09:00:00",
    },
    {
        "customer_id": "C002",
        "customer_name": "Meera Iyer",
        "city": "Pune",
        "loyalty_tier": "Silver",
        "is_active": "true",
        "updated_at": "2026-01-01T09:10:00",
    },
    {
        "customer_id": "C003",
        "customer_name": "Sara Khan",
        "city": "Jaipur",
        "loyalty_tier": "Bronze",
        "is_active": "true",
        "updated_at": "2026-01-01T09:20:00",
    },
]

write_csv_if_missing(
    f"{CUSTOMERS_PATH}/customers_batch_001.csv",
    CUSTOMER_COLUMNS,
    customers,
)

# COMMAND ----------

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_amount",
    "currency",
    "order_status",
    "order_timestamp",
]

orders = [
    {
        "order_id": "O1001",
        "customer_id": "C001",
        "order_amount": "2500.50",
        "currency": "INR",
        "order_status": "COMPLETED",
        "order_timestamp": "2026-01-01T10:00:00",
    },
    {
        "order_id": "O1002",
        "customer_id": "C002",
        "order_amount": "850.50",
        "currency": "INR",
        "order_status": "COMPLETED",
        "order_timestamp": "2026-01-01T10:15:00",
    },
    {
        "order_id": "O1003",
        "customer_id": "C003",
        "order_amount": "1200.00",
        "currency": "INR",
        "order_status": "PENDING",
        "order_timestamp": "2026-01-01T10:30:00",
    },
]

write_csv_if_missing(
    f"{ORDERS_PATH}/orders_batch_001.csv",
    ORDER_COLUMNS,
    orders,
)

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

cdc_events = [
    {
        "event_id": "E0001",
        "customer_id": "C004",
        "operation": "INSERT",
        "customer_name": "Ravi Kumar",
        "city": "Chennai",
        "loyalty_tier": "Bronze",
        "is_active": "true",
        "event_timestamp": "2026-01-01T11:00:00",
        "sequence_number": "1",
    },
]

write_csv_if_missing(
    f"{CUSTOMER_CDC_PATH}/customer_cdc_batch_001.csv",
    CDC_COLUMNS,
    cdc_events,
)

# COMMAND ----------

display(dbutils.fs.ls(CUSTOMERS_PATH))
display(dbutils.fs.ls(ORDERS_PATH))
display(dbutils.fs.ls(CUSTOMER_CDC_PATH))

customer_preview = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{CUSTOMERS_PATH}/customers_batch_001.csv")
)

display(customer_preview)
print("PASS: Baseline CSV deliveries are ready.")

