# Databricks notebook source
# MAGIC %md
# MAGIC # TLC Yellow Taxi — Bronze → Silver (Step 1: Read, Rename, Cast)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_tlc_deliveries/` |
# MAGIC | **Table** | `bronze_tlc_deliveries` (Delta) |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Auth** — sets the ADLS Gen2 account key from the `omnicart-kv`
# MAGIC    Key Vault secret scope so subsequent ABFSS paths resolve without
# MAGIC    needing cluster-level credentials.
# MAGIC 2. **Config** — path constants for the bronze and silver containers.
# MAGIC 3. **Read** — loads the `bronze_tlc_deliveries` Delta table.
# MAGIC 4. **Rename** — maps raw TLC column names onto a delivery-framing
# MAGIC    vocabulary (`pickup_ts`, `dropoff_ts`, `trip_distance_miles`, etc.).
# MAGIC 5. **Backfill missing columns** — `congestion_surcharge` and
# MAGIC    `airport_fee` are absent from some early-2023 source months; add
# MAGIC    them as null `DoubleType` columns when missing so the schema is
# MAGIC    stable across the whole table.
# MAGIC 6. **Cast** — casts every column to its correct type.
# MAGIC 7. **Preview** — prints the bronze row count and displays a sample so
# MAGIC    the renamed/typed schema can be checked before validity rules are
# MAGIC    added in the next step.
# MAGIC
# MAGIC **Note** — this step only reads, renames, and casts. No null handling,
# MAGIC dedup, or write to the silver container yet.

# COMMAND ----------

# ── 1. Auth ───────────────────────────────────────────────────────────────────
spark.conf.set(
    "fs.azure.account.key.omnicartdatalake.dfs.core.windows.net",
    dbutils.secrets.get(scope="omnicart-kv", key="adls-account-key"),
)

# COMMAND ----------

# ── 2. Config ─────────────────────────────────────────────────────────────────
BRONZE_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/"
SILVER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"

# COMMAND ----------

# ── 3. Read bronze Delta table ────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

bronze_df = spark.read.format("delta").load(BRONZE_PATH + "bronze_tlc_deliveries")

# COMMAND ----------

# ── 4. Rename columns to delivery-framing vocabulary ──────────────────────────
RENAME_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance_miles",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}

renamed_df = bronze_df
for source_col, target_col in RENAME_MAP.items():
    if source_col in renamed_df.columns:
        renamed_df = renamed_df.withColumnRenamed(source_col, target_col)

# COMMAND ----------

# ── 5. Backfill columns missing from early-2023 source months ────────────────
for optional_col in ("congestion_surcharge", "airport_fee"):
    if optional_col not in renamed_df.columns:
        renamed_df = renamed_df.withColumn(
            optional_col, F.lit(None).cast(DoubleType())
        )

# COMMAND ----------

# ── 6. Cast columns to correct types ──────────────────────────────────────────
CAST_MAP = {
    "vendor_id": "int",
    "pickup_ts": "timestamp",
    "dropoff_ts": "timestamp",
    "passenger_count": "int",
    "trip_distance_miles": "double",
    "rate_code_id": "int",
    "store_and_fwd_flag": "string",
    "pickup_location_id": "int",
    "dropoff_location_id": "int",
    "payment_type_id": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

silver_df = renamed_df
for col_name, target_type in CAST_MAP.items():
    silver_df = silver_df.withColumn(col_name, F.col(col_name).cast(target_type))

# COMMAND ----------

# ── 7. Preview renamed/typed schema ───────────────────────────────────────────
print(f"Bronze row count: {bronze_df.count()}")
display(silver_df.limit(20))
