# Databricks notebook source
# MAGIC %md
# MAGIC # TLC Yellow Taxi — Bronze → Silver (Step 1: Read, Rename)
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
# MAGIC 3. **Read** — loads the `bronze_tlc_deliveries` Delta table (plain
# MAGIC    batch read; bronze itself is no longer a streaming/Auto Loader
# MAGIC    table — see Session 2.3's history comments in
# MAGIC    `databricks/bronze/tlc_bronze.py`).
# MAGIC 4. **Rename** — maps raw TLC column names onto a delivery-framing
# MAGIC    vocabulary (`pickup_ts`, `dropoff_ts`, `trip_distance_miles`, etc.).
# MAGIC    `_source_file`/`_ingested_at` (bronze audit columns) pass through
# MAGIC    unchanged.
# MAGIC 5. **Preview** — prints the bronze row count and displays a sample so
# MAGIC    the renamed schema can be checked before validity rules are added
# MAGIC    in the next step.
# MAGIC
# MAGIC **Note (Session 3.1a retry)** — this is a rewrite against the
# MAGIC reprocessed bronze table (38,310,226 rows, confirmed 0% nulls on
# MAGIC `VendorID`/`PULocationID` across all 12 months). The previous version
# MAGIC of this notebook was built against the pre-fix bronze data and
# MAGIC included a cast step and a null-column backfill that no longer apply:
# MAGIC bronze already stores `VendorID`/`PULocationID`/`DOLocationID`/
# MAGIC `payment_type` as correct `LongType`, and its per-file
# MAGIC `unionByName(allowMissingColumns=True)` already guarantees
# MAGIC `congestion_surcharge`/`airport_fee` exist (nulled where the source
# MAGIC month didn't have them) for every row in the table.
# MAGIC
# MAGIC **This step only reads and renames.** No casting, null handling,
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

# ── 3. Read bronze Delta table (batch) ────────────────────────────────────────
from pyspark.sql import functions as F

bronze_df = spark.read.format("delta").load(BRONZE_PATH + "bronze_tlc_deliveries")

# COMMAND ----------

# ── 4. Rename columns to delivery-framing vocabulary ──────────────────────────
# _source_file and _ingested_at are bronze audit columns and pass through
# unchanged (not part of this map).
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

silver_df = bronze_df
for source_col, target_col in RENAME_MAP.items():
    if source_col in silver_df.columns:
        silver_df = silver_df.withColumnRenamed(source_col, target_col)

# COMMAND ----------

# ── 5. Preview renamed schema ──────────────────────────────────────────────────
print(f"Bronze row count: {bronze_df.count()}")
display(silver_df.limit(20))
