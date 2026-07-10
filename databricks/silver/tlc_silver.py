# Databricks notebook source
# MAGIC %md
# MAGIC # TLC Yellow Taxi — Bronze → Silver (Steps 1-2: Read, Rename, Validate, Dedup)
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
# MAGIC    the renamed schema can be checked before validity rules are added.
# MAGIC 6. **Derived columns** — `delivery_id` (a deterministic SHA-256 hash
# MAGIC    of `vendor_id` + `pickup_ts` + `pickup_location_id` +
# MAGIC    `dropoff_location_id`, so reruns are idempotent instead of a random
# MAGIC    UUID), `trip_duration_minutes`, and `pickup_date`.
# MAGIC 7. **Validity rules** — splits the dataframe into `valid_df` and
# MAGIC    `rejected_df` (rejects are kept, not silently dropped) based on
# MAGIC    non-null/ordered timestamps, trip duration and distance bounds,
# MAGIC    non-negative fare, a sane passenger count, and non-null location
# MAGIC    IDs.
# MAGIC 8. **Deduplicate** — `valid_df` is deduplicated on `delivery_id`,
# MAGIC    keeping the row with the most recent `_ingested_at` per group.
# MAGIC 9. **Summary** — prints bronze row count, valid row count, rejected
# MAGIC    row count, and reject rate, to sanity-check the validity rules.
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
# MAGIC **Session 3.1b** adds derived columns, validity rules, and dedup.
# MAGIC **Still no write to the silver container** — that's step 3.

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

# COMMAND ----------

# ── 6. Derived columns ─────────────────────────────────────────────────────────
# delivery_id is a deterministic SHA-256 hash of vendor_id + pickup_ts +
# pickup_location_id + dropoff_location_id — not a random UUID — so
# re-running this notebook on the same bronze data produces the same IDs
# instead of minting new ones every run.
silver_df = (
    silver_df
        .withColumn(
            "delivery_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("vendor_id").cast("string"),
                    F.col("pickup_ts").cast("string"),
                    F.col("pickup_location_id").cast("string"),
                    F.col("dropoff_location_id").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
)

# COMMAND ----------

# ── 7. Validity rules — split into valid_df / rejected_df ────────────────────
# Rejected rows are kept in rejected_df rather than filtered and dropped
# silently, so they can be inspected/audited later.
VALID_CONDITION = (
    F.col("pickup_ts").isNotNull()
    & F.col("dropoff_ts").isNotNull()
    & (F.col("dropoff_ts") > F.col("pickup_ts"))
    & F.col("trip_duration_minutes").between(0, 180)
    & F.col("trip_distance_miles").between(0, 200)
    & (F.col("fare_amount") >= 0)
    & (F.col("passenger_count").isNull() | F.col("passenger_count").between(0, 9))
    & F.col("pickup_location_id").isNotNull()
    & F.col("dropoff_location_id").isNotNull()
)

flagged_df = silver_df.withColumn("_is_valid", VALID_CONDITION)
valid_df = flagged_df.filter(F.col("_is_valid")).drop("_is_valid")
rejected_df = flagged_df.filter(~F.col("_is_valid")).drop("_is_valid")

# COMMAND ----------

# ── 8. Deduplicate valid_df on delivery_id ────────────────────────────────────
# Keeps the row with the most recent _ingested_at per delivery_id in case
# of any overlap.
from pyspark.sql.window import Window

dedup_window = Window.partitionBy("delivery_id").orderBy(F.col("_ingested_at").desc())

valid_df = (
    valid_df
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
)

# COMMAND ----------

# ── 9. Summary ─────────────────────────────────────────────────────────────────
bronze_row_count = bronze_df.count()
valid_row_count = valid_df.count()
rejected_row_count = rejected_df.count()
reject_rate_pct = (rejected_row_count / bronze_row_count) * 100

print(f"Bronze row count:   {bronze_row_count:,}")
print(f"Valid row count:    {valid_row_count:,}")
print(f"Rejected row count: {rejected_row_count:,}")
print(f"Reject rate:        {reject_rate_pct:.4f}%")
