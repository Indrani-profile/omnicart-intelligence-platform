# Databricks notebook source
# MAGIC %md
# MAGIC # TLC Yellow Taxi — Bronze Ingestion (Auto Loader)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://raw@omnicartdatalake.dfs.core.windows.net/tlc/` |
# MAGIC | **Target** | `abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_tlc_deliveries/` |
# MAGIC | **Table** | `bronze_tlc_deliveries` (Delta) |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Config** — path constants and Auto Loader options.
# MAGIC 2. **Read stream** — Auto Loader (`cloudFiles`) reads parquet files
# MAGIC    incrementally, against an **explicit schema** (see note below)
# MAGIC    rather than inferring one.
# MAGIC 3. **Transform** — two audit columns are appended to every row:
# MAGIC    `_source_file` (the ABFSS path of the originating file) and
# MAGIC    `_ingested_at` (wall-clock timestamp of the ingestion micro-batch).
# MAGIC    These columns make lineage traceable back to the exact source file
# MAGIC    without touching any upstream data.
# MAGIC 4. **Write stream** — appends enriched rows to the Delta table at the
# MAGIC    target path. `trigger(availableNow=True)` drains all files not yet
# MAGIC    seen by the checkpoint, then stops the stream. Re-running the
# MAGIC    notebook is safe: Auto Loader's checkpoint prevents any file from
# MAGIC    being re-ingested.
# MAGIC 5. **Verify** — queries the Delta table for a row count so the operator
# MAGIC    can confirm data landed correctly before declaring the run complete.
# MAGIC 6. **Full reprocess (destructive, manual)** — drops the existing
# MAGIC    `bronze_tlc_deliveries` table, checkpoint, and schema location so
# MAGIC    the notebook can be re-run top to bottom against the fixed schema.
# MAGIC 7. **Post-reprocess check** — per-month null rate for `VendorID` /
# MAGIC    `PULocationID`, to confirm all 12 months populated correctly.
# MAGIC
# MAGIC **Root cause note (Session 3.1c)** — Auto Loader's schema inference
# MAGIC locked onto the casing of the first-processed file (January 2023).
# MAGIC 11 of the 12 monthly TLC files use slightly different column-name
# MAGIC casing (e.g. `Airport_fee` vs `airport_fee`), so those columns failed
# MAGIC to match the locked schema and were rescued into `_rescued_data`
# MAGIC instead of populating the real typed columns — ~92% of rows ended up
# MAGIC null for `vendor_id`, `passenger_count`, `rate_code_id`,
# MAGIC `pickup_location_id`, `dropoff_location_id`. The fix below reads
# MAGIC against an explicit schema instead of an inferred one; Spark's column
# MAGIC resolution is case-insensitive by default
# MAGIC (`spark.sql.caseSensitive` = `false`), so every differently-cased
# MAGIC variant of a column now maps onto the same logical field regardless
# MAGIC of which file it came from.

# COMMAND ----------

# ── 1. Config ─────────────────────────────────────────────────────────────────
# ADLS auth is configured at the cluster level via the omnicart-kv Key Vault
# secret scope (set up in Session 1.3); no explicit credentials are needed here.

SOURCE_PATH     = "abfss://raw@omnicartdatalake.dfs.core.windows.net/tlc/"
TARGET_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_tlc_deliveries/"
CHECKPOINT_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/checkpoints/tlc/"
SCHEMA_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/schemas/tlc/"

# COMMAND ----------

# ── 2. Read stream (Auto Loader, explicit schema) ─────────────────────────────
# An explicit schema replaces inferColumnTypes so that Auto Loader never
# locks onto the casing of whichever file happens to be processed first.
# Field names below match the January 2023 file (the shape every downstream
# notebook already expects); Spark's default case-insensitive column
# resolution maps every other month's casing variants onto these same
# fields instead of routing them to _rescued_data.
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

TLC_SCHEMA = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])

# Some monthly TLC files store a DoubleType field (e.g. passenger_count,
# RatecodeID, congestion_surcharge, airport_fee) as physical INT64 instead
# of DOUBLE, likely because every value in that month's file happened to be
# a whole number and the parquet writer picked the narrower type. Spark's
# vectorized Parquet reader reads columns as fixed-width batches matching
# the physical type and won't safely widen INT64 -> DOUBLE, raising
# SchemaColumnConvertNotSupportedException. The row-based reader does
# support that coercion, so we trade a bit of read speed (acceptable at
# this data volume — ~540MB, 38.3M rows) for correctness across all 12
# months. This is a reader-level setting, so it covers every DoubleType
# column in TLC_SCHEMA, not just passenger_count.
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

raw_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema(TLC_SCHEMA)
        .load(SOURCE_PATH)
)

# COMMAND ----------

# ── 3. Add audit columns ──────────────────────────────────────────────────────
enriched_stream = (
    raw_stream
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# ── 4. Write stream → Delta (append, availableNow) ────────────────────────────
write_query = (
    enriched_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(TARGET_PATH)
)

write_query.awaitTermination()
print(f"Ingestion complete — final status: {write_query.status}")

# COMMAND ----------

# ── 5. Verify row count ───────────────────────────────────────────────────────
display(spark.sql(f"SELECT COUNT(*) AS row_count FROM delta.`{TARGET_PATH}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Full reprocess (destructive — manual confirmation required)
# MAGIC
# MAGIC The existing `bronze_tlc_deliveries` table, its checkpoint, and its
# MAGIC schema location were all built under the old case-sensitive inferred
# MAGIC schema — 11 of 12 months are sitting in there with the real columns
# MAGIC null and the data stuck in `_rescued_data`. Re-running the notebook
# MAGIC as-is will NOT fix this: the checkpoint has already marked every file
# MAGIC as processed, so Auto Loader will skip them all on the next run.
# MAGIC
# MAGIC A full reprocess means: drop the Delta table data, delete the
# MAGIC checkpoint directory, delete the schema location, then re-run this
# MAGIC notebook top to bottom so Auto Loader treats all 12 months as new.
# MAGIC
# MAGIC **This deletes `bronze_tlc_deliveries` and cannot be undone.**
# MAGIC `CONFIRM_REPROCESS` defaults to `False` — flip it to `True` only after
# MAGIC you've confirmed it's safe to wipe the table, then run this cell,
# MAGIC then re-run cells 1-5 above.
# MAGIC
# MAGIC Nothing in this repo registers `bronze_tlc_deliveries` as a Unity
# MAGIC Catalog table (no `CREATE TABLE ... USING DELTA LOCATION` anywhere in
# MAGIC `databricks/`) — both bronze and silver only ever address it by path.
# MAGIC The `DROP TABLE IF EXISTS` below is a no-op if that holds in the
# MAGIC workspace too; it only matters if the table was registered manually
# MAGIC or by something outside this repo.
# MAGIC
# MAGIC Reprocessing also takes noticeably longer than the original bronze
# MAGIC run: with the checkpoint cleared, Auto Loader has no incremental
# MAGIC skip and reads all 12 months fresh (~540MB, 38.3M rows) instead of
# MAGIC picking up only new files.

# COMMAND ----------

CONFIRM_REPROCESS = False  # set to True only after confirming it's safe to wipe bronze_tlc_deliveries

if CONFIRM_REPROCESS:
    spark.sql("DROP TABLE IF EXISTS bronze_tlc_deliveries")
    dbutils.fs.rm(TARGET_PATH, recurse=True)
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    dbutils.fs.rm(SCHEMA_PATH, recurse=True)
    print("Dropped bronze_tlc_deliveries (catalog entry, if any, data, checkpoint, and schema location).")
    print("Now re-run cells 1-5 above to reprocess all 12 months with the fixed schema.")
else:
    print("CONFIRM_REPROCESS is False — nothing was dropped. Flip to True to proceed.")

# COMMAND ----------

# ── 7. Post-reprocess check — per-month null rate for VendorID/PULocationID ──
# Run this after the full reprocess completes to confirm all 12 months are
# now populated (i.e. no month shows an elevated null rate vs. the others).
post_reprocess_df = spark.read.format("delta").load(TARGET_PATH)

post_reprocess_monthly = (
    post_reprocess_df
        .withColumn("_source_month", F.regexp_extract(F.col("_source_file"), r"(\d{4}-\d{2})", 1))
        .groupBy("_source_month")
        .agg(
            F.count(F.lit(1)).alias("total_rows"),
            F.sum(F.col("VendorID").isNull().cast("int")).alias("vendor_id_nulls"),
            F.sum(F.col("PULocationID").isNull().cast("int")).alias("pu_location_id_nulls"),
        )
        .withColumn("vendor_id_null_pct", (F.col("vendor_id_nulls") / F.col("total_rows")) * 100)
        .withColumn("pu_location_id_null_pct", (F.col("pu_location_id_nulls") / F.col("total_rows")) * 100)
        .orderBy("_source_month")
)

display(post_reprocess_monthly)
