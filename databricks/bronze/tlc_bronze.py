# Databricks notebook source
# MAGIC %md
# MAGIC # TLC Yellow Taxi — Bronze Ingestion (Batch, per-file read + union)
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
# MAGIC 1. **Config** — path constants.
# MAGIC 2. **Discover files** — list the monthly parquet files under
# MAGIC    `SOURCE_PATH`.
# MAGIC 3. **Read + cast, per file** — each file is read on its own so Spark
# MAGIC    infers *that file's* own correct native schema (no cross-file
# MAGIC    type conflict is possible when only one file is involved), then
# MAGIC    the four ID columns are cast to `LongType` within that file's
# MAGIC    DataFrame using Spark's own `.cast()` — which freely handles
# MAGIC    Long↔Double, unlike the Parquet reader.
# MAGIC 4. **Union** — all 12 per-file DataFrames are combined with
# MAGIC    `unionByName(allowMissingColumns=True)` (the `allowMissingColumns`
# MAGIC    handles `congestion_surcharge`/`airport_fee` being absent from
# MAGIC    some early-2023 files).
# MAGIC 5. **Transform** — two audit columns appended to every row:
# MAGIC    `_source_file` (from the hidden `_metadata.file_path` column) and
# MAGIC    `_ingested_at` (wall-clock time of this run).
# MAGIC 6. **Write (batch, overwrite)** — a full overwrite of the Delta table
# MAGIC    at the target path. Safe here because this is a one-time load of
# MAGIC    12 static files, not an ongoing incremental feed.
# MAGIC 7. **Verify** — row count check.
# MAGIC 8. **Post-load check** — per-month null rate for `VendorID` /
# MAGIC    `PULocationID`, to confirm all 12 months are populated correctly.
# MAGIC
# MAGIC **Session 2.3 history** — this notebook went through five rounds of
# MAGIC fixes against the same underlying problem: the 12 monthly TLC files
# MAGIC are not schema-consistent with each other.
# MAGIC 1. Auto Loader's inferred schema locked onto January's column-name
# MAGIC    casing, rescuing 11 months' worth of differently-cased columns
# MAGIC    into `_rescued_data` (~92% null on the affected columns) — fixed
# MAGIC    with an explicit schema so case-insensitive resolution applied.
# MAGIC 2. That surfaced `passenger_count` stored as INT64 in some months
# MAGIC    against a declared `DoubleType` — fixed by disabling the
# MAGIC    vectorized Parquet reader (row-based reader tolerates the
# MAGIC    INT64→DOUBLE widening).
# MAGIC 3. That surfaced the reverse: `VendorID`/`PULocationID`/
# MAGIC    `DOLocationID` stored as DOUBLE in some months against a declared
# MAGIC    `LongType` — fixed by declaring every numeric field as
# MAGIC    `DoubleType` (the one universally-safe widening target) and
# MAGIC    casting the ID columns back to `LongType` afterward.
# MAGIC 4. Even with an all-`DoubleType` schema, the same
# MAGIC    `ClassCastException` (`MutableDouble` cannot be cast to
# MAGIC    `MutableLong`) recurred on `2023-10`, confirmed via a fresh query
# MAGIC    ID (not stale state) — a genuine Parquet-reader limitation, not
# MAGIC    something fixable via further schema declarations.
# MAGIC 5. A batch `mergeSchema=true` read was considered next, but ruled
# MAGIC    out before implementation: Spark's Parquet schema merge only
# MAGIC    reconciles differing *sets* of columns across files (schema
# MAGIC    evolution) and `NullType`/decimal-precision widening — it does
# MAGIC    **not** reconcile two files genuinely disagreeing on a column's
# MAGIC    primitive type (e.g. `LongType` vs `DoubleType`), and would have
# MAGIC    thrown `Failed to merge incompatible data types` at read time,
# MAGIC    the same underlying conflict resurfacing as a different error.
# MAGIC
# MAGIC **This step (part 5)** reads each of the 12 files individually so
# MAGIC every file is read at its own correct native type with zero reader-
# MAGIC level coercion, casts the ID columns using Spark's own `.cast()`
# MAGIC (unrestricted, unlike the Parquet reader) within each per-file
# MAGIC DataFrame, and unions the results — the Parquet reader is never
# MAGIC asked to reconcile two conflicting physical types in a single read.

# COMMAND ----------

# ── 1. Config ─────────────────────────────────────────────────────────────────
# ADLS auth is configured at the cluster level via the omnicart-kv Key Vault
# secret scope (set up in Session 1.3); no explicit credentials are needed here.
# No checkpoint or schema-inference location is needed — those are Auto
# Loader/streaming concepts that don't apply to a plain batch read.

SOURCE_PATH = "abfss://raw@omnicartdatalake.dfs.core.windows.net/tlc/"
TARGET_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_tlc_deliveries/"

# COMMAND ----------

# ── 2. Discover files ──────────────────────────────────────────────────────────
source_files = sorted(
    [f.path for f in dbutils.fs.ls(SOURCE_PATH) if f.path.endswith(".parquet")]
)

print(f"Discovered {len(source_files)} parquet files under {SOURCE_PATH}")
for path in source_files:
    print(f"  {path}")

# COMMAND ----------

# ── 3. Read + cast, per file ───────────────────────────────────────────────────
# Each file is read on its own so Spark infers that file's own correct
# native schema — with only one file involved there's no cross-file type
# conflict for the reader to hit. VendorID/PULocationID/DOLocationID/
# payment_type are cast to LongType immediately, using Spark's own .cast()
# (which freely handles Long<->Double), regardless of which numeric type
# that particular file's inference produced.
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from functools import reduce

ID_COLUMNS = ["VendorID", "PULocationID", "DOLocationID", "payment_type"]

per_file_dfs = []
for path in source_files:
    file_df = spark.read.parquet(path)
    for col_name in ID_COLUMNS:
        file_df = file_df.withColumn(col_name, F.col(col_name).cast(LongType()))
    row_count = file_df.count()
    print(f"Read {path} — {row_count:,} rows")
    per_file_dfs.append(file_df)

# COMMAND ----------

# ── 4. Union all files ─────────────────────────────────────────────────────────
# allowMissingColumns=True handles congestion_surcharge/airport_fee being
# absent from some early-2023 files — missing columns are filled with null.
unioned_df = reduce(
    lambda left, right: left.unionByName(right, allowMissingColumns=True),
    per_file_dfs,
)

# COMMAND ----------

# ── 5. Add audit columns ──────────────────────────────────────────────────────
enriched_df = (
    unioned_df
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# ── 6. Write (batch, overwrite) ───────────────────────────────────────────────
# overwrite is correct here: this is a full reprocess of a static 12-file
# dataset, not an incremental append scenario. overwriteSchema handles the
# target table's schema having changed across the earlier (streaming)
# attempts at this fix.
(
    enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(TARGET_PATH)
)

print("Batch ingestion complete.")

# COMMAND ----------

# ── 7. Verify row count ───────────────────────────────────────────────────────
display(spark.sql(f"SELECT COUNT(*) AS row_count FROM delta.`{TARGET_PATH}`"))

# COMMAND ----------

# ── 8. Post-load check — per-month null rate for VendorID/PULocationID ───────
# Confirms all 12 months are populated evenly (i.e. no month shows an
# elevated null rate vs. the others).
post_load_df = spark.read.format("delta").load(TARGET_PATH)

post_load_monthly = (
    post_load_df
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

display(post_load_monthly)
