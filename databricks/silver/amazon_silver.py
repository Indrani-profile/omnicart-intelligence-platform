# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon Reviews — Bronze → Silver (Read, Rename, Validate, Dedup, Write)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_amazon_reviews/` |
# MAGIC | **Target** | `abfss://silver@omnicartdatalake.dfs.core.windows.net/amazon_reviews/` |
# MAGIC | **Rejects** | `abfss://silver@omnicartdatalake.dfs.core.windows.net/_rejects/amazon_reviews/` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Auth** — sets the ADLS Gen2 account key from the `omnicart-kv`
# MAGIC    Key Vault secret scope so subsequent ABFSS paths resolve without
# MAGIC    needing cluster-level credentials.
# MAGIC 2. **Config** — path constants for the bronze and silver containers.
# MAGIC 3. **Read** — loads the `bronze_amazon_reviews` Delta table (plain
# MAGIC    batch read).
# MAGIC 4. **Schema inspection** — prints `printSchema()` and `dtypes` so the
# MAGIC    real structure is visible before any rename/cast/parse logic is
# MAGIC    written.
# MAGIC 5. **Row count** — prints the total bronze row count.
# MAGIC 6. **Preview** — displays a sample of raw rows.
# MAGIC 7. **Nested-field notes** — flags array/struct-typed columns (e.g.
# MAGIC    `images`) that will need parsing in a later step.
# MAGIC 8. **Rescued-data check** — prints the count/percentage of non-null
# MAGIC    `_rescued_data` rows and a few sample values, if any exist. TLC's
# MAGIC    bronze table (Session 2.3) had a schema-drift bug hiding in
# MAGIC    `_rescued_data` despite an otherwise-plausible-looking schema, so
# MAGIC    this isn't skipped just because the inferred schema looks clean.
# MAGIC 9. **Rename + timestamp** — `asin`, `parent_asin`, `user_id`, `rating`,
# MAGIC    `text`, `title`, `verified_purchase`, `helpful_vote` pass through
# MAGIC    unchanged (already good names); `timestamp` (epoch milliseconds) is
# MAGIC    converted to a `TimestampType` column named `review_ts`.
# MAGIC 10. **Parse images** — derives `image_count` (size of the `images`
# MAGIC     array) and `primary_image_url` (`large_image_url` of the first
# MAGIC     element, null if empty) as new top-level columns. The original
# MAGIC     `images` array is kept, not dropped.
# MAGIC 11. **Preview parsed schema** — prints the bronze row count and
# MAGIC     displays a sample of the renamed/parsed dataframe.
# MAGIC 12. **Derived columns** — `review_id` (a deterministic SHA-256 hash of
# MAGIC     `asin` + `user_id` + `review_ts`, so reruns are idempotent instead
# MAGIC     of a random UUID — matching `delivery_id` in `tlc_silver.py`) and
# MAGIC     `category` (parsed from `_source_file`, since each of the 5 source
# MAGIC     categories was ingested as its own `.jsonl` file — see
# MAGIC     `ingestion/download_amazon.py`).
# MAGIC 13. **Validity rules** — splits the dataframe into `valid_df` and
# MAGIC     `rejected_df` (rejects are kept, not silently dropped) based on
# MAGIC     non-null `asin`/`user_id`/`review_ts`, `rating` within Amazon's
# MAGIC     1–5 scale, and non-null `text` (an empty-string review is still
# MAGIC     valid — only a missing one is rejected).
# MAGIC 14. **Deduplicate** — `valid_df` is deduplicated on `review_id`,
# MAGIC     keeping the row with the most recent `_ingested_at` per group.
# MAGIC 15. **Summary** — prints bronze row count, valid row count, rejected
# MAGIC     row count, and reject rate.
# MAGIC 16. **Write valid** — writes `valid_df` to
# MAGIC     `SILVER_PATH + "amazon_reviews"` as Delta, batch overwrite,
# MAGIC     partitioned by `category` (5 values, derived in step 12 — a
# MAGIC     natural low-cardinality partition key here since the source data
# MAGIC     is already split one file per category, unlike TLC where the
# MAGIC     partition key is the business date column instead).
# MAGIC 17. **Write rejects** — writes `rejected_df` to
# MAGIC     `SILVER_PATH + "_rejects/amazon_reviews"` as Delta, batch
# MAGIC     overwrite, unpartitioned.
# MAGIC 18. **Confirm** — prints the row count written to the valid-data path.
# MAGIC     No Unity Catalog table registration, matching the deferred,
# MAGIC     path-based-only approach in `tlc_silver.py` (Session 3.1c) — the
# MAGIC     managed identity still lacks Read/List/Write on the silver
# MAGIC     External Location.
# MAGIC
# MAGIC **Note (Session 3.2a)** — this is read-only inspection. Given how many
# MAGIC assumptions about TLC's bronze schema turned out wrong in Session 2.3
# MAGIC (cross-month type drift resolved via per-file read/cast/union — see
# MAGIC `databricks/bronze/tlc_bronze.py`), no renaming, casting, or transform
# MAGIC logic is written here. The Amazon Reviews bronze table spans 5
# MAGIC categories (Automotive, Cell_Phones, Clothing, Electronics, Sports)
# MAGIC ingested via Auto Loader with `cloudFiles.inferColumnTypes` in Session
# MAGIC 2.4 — inferred JSON schemas can vary by category or by which fields
# MAGIC happened to be present/null in the first files Auto Loader saw, so the
# MAGIC actual schema needs to be confirmed before designing the rename/parse
# MAGIC step.
# MAGIC
# MAGIC **Session 3.2b** confirmed the bronze schema is clean and well-typed
# MAGIC (`asin` string, `helpful_vote` long, `images`
# MAGIC `array<struct<attachment_type, large_image_url, medium_image_url,
# MAGIC small_image_url>>`, `parent_asin` string, `rating` double, `text`
# MAGIC string, `timestamp` long epoch-millis, `title` string, `user_id`
# MAGIC string, `verified_purchase` boolean) — simpler than TLC's cross-file
# MAGIC drift, since this is JSON with an already-inferred schema from Auto
# MAGIC Loader rather than per-file physical Parquet types. This step adds the
# MAGIC `_rescued_data` check, rename, `review_ts` conversion, and images
# MAGIC parsing.
# MAGIC
# MAGIC **Session 3.2c** verified step 2 clean (`_rescued_data` 0%, `review_ts`
# MAGIC shows correct 2014–2022 dates with no epoch-zero artifacts,
# MAGIC `image_count`/`primary_image_url` populated only where images exist)
# MAGIC and adds `review_id`/`category` derived columns, validity rules,
# MAGIC dedup, and the write to `silver`.

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

bronze_df = spark.read.format("delta").load(BRONZE_PATH + "bronze_amazon_reviews")

# COMMAND ----------

# ── 4. Schema inspection ───────────────────────────────────────────────────────
bronze_df.printSchema()
print(bronze_df.dtypes)

# COMMAND ----------

# ── 5. Row count ───────────────────────────────────────────────────────────────
bronze_row_count = bronze_df.count()
print(f"Bronze row count: {bronze_row_count:,}")

# COMMAND ----------

# ── 6. Preview raw rows ────────────────────────────────────────────────────────
display(bronze_df.limit(20))

# COMMAND ----------

# ── 7. Nested-field notes ──────────────────────────────────────────────────────
# Flags any array/struct-typed columns so we know what will need
# explode/parsing logic in step 2, rather than assuming based on the
# HuggingFace dataset docs.
from pyspark.sql.types import ArrayType, StructType

nested_fields = [
    (field.name, field.dataType)
    for field in bronze_df.schema.fields
    if isinstance(field.dataType, (ArrayType, StructType))
]

if nested_fields:
    print("Nested (array/struct) fields found — may need parsing in step 2:")
    for name, dtype in nested_fields:
        print(f"  - {name}: {dtype.simpleString()}")
else:
    print("No array/struct fields found at the top level.")

# COMMAND ----------

# ── 8. Rescued-data check ──────────────────────────────────────────────────────
# Don't assume _rescued_data is empty just because the inferred schema looks
# well-typed — TLC's bronze table (Session 2.3) had a schema-drift bug hiding
# in _rescued_data despite an otherwise-plausible schema.
rescued_count = bronze_df.filter(F.col("_rescued_data").isNotNull()).count()
rescued_pct = (rescued_count / bronze_row_count) * 100 if bronze_row_count else 0.0

print(f"Non-null _rescued_data rows: {rescued_count:,} ({rescued_pct:.4f}%)")

if rescued_count > 0:
    display(
        bronze_df
            .filter(F.col("_rescued_data").isNotNull())
            .select("_rescued_data", "_source_file")
            .limit(10)
    )

# COMMAND ----------

# ── 9. Rename + timestamp conversion ──────────────────────────────────────────
# asin/parent_asin/user_id/rating/text/title/verified_purchase/helpful_vote
# already match the fact_reviews vocabulary and pass through unchanged.
# timestamp (epoch milliseconds) becomes review_ts (TimestampType).
silver_df = bronze_df.withColumn(
    "review_ts", (F.col("timestamp") / 1000).cast("timestamp")
)

# COMMAND ----------

# ── 10. Parse images array ─────────────────────────────────────────────────────
# image_count and primary_image_url are derived as new top-level columns;
# the original images array is kept as-is in case it's needed later.
# F.size() on a null array returns -1 (not null/0) under Spark's default
# legacy sizeOfNull behavior, so a null-array case is handled explicitly
# rather than trusting size() alone.
image_count_expr = F.when(F.col("images").isNull(), F.lit(0)).otherwise(
    F.size(F.col("images"))
)

silver_df = (
    silver_df
        .withColumn("image_count", image_count_expr)
        .withColumn(
            "primary_image_url",
            F.when(
                image_count_expr > 0,
                F.col("images")[0]["large_image_url"],
            ).otherwise(F.lit(None).cast("string")),
        )
)

# COMMAND ----------

# ── 11. Preview renamed/parsed schema ──────────────────────────────────────────
print(f"Bronze row count: {bronze_row_count:,}")
display(silver_df.limit(20))

# COMMAND ----------

# ── 12. Derived columns ────────────────────────────────────────────────────────
# review_id is a deterministic SHA-256 hash of asin + user_id + review_ts —
# not a random UUID — so re-running this notebook on the same bronze data
# produces the same IDs instead of minting new ones every run (matching the
# delivery_id pattern in tlc_silver.py).
#
# category is parsed from _source_file rather than assumed: each of the 5
# source categories was downloaded and uploaded as its own .jsonl file (see
# ingestion/download_amazon.py), so the file's base name is the category.
silver_df = (
    silver_df
        .withColumn(
            "review_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("asin").cast("string"),
                    F.col("user_id").cast("string"),
                    F.col("review_ts").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "category",
            F.regexp_extract(F.col("_source_file"), r"([^/]+)\.jsonl$", 1),
        )
)

# COMMAND ----------

# ── 13. Validity rules — split into valid_df / rejected_df ───────────────────
# Rejected rows are kept in rejected_df rather than filtered and dropped
# silently, so they can be inspected/audited later.
VALID_CONDITION = (
    F.col("asin").isNotNull()
    & F.col("user_id").isNotNull()
    & F.col("rating").between(1, 5)
    & F.col("review_ts").isNotNull()
    & F.col("text").isNotNull()
)

flagged_df = silver_df.withColumn("_is_valid", VALID_CONDITION)
valid_df = flagged_df.filter(F.col("_is_valid")).drop("_is_valid")
rejected_df = flagged_df.filter(~F.col("_is_valid")).drop("_is_valid")

# COMMAND ----------

# ── 14. Deduplicate valid_df on review_id ─────────────────────────────────────
# Keeps the row with the most recent _ingested_at per review_id in case of
# any overlap.
from pyspark.sql.window import Window

dedup_window = Window.partitionBy("review_id").orderBy(F.col("_ingested_at").desc())

valid_df = (
    valid_df
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
)

# COMMAND ----------

# ── 15. Summary ─────────────────────────────────────────────────────────────────
valid_row_count = valid_df.count()
rejected_row_count = rejected_df.count()
reject_rate_pct = (rejected_row_count / bronze_row_count) * 100

print(f"Bronze row count:   {bronze_row_count:,}")
print(f"Valid row count:    {valid_row_count:,}")
print(f"Rejected row count: {rejected_row_count:,}")
print(f"Reject rate:        {reject_rate_pct:.4f}%")

# COMMAND ----------

# ── 16. Write valid_df → Delta (batch, overwrite, partitioned by category) ───
# category is a natural low-cardinality (5-value) partition key here, since
# the source data already arrives one file per category — unlike TLC, where
# the partition key is a business date column instead.
SILVER_TABLE_PATH = SILVER_PATH + "amazon_reviews"

(
    valid_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("category")
        .save(SILVER_TABLE_PATH)
)

# COMMAND ----------

# ── 17. Write rejected_df → Delta (batch, overwrite, unpartitioned) ──────────
# Kept for auditing/debugging, not for downstream consumption.
SILVER_REJECTS_PATH = SILVER_PATH + "_rejects/amazon_reviews"

(
    rejected_df.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_REJECTS_PATH)
)

# COMMAND ----------

# ── 18. Confirm write ─────────────────────────────────────────────────────────
# No Unity Catalog table registration — path-based access only, matching the
# deferred approach in tlc_silver.py (Session 3.1c): the managed identity
# still lacks Read/List/Write on the silver External Location.
print(f"Wrote {valid_row_count:,} rows to {SILVER_TABLE_PATH}")
