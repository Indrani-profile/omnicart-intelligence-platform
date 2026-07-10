# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon Reviews — Bronze → Silver (Read, Rescued-Data Check, Rename, Parse)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_amazon_reviews/` |
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
