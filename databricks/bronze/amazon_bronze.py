# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon Reviews — Bronze Ingestion (Auto Loader)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `abfss://raw@omnicartdatalake.dfs.core.windows.net/amazon/` |
# MAGIC | **Target** | `abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_amazon_reviews/` |
# MAGIC | **Table** | `bronze_amazon_reviews` (Delta) |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Auth** — sets the ADLS Gen2 account key from the `omnicart-kv`
# MAGIC    Key Vault secret scope so subsequent ABFSS paths resolve without
# MAGIC    needing cluster-level credentials.
# MAGIC 2. **Config** — path constants for source, target, checkpoint, and
# MAGIC    schema inference location.
# MAGIC 3. **Read stream** — Auto Loader (`cloudFiles`) reads `.jsonl` files
# MAGIC    incrementally. On the first run it infers column types and persists
# MAGIC    the schema to `schemaLocation`; subsequent runs are schema-stable
# MAGIC    and will merge in any new fields added by future uploads.
# MAGIC 4. **Transform** — two audit columns are appended to every row:
# MAGIC    `_source_file` (the ABFSS path of the originating file, sourced from
# MAGIC    the Unity Catalog–compatible `_metadata.file_path` column) and
# MAGIC    `_ingested_at` (wall-clock timestamp of the ingestion micro-batch).
# MAGIC 5. **Write stream** — appends enriched rows to the Delta table at the
# MAGIC    target path. `trigger(availableNow=True)` drains all files not yet
# MAGIC    seen by the checkpoint, then stops the stream. Re-running the
# MAGIC    notebook is safe: Auto Loader's checkpoint prevents any file from
# MAGIC    being re-ingested.
# MAGIC 6. **Verify** — queries the Delta table for a row count so the operator
# MAGIC    can confirm data landed correctly before declaring the run complete.

# COMMAND ----------

# ── 1. Auth ───────────────────────────────────────────────────────────────────
spark.conf.set(
    "fs.azure.account.key.omnicartdatalake.dfs.core.windows.net",
    dbutils.secrets.get(scope="omnicart-kv", key="adls-account-key"),
)

# COMMAND ----------

# ── 2. Config ─────────────────────────────────────────────────────────────────
SOURCE_PATH     = "abfss://raw@omnicartdatalake.dfs.core.windows.net/amazon/"
TARGET_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_amazon_reviews/"
CHECKPOINT_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/checkpoints/amazon/"
SCHEMA_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/schemas/amazon/"

# COMMAND ----------

# ── 3. Read stream (Auto Loader) ──────────────────────────────────────────────
from pyspark.sql import functions as F

raw_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .load(SOURCE_PATH)
)

# COMMAND ----------

# ── 4. Add audit columns ──────────────────────────────────────────────────────
# _metadata.file_path is the Unity Catalog–compatible replacement for
# input_file_name(), which is deprecated for streaming sources in Spark 4.0.
enriched_stream = (
    raw_stream
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

# ── 5. Write stream → Delta (append, availableNow) ────────────────────────────
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

# ── 6. Verify row count ───────────────────────────────────────────────────────
display(spark.sql(f"SELECT COUNT(*) AS row_count FROM delta.`{TARGET_PATH}`"))
