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
# MAGIC    incrementally. On the first run it infers the schema and persists it
# MAGIC    to `schemaLocation` so subsequent runs are schema-stable.
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

# COMMAND ----------

# ── 1. Config ─────────────────────────────────────────────────────────────────
# ADLS auth is configured at the cluster level via the omnicart-kv Key Vault
# secret scope (set up in Session 1.3); no explicit credentials are needed here.

SOURCE_PATH     = "abfss://raw@omnicartdatalake.dfs.core.windows.net/tlc/"
TARGET_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/bronze_tlc_deliveries/"
CHECKPOINT_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/checkpoints/tlc/"
SCHEMA_PATH     = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/schemas/tlc/"

# COMMAND ----------

# ── 2. Read stream (Auto Loader) ──────────────────────────────────────────────
from pyspark.sql import functions as F

raw_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .load(SOURCE_PATH)
)

# COMMAND ----------

# ── 3. Add audit columns ──────────────────────────────────────────────────────
enriched_stream = (
    raw_stream
        .withColumn("_source_file", F.input_file_name())
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
