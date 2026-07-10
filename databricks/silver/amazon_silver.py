# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon Reviews — Bronze → Silver (Step 1: Inspect Bronze Schema)
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
bronze_df = spark.read.format("delta").load(BRONZE_PATH + "bronze_amazon_reviews")

# COMMAND ----------

# ── 4. Schema inspection ───────────────────────────────────────────────────────
bronze_df.printSchema()
print(bronze_df.dtypes)

# COMMAND ----------

# ── 5. Row count ───────────────────────────────────────────────────────────────
print(f"Bronze row count: {bronze_df.count()}")

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
