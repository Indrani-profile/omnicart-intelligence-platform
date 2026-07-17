# Databricks notebook source
# MAGIC %md
# MAGIC # Review Summary — Gold
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `omnicart_databricks.silver.amazon_reviews` (Session 3.2c) |
# MAGIC | **Target** | `omnicart_databricks.gold.review_summary` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Read** — loads `amazon_reviews` via its Unity Catalog table name.
# MAGIC    Read via `spark.read.table(...)` rather than a raw `abfss://` path
# MAGIC    — direct-path reads on this workspace resolve to a restricted
# MAGIC    workspace-default storage credential regardless of External
# MAGIC    Location grants, so catalog-qualified table reads are used
# MAGIC    everywhere instead (see Session 4.x infra notes). No date filter
# MAGIC    is needed here, unlike the TLC-derived gold tables — this dataset
# MAGIC    doesn't have the corrupted-timestamp issue. No join is needed
# MAGIC    either.
# MAGIC 2. **Derive `category` and `review_month`** — the registered table
# MAGIC    doesn't expose `category` as a column directly, so it's re-derived
# MAGIC    from `_source_file` (paths like
# MAGIC    `abfss://raw@omnicartdatalake.dfs.core.windows.net/amazon/Electronics.jsonl`)
# MAGIC    via the same regex extraction as `amazon_silver.py`'s derived-columns
# MAGIC    step. `review_month` is `review_ts` formatted as `yyyy-MM`.
# MAGIC 3. **Aggregate by `category` + `review_month`** — `review_count`
# MAGIC    (`F.count("*")`), `avg_rating` (2 decimals), `verified_purchase_pct`
# MAGIC    (average of `verified_purchase` cast to int, 4 decimals — the
# MAGIC    fraction of reviews that are verified purchases).
# MAGIC 4. **Preview** — prints the row count and displays the full result,
# MAGIC    ordered by `category`, `review_month`.
# MAGIC 5. **Write** — writes the verified result to
# MAGIC    `omnicart_databricks.gold.review_summary` as a Unity Catalog-managed
# MAGIC    Delta table (creates the `gold` schema first if it doesn't already
# MAGIC    exist).
# COMMAND ----------
# ── 1. Read amazon_reviews (Unity Catalog table) ──────────────────────────────
amazon_reviews_df = spark.read.table("omnicart_databricks.silver.amazon_reviews")
# COMMAND ----------
# ── 2. Derive category and review_month ────────────────────────────────────────
# category isn't exposed as a column on the registered table, so it's
# re-derived from _source_file the same way amazon_silver.py does it — each
# of the 5 source categories was ingested as its own .jsonl file, so the
# file's base name is the category.
from pyspark.sql import functions as F

reviews_df = (
    amazon_reviews_df
        .withColumn(
            "category",
            F.regexp_extract(F.col("_source_file"), r"([^/]+)\.jsonl$", 1),
        )
        .withColumn(
            "review_month",
            F.date_format(F.col("review_ts"), "yyyy-MM"),
        )
)
# COMMAND ----------
# ── 3. Aggregate by category + review_month ────────────────────────────────────
review_summary_df = (
    reviews_df
        .groupBy("category", "review_month")
        .agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(
                F.avg(F.col("verified_purchase").cast("int")), 4
            ).alias("verified_purchase_pct"),
        )
        .orderBy("category", "review_month")
)
# COMMAND ----------
# ── 4. Preview ─────────────────────────────────────────────────────────────────
review_summary_row_count = review_summary_df.count()
print(f"Review summary row count: {review_summary_row_count:,}")
display(review_summary_df)
# COMMAND ----------
# ── 5. Write to Gold (registered in Unity Catalog) ─────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

review_summary_df.write.format("delta").mode("overwrite").saveAsTable(
    "omnicart_databricks.gold.review_summary"
)

print("Write complete: omnicart_databricks.gold.review_summary")
