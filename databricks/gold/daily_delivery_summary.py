# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Delivery Summary — Gold (Build & Preview, No Write Yet)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `SILVER_PATH + "tlc_deliveries_weather"` (Session 3.3c, validated Session 3.4) |
# MAGIC | **Target** | none yet — this step only builds and eyeballs the aggregate |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Auth** — same Key Vault pattern as the silver notebooks.
# MAGIC 2. **Read** — loads `tlc_deliveries_weather` (trip + weather already
# MAGIC    joined in Session 3.3c, so no join is needed here).
# MAGIC 3. **Aggregate by `pickup_date`** — `trip_count`, `avg_fare_amount`,
# MAGIC    `avg_trip_distance_miles`, `avg_trip_duration_minutes`,
# MAGIC    `total_revenue` (sum of `total_amount`), plus that date's
# MAGIC    `temp_max_c`/`temp_min_c`/`precipitation_mm`/`snowfall_cm` passed
# MAGIC    through via `first()` — these are already per-date-constant from
# MAGIC    the weather join, so this is deduplication of a repeated value,
# MAGIC    not an aggregation choice.
# MAGIC 4. **Preview** — prints the row count (expect 365, one per day of
# MAGIC    2023) and displays the full result.
# MAGIC
# MAGIC **Note (Session 4.1a)** — read-only build/preview step. No write to
# MAGIC ADLS yet; partitioning/write strategy is designed in a later step of
# MAGIC this session.

# COMMAND ----------

# ── 1. Auth ───────────────────────────────────────────────────────────────────
spark.conf.set(
    "fs.azure.account.key.omnicartdatalake.dfs.core.windows.net",
    dbutils.secrets.get(scope="omnicart-kv", key="adls-account-key"),
)

# COMMAND ----------

# ── 2. Read tlc_deliveries_weather (batch) ────────────────────────────────────
# Trip and weather data are already joined (Session 3.3c) — no join needed.
SILVER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"

tlc_weather_df = spark.read.format("delta").load(SILVER_PATH + "tlc_deliveries_weather")

# COMMAND ----------

# ── 3. Aggregate by pickup_date ────────────────────────────────────────────────
# temp_max_c/temp_min_c/precipitation_mm/snowfall_cm are already constant per
# pickup_date from the weather join in Session 3.3c — first() here is just
# deduplicating that repeated value, not making an aggregation choice.
from pyspark.sql import functions as F

daily_summary_df = (
    tlc_weather_df
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("trip_distance_miles").alias("avg_trip_distance_miles"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.sum("total_amount").alias("total_revenue"),
            F.first("temp_max_c").alias("temp_max_c"),
            F.first("temp_min_c").alias("temp_min_c"),
            F.first("precipitation_mm").alias("precipitation_mm"),
            F.first("snowfall_cm").alias("snowfall_cm"),
        )
        .orderBy("pickup_date")
)

# COMMAND ----------

# ── 4. Preview ─────────────────────────────────────────────────────────────────
daily_summary_row_count = daily_summary_df.count()
print(f"Daily summary row count: {daily_summary_row_count:,} (expected 365)")

display(daily_summary_df)
