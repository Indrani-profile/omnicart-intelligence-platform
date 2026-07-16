# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Delivery Summary — Gold
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `omnicart_databricks.silver.tlc_deliveries_weather` (Session 3.3c, validated Session 3.4) |
# MAGIC | **Target** | `omnicart_databricks.gold.daily_delivery_summary` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Read** — loads `tlc_deliveries_weather` via its Unity Catalog table
# MAGIC    name (trip + weather already joined in Session 3.3c, so no join is
# MAGIC    needed here). Read via `spark.read.table(...)` rather than a raw
# MAGIC    `abfss://` path — direct-path reads on this workspace resolve to a
# MAGIC    restricted workspace-default storage credential regardless of
# MAGIC    External Location grants, so catalog-qualified table reads are used
# MAGIC    everywhere instead (see Session 4.x infra notes).
# MAGIC 2. **Filter to calendar year 2023** — a small number of source rows
# MAGIC    (71, ~0.0002%) carry corrupted `pickup_ts` values from years far
# MAGIC    outside 2023 (e.g. 2001, 2009, 2022-12-31 spillover) that passed
# MAGIC    Week 3's validity rules (duration/distance/fare/passenger-count
# MAGIC    checks) but were never explicitly date-range-checked. These are
# MAGIC    excluded here at the gold layer; worth adding as an explicit check
# MAGIC    in `silver_validation.py` as a fast-follow.
# MAGIC 3. **Aggregate by `pickup_date`** — `trip_count`, `avg_fare_amount`,
# MAGIC    `avg_trip_distance_miles`, `avg_trip_duration_minutes`,
# MAGIC    `total_revenue` (sum of `total_amount`), plus that date's
# MAGIC    `temp_max_c`/`temp_min_c`/`precipitation_mm`/`snowfall_cm` passed
# MAGIC    through via `first()` — these are already per-date-constant from
# MAGIC    the weather join, so this is deduplication of a repeated value,
# MAGIC    not an aggregation choice.
# MAGIC 4. **Preview** — prints the row count (expect 365, one per day of
# MAGIC    2023) and displays the full result.
# MAGIC 5. **Write** — writes the verified result to
# MAGIC    `omnicart_databricks.gold.daily_delivery_summary` as a Unity
# MAGIC    Catalog-managed Delta table.
# COMMAND ----------
# ── 1. Read tlc_deliveries_weather (Unity Catalog table) ──────────────────────
# Trip and weather data are already joined (Session 3.3c) — no join needed.
tlc_weather_df = spark.read.table("omnicart_databricks.silver.tlc_deliveries_weather")
# COMMAND ----------
# ── 2. Filter to calendar year 2023 ────────────────────────────────────────────
from pyspark.sql import functions as F

before_count = tlc_weather_df.count()

tlc_weather_df = tlc_weather_df.filter(
    (F.col("pickup_date") >= "2023-01-01") &
    (F.col("pickup_date") <= "2023-12-31")
)

after_count = tlc_weather_df.count()
print(f"Before filter: {before_count:,} rows")
print(f"After filter: {after_count:,} rows")
print(f"Filtered out: {before_count - after_count} rows with pickup_date outside 2023")
# COMMAND ----------
# ── 3. Aggregate by pickup_date ────────────────────────────────────────────────
# temp_max_c/temp_min_c/precipitation_mm/snowfall_cm are already constant per
# pickup_date from the weather join in Session 3.3c — first() here is just
# deduplicating that repeated value, not making an aggregation choice.
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
# COMMAND ----------
# ── 5. Write to Gold (registered in Unity Catalog) ─────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

daily_summary_df.write.format("delta").mode("overwrite").saveAsTable(
    "omnicart_databricks.gold.daily_delivery_summary"
)

print("Write complete: omnicart_databricks.gold.daily_delivery_summary")