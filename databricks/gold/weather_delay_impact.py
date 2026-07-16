# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Delay Impact — Gold
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `omnicart_databricks.silver.tlc_deliveries_weather` (Session 3.3c, validated Session 3.4) |
# MAGIC | **Target** | `omnicart_databricks.gold.weather_delay_impact` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Read** — loads `tlc_deliveries_weather` via its Unity Catalog table
# MAGIC    name, same as `delivery_sla.py` — `spark.read.table(...)` rather
# MAGIC    than a raw `abfss://` path (see Session 4.x infra notes).
# MAGIC 2. **Filter to calendar year 2023** — same exclusion as
# MAGIC    `daily_delivery_summary.py`/`delivery_sla.py`: 71 rows (~0.0002%)
# MAGIC    with corrupted `pickup_ts` values outside 2023.
# MAGIC 3. **SLA classification** — the same distance-tiered
# MAGIC    `expected_max_minutes`/`is_on_time`/`delay_minutes` logic as
# MAGIC    `delivery_sla.py`. This is intentional duplication, not a shared
# MAGIC    import — same classification, different grouping (weather severity
# MAGIC    here instead of pickup location). Verified manually in Databricks:
# MAGIC    32,688,774 on-time / 5,164,178 delayed across the full 2023
# MAGIC    dataset — printed here as a sanity check, not recomputed logic.
# MAGIC 4. **Weather severity classification** — each trip's day is bucketed
# MAGIC    into one severity tier, checked in priority order: `Snow`
# MAGIC    (`snowfall_cm > 0`) → `Rain` (`precipitation_mm > 0` and no snow)
# MAGIC    → `Extreme Cold` (`temp_min_c < -5` and no precip/snow) → `Clear`
# MAGIC    (none of the above).
# MAGIC 5. **Aggregate by `pickup_date` + `weather_severity`** —
# MAGIC    `total_trips`, `on_time_trips`, `delayed_trips`, `on_time_rate`
# MAGIC    (4 decimals), `avg_delay_minutes` (2 decimals, delayed trips
# MAGIC    only — `F.avg` ignores the nulled on-time rows).
# MAGIC 6. **Preview** — prints the row count and displays the full result.
# MAGIC 7. **Write** — writes the verified result to
# MAGIC    `omnicart_databricks.gold.weather_delay_impact` as a Unity
# MAGIC    Catalog-managed Delta table (creates the `gold` schema first if it
# MAGIC    doesn't already exist).
# COMMAND ----------
# ── 1. Read tlc_deliveries_weather (Unity Catalog table) ──────────────────────
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
# ── 3. SLA classification — distance-tiered expected duration ─────────────────
# Same logic as delivery_sla.py (intentional duplication — same
# classification, different downstream grouping). Tiers: 0-2mi -> 15min,
# 2-5mi -> 25min, 5-10mi -> 40min, 10+mi -> 60min. delay_minutes is the
# overage for delayed trips only (0 for on-time trips).
sla_df = (
    tlc_weather_df
        .withColumn(
            "expected_max_minutes",
            F.when(F.col("trip_distance_miles") <= 2, 15)
             .when(F.col("trip_distance_miles") <= 5, 25)
             .when(F.col("trip_distance_miles") <= 10, 40)
             .otherwise(60)
        )
        .withColumn(
            "is_on_time",
            F.col("trip_duration_minutes") <= F.col("expected_max_minutes")
        )
        .withColumn(
            "delay_minutes",
            F.when(
                ~F.col("is_on_time"),
                F.col("trip_duration_minutes") - F.col("expected_max_minutes")
            ).otherwise(0.0)
        )
)

on_time_count = sla_df.filter(F.col("is_on_time")).count()
delayed_count = sla_df.filter(~F.col("is_on_time")).count()
print(f"On-time: {on_time_count:,} (expected 32,688,774)")
print(f"Delayed: {delayed_count:,} (expected 5,164,178)")
# COMMAND ----------
# ── 4. Weather severity classification ─────────────────────────────────────────
# Priority order matters: Snow is checked first, then Rain only if there was
# no snow, then Extreme Cold only if there was neither precip nor snow —
# F.when/.otherwise short-circuits in this order, so a snowy sub-zero day is
# classified as Snow, not Extreme Cold.
sla_df = sla_df.withColumn(
    "weather_severity",
    F.when(F.col("snowfall_cm") > 0, "Snow")
     .when(F.col("precipitation_mm") > 0, "Rain")
     .when(F.col("temp_min_c") < -5, "Extreme Cold")
     .otherwise("Clear")
)
# COMMAND ----------
# ── 5. Aggregate by pickup_date + weather_severity ────────────────────────────
weather_impact_df = (
    sla_df
        .groupBy("pickup_date", "weather_severity")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum(F.when(F.col("is_on_time"), 1).otherwise(0)).alias("on_time_trips"),
            F.sum(F.when(~F.col("is_on_time"), 1).otherwise(0)).alias("delayed_trips"),
            F.round(
                F.sum(F.when(F.col("is_on_time"), 1).otherwise(0)) / F.count("*"), 4
            ).alias("on_time_rate"),
            F.round(
                F.avg(F.when(~F.col("is_on_time"), F.col("delay_minutes"))), 2
            ).alias("avg_delay_minutes"),
        )
        .orderBy("pickup_date", "weather_severity")
)
# COMMAND ----------
# ── 6. Preview ─────────────────────────────────────────────────────────────────
weather_impact_row_count = weather_impact_df.count()
print(f"Weather delay impact row count: {weather_impact_row_count:,}")
display(weather_impact_df)
# COMMAND ----------
# ── 7. Write to Gold (registered in Unity Catalog) ─────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

weather_impact_df.write.format("delta").mode("overwrite").saveAsTable(
    "omnicart_databricks.gold.weather_delay_impact"
)

print("Write complete: omnicart_databricks.gold.weather_delay_impact")
