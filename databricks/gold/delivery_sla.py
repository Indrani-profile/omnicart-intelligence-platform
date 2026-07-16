# Databricks notebook source
# MAGIC %md
# MAGIC # Delivery SLA — Gold
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `omnicart_databricks.silver.tlc_deliveries_weather` (Session 3.3c, validated Session 3.4) |
# MAGIC | **Target** | `omnicart_databricks.gold.delivery_sla` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Read** — loads `tlc_deliveries_weather` via its Unity Catalog table
# MAGIC    name. Read via `spark.read.table(...)` rather than a raw `abfss://`
# MAGIC    path — direct-path reads on this workspace resolve to a restricted
# MAGIC    workspace-default storage credential regardless of External
# MAGIC    Location grants, so catalog-qualified table reads are used
# MAGIC    everywhere instead (see Session 4.x infra notes).
# MAGIC 2. **Filter to calendar year 2023** — same exclusion as
# MAGIC    `daily_delivery_summary.py`: 71 rows (~0.0002%) carry corrupted
# MAGIC    `pickup_ts` values from years outside 2023 that passed Week 3's
# MAGIC    validity rules but were never explicitly date-range-checked.
# MAGIC 3. **SLA classification** — a distance-tiered expected duration is
# MAGIC    applied to every trip: 0–2mi → 15min, 2–5mi → 25min, 5–10mi →
# MAGIC    40min, 10+mi → 60min (`expected_max_minutes`). A trip is
# MAGIC    `is_on_time` if `trip_duration_minutes` is within that budget;
# MAGIC    `delay_minutes` is the overage for delayed trips only (0 for
# MAGIC    on-time trips). Verified manually in Databricks against the full
# MAGIC    2023 dataset: 32,688,774 on-time / 5,164,178 delayed (~86%/14%) —
# MAGIC    printed here as a sanity check, not recomputed logic.
# MAGIC 4. **Aggregate by `pickup_date` + `pickup_location_id`** —
# MAGIC    `total_trips`, `on_time_trips`, `delayed_trips`, `on_time_rate`
# MAGIC    (4 decimals), `avg_delay_minutes` (2 decimals, delayed trips
# MAGIC    only — `F.avg` ignores the nulled on-time rows).
# MAGIC 5. **Preview** — prints the row count and displays the full result.
# MAGIC 6. **Write** — writes the verified result to
# MAGIC    `omnicart_databricks.gold.delivery_sla` as a Unity Catalog-managed
# MAGIC    Delta table (creates the `gold` schema first if it doesn't already
# MAGIC    exist).
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
# Tiers: 0-2mi -> 15min, 2-5mi -> 25min, 5-10mi -> 40min, 10+mi -> 60min.
# delay_minutes is the overage for delayed trips only (0 for on-time trips).
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
# ── 4. Aggregate by pickup_date + pickup_location_id ──────────────────────────
sla_summary_df = (
    sla_df
        .groupBy("pickup_date", "pickup_location_id")
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
        .orderBy("pickup_date", "pickup_location_id")
)
# COMMAND ----------
# ── 5. Preview ─────────────────────────────────────────────────────────────────
sla_summary_row_count = sla_summary_df.count()
print(f"Delivery SLA summary row count: {sla_summary_row_count:,}")
display(sla_summary_df)
# COMMAND ----------
# ── 6. Write to Gold (registered in Unity Catalog) ─────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

sla_summary_df.write.format("delta").mode("overwrite").saveAsTable(
    "omnicart_databricks.gold.delivery_sla"
)

print("Write complete: omnicart_databricks.gold.delivery_sla")
