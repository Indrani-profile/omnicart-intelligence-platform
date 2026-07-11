# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Validation — Row Count & Schema Audit (Read-Only)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Scope** | 3 silver tables + 2 rejects tables, all under `abfss://silver@omnicartdatalake.dfs.core.windows.net/` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC | **Writes** | None — this notebook is read-only auditing, no transforms |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Auth** — same Key Vault pattern as the other silver notebooks.
# MAGIC 2. **`tlc_deliveries`** (Session 3.1c) — fresh read, row count vs.
# MAGIC    expected 37,853,023, PASS/FAIL, `printSchema()`.
# MAGIC 3. **`amazon_reviews`** (Session 3.2c) — fresh read, row count vs.
# MAGIC    expected 168,434,728, PASS/FAIL, `printSchema()`.
# MAGIC 4. **`tlc_deliveries_weather`** (Session 3.3c) — fresh read, row count
# MAGIC    vs. expected 37,853,023, PASS/FAIL, `printSchema()`.
# MAGIC 5. **`_rejects/tlc_deliveries`** — fresh read, row count vs. expected
# MAGIC    433,095, PASS/FAIL. No schema print — rejects aren't a consumption
# MAGIC    surface.
# MAGIC 6. **`_rejects/amazon_reviews`** — fresh read, row count vs. expected
# MAGIC    2, PASS/FAIL. No schema print.
# MAGIC 7. **Summary** — re-reads all 5 tables independently (does not reuse
# MAGIC    any count/dataframe variable from sections 2-6) and prints a single
# MAGIC    table/dataframe name, expected count, actual count, PASS/FAIL for
# MAGIC    all five.
# MAGIC 8. **`tlc_deliveries` null checks** — null count/percentage on
# MAGIC    `pickup_ts`, `dropoff_ts`, `pickup_location_id`,
# MAGIC    `dropoff_location_id`, `delivery_id`. Expected 0% given the
# MAGIC    validity rules from Session 3.1b.
# MAGIC 9. **`amazon_reviews` null checks + rating range** — null
# MAGIC    count/percentage on `asin`, `user_id`, `review_ts`, `review_id`
# MAGIC    (expected 0% given Session 3.2c's validity rules), plus
# MAGIC    `rating` min/max (expected within Amazon's 1–5 scale).
# MAGIC 10. **`tlc_deliveries_weather` null check** — null count/percentage on
# MAGIC     `temp_max_c`. Session 3.3c measured 71 rows / 0.0002%; this
# MAGIC     confirms it's still exactly that, not drifted.
# MAGIC 11. **Duplicate checks** — count of duplicate `delivery_id` in
# MAGIC     `tlc_deliveries` and duplicate `review_id` in `amazon_reviews`.
# MAGIC     Expected 0 for both, since both were deduplicated in their
# MAGIC     respective step 2s (Session 3.1b / 3.2c).
# MAGIC 12. **Quality summary** — re-reads all data independently (same
# MAGIC     no-shared-state approach as section 7) and prints overall
# MAGIC     PASS/FAIL across every check in sections 8-11.
# MAGIC
# MAGIC **Note (Session 3.4a)** — every section below is fully self-contained:
# MAGIC each hardcodes its own ABFSS path and expected count and re-reads the
# MAGIC Delta table from disk, rather than depending on any variable set by an
# MAGIC earlier cell. This follows the fix applied to `weather_enrichment.py`'s
# MAGIC confirm-write cell in Session 3.3c, after a run where the notebook's
# MAGIC Python session had silently lost state between cells. Every cell here
# MAGIC — including the summary — can be re-run on its own, in any order,
# MAGIC regardless of session/kernel state.
# MAGIC
# MAGIC **Session 3.4a** confirmed all 5 tables PASS on row count. **Session
# MAGIC 3.4b** adds a lighter quality-check pass on top — null rates and
# MAGIC duplicate keys — following the same self-contained pattern.

# COMMAND ----------

# ── 1. Auth ───────────────────────────────────────────────────────────────────
spark.conf.set(
    "fs.azure.account.key.omnicartdatalake.dfs.core.windows.net",
    dbutils.secrets.get(scope="omnicart-kv", key="adls-account-key"),
)

# COMMAND ----------

# ── 2. tlc_deliveries — row count + schema ────────────────────────────────────
# Fully self-contained: hardcoded path and expected count, independent of any
# other cell in this notebook.
TLC_DELIVERIES_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/tlc_deliveries"
TLC_DELIVERIES_EXPECTED = 37_853_023

tlc_deliveries_df = spark.read.format("delta").load(TLC_DELIVERIES_PATH)
tlc_deliveries_actual = tlc_deliveries_df.count()
tlc_deliveries_status = "PASS" if tlc_deliveries_actual == TLC_DELIVERIES_EXPECTED else "FAIL"

print(f"[{tlc_deliveries_status}] tlc_deliveries")
print(f"  expected: {TLC_DELIVERIES_EXPECTED:,}")
print(f"  actual:   {tlc_deliveries_actual:,}")
if tlc_deliveries_status == "FAIL":
    print(f"  MISMATCH: {tlc_deliveries_actual - TLC_DELIVERIES_EXPECTED:+,} rows vs. expected")

print("\nSchema:")
tlc_deliveries_df.printSchema()

# COMMAND ----------

# ── 3. amazon_reviews — row count + schema ────────────────────────────────────
# Fully self-contained: hardcoded path and expected count, independent of any
# other cell in this notebook.
AMAZON_REVIEWS_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/amazon_reviews"
AMAZON_REVIEWS_EXPECTED = 168_434_728

amazon_reviews_df = spark.read.format("delta").load(AMAZON_REVIEWS_PATH)
amazon_reviews_actual = amazon_reviews_df.count()
amazon_reviews_status = "PASS" if amazon_reviews_actual == AMAZON_REVIEWS_EXPECTED else "FAIL"

print(f"[{amazon_reviews_status}] amazon_reviews")
print(f"  expected: {AMAZON_REVIEWS_EXPECTED:,}")
print(f"  actual:   {amazon_reviews_actual:,}")
if amazon_reviews_status == "FAIL":
    print(f"  MISMATCH: {amazon_reviews_actual - AMAZON_REVIEWS_EXPECTED:+,} rows vs. expected")

print("\nSchema:")
amazon_reviews_df.printSchema()

# COMMAND ----------

# ── 4. tlc_deliveries_weather — row count + schema ────────────────────────────
# Fully self-contained: hardcoded path and expected count, independent of any
# other cell in this notebook.
TLC_WEATHER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/tlc_deliveries_weather"
TLC_WEATHER_EXPECTED = 37_853_023

tlc_weather_df = spark.read.format("delta").load(TLC_WEATHER_PATH)
tlc_weather_actual = tlc_weather_df.count()
tlc_weather_status = "PASS" if tlc_weather_actual == TLC_WEATHER_EXPECTED else "FAIL"

print(f"[{tlc_weather_status}] tlc_deliveries_weather")
print(f"  expected: {TLC_WEATHER_EXPECTED:,}")
print(f"  actual:   {tlc_weather_actual:,}")
if tlc_weather_status == "FAIL":
    print(f"  MISMATCH: {tlc_weather_actual - TLC_WEATHER_EXPECTED:+,} rows vs. expected")

print("\nSchema:")
tlc_weather_df.printSchema()

# COMMAND ----------

# ── 5. _rejects/tlc_deliveries — row count only ───────────────────────────────
# Fully self-contained: hardcoded path and expected count. No schema print —
# rejects tables aren't a downstream consumption surface.
TLC_REJECTS_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/_rejects/tlc_deliveries"
TLC_REJECTS_EXPECTED = 433_095

tlc_rejects_df = spark.read.format("delta").load(TLC_REJECTS_PATH)
tlc_rejects_actual = tlc_rejects_df.count()
tlc_rejects_status = "PASS" if tlc_rejects_actual == TLC_REJECTS_EXPECTED else "FAIL"

print(f"[{tlc_rejects_status}] _rejects/tlc_deliveries")
print(f"  expected: {TLC_REJECTS_EXPECTED:,}")
print(f"  actual:   {tlc_rejects_actual:,}")
if tlc_rejects_status == "FAIL":
    print(f"  MISMATCH: {tlc_rejects_actual - TLC_REJECTS_EXPECTED:+,} rows vs. expected")

# COMMAND ----------

# ── 6. _rejects/amazon_reviews — row count only ───────────────────────────────
# Fully self-contained: hardcoded path and expected count. No schema print —
# rejects tables aren't a downstream consumption surface.
AMAZON_REJECTS_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/_rejects/amazon_reviews"
AMAZON_REJECTS_EXPECTED = 2

amazon_rejects_df = spark.read.format("delta").load(AMAZON_REJECTS_PATH)
amazon_rejects_actual = amazon_rejects_df.count()
amazon_rejects_status = "PASS" if amazon_rejects_actual == AMAZON_REJECTS_EXPECTED else "FAIL"

print(f"[{amazon_rejects_status}] _rejects/amazon_reviews")
print(f"  expected: {AMAZON_REJECTS_EXPECTED:,}")
print(f"  actual:   {amazon_rejects_actual:,}")
if amazon_rejects_status == "FAIL":
    print(f"  MISMATCH: {amazon_rejects_actual - AMAZON_REJECTS_EXPECTED:+,} rows vs. expected")

# COMMAND ----------

# ── 7. Summary — all 5 tables ─────────────────────────────────────────────────
# Deliberately re-reads every table from scratch rather than reusing the
# *_actual variables set by sections 2-6, so this cell can be run on its own
# (e.g. after a session restart) and still produce a complete summary.
SILVER_ROOT = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"

VALIDATION_TARGETS = [
    ("tlc_deliveries",            SILVER_ROOT + "tlc_deliveries",              37_853_023),
    ("amazon_reviews",            SILVER_ROOT + "amazon_reviews",             168_434_728),
    ("tlc_deliveries_weather",    SILVER_ROOT + "tlc_deliveries_weather",      37_853_023),
    ("_rejects/tlc_deliveries",   SILVER_ROOT + "_rejects/tlc_deliveries",        433_095),
    ("_rejects/amazon_reviews",   SILVER_ROOT + "_rejects/amazon_reviews",              2),
]

summary_rows = []
for table_name, table_path, expected_count in VALIDATION_TARGETS:
    actual_count = spark.read.format("delta").load(table_path).count()
    status = "PASS" if actual_count == expected_count else "FAIL"
    summary_rows.append((table_name, expected_count, actual_count, status))

name_width = max(len(row[0]) for row in summary_rows)
print(f"{'table':<{name_width}}  {'expected':>14}  {'actual':>14}  status")
print(f"{'-' * name_width}  {'-' * 14}  {'-' * 14}  ------")
for table_name, expected_count, actual_count, status in summary_rows:
    print(f"{table_name:<{name_width}}  {expected_count:>14,}  {actual_count:>14,}  {status}")

overall_status = "PASS" if all(row[3] == "PASS" for row in summary_rows) else "FAIL"
print(f"\nOverall: {overall_status}")

# COMMAND ----------

# ── 8. tlc_deliveries — null checks ───────────────────────────────────────────
# Fully self-contained: hardcoded path, independent read, independent of any
# other cell in this notebook. Expected 0% on all five given the validity
# rules from Session 3.1b (non-null timestamps/location IDs; delivery_id is
# a derived hash so it can only be null if its inputs were, which the
# validity rules already reject).
from pyspark.sql import functions as F

TLC_DELIVERIES_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/tlc_deliveries"
TLC_NULL_CHECK_COLUMNS = [
    "pickup_ts",
    "dropoff_ts",
    "pickup_location_id",
    "dropoff_location_id",
    "delivery_id",
]

tlc_deliveries_df = spark.read.format("delta").load(TLC_DELIVERIES_PATH)
tlc_deliveries_total = tlc_deliveries_df.count()

print(f"tlc_deliveries — null checks (total rows: {tlc_deliveries_total:,})")
for column_name in TLC_NULL_CHECK_COLUMNS:
    null_count = tlc_deliveries_df.filter(F.col(column_name).isNull()).count()
    null_pct = (null_count / tlc_deliveries_total) * 100 if tlc_deliveries_total else 0.0
    status = "PASS" if null_count == 0 else "FAIL"
    print(f"  [{status}] {column_name}: {null_count:,} nulls ({null_pct:.4f}%)")

# COMMAND ----------

# ── 9. amazon_reviews — null checks + rating range ────────────────────────────
# Fully self-contained: hardcoded path, independent read. Null checks expect
# 0% given Session 3.2c's validity rules (non-null asin/user_id/review_ts;
# review_id is a derived hash of those three, so it can only be null if its
# inputs were). rating is expected within Amazon's 1-5 scale per the same
# validity rules.
from pyspark.sql import functions as F

AMAZON_REVIEWS_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/amazon_reviews"
AMAZON_NULL_CHECK_COLUMNS = ["asin", "user_id", "review_ts", "review_id"]

amazon_reviews_df = spark.read.format("delta").load(AMAZON_REVIEWS_PATH)
amazon_reviews_total = amazon_reviews_df.count()

print(f"amazon_reviews — null checks (total rows: {amazon_reviews_total:,})")
for column_name in AMAZON_NULL_CHECK_COLUMNS:
    null_count = amazon_reviews_df.filter(F.col(column_name).isNull()).count()
    null_pct = (null_count / amazon_reviews_total) * 100 if amazon_reviews_total else 0.0
    status = "PASS" if null_count == 0 else "FAIL"
    print(f"  [{status}] {column_name}: {null_count:,} nulls ({null_pct:.4f}%)")

rating_min, rating_max = amazon_reviews_df.select(
    F.min("rating"), F.max("rating")
).first()
rating_status = "PASS" if (rating_min >= 1 and rating_max <= 5) else "FAIL"
print(f"  [{rating_status}] rating range: min={rating_min}, max={rating_max} (expected within 1-5)")

# COMMAND ----------

# ── 10. tlc_deliveries_weather — null check on temp_max_c ─────────────────────
# Fully self-contained: hardcoded path, independent read. Session 3.3c
# measured 71 rows / 0.0002% null on temp_max_c (dates outside the 2023
# Open-Meteo coverage window); this confirms it's still exactly that count,
# not drifted from a rerun of the join upstream.
from pyspark.sql import functions as F

TLC_WEATHER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/tlc_deliveries_weather"
TLC_WEATHER_NULL_TEMP_EXPECTED = 71

tlc_weather_df = spark.read.format("delta").load(TLC_WEATHER_PATH)
tlc_weather_total = tlc_weather_df.count()
temp_max_null_count = tlc_weather_df.filter(F.col("temp_max_c").isNull()).count()
temp_max_null_pct = (
    (temp_max_null_count / tlc_weather_total) * 100 if tlc_weather_total else 0.0
)
temp_max_status = "PASS" if temp_max_null_count == TLC_WEATHER_NULL_TEMP_EXPECTED else "FAIL"

print(f"tlc_deliveries_weather — null check (total rows: {tlc_weather_total:,})")
print(f"  [{temp_max_status}] temp_max_c: {temp_max_null_count:,} nulls ({temp_max_null_pct:.4f}%), expected {TLC_WEATHER_NULL_TEMP_EXPECTED:,}")
if temp_max_status == "FAIL":
    print(f"  DRIFT: {temp_max_null_count - TLC_WEATHER_NULL_TEMP_EXPECTED:+,} rows vs. Session 3.3c's measurement")

# COMMAND ----------

# ── 11. Duplicate key checks ───────────────────────────────────────────────────
# Fully self-contained: hardcoded paths, independent reads. Both tables were
# deduplicated on their key column during their respective silver builds
# (delivery_id in Session 3.1b, review_id in Session 3.2c), so any duplicate
# found here means dedup logic regressed or the table was rewritten since.
from pyspark.sql import functions as F

TLC_DELIVERIES_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/tlc_deliveries"
AMAZON_REVIEWS_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/amazon_reviews"

tlc_deliveries_df = spark.read.format("delta").load(TLC_DELIVERIES_PATH)
tlc_duplicate_delivery_ids = (
    tlc_deliveries_df.groupBy("delivery_id").count().filter(F.col("count") > 1).count()
)
tlc_dup_status = "PASS" if tlc_duplicate_delivery_ids == 0 else "FAIL"
print(f"[{tlc_dup_status}] tlc_deliveries duplicate delivery_id groups: {tlc_duplicate_delivery_ids:,}")

amazon_reviews_df = spark.read.format("delta").load(AMAZON_REVIEWS_PATH)
amazon_duplicate_review_ids = (
    amazon_reviews_df.groupBy("review_id").count().filter(F.col("count") > 1).count()
)
amazon_dup_status = "PASS" if amazon_duplicate_review_ids == 0 else "FAIL"
print(f"[{amazon_dup_status}] amazon_reviews duplicate review_id groups: {amazon_duplicate_review_ids:,}")

# COMMAND ----------

# ── 12. Quality summary — all checks from sections 8-11 ──────────────────────
# Deliberately re-reads every table from scratch and recomputes every check
# rather than reusing variables from sections 8-11, so this cell can be run
# on its own (e.g. after a session restart) and still produce a complete
# summary.
from pyspark.sql import functions as F

SILVER_ROOT = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"

quality_check_rows = []

tlc_df = spark.read.format("delta").load(SILVER_ROOT + "tlc_deliveries")
tlc_total = tlc_df.count()
for column_name in ["pickup_ts", "dropoff_ts", "pickup_location_id", "dropoff_location_id", "delivery_id"]:
    null_count = tlc_df.filter(F.col(column_name).isNull()).count()
    quality_check_rows.append(
        (f"tlc_deliveries.{column_name} nulls", "0", f"{null_count:,}", "PASS" if null_count == 0 else "FAIL")
    )

amazon_df = spark.read.format("delta").load(SILVER_ROOT + "amazon_reviews")
amazon_total = amazon_df.count()
for column_name in ["asin", "user_id", "review_ts", "review_id"]:
    null_count = amazon_df.filter(F.col(column_name).isNull()).count()
    quality_check_rows.append(
        (f"amazon_reviews.{column_name} nulls", "0", f"{null_count:,}", "PASS" if null_count == 0 else "FAIL")
    )

amazon_rating_min, amazon_rating_max = amazon_df.select(F.min("rating"), F.max("rating")).first()
amazon_rating_ok = amazon_rating_min >= 1 and amazon_rating_max <= 5
quality_check_rows.append(
    ("amazon_reviews.rating range", "1-5", f"{amazon_rating_min}-{amazon_rating_max}", "PASS" if amazon_rating_ok else "FAIL")
)

weather_df = spark.read.format("delta").load(SILVER_ROOT + "tlc_deliveries_weather")
weather_null_count = weather_df.filter(F.col("temp_max_c").isNull()).count()
quality_check_rows.append(
    ("tlc_deliveries_weather.temp_max_c nulls", "71", f"{weather_null_count:,}", "PASS" if weather_null_count == 71 else "FAIL")
)

tlc_dup_count = tlc_df.groupBy("delivery_id").count().filter(F.col("count") > 1).count()
quality_check_rows.append(
    ("tlc_deliveries duplicate delivery_id", "0", f"{tlc_dup_count:,}", "PASS" if tlc_dup_count == 0 else "FAIL")
)

amazon_dup_count = amazon_df.groupBy("review_id").count().filter(F.col("count") > 1).count()
quality_check_rows.append(
    ("amazon_reviews duplicate review_id", "0", f"{amazon_dup_count:,}", "PASS" if amazon_dup_count == 0 else "FAIL")
)

check_width = max(len(row[0]) for row in quality_check_rows)
print(f"{'check':<{check_width}}  {'expected':>10}  {'actual':>12}  status")
print(f"{'-' * check_width}  {'-' * 10}  {'-' * 12}  ------")
for check_name, expected, actual, status in quality_check_rows:
    print(f"{check_name:<{check_width}}  {expected:>10}  {actual:>12}  {status}")

quality_overall_status = "PASS" if all(row[3] == "PASS" for row in quality_check_rows) else "FAIL"
print(f"\nOverall quality check status: {quality_overall_status}")
