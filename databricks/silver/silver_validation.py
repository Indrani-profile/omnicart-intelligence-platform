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
# MAGIC
# MAGIC **Note (Session 3.4a)** — every section below is fully self-contained:
# MAGIC each hardcodes its own ABFSS path and expected count and re-reads the
# MAGIC Delta table from disk, rather than depending on any variable set by an
# MAGIC earlier cell. This follows the fix applied to `weather_enrichment.py`'s
# MAGIC confirm-write cell in Session 3.3c, after a run where the notebook's
# MAGIC Python session had silently lost state between cells. Every cell here
# MAGIC — including the summary — can be re-run on its own, in any order,
# MAGIC regardless of session/kernel state.

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
