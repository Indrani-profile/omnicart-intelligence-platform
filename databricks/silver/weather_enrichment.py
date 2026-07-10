# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Enrichment — Open-Meteo NYC Daily (Fetch, Join, Write)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `https://archive-api.open-meteo.com/v1/archive` (no API key required) |
# MAGIC | **Coverage** | NYC (40.7128 N, -74.0060 W), daily, 2023-01-01 to 2023-12-31 |
# MAGIC | **Joined against** | `silver_tlc_deliveries` (`SILVER_PATH + "tlc_deliveries"`, Session 3.1c) |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Config** — API endpoint, NYC coordinates, date range, and the
# MAGIC    daily variables requested (max/min temperature, precipitation,
# MAGIC    snowfall, max windspeed — the ones most likely to affect
# MAGIC    delivery/trip patterns). No ADLS auth is needed in this step: it's
# MAGIC    a driver-side HTTP call with no write yet.
# MAGIC 2. **Fetch** — calls the Open-Meteo archive API with `requests` from
# MAGIC    the driver node.
# MAGIC 3. **Inspect raw response** — prints the JSON's top-level keys,
# MAGIC    `daily_units`, and `daily` sub-keys/lengths, so the actual response
# MAGIC    shape is confirmed before assuming a structure.
# MAGIC 4. **Convert to Spark DataFrame** — builds `weather_df` with a
# MAGIC    `weather_date` column (`DateType`, matching `pickup_date` in
# MAGIC    `silver_tlc_deliveries` for the eventual join) and one column per
# MAGIC    requested weather variable.
# MAGIC 5. **Verify** — prints the row count (expected 365) and displays the
# MAGIC    full dataframe (small enough not to need `.limit()`).
# MAGIC 6. **Auth** — sets the ADLS Gen2 account key from the `omnicart-kv`
# MAGIC    Key Vault secret scope, same pattern as `tlc_silver.py`. Not needed
# MAGIC    until this step, since step 1 was a driver-side HTTP call only.
# MAGIC 7. **Read silver_tlc_deliveries** — loads the Session 3.1c Delta table
# MAGIC    (plain batch read) from `SILVER_PATH + "tlc_deliveries"`.
# MAGIC 8. **Join** — LEFT joins `weather_df` onto `tlc_df` on
# MAGIC    `pickup_date = weather_date` (LEFT rather than INNER so a trip whose
# MAGIC    `pickup_date` somehow falls outside 2023's weather coverage keeps
# MAGIC    its trip data with null weather columns instead of disappearing),
# MAGIC    then drops the now-redundant `weather_date` column.
# MAGIC 9. **Verify the join** — prints the joined row count (expected to
# MAGIC    match TLC silver's 37,853,023) and the count/percentage of rows
# MAGIC    where `temp_max_c` is null, to confirm the join actually matched
# MAGIC    rather than assuming a LEFT join with 0 unmatched rows by default.
# MAGIC 10. **Preview joined sample** — displays `pickup_ts`, `pickup_date`,
# MAGIC     and the weather columns together so the date alignment can be
# MAGIC     checked visually.
# MAGIC
# MAGIC **Note (Session 3.3a)** — this step only fetches and shapes the
# MAGIC weather data in isolation; it does not read `silver_tlc_deliveries` or
# MAGIC write anything to ADLS yet. The join and write land in later steps of
# MAGIC this session.
# MAGIC
# MAGIC **Session 3.3b** confirmed step 1 clean (365 rows, one per day of
# MAGIC 2023, sensible NYC values) and adds the read + LEFT join against
# MAGIC `silver_tlc_deliveries`. Still no write — that's step 3.

# COMMAND ----------

# ── 1. Config ──────────────────────────────────────────────────────────────────
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060

START_DATE = "2023-01-01"
END_DATE = "2023-12-31"

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "snowfall_sum",
    "windspeed_10m_max",
]

# COMMAND ----------

# ── 2. Fetch from Open-Meteo ──────────────────────────────────────────────────
import requests

response = requests.get(
    OPEN_METEO_URL,
    params={
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "America/New_York",
    },
    timeout=60,
)
response.raise_for_status()
weather_json = response.json()

# COMMAND ----------

# ── 3. Inspect raw response structure ─────────────────────────────────────────
print("Top-level keys:", list(weather_json.keys()))
print("daily_units:", weather_json.get("daily_units"))
print("daily sub-keys:", list(weather_json.get("daily", {}).keys()))
print("daily.time length:", len(weather_json.get("daily", {}).get("time", [])))

# COMMAND ----------

# ── 4. Convert to Spark DataFrame ─────────────────────────────────────────────
# weather_date uses DateType to match pickup_date in silver_tlc_deliveries,
# so the eventual join in a later step doesn't need a cast on either side.
from datetime import datetime

from pyspark.sql.types import DateType, DoubleType, StructField, StructType

daily = weather_json["daily"]

weather_rows = [
    (
        datetime.strptime(date_str, "%Y-%m-%d").date(),
        temp_max,
        temp_min,
        precipitation,
        snowfall,
        windspeed_max,
    )
    for date_str, temp_max, temp_min, precipitation, snowfall, windspeed_max in zip(
        daily["time"],
        daily["temperature_2m_max"],
        daily["temperature_2m_min"],
        daily["precipitation_sum"],
        daily["snowfall_sum"],
        daily["windspeed_10m_max"],
    )
]

WEATHER_SCHEMA = StructType(
    [
        StructField("weather_date", DateType(), False),
        StructField("temp_max_c", DoubleType(), True),
        StructField("temp_min_c", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("snowfall_cm", DoubleType(), True),
        StructField("windspeed_max_kmh", DoubleType(), True),
    ]
)

weather_df = spark.createDataFrame(weather_rows, schema=WEATHER_SCHEMA)

# COMMAND ----------

# ── 5. Verify ──────────────────────────────────────────────────────────────────
print(f"Weather row count: {weather_df.count()}")
display(weather_df)

# COMMAND ----------

# ── 6. Auth ────────────────────────────────────────────────────────────────────
spark.conf.set(
    "fs.azure.account.key.omnicartdatalake.dfs.core.windows.net",
    dbutils.secrets.get(scope="omnicart-kv", key="adls-account-key"),
)

# COMMAND ----------

# ── 7. Read silver_tlc_deliveries (batch) ─────────────────────────────────────
SILVER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"

tlc_df = spark.read.format("delta").load(SILVER_PATH + "tlc_deliveries")

# COMMAND ----------

# ── 8. Left join weather onto TLC silver, on pickup_date = weather_date ──────
# LEFT (not INNER) so a trip whose pickup_date somehow falls outside 2023's
# weather coverage keeps its trip data with null weather columns, rather than
# silently disappearing. weather_date is dropped after the join since
# pickup_date already covers that.
from pyspark.sql import functions as F

joined_df = (
    tlc_df.join(
        weather_df,
        tlc_df["pickup_date"] == weather_df["weather_date"],
        "left",
    )
    .drop("weather_date")
)

# COMMAND ----------

# ── 9. Verify the join actually matched ───────────────────────────────────────
# A LEFT join silently "succeeds" even if every row fails to match, so the
# null rate on a joined-in column is checked explicitly rather than assumed.
joined_row_count = joined_df.count()
temp_max_null_count = joined_df.filter(F.col("temp_max_c").isNull()).count()
temp_max_null_pct = (
    (temp_max_null_count / joined_row_count) * 100 if joined_row_count else 0.0
)

print(f"Joined row count:        {joined_row_count:,}")
print(f"temp_max_c null rows:    {temp_max_null_count:,} ({temp_max_null_pct:.4f}%)")

# COMMAND ----------

# ── 10. Preview joined sample ─────────────────────────────────────────────────
display(
    joined_df.select(
        "pickup_ts",
        "pickup_date",
        "temp_max_c",
        "temp_min_c",
        "precipitation_mm",
        "snowfall_cm",
        "windspeed_max_kmh",
    ).limit(20)
)
