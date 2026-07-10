# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Enrichment — Open-Meteo NYC Daily (Step 1: Fetch and Shape)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `https://archive-api.open-meteo.com/v1/archive` (no API key required) |
# MAGIC | **Coverage** | NYC (40.7128 N, -74.0060 W), daily, 2023-01-01 to 2023-12-31 |
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
# MAGIC
# MAGIC **Note (Session 3.3a)** — this step only fetches and shapes the
# MAGIC weather data in isolation; it does not read `silver_tlc_deliveries` or
# MAGIC write anything to ADLS yet. The join and write land in later steps of
# MAGIC this session.

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
