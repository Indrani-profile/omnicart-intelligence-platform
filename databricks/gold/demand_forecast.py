# Databricks notebook source
# MAGIC %md
# MAGIC # Demand Forecast — Gold
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `omnicart_databricks.gold.daily_delivery_summary` |
# MAGIC | **Target** | `omnicart_databricks.gold.demand_forecast` |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC | **Library** | `prophet` (installed via serverless job environment spec — see submit config, not a notebook `%pip install` cell, since this runs headless via `databricks jobs submit`) |
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Read** — loads `daily_delivery_summary` via its Unity Catalog table
# MAGIC    name, `spark.table(...)`, same pattern as `export_gold_to_adls.py` —
# MAGIC    never a raw `abfss://` path (see Session 4.x infra notes).
# MAGIC 2. **Prepare for Prophet** — Prophet requires columns named exactly `ds`
# MAGIC    (date) and `y` (value to forecast); rename `pickup_date` -> `ds` and
# MAGIC    `trip_count` -> `y`.
# MAGIC 3. **Validation split** — hold out the last 30 real days, train on
# MAGIC    everything before that.
# MAGIC 4. **Fit + forecast (validation)** — Prophet with weekly seasonality
# MAGIC    explicit, forecast the 30 held-out days, compute MAPE.
# MAGIC 5. **Naive baseline** — same-day-last-week prediction for the same 30
# MAGIC    days, compute its MAPE for comparison.
# MAGIC 6. **Compare** — print both MAPEs and whether Prophet actually beats
# MAGIC    the naive baseline.
# MAGIC 7. **Retrain on full data** — refit Prophet on all 365 days, forecast
# MAGIC    30 genuine future days beyond the historical data.
# MAGIC 8. **Build + write** — assemble `forecast_date`, `actual_trip_count`
# MAGIC    (null for future dates), `predicted_trip_count`,
# MAGIC    `predicted_lower_bound`, `predicted_upper_bound`, `is_future`, and
# MAGIC    write to `omnicart_databricks.gold.demand_forecast`.
# COMMAND ----------
# ── 1. Read daily_delivery_summary (Unity Catalog table) ──────────────────────
# spark.table(...) with a catalog-qualified name, same pattern as
# export_gold_to_adls.py — never a raw abfss:// path.
import pandas as pd
from prophet import Prophet

df = spark.table("omnicart_databricks.gold.daily_delivery_summary").toPandas()
# COMMAND ----------
# ── 2. Prepare for Prophet ─────────────────────────────────────────────────────
# Prophet requires columns named exactly ds (date) and y (value to forecast).
df = df[["pickup_date", "trip_count"]].rename(columns={"pickup_date": "ds", "trip_count": "y"})
df["ds"] = pd.to_datetime(df["ds"])
df = df.sort_values("ds").reset_index(drop=True)

print(f"Total days available: {len(df)}")
# COMMAND ----------
# ── 3. Validation split: last 30 days held out ────────────────────────────────
holdout_days = 30
train_df = df.iloc[:-holdout_days]
test_df = df.iloc[-holdout_days:]

print(f"Training on {len(train_df)} days, validating against {len(test_df)} held-out days")
# COMMAND ----------
# ── 4. Fit Prophet on training data, forecast the holdout ─────────────────────
# weekly_seasonality is on by default for daily-grain data, but made explicit
# here since delivery volume is expected to have a weekday/weekend pattern.
model_validation = Prophet(weekly_seasonality=True, yearly_seasonality=False)
model_validation.fit(train_df)

future_validation = model_validation.make_future_dataframe(periods=holdout_days)
forecast_validation = model_validation.predict(future_validation)

predicted_holdout = forecast_validation.set_index("ds").loc[test_df["ds"], "yhat"].values
actual_holdout = test_df["y"].values

prophet_mape = (abs((actual_holdout - predicted_holdout) / actual_holdout)).mean() * 100
print(f"Prophet MAPE on 30-day holdout: {prophet_mape:.2f}%")
# COMMAND ----------
# ── 5. Naive baseline: same-day-last-week ──────────────────────────────────────
naive_predictions = df.set_index("ds").loc[test_df["ds"] - pd.Timedelta(days=7), "y"].values
naive_mape = (abs((actual_holdout - naive_predictions) / actual_holdout)).mean() * 100
print(f"Naive (same-day-last-week) MAPE on 30-day holdout: {naive_mape:.2f}%")
# COMMAND ----------
# ── 6. Compare ─────────────────────────────────────────────────────────────────
beats_baseline = prophet_mape < naive_mape
print(f"\nProphet {'beats' if beats_baseline else 'does NOT beat'} the naive baseline "
      f"({prophet_mape:.2f}% vs {naive_mape:.2f}% MAPE)")
# COMMAND ----------
# ── 7. Retrain on FULL data, forecast 30 genuine future days ──────────────────
model_full = Prophet(weekly_seasonality=True, yearly_seasonality=False)
model_full.fit(df)

future_full = model_full.make_future_dataframe(periods=30)
forecast_full = model_full.predict(future_full)
# COMMAND ----------
# ── 8. Build the output table: historical actuals + future forecast ──────────
result = forecast_full[["ds", "yhat", "yhat_lower", "yhat_upper"]].rename(columns={
    "ds": "forecast_date",
    "yhat": "predicted_trip_count",
    "yhat_lower": "predicted_lower_bound",
    "yhat_upper": "predicted_upper_bound",
})

actuals = df.rename(columns={"ds": "forecast_date", "y": "actual_trip_count"})
result = result.merge(actuals, on="forecast_date", how="left")
result["is_future"] = result["actual_trip_count"].isna()

result = result[[
    "forecast_date",
    "actual_trip_count",
    "predicted_trip_count",
    "predicted_lower_bound",
    "predicted_upper_bound",
    "is_future",
]]

print(f"\nFinal table: {len(result)} rows ({(~result['is_future']).sum()} historical, {result['is_future'].sum()} future)")
display(result)
# COMMAND ----------
# ── 9. Write to Gold ───────────────────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

result_spark = spark.createDataFrame(result)
result_spark.write.format("delta").mode("overwrite").saveAsTable("omnicart_databricks.gold.demand_forecast")

print("Written to omnicart_databricks.gold.demand_forecast")
# COMMAND ----------
# ── 10. Headless run summary ──────────────────────────────────────────────────
# print() output isn't captured by `databricks jobs get-run-output` for
# notebook tasks run via the Jobs API — dbutils.notebook.exit() is, so the
# key results are surfaced here for that path. The print() calls above still
# show up if this notebook is opened and run interactively in the workspace.
import json

dbutils.notebook.exit(json.dumps({
    "total_days": len(df),
    "prophet_mape": round(float(prophet_mape), 4),
    "naive_mape": round(float(naive_mape), 4),
    "prophet_beats_naive": bool(beats_baseline),
    "output_rows": len(result),
    "historical_rows": int((~result["is_future"]).sum()),
    "future_rows": int(result["is_future"].sum()),
}))
