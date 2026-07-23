# ── Export Gold tables to external storage for Snowflake access ────────────────
# Unity Catalog manages these tables' actual data files in Databricks' internal
# metastore storage account (dbstoragedxmd4jtkt35ry), which isn't meant for
# direct external/cross-cloud access. Instead, we write a plain-Parquet copy of
# each Gold table out to our own ADLS account (omnicartdatalake), under a
# dedicated 'exports' prefix — this becomes the stable, externally-readable
# location Snowflake's external stage (omnicart_gold_stage) points at.
#
# mode("overwrite") means re-running this is safe/idempotent: each run replaces
# the prior export with the current state of the table. For this dev/portfolio
# project this is a manual step run on demand; in a production dbt pipeline
# this export would typically run on a schedule (e.g. daily, right after the
# Gold layer refreshes), followed by ALTER EXTERNAL TABLE ... REFRESH on the
# Snowflake side (see snowflake_setup.sql, section 5).
#
# Verified run (2026-07-21): all five row counts matched Session 4.1/4.3
# figures exactly — 365 / 80,752 / 365 / 1,456 / 250.

GOLD_TABLES = [
    "daily_delivery_summary",
    "delivery_sla",
    "weather_delay_impact",
    "review_summary",
    "order_status_current",
    "demand_forecast",
]

EXPORT_BASE_PATH = "abfss://gold@omnicartdatalake.dfs.core.windows.net/exports"

for table_name in GOLD_TABLES:
    source_table = f"omnicart_databricks.gold.{table_name}"
    export_path = f"{EXPORT_BASE_PATH}/{table_name}/"

    print(f"Exporting {source_table} -> {export_path}")

    df = spark.table(source_table)
    row_count = df.count()

    df.write.mode("overwrite").parquet(export_path)

    print(f"  Done: {row_count} rows written")

print("\nAll Gold tables exported successfully.")
