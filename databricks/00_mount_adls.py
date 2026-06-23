# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS Gen2 paths
# MAGIC Defines ABFSS path constants for the `raw`, `bronze`, `silver`, and
# MAGIC `gold` containers and configures Spark to authenticate against the
# MAGIC OmniCart ADLS Gen2 storage account using the account key from the
# MAGIC `omnicart-kv` secret scope.
# MAGIC
# MAGIC `dbutils.fs.mount` is deprecated under Unity Catalog, so other
# MAGIC notebooks should `%run` this notebook and reference these path
# MAGIC constants directly instead of mounting.

# COMMAND ----------

STORAGE_ACCOUNT = "omnicartdatalake"
RAW_PATH = "abfss://raw@omnicartdatalake.dfs.core.windows.net/"
BRONZE_PATH = "abfss://bronze@omnicartdatalake.dfs.core.windows.net/"
SILVER_PATH = "abfss://silver@omnicartdatalake.dfs.core.windows.net/"
GOLD_PATH = "abfss://gold@omnicartdatalake.dfs.core.windows.net/"

SECRET_SCOPE = "omnicart-kv"
SECRET_KEY = "adls-connection-string"

# COMMAND ----------

connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
account_key = dict(
    pair.split("=", 1) for pair in connection_string.split(";") if pair
)["AccountKey"]

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", account_key
)

# COMMAND ----------

print(f"RAW_PATH:    {RAW_PATH}")
print(f"BRONZE_PATH: {BRONZE_PATH}")
print(f"SILVER_PATH: {SILVER_PATH}")
print(f"GOLD_PATH:   {GOLD_PATH}")
