# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen2 containers
# MAGIC Mounts the `raw`, `bronze`, `silver`, and `gold` containers from the
# MAGIC OmniCart ADLS Gen2 storage account under `/mnt/omnicart/`. Safe to
# MAGIC re-run: existing mounts are left untouched.

# COMMAND ----------

STORAGE_ACCOUNT_NAME = "omnicartdatalake"
CONTAINERS = ["raw", "bronze", "silver", "gold"]
MOUNT_ROOT = "/mnt/omnicart"
SECRET_SCOPE = "omnicart-kv"
SECRET_KEY = "adls-connection-string"

# COMMAND ----------

connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
account_key = dict(
    pair.split("=", 1) for pair in connection_string.split(";") if pair
)["AccountKey"]

# COMMAND ----------

existing_mounts = {mount.mountPoint for mount in dbutils.fs.mounts()}

for container in CONTAINERS:
    mount_point = f"{MOUNT_ROOT}/{container}"

    if mount_point in existing_mounts:
        print(f"Already mounted, skipping: {mount_point}")
        continue

    dbutils.fs.mount(
        source=f"abfss://{container}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": account_key
        },
    )
    print(f"Mounted: {mount_point}")
