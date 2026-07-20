# Databricks notebook source
# MAGIC %md
# MAGIC # Order Events Consumer — Structured Streaming (Event Hubs → Bronze + Gold)
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `order-events` Event Hub (Session 1.2 — 3 partitions, 7-day retention), consumer group `databricks-consumer`. Populated by `ingestion/event_producer.py` (Session 4.2 — 1000 events / 250 orders, verified in Event Hubs metrics). |
# MAGIC | **Target 1** | `omnicart_databricks.bronze.order_events_raw` — append-only, full event history |
# MAGIC | **Target 2** | `omnicart_databricks.gold.order_status_current` — one row per order, current lifecycle state |
# MAGIC | **Runtime** | Databricks 17.3 / Spark 4.0 — Unity Catalog enabled |
# MAGIC
# MAGIC **Session 4.3 note** — this is the project's first Structured Streaming +
# MAGIC `foreachBatch` + `MERGE` notebook (everything before this has been batch
# MAGIC reads/writes or plain streaming appends). Flagged in session notes as new
# MAGIC territory; treat the `foreachBatch` dedup logic and the `MERGE` condition
# MAGIC as the most likely places to need follow-up fixes. The Event Hubs
# MAGIC connection originally used the `azure-eventhubs-spark` connector; that was
# MAGIC unmaintained and unconfirmed against Spark 4.0, so this reads via Event
# MAGIC Hubs' Kafka-compatible endpoint instead, using Spark's built-in
# MAGIC `spark-sql-kafka-0-10` connector (ships with the runtime — no cluster
# MAGIC library install needed).
# MAGIC
# MAGIC **Sections**
# MAGIC
# MAGIC 1. **Config** — Event Hub namespace/name, consumer group, table names,
# MAGIC    checkpoint paths.
# MAGIC 2. **Event Hubs connection config (Kafka-compatible endpoint)** — reads
# MAGIC    the connection string from the `omnicart-kv` secret scope (never
# MAGIC    hardcoded) and builds the `kafka.*` options for `spark-sql-kafka-0-10`:
# MAGIC    SASL/PLAIN auth over SSL against the namespace's `:9093` Kafka
# MAGIC    endpoint, with the connection string passed as the SASL password
# MAGIC    (`username="$ConnectionString"` is a literal required by Event Hubs'
# MAGIC    Kafka surface, not a placeholder to fill in).
# MAGIC 3. **Event schema** — matches the producer's JSON shape exactly.
# MAGIC 4. **Read + parse stream** — the Kafka connector's `value` column arrives
# MAGIC    as binary (this replaces `body` from the old Event Hubs-native
# MAGIC    connector); cast to string, parse as JSON against the schema from
# MAGIC    section 3, and parse `event_timestamp` into a real `TimestampType`.
# MAGIC 5. **Create target tables** — `bronze` schema + the `gold.order_status_current`
# MAGIC    table are created up front (`CREATE TABLE IF NOT EXISTS`) so the `MERGE`
# MAGIC    in section 7 always has a target to merge into, even on a fresh run.
# MAGIC 6. **Bronze `foreachBatch`** — every parsed event appended as-is, no dedup,
# MAGIC    no merge. Full audit trail.
# MAGIC 7. **Gold `foreachBatch` + `MERGE`** — per micro-batch: dedup down to the
# MAGIC    latest event per `order_id` (by `event_timestamp`), then `MERGE INTO`
# MAGIC    `order_status_current` so each order always reflects its most recent
# MAGIC    known lifecycle stage, never a stale one.
# MAGIC 8. **Start both streams** — `trigger(availableNow=True)` on both writes
# MAGIC    (processes everything currently in the hub, then stops — this is a
# MAGIC    bounded 1000-event batch, not an always-on stream), then
# MAGIC    `awaitTermination()` on both.
# MAGIC 9. **Verification** — row counts on both tables, a preview of
# MAGIC    `order_status_current`, sanity checks against the known producer totals
# MAGIC    (250 distinct orders, 1000 raw events), and a `current_status` breakdown.

# COMMAND ----------
# ── 1. Config ───────────────────────────────────────────────────────────────────
# spark-sql-kafka-0-10 ships with the Databricks Runtime — no cluster library
# install needed (unlike the azure-eventhubs-spark connector this replaced).

EVENTHUB_NAMESPACE = "omnicart-events"  # Event Hubs namespace name (Session 1.2)
EVENTHUB_NAME = "order-events"

BRONZE_TABLE = "omnicart_databricks.bronze.order_events_raw"
GOLD_TABLE = "omnicart_databricks.gold.order_status_current"

BRONZE_CHECKPOINT = "abfss://gold@omnicartdatalake.dfs.core.windows.net/_checkpoints/order_events_raw/"
GOLD_CHECKPOINT = "abfss://gold@omnicartdatalake.dfs.core.windows.net/_checkpoints/order_status_current/"

# COMMAND ----------
# ── 2. Event Hubs connection config (Kafka-compatible endpoint) ────────────────
# Connection string comes from the omnicart-kv secret scope — never hardcoded.
# Create the secret first (one-time, from a local shell with the Databricks CLI
# configured):
#   databricks secrets put-secret omnicart-kv eventhub-connection-string
#
# Event Hubs exposes a Kafka-compatible endpoint on port 9093; authenticating
# against it uses Kafka's SASL/PLAIN mechanism over SSL, with the Event Hubs
# connection string as the SASL password. username="$ConnectionString" is a
# fixed literal Event Hubs' Kafka surface requires — not a placeholder.
#
# consumerGroup isn't a Kafka option — the Kafka protocol has no concept of
# named consumer groups the way the native Event Hubs SDK does (Kafka's
# "group.id" is for consumer-side offset coordination, which Structured
# Streaming manages itself via checkpoints). The databricks-consumer consumer
# group from Session 1.2 was specific to the native connector and has no
# equivalent setting here.
eventhub_namespace = EVENTHUB_NAMESPACE
eventhub_name = EVENTHUB_NAME
connection_string = dbutils.secrets.get(scope="omnicart-kv", key="eventhub-connection-string")

kafka_options = {
    "kafka.bootstrap.servers": f"{eventhub_namespace}.servicebus.windows.net:9093",
    "subscribe": eventhub_name,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{connection_string}";'
    ),
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}

# COMMAND ----------
# ── 3. Event schema ─────────────────────────────────────────────────────────────
# Matches ingestion/event_producer.py's build_event() output exactly.
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

EVENT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("delivery_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("pickup_location_id", LongType(), True),
    StructField("vendor_id", LongType(), True),
    StructField("order_value", DoubleType(), True),
])

# COMMAND ----------
# ── 4. Read + parse stream ──────────────────────────────────────────────────────
# value arrives as binary from the Kafka connector (this replaces body from the
# old Event Hubs-native connector); cast to string, parse as JSON, then parse
# event_timestamp (ISO8601 string) into a real TimestampType.
from pyspark.sql import functions as F

raw_stream_df = spark.readStream.format("kafka").options(**kafka_options).load()

parsed_stream_df = (
    raw_stream_df
        .select(F.col("value").cast("string").alias("json_body"))
        .select(F.from_json(F.col("json_body"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
)

# COMMAND ----------
# ── 5. Create target tables ─────────────────────────────────────────────────────
# order_status_current is created explicitly (not left to saveAsTable) so the
# MERGE in section 7 has a target on the very first micro-batch. bronze.order_events_raw
# is created implicitly by saveAsTable on its first append (section 6) — no
# MERGE dependency there, so no explicit CREATE TABLE is needed for it.
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS omnicart_databricks.gold")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        order_id STRING,
        delivery_id STRING,
        pickup_location_id LONG,
        vendor_id LONG,
        order_value DOUBLE,
        current_status STRING,
        last_event_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------
# ── 6. Bronze foreachBatch — append-only, full history ─────────────────────────
# No dedup, no merge: every parsed event lands in bronze exactly once, unmodified,
# as the full audit trail. foreachBatch is used here (rather than a plain
# writeStream.outputMode("append")) mainly for the per-batch row-count logging;
# functionally this is equivalent to a straight append.
def write_bronze_batch(batch_df, batch_id):
    batch_count = batch_df.count()
    if batch_count == 0:
        print(f"[bronze] batch {batch_id}: empty, skipping")
        return
    batch_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(f"[bronze] batch {batch_id}: appended {batch_count} rows")

# COMMAND ----------
# ── 7. Gold foreachBatch + MERGE — current lifecycle state per order ───────────
# A micro-batch can contain multiple events for the same order_id (e.g.
# order_created and picked_up both land in the same batch). Deduplicate to the
# latest event per order_id by event_timestamp *before* merging, otherwise the
# MERGE's "ON target.order_id = source.order_id" would match multiple source
# rows to one target row, which Delta's MERGE rejects at runtime.
#
# The MERGE condition (source.last_event_timestamp > target.last_event_timestamp)
# additionally guards against out-of-order delivery across batches: if a batch
# somehow ships an older event after a newer one already merged, it's a no-op
# rather than clobbering the current state with stale data.
from pyspark.sql import Window

def write_gold_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[gold] batch {batch_id}: empty, skipping")
        return

    latest_per_order_window = Window.partitionBy("order_id").orderBy(F.col("event_timestamp").desc())

    deduped_df = (
        batch_df
            .withColumn("_rn", F.row_number().over(latest_per_order_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select(
                "order_id",
                "delivery_id",
                "pickup_location_id",
                "vendor_id",
                "order_value",
                F.col("event_type").alias("current_status"),
                F.col("event_timestamp").alias("last_event_timestamp"),
            )
            .withColumn("updated_at", F.current_timestamp())
    )

    deduped_df.createOrReplaceGlobalTempView(f"order_status_updates_{batch_id}")
    update_view = f"global_temp.order_status_updates_{batch_id}"

    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} AS target
        USING {update_view} AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.last_event_timestamp > target.last_event_timestamp THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"DROP VIEW IF EXISTS {update_view}")
    print(f"[gold] batch {batch_id}: merged {deduped_df.count()} deduped order updates")

# COMMAND ----------
# ── 8. Start both streams and wait for completion ──────────────────────────────
# availableNow=True processes everything currently sitting in the hub (the
# producer's 1000-event run) and then stops — appropriate for this bounded
# batch rather than an infinite always-on stream. Two independent streaming
# queries share the same parsed_stream_df source but write to different
# tables with different checkpoints, so each tracks its own Event Hubs offsets
# independently.
bronze_query = (
    parsed_stream_df.writeStream
        .foreachBatch(write_bronze_batch)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
)

gold_query = (
    parsed_stream_df.writeStream
        .foreachBatch(write_gold_batch)
        .option("checkpointLocation", GOLD_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
)

bronze_query.awaitTermination()
gold_query.awaitTermination()

print("Both streaming writes complete.")

# COMMAND ----------
# ── 9. Verification ─────────────────────────────────────────────────────────────
# Self-contained: re-reads both tables from Unity Catalog rather than reusing
# any in-memory DataFrame/count from earlier cells, so this cell can be re-run
# on its own after a session restart.
bronze_row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}").collect()[0]["n"]
gold_row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {GOLD_TABLE}").collect()[0]["n"]
gold_distinct_orders = spark.sql(f"SELECT COUNT(DISTINCT order_id) AS n FROM {GOLD_TABLE}").collect()[0]["n"]

print(f"{BRONZE_TABLE}: {bronze_row_count:,} rows (expected 1000)")
print(f"{GOLD_TABLE}: {gold_row_count:,} rows, {gold_distinct_orders:,} distinct order_id (expected 250 / 250)")

display(spark.sql(f"SELECT * FROM {GOLD_TABLE} ORDER BY updated_at DESC"))

# COMMAND ----------
assert bronze_row_count == 1000, f"Expected 1000 rows in {BRONZE_TABLE}, got {bronze_row_count}"
assert gold_distinct_orders == 250, f"Expected 250 distinct order_id in {GOLD_TABLE}, got {gold_distinct_orders}"
print("Sanity checks passed: 1000 raw events, 250 distinct orders in current-state table.")

# COMMAND ----------
# current_status breakdown — every order should have settled at a final status
# (delivered or cancelled), since MERGE always keeps the latest event per order
# and every order's queue ends at one of those two stages.
display(
    spark.sql(f"""
        SELECT current_status, COUNT(*) AS order_count
        FROM {GOLD_TABLE}
        GROUP BY current_status
        ORDER BY order_count DESC
    """)
)
