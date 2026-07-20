"""Generates synthetic delivery order lifecycle events and streams them to the
order-events Event Hub, using real delivery/pickup/vendor IDs sampled from
omnicart_databricks.silver.tlc_deliveries for realism.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from databricks import sql as databricks_sql

CATALOG_TABLE = "omnicart_databricks.silver.tlc_deliveries"
SAMPLE_ROWS = 5000

ORDER_COUNT = 250
TOTAL_EVENTS = 1000  # ORDER_COUNT * len(LIFECYCLE_STAGES) below
MAX_CONCURRENT_ORDERS = 25

LIFECYCLE_STAGES = ("order_created", "picked_up", "in_transit")
DELIVERED_WEIGHT = 0.9  # vs. cancelled

ORDER_VALUE_MIN = 15.00
ORDER_VALUE_MAX = 250.00

SEND_DELAY_MIN_SECONDS = 0.5
SEND_DELAY_MAX_SECONDS = 1.5

PROGRESS_LOG_INTERVAL = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger("event_producer")


def sample_delivery_rows(sample_size=SAMPLE_ROWS):
    """Query a bounded sample of real delivery/pickup/vendor IDs from Databricks.

    Uses TABLESAMPLE so a 37M-row table doesn't get fully scanned.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    if not all([server_hostname, http_path, access_token]):
        raise RuntimeError(
            "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN "
            "environment variables must all be set"
        )

    logger.info("Connecting to Databricks SQL warehouse at %s", server_hostname)
    with databricks_sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        with connection.cursor() as cursor:
            logger.info(
                "Sampling %d rows from %s (TABLESAMPLE, no full scan)",
                sample_size, CATALOG_TABLE,
            )
            cursor.execute(
                f"""
                SELECT delivery_id, pickup_location_id, vendor_id
                FROM {CATALOG_TABLE}
                TABLESAMPLE ({sample_size} ROWS)
                """
            )
            rows = cursor.fetchall()

    if not rows:
        raise RuntimeError(f"No rows sampled from {CATALOG_TABLE}")

    sample = [
        {
            "delivery_id": row.delivery_id,
            "pickup_location_id": row.pickup_location_id,
            "vendor_id": row.vendor_id,
        }
        for row in rows
    ]
    logger.info("Cached %d sampled delivery rows for this run", len(sample))
    return sample


def build_order_queues(order_count=ORDER_COUNT):
    """Build one lifecycle event-type queue per synthetic order.

    Each order gets order_created -> picked_up -> in_transit -> (delivered|cancelled),
    with delivered/cancelled chosen by DELIVERED_WEIGHT.
    """
    orders = []
    for _ in range(order_count):
        final_stage = "delivered" if random.random() < DELIVERED_WEIGHT else "cancelled"
        queue = list(LIFECYCLE_STAGES) + [final_stage]
        orders.append({"order_id": str(uuid.uuid4()), "queue": queue})
    return orders


def interleave_order_events(orders, max_concurrent=MAX_CONCURRENT_ORDERS):
    """Yield (order_id, event_type) pairs across orders in an interleaved order.

    Keeps a bounded pool of "active" orders and, at each step, advances a
    randomly chosen active order by one event, so the stream simulates several
    orders progressing concurrently instead of finishing one before the next
    starts.
    """
    pending = list(orders)
    active = []

    while pending or active:
        while len(active) < max_concurrent and pending:
            active.append(pending.pop(0))

        order = random.choice(active)
        event_type = order["queue"].pop(0)
        yield order["order_id"], event_type

        if not order["queue"]:
            active.remove(order)


def build_event(order_id, event_type, sampled_row):
    return {
        "order_id": order_id,
        "delivery_id": sampled_row["delivery_id"],
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "pickup_location_id": sampled_row["pickup_location_id"],
        "vendor_id": sampled_row["vendor_id"],
        "order_value": round(random.uniform(ORDER_VALUE_MIN, ORDER_VALUE_MAX), 2),
    }


def send_events(producer, event_stream, id_samples, total_events=TOTAL_EVENTS):
    """Send events one at a time with a randomized delay between sends.

    Returns a summary dict with counts used for the final report.
    """
    event_type_counts = {}
    orders_seen = set()
    failed_events = 0
    start_time = time.monotonic()

    for i, (order_id, event_type) in enumerate(event_stream, start=1):
        sampled_row = random.choice(id_samples)
        event = build_event(order_id, event_type, sampled_row)
        orders_seen.add(order_id)

        try:
            producer.send_event(EventData(json.dumps(event)))
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
            logger.info(
                "Event %d/%d sent: order_id=%s event_type=%s delivery_id=%s",
                i, total_events, order_id, event_type, sampled_row["delivery_id"],
            )
        except Exception as exc:
            failed_events += 1
            logger.error(
                "Event %d/%d FAILED to send: order_id=%s event_type=%s delivery_id=%s error=%s",
                i, total_events, order_id, event_type, sampled_row["delivery_id"], exc,
            )

        if i % PROGRESS_LOG_INTERVAL == 0 or i == total_events:
            elapsed = time.monotonic() - start_time
            logger.info(
                "Progress: %d/%d events sent, %d orders touched so far, elapsed %.1fs",
                i, total_events, len(orders_seen), elapsed,
            )

        if i < total_events:
            time.sleep(random.uniform(SEND_DELAY_MIN_SECONDS, SEND_DELAY_MAX_SECONDS))

    elapsed_total = time.monotonic() - start_time
    return {
        "total_sent": sum(event_type_counts.values()),
        "total_orders": len(orders_seen),
        "event_type_counts": event_type_counts,
        "failed_events": failed_events,
        "elapsed_seconds": elapsed_total,
    }


def main():
    eventhub_connection_string = os.environ.get("EVENTHUB_CONNECTION_STRING")
    eventhub_name = os.environ.get("EVENTHUB_NAME")
    if not eventhub_connection_string or not eventhub_name:
        raise RuntimeError(
            "EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME environment variables must both be set"
        )

    id_samples = sample_delivery_rows()

    orders = build_order_queues()
    event_stream = interleave_order_events(orders)

    producer = EventHubProducerClient.from_connection_string(
        conn_str=eventhub_connection_string,
        eventhub_name=eventhub_name,
    )

    logger.info(
        "Starting run: %d orders, %d total events, target runtime ~%.0f-%.0f min",
        ORDER_COUNT, TOTAL_EVENTS,
        TOTAL_EVENTS * SEND_DELAY_MIN_SECONDS / 60, TOTAL_EVENTS * SEND_DELAY_MAX_SECONDS / 60,
    )

    try:
        summary = send_events(producer, event_stream, id_samples)
    finally:
        producer.close()

    logger.info("Run complete")
    print("\n=== Final Summary ===")
    print(f"Total events sent: {summary['total_sent']}/{TOTAL_EVENTS}")
    print(f"Total orders simulated: {summary['total_orders']}")
    print("Event type breakdown:")
    for event_type, count in sorted(summary["event_type_counts"].items()):
        print(f"  {event_type}: {count}")
    print(f"Failed sends: {summary['failed_events']}")
    print(f"Total elapsed time: {summary['elapsed_seconds'] / 60:.1f} minutes")


if __name__ == "__main__":
    main()
