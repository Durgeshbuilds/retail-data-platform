"""Reconcile the live store against Kafka: was every event applied exactly once?

Run it once the streaming job has caught up (or after --available-now).

    python -m retail.streaming.check
"""

import logging
import sys

import psycopg
from confluent_kafka import Consumer, TopicPartition

from retail import config
from retail.catalog import TARGET_STOCK_MULTIPLE

log = logging.getLogger(__name__)

# Seeded opening stock is reorder_level x TARGET_STOCK_MULTIPLE, so every row must
# satisfy: stock = opening - units sold + units received.
STOCK_MISMATCHES = """
WITH sold AS (
    SELECT store_id, product_id, sum(units) AS units FROM live_sales_daily GROUP BY 1, 2
), received AS (
    SELECT store_id, product_id, sum(qty) AS qty FROM stock_receipts GROUP BY 1, 2
)
SELECT i.store_id, i.product_id, i.stock_qty,
       i.reorder_level * %(multiple)s - coalesce(s.units, 0) + coalesce(r.qty, 0) AS expected
FROM inventory_current i
LEFT JOIN sold s USING (store_id, product_id)
LEFT JOIN received r USING (store_id, product_id)
WHERE i.stock_qty <> i.reorder_level * %(multiple)s - coalesce(s.units, 0) + coalesce(r.qty, 0)
"""


def topic_size(topic: str) -> int:
    """Messages currently in a topic (nothing has aged out at this retention)."""
    consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "retail-check"})
    try:
        partitions = consumer.list_topics(topic, timeout=5).topics[topic].partitions
        total = 0
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            total += high - low
        return total
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        invoices = conn.execute(
            "SELECT count(DISTINCT invoice_id) FROM processed_lines").fetchone()[0]
        receipts = conn.execute("SELECT count(*) FROM stock_receipts").fetchone()[0]
        mismatches = conn.execute(STOCK_MISMATCHES, {"multiple": TARGET_STOCK_MULTIPLE}).fetchall()
        negative = conn.execute(
            "SELECT count(*) FROM inventory_current WHERE stock_qty < 0").fetchone()[0]

    checks = [
        ("every invoice in Kafka applied once", topic_size(config.TOPIC_INVOICES), invoices),
        ("every stock receipt in Kafka applied once",
         topic_size(config.TOPIC_STOCK_RECEIPTS), receipts),
        ("stock = opening - sold + received, for every row", 0, len(mismatches)),
    ]
    failed = False
    for name, expected, actual in checks:
        ok = expected == actual
        failed |= not ok
        log.info("%s %-50s expected=%s actual=%s", "PASS" if ok else "FAIL", name, expected, actual)
    for row in mismatches[:10]:
        log.info("  mismatch %s", row)
    log.info("items with negative stock (flagged, not an error): %d", negative)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
