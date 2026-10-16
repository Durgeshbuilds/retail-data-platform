"""Streaming job: Kafka -> Postgres live store + Delta bronze.

One Structured Streaming query subscribes to all three topics. Each micro-batch:

1. appends the raw invoice events to bronze/invoices (an idempotent Delta write,
   keyed on the batch id), so the batch lake build picks up streamed sales;
2. parses and validates invoices with the same code silver uses, plus the
   price-update and stock-receipt events;
3. applies everything to Postgres in one transaction (see live_store.py).

The Kafka offsets of each batch are committed to the checkpoint only after the
batch succeeds; if the job dies mid-batch, Spark replays it and both sinks
recognise the work they have already done.

    python -m retail.streaming.job                 # run until Ctrl-C
    python -m retail.streaming.job --available-now # drain what's in Kafka, then exit
"""

import argparse
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail import config
from retail.lake import silver
from retail.lake.schemas import BRONZE, PRICE_UPDATE_EVENT, STOCK_RECEIPT_EVENT
from retail.lake.spark import create_spark
from retail.lake.tables import Lake, ensure_table
from retail.streaming.live_store import PriceChange, Receipt, SaleLine, apply_batch

log = logging.getLogger("retail.streaming")

TOPICS = [config.TOPIC_INVOICES, config.TOPIC_PRICE_UPDATES, config.TOPIC_STOCK_RECEIPTS]
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
BRONZE_APP_ID = "retail.streaming.bronze"


def _utc(micros: int) -> datetime:
    # Timestamps leave Spark as epoch micros: collecting a TimestampType directly
    # would turn it into a naive datetime in the laptop's local time zone.
    return datetime.fromtimestamp(micros / 1_000_000, UTC)


def to_bronze(batch: DataFrame) -> DataFrame:
    return batch.filter(F.col("topic") == config.TOPIC_INVOICES).select(
        F.col("value").cast("string").alias("raw"),
        F.lit("kafka").alias("source"),
        F.concat_ws("/", "topic", "partition", "offset").alias("source_ref"),
        F.current_timestamp().alias("ingested_at"),
    )


def sale_lines(bronze_rows: DataFrame) -> tuple[list[SaleLine], int, datetime | None]:
    """Valid invoice lines, the number of rejected invoices, and the newest event time."""
    parsed = silver.parse_invoices(bronze_rows).persist()
    try:
        rejected = parsed.filter(F.col("reject_reason").isNotNull()).count()
        valid = parsed.filter(F.col("reject_reason").isNull()).dropDuplicates(["invoice_id"])
        rows = silver.invoice_items(valid).select(
            "invoice_id", "line_no", "store_id", "product_id", "qty", "unit_price",
            "event_date", F.unix_micros("event_time").alias("ts"),
        ).collect()
    finally:
        parsed.unpersist()
    lines = [SaleLine(r.invoice_id, r.line_no, r.store_id, r.product_id, r.qty, r.unit_price,
                      r.event_date) for r in rows]
    newest = _utc(max(r.ts for r in rows)) if rows else None
    return lines, rejected, newest


def receipts(batch: DataFrame) -> list[Receipt]:
    rows = (
        batch.filter(F.col("topic") == config.TOPIC_STOCK_RECEIPTS)
        .select(F.from_json(F.col("value").cast("string"), STOCK_RECEIPT_EVENT).alias("e"))
        .filter("e.schema_version = 1 AND e.receipt_id IS NOT NULL AND e.qty > 0"
                " AND e.store_id IS NOT NULL AND e.product_id IS NOT NULL"
                " AND e.received_at IS NOT NULL")
        .select("e.receipt_id", "e.store_id", "e.product_id", "e.qty",
                F.unix_micros("e.received_at").alias("ts"))
        .collect()
    )
    return [Receipt(r.receipt_id, r.store_id, r.product_id, r.qty, _utc(r.ts)) for r in rows]


def price_changes(batch: DataFrame) -> list[PriceChange]:
    rows = (
        batch.filter(F.col("topic") == config.TOPIC_PRICE_UPDATES)
        .select(F.from_json(F.col("value").cast("string"), PRICE_UPDATE_EVENT).alias("e"))
        .filter("e.schema_version = 1 AND e.product_id IS NOT NULL AND e.new_price > 0"
                " AND e.effective_at IS NOT NULL")
        .select("e.product_id", "e.new_price", F.unix_micros("e.effective_at").alias("ts"))
        .collect()
    )
    return [PriceChange(r.product_id, r.new_price, _utc(r.ts)) for r in rows]


class BatchProcessor:
    """The foreachBatch function. Runs on the driver, once per micro-batch."""

    def __init__(self, lake: Lake, dsn: str) -> None:
        self.lake, self.dsn = lake, dsn

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        began = time.monotonic()
        batch = batch.select("topic", "partition", "offset", "value").persist()
        try:
            bronze_rows = to_bronze(batch).persist()
            if not bronze_rows.isEmpty():
                # txnAppId/txnVersion: if this batch id was already committed, Delta
                # skips the write instead of appending the rows a second time.
                (
                    bronze_rows.write.format("delta").mode("append")
                    .option("txnAppId", BRONZE_APP_ID).option("txnVersion", batch_id)
                    .save(self.lake.bronze_invoices)
                )
            lines, rejected, newest = sale_lines(bronze_rows)
            bronze_rows.unpersist()
            got_receipts, got_prices = receipts(batch), price_changes(batch)

            with psycopg.connect(self.dsn) as conn:
                result = apply_batch(conn, lines, got_receipts, got_prices)
        finally:
            batch.unpersist()

        lag = f"{(datetime.now(UTC) - newest).total_seconds():.1f}s" if newest else "-"
        log.info(
            "batch %d: lines %d applied/%d dup, receipts %d/%d, prices %d/%d, "
            "rejected invoices %d, event lag %s, took %.1fs",
            batch_id, result.lines_applied, result.lines_skipped,
            result.receipts_applied, result.receipts_skipped,
            result.prices_applied, result.prices_skipped,
            rejected, lag, time.monotonic() - began,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Postgres and bronze.")
    parser.add_argument("--trigger-seconds", type=int, default=5,
                        help="micro-batch interval")
    parser.add_argument("--available-now", action="store_true",
                        help="process everything in Kafka now, then stop")
    parser.add_argument("--max-offsets", type=int, default=20_000,
                        help="cap on messages per micro-batch, so a backlog drains in steps")
    parser.add_argument("--lake-root", type=Path, default=config.LAKE_DIR)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)

    lake = Lake(args.lake_root)
    spark = create_spark("retail-stream", master="local[2]", driver_memory="2g",
                         extra_packages=[KAFKA_PACKAGE])
    ensure_table(spark, lake.bronze_invoices, BRONZE)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", "earliest")  # only used on the very first run
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    writer = (
        source.writeStream.foreachBatch(BatchProcessor(lake, config.POSTGRES_DSN))
        .option("checkpointLocation", lake.checkpoint("stream_kafka"))
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start()
    log.info("streaming %s -> Postgres + bronze; Ctrl-C to stop", ", ".join(TOPICS))
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("stopping (the in-flight batch is replayed on next start if unfinished)")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
