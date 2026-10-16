"""Silver: parse, validate and deduplicate invoices from bronze.

Reads bronze incrementally (a Delta streaming source with an availableNow
trigger) and, per micro-batch:

* valid invoices   -> silver/invoices       (one row per invoice)
                   -> silver/invoice_items  (one row per line)
* invalid events   -> silver/invoice_rejects, with the reason

Both silver tables are written with insert-only MERGEs keyed on invoice_id and
(invoice_id, line_no), so duplicates and replayed batches are absorbed.
"""

import logging
from functools import partial

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from retail.catalog import STORE_TZ
from retail.lake.schemas import BRONZE, INVOICE_EVENT
from retail.lake.tables import Lake, ensure_table, insert_new

log = logging.getLogger(__name__)

APP_ID = "retail.silver.invoices"


def _lines_total(lines: Column) -> Column:
    zero = F.lit(0).cast("decimal(14,2)")
    return F.aggregate(
        lines, zero, lambda acc, line: (acc + line.qty * line.unit_price).cast("decimal(14,2)")
    )


def parse_invoices(bronze: DataFrame) -> DataFrame:
    """Typed invoice columns plus `reject_reason`, which is null for valid rows."""
    e = F.from_json("raw", INVOICE_EVENT)
    parsed = bronze.select("raw", "source", "source_ref", e.alias("e")).select(
        "raw", "source", "source_ref", "e.*"
    )
    bad_line = F.exists(
        "lines",
        lambda line: line.product_id.isNull() | line.line_no.isNull()
        | line.qty.isNull() | (line.qty <= 0)
        | line.unit_price.isNull() | (line.unit_price <= 0),
    )
    unique_line_nos = F.size(F.array_distinct(F.transform("lines", lambda line: line.line_no)))
    reason = (
        F.when(F.col("invoice_id").isNull(), "unparseable or missing invoice_id")
        .when(F.col("schema_version") != 1, "unsupported schema_version")
        .when(F.col("store_id").isNull() | F.col("event_time").isNull(),
              "missing store_id or event_time")
        .when(F.col("lines").isNull() | (F.size("lines") == 0), "no lines")
        .when(bad_line, "invalid line")
        .when(unique_line_nos != F.size("lines"), "duplicate line_no")
        .when(F.col("total_amount").isNull()
              | (_lines_total(F.col("lines")) != F.col("total_amount")), "total mismatch")
    )
    return parsed.withColumn("reject_reason", reason).withColumn(
        "event_date", F.to_date(F.from_utc_timestamp("event_time", STORE_TZ.key))
    )


def invoice_headers(valid: DataFrame) -> DataFrame:
    return valid.select(
        "invoice_id", "store_id", "event_time", "event_date", "payment_method",
        F.size("lines").alias("line_count"), "total_amount", "source",
    )


def invoice_items(valid: DataFrame) -> DataFrame:
    return valid.select(
        "invoice_id", "store_id", "event_time", "event_date", F.inline("lines")
    ).withColumn("line_amount", (F.col("qty") * F.col("unit_price")).cast("decimal(14,2)"))


def rejects(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("reject_reason").isNotNull()).select(
        "raw", "source", "source_ref", "reject_reason",
        F.current_timestamp().alias("rejected_at"),
    )


def _process_batch(batch: DataFrame, batch_id: int, lake: Lake) -> None:
    spark = batch.sparkSession
    parsed = parse_invoices(batch).persist()
    try:
        valid = parsed.filter(F.col("reject_reason").isNull()).dropDuplicates(["invoice_id"])
        insert_new(spark, lake.silver_invoices, invoice_headers(valid), ["invoice_id"])
        insert_new(spark, lake.silver_invoice_items, invoice_items(valid),
                   ["invoice_id", "line_no"])
        # Rejects are appended, not merged. txnAppId/txnVersion make the append
        # idempotent: if this batch is replayed, Delta skips the second write.
        (
            rejects(parsed).write.format("delta").mode("append")
            .option("txnAppId", APP_ID).option("txnVersion", batch_id)
            .save(lake.silver_invoice_rejects)
        )
    finally:
        parsed.unpersist()


def _ensure_tables(spark: SparkSession, lake: Lake) -> None:
    # Derive table schemas from the transforms themselves, so they cannot drift.
    empty = parse_invoices(spark.createDataFrame([], BRONZE))
    ensure_table(spark, lake.silver_invoices, invoice_headers(empty).schema)
    ensure_table(spark, lake.silver_invoice_items, invoice_items(empty).schema)
    ensure_table(spark, lake.silver_invoice_rejects, rejects(empty).schema)


def build(spark: SparkSession, lake: Lake) -> int:
    """Process bronze rows not yet seen by silver. Returns bronze rows processed."""
    _ensure_tables(spark, lake)
    query = (
        spark.readStream.format("delta").load(lake.bronze_invoices)
        .writeStream.foreachBatch(partial(_process_batch, lake=lake))
        .option("checkpointLocation", lake.checkpoint("silver_invoices"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    processed = sum(p["numInputRows"] for p in query.recentProgress)
    log.info("silver: processed %d bronze rows", processed)
    return processed
