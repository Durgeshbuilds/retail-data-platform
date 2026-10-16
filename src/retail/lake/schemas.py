"""Spark schemas for the raw event JSON. Must stay in step with retail.events."""

from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY = DecimalType(10, 2)

INVOICE_LINE = StructType([
    StructField("line_no", IntegerType()),
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", MONEY),
])

INVOICE_EVENT = StructType([
    StructField("schema_version", IntegerType()),
    StructField("invoice_id", StringType()),
    StructField("store_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("payment_method", StringType()),
    StructField("lines", ArrayType(INVOICE_LINE)),
    StructField("total_amount", MONEY),
])

# Bronze keeps each event exactly as received, plus where it came from.
BRONZE = StructType([
    StructField("raw", StringType()),
    StructField("source", StringType()),      # "file" or "kafka"
    StructField("source_ref", StringType()),  # file path, or topic/partition/offset
    StructField("ingested_at", TimestampType()),
])

PRICE_UPDATE_EVENT = StructType([
    StructField("schema_version", IntegerType()),
    StructField("product_id", StringType()),
    StructField("old_price", MONEY),
    StructField("new_price", MONEY),
    StructField("effective_at", TimestampType()),
])

STOCK_RECEIPT_EVENT = StructType([
    StructField("schema_version", IntegerType()),
    StructField("receipt_id", StringType()),
    StructField("store_id", StringType()),
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("received_at", TimestampType()),
])
