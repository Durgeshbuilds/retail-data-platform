"""The foreachBatch processor, fed a hand-built Kafka batch (no Kafka needed)."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from retail.events import Invoice, InvoiceLine, PriceUpdate, StockReceipt
from retail.lake.tables import Lake
from retail.streaming.job import BatchProcessor

pytestmark = [pytest.mark.spark, pytest.mark.postgres]

TS = datetime(2026, 10, 16, 19, 0, tzinfo=UTC)  # 00:30 IST on the 17th


def checkpoint(path, query_id):
    """A checkpoint folder as Spark leaves it: metadata holds the query's id."""
    path.mkdir(parents=True)
    (path / "metadata").write_text(json.dumps({"id": query_id}))
    return path


def kafka_batch(spark, events):
    rows = [(topic, 0, offset, event if isinstance(event, str) else event.model_dump_json())
            for offset, (topic, event) in enumerate(events)]
    return spark.createDataFrame(rows, "topic STRING, partition INT, offset LONG, v STRING") \
        .selectExpr("topic", "partition", "offset", "CAST(v AS BINARY) AS value")


def test_batch_lands_once_in_postgres_and_bronze(spark, pg_dsn, tmp_path):
    invoice = Invoice(
        invoice_id="i-1", store_id="BLR01", event_time=TS, payment_method="card",
        lines=[InvoiceLine(line_no=1, product_id="P0001", qty=3, unit_price=Decimal("28.00"))],
        total_amount=Decimal("84.00"),
    )
    events = [
        ("invoices", invoice),
        ("invoices", invoice),  # duplicate delivery
        ("invoices", json.dumps({"schema_version": 1, "invoice_id": "bad"})),
        ("stock-receipts", StockReceipt(receipt_id="r-1", store_id="BLR01",
                                        product_id="P0001", qty=10, received_at=TS)),
        ("price-updates", PriceUpdate(product_id="P0002", old_price=Decimal("34.00"),
                                      new_price=Decimal("36.00"), effective_at=TS)),
    ]
    lake = Lake(tmp_path / "lake")
    process = BatchProcessor(lake, pg_dsn, checkpoint(tmp_path / "cp1", "query-1"))
    with psycopg.connect(pg_dsn) as conn:
        before = conn.execute("SELECT stock_qty FROM inventory_current"
                              " WHERE store_id = 'BLR01' AND product_id = 'P0001'").fetchone()[0]

    batch = kafka_batch(spark, events)
    process(batch, 0)
    process(batch, 0)  # Spark replays the same batch after a crash

    with psycopg.connect(pg_dsn) as conn:
        after = conn.execute("SELECT stock_qty FROM inventory_current"
                             " WHERE store_id = 'BLR01' AND product_id = 'P0001'").fetchone()[0]
        sale_date = conn.execute("SELECT sale_date FROM live_sales_daily").fetchone()[0]
        price = conn.execute(
            "SELECT current_price FROM products WHERE product_id = 'P0002'").fetchone()[0]
    assert after == before - 3 + 10
    assert str(sale_date) == "2026-10-17"  # store-local date
    assert price == Decimal("36.00")

    bronze = spark.read.format("delta").load(lake.bronze_invoices)
    assert bronze.count() == 3  # invoice events only, written once despite the replay
    assert {r.source_ref for r in bronze.collect()} == {"invoices/0/0", "invoices/0/1",
                                                         "invoices/0/2"}


def test_new_checkpoint_does_not_lose_bronze_rows(spark, pg_dsn, tmp_path):
    """Batch ids restart at 0 with a fresh checkpoint; bronze must still get the rows."""
    lake = Lake(tmp_path / "lake")
    first = BatchProcessor(lake, pg_dsn, checkpoint(tmp_path / "old", "query-old"))
    second = BatchProcessor(lake, pg_dsn, checkpoint(tmp_path / "new", "query-new"))
    invoice = lambda n: Invoice(  # noqa: E731
        invoice_id=f"i-{n}", store_id="BLR01", event_time=TS, payment_method="upi",
        lines=[InvoiceLine(line_no=1, product_id="P0001", qty=1, unit_price=Decimal("28.00"))],
        total_amount=Decimal("28.00"),
    )
    first(kafka_batch(spark, [("invoices", invoice(1))]), 0)
    second(kafka_batch(spark, [("invoices", invoice(2))]), 0)  # same batch id, new query

    ids = {json.loads(r.raw)["invoice_id"]
           for r in spark.read.format("delta").load(lake.bronze_invoices).collect()}
    assert ids == {"i-1", "i-2"}
