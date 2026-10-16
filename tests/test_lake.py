"""End-to-end tests of the Delta pipeline on a throwaway lake. Slow (~30s: starts Spark)."""

import gzip
import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from retail.catalog import load_products, load_stores
from retail.lake import bronze, checks, dims, gold, silver
from retail.lake.spark import create_spark
from retail.lake.tables import Lake
from retail.simulator.history import generate

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = create_spark("retail-tests", master="local[2]", driver_memory="1g")
    yield session
    session.stop()


def invoice_json(invoice_id: str, event_time: str, qty: int = 2, total: float = 56.0) -> str:
    return json.dumps({
        "schema_version": 1, "invoice_id": invoice_id, "store_id": "BLR01",
        "event_time": event_time, "payment_method": "upi",
        "lines": [{"line_no": 1, "product_id": "P0001", "qty": qty, "unit_price": 28.0}],
        "total_amount": total,
    })


def write_raw(raw_dir, name: str, lines: list[str]) -> None:
    path = raw_dir / "2026-10" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        f.write("\n".join(lines) + "\n")


def read(spark, path):
    return spark.read.format("delta").load(path)


def test_pipeline_end_to_end(spark, tmp_path):
    lake, raw = Lake(tmp_path / "lake"), tmp_path / "raw"
    written = generate(load_products(), load_stores(), date(2026, 9, 29), date(2026, 10, 2),
                       invoices_per_day=30, out_dir=raw, seed=5)

    dims.build(spark, lake)
    assert bronze.ingest_files(spark, lake, raw) == written
    silver.build(spark, lake)
    gold.build(spark, lake)
    failures = [c for c in checks.reconcile(spark, lake) if not c.ok]
    assert failures == []

    # A re-run finds nothing new.
    assert bronze.ingest_files(spark, lake, raw) == 0
    assert silver.build(spark, lake) == 0

    # A new file holding a duplicate, a late arrival and four bad events.
    first = read(spark, lake.bronze_invoices).first()["raw"]
    write_raw(raw, "LATE.jsonl.gz", [
        first,                                                       # duplicate
        invoice_json("late-1", "2026-09-30T19:00:00.000+00:00"),     # valid, late
        "{not json",                                                 # unparseable
        invoice_json("bad-qty", "2026-10-01T10:00:00.000+00:00", qty=0, total=0.01),
        invoice_json("bad-total", "2026-10-01T10:00:00.000+00:00", total=99.0),
        json.dumps({"schema_version": 2, "invoice_id": "v2"}),       # unknown version
    ])
    assert bronze.ingest_files(spark, lake, raw) == 6
    silver.build(spark, lake)
    gold.build(spark, lake)

    assert read(spark, lake.silver_invoices).count() == written + 1  # only late-1 added
    reasons = {r["reject_reason"] for r in read(spark, lake.silver_invoice_rejects).collect()}
    assert reasons == {"unparseable or missing invoice_id", "invalid line", "total mismatch",
                       "unsupported schema_version"}
    assert [c for c in checks.reconcile(spark, lake) if not c.ok] == []

    # 19:00 UTC on 30 Sep is 00:30 IST on 1 Oct: it belongs to 1 Oct's sales.
    late = read(spark, lake.silver_invoices).filter("invoice_id = 'late-1'").first()
    assert late["event_date"] == date(2026, 10, 1)


def test_parse_keeps_exact_money(spark):
    df = spark.createDataFrame(
        [(invoice_json("x", "2026-10-16T09:30:00.123+00:00"), "file", "f", None)],
        "raw STRING, source STRING, source_ref STRING, ingested_at TIMESTAMP",
    )
    row = (
        silver.invoice_items(silver.parse_invoices(df))
        # Format inside Spark (session time zone is UTC): collecting a timestamp into
        # Python would convert it to the laptop's local time zone.
        .withColumn("utc", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss.SSS"))
        .first()
    )
    assert row["unit_price"] == Decimal("28.00")
    assert row["line_amount"] == Decimal("56.00")
    assert row["utc"] == "2026-10-16 09:30:00.123"


def test_gold_incremental_matches_full(spark, tmp_path):
    lake, raw = Lake(tmp_path / "lake"), tmp_path / "raw"
    generate(load_products(), load_stores(), date(2026, 9, 25), date(2026, 10, 5),
             invoices_per_day=20, out_dir=raw, seed=9)
    dims.build(spark, lake)
    bronze.ingest_files(spark, lake, raw)
    silver.build(spark, lake)

    gold.build(spark, lake)
    full = read(spark, lake.gold_sales_monthly).orderBy("month", "store_id", "category").collect()
    gold.build(spark, lake, from_date=date(2026, 10, 3))
    incremental = (
        read(spark, lake.gold_sales_monthly).orderBy("month", "store_id", "category").collect()
    )
    assert incremental == full
    assert read(spark, lake.gold_sales_daily).agg(F.min("event_date")).first()[0] == date(
        2026, 9, 25
    )
