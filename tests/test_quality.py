"""Each data-quality check passes on good data and fires on a planted fault."""

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from retail.ops import quality
from retail.ops.monitoring import TopicLag, committed_offsets
from retail.ops.quality import Lake

TODAY = date(2026, 10, 16)


@pytest.fixture
def lake(tmp_path):
    """A Lake whose views are small in-memory tables: 30 clean days for one store."""
    lake = Lake(tmp_path)  # no Delta tables here, so no views yet
    con = lake.con
    con.execute("CREATE TABLE products AS SELECT 'P1' AS product_id, 100.0 AS list_price")
    con.execute("CREATE TABLE stores AS SELECT 'S1' AS store_id, DATE '2026-01-01' AS opened_on")
    con.execute("""CREATE TABLE items AS SELECT 'i' || d AS invoice_id, 1 AS line_no,
        'P1' AS product_id, 'S1' AS store_id, DATE '2026-10-15' - d::INT AS event_date,
        1 AS qty, 100.0 AS unit_price, 100.0 AS line_amount FROM range(30) t(d)""")
    con.execute("CREATE TABLE rejects (raw VARCHAR, reject_reason VARCHAR)")
    con.execute("""CREATE TABLE store_daily AS SELECT 'S1' AS store_id, event_date,
        100 + (hash(event_date) % 7)::INT AS invoices FROM items""")
    con.execute("""CREATE TABLE sales_daily AS SELECT event_date, store_id, product_id,
        sum(line_amount) AS revenue FROM items GROUP BY ALL""")
    return lake


def status(check, lake) -> str:
    return check(lake, TODAY).status


def test_clean_lake_passes(lake):
    for check in quality.LAKE_CHECKS:
        result = check(lake, TODAY)
        if check is quality.gold_store_days_complete:
            continue  # only 30 days of data in a 90-day window, covered below
        assert result.status == "pass", result


def test_faults_are_caught(lake):
    con = lake.con
    con.execute("INSERT INTO items VALUES (NULL, 1, 'P1', 'S1', DATE '2026-10-15', 1, 100, 100)")
    assert status(quality.silver_null_keys, lake) == "fail"

    con.execute("INSERT INTO items VALUES ('x', 1, 'P9', 'S1', DATE '2026-10-15', 1, 100, 100)")
    assert status(quality.silver_known_keys, lake) == "fail"

    con.execute("INSERT INTO items VALUES ('y', 1, 'P1', 'S1', DATE '2026-10-15', 1, 1.0, 1.0)")
    result = quality.silver_price_plausible(lake, TODAY)
    assert result.status == "warn" and "P1 sold at 1" in result.details

    con.execute("INSERT INTO rejects VALUES ('{bad', 'total mismatch')")
    assert "total mismatch" in quality.silver_rejects(lake, TODAY).details

    assert status(quality.gold_reconciles, lake) == "fail"  # silver gained rows, gold didn't


def test_gold_freshness_and_gaps(lake):
    assert quality.gold_freshness(lake, TODAY + timedelta(days=3)).status == "warn"
    lake.con.execute("DELETE FROM store_daily WHERE event_date = DATE '2026-10-10'")
    lake.con.execute("UPDATE stores SET opened_on = DATE '2026-09-16'")  # inside the data
    result = quality.gold_store_days_complete(lake, TODAY)
    assert result.status == "fail" and "S1 on 2026-10-10" in result.details


def test_volume_spike(lake):
    lake.con.execute("UPDATE store_daily SET invoices = 400 WHERE event_date = DATE '2026-10-15'")
    result = quality.gold_volume_normal(lake, TODAY)
    assert result.status == "warn" and "400 invoices" in result.observed


@pytest.mark.postgres
def test_live_checks(pg_dsn):
    reader = make_conninfo(pg_dsn, user="retail_reader", password="retail_reader")
    assert {r.status for r in quality.live_checks(reader)} == {"pass"}
    with psycopg.connect(pg_dsn) as conn:
        conn.execute("UPDATE inventory_current SET stock_qty = -3"
                     " WHERE store_id = 'BLR01' AND product_id = 'P0001'")
        # A gap in ghee's price history (a second open price is already blocked by a
        # unique index, so a gap is the fault this check exists to catch).
        conn.execute("""UPDATE product_prices SET valid_to = valid_to - interval '1 day'
            WHERE product_id = 'P0013' AND valid_from = (
                SELECT min(valid_from) FROM product_prices WHERE product_id = 'P0013')""")
    stock, negative, prices = quality.live_checks(reader)
    assert (stock.status, negative.status, prices.status) == ("fail", "warn", "fail")


def write_checkpoint(root, batch: int, offsets: str, age: timedelta):
    (root / "offsets").mkdir(parents=True)
    (root / "commits").mkdir()
    (root / "offsets" / str(batch)).write_text(f'v1\n{{"batchWatermarkMs":0}}\n{offsets}\n')
    commit = root / "commits" / str(batch)
    commit.write_text("v1\n{}\n")
    stamp = (datetime.now(UTC) - age).timestamp()
    import os
    os.utime(commit, (stamp, stamp))


def test_checkpoint_parsing(tmp_path):
    write_checkpoint(tmp_path, 7, '{"invoices":{"0":10,"1":5}}', timedelta(0))
    assert committed_offsets(tmp_path) == {"invoices": {0: 10, 1: 5}}
    assert committed_offsets(tmp_path / "missing") is None


def test_stream_checks(tmp_path, monkeypatch):
    write_checkpoint(tmp_path, 3, '{"invoices":{"0":1}}', timedelta(minutes=30))
    monkeypatch.setattr(quality, "stream_lag",
                        lambda cp: [TopicLag("invoices", 1, 5001, 5000)])
    lag, idle = quality.stream_checks(tmp_path, datetime.now(UTC))
    assert lag.status == "warn" and "5,000" in lag.observed
    assert idle.status == "warn" and "make stream" in idle.details

    monkeypatch.setattr(quality, "stream_lag", lambda cp: [TopicLag("invoices", 9, 9, 0)])
    assert [r.status for r in quality.stream_checks(tmp_path, datetime.now(UTC))] == ["pass"]
